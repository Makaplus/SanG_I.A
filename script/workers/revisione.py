#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - WORKER: REVISIONE (v2) — OCR forte + classificazione + tentativo RGNR

Uso:
  python script/workers/revisione.py --file "C:\...\libreria\revisione\occ\file.pdf"
  python script/workers/revisione.py --folder "libreria\vari_verbali"

Cosa fa:
- OCR forte su pagine 1-4 (PDF) e combina con preview DB se presente
- Classifica categoria_secondaria (SENTENZA / SEQUESTRO / VERBALE / GIORNALE / ANNOTAZIONI / ...)
- Cerca RGNR robusto e, se lo trova con num/anno, aggiorna anche i campi rgnr + anno (per OCC)
- Salva hint_text + snippet_testo

Nota: non sposta file; decide il router se rinominare/spostare o richiedere manuale.
"""

import argparse
import hashlib
import re
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, List, Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"

DEFAULT_PAGES = 4
DEFAULT_DPI = 320

CATEGORIES = [
    ("SENTENZA", re.compile(r"(?i)\bSENTENZA\b|\bMOTIVAZIONI?\b|\bP\.Q\.M\.?\b")),
    ("SEQUESTRO", re.compile(r"(?i)\bSEQUESTRO\b|\bPREVENTIVO\b|\bPROBATORIO\b|\bRIESAME\b")),
    ("DECRETO", re.compile(r"(?i)\bDECRETO\b|\bDECRETO\s+PENALE\b|\bINGIUNZIONE\b")),
    ("PERQUISIZIONE", re.compile(r"(?i)\bPERQUISIZIONE\b|\bISPEZIONE\b")),
    ("VERBALE", re.compile(r"(?i)\bVERBALE\b|\bSOMMARIE\s+INFORMAZIONI\b|\bS\.I\.T\.?\b|\bINTERROGATORIO\b")),
    ("GIORNALE", re.compile(r"(?i)\bRassegna\s+stampa\b|\bquotidiano\b|\bgiornale\b|\barticolo\b|\bANSA\b")),
    ("ANNOTAZIONI", re.compile(r"(?i)\bANNOTAZIONE\b|\bINFORMATIVA\b|\bRELAZIONE\b|\bSERVIZIO\b")),
]

RE_TRIBUNALE = re.compile(r"(?i)\bTRIBUNALE\b|\bPROCURA\b|\bCORTE\b")

RE_RGNR_LABEL = re.compile(r"(?i)\bRGNR\b|R\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R\s*\.?")
RE_RGNR_NUMYEAR = re.compile(
    r"(?i)\b(?:R\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R|RGNR)\s*"
    r"(?:D\s*\.?\s*D\s*\.?\s*A\s*)?"
    r"[^0-9]{0,25}"
    r"(\d{1,7})\s*[\/_\-]\s*(\d{2,4})"
)


def normalize_year(y: int) -> int:
    if y < 100:
        return 2000 + y if y <= 50 else 1900 + y
    return y


def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def ensure_cols(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute("PRAGMA table_info(documenti)")
    cols = {row[1] for row in cur.fetchall()}

    def add(name: str, coltype: str):
        if name not in cols:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {name} {coltype}")

    add("categoria_secondaria", "TEXT")
    add("hint_text", "TEXT")
    add("is_scan", "INTEGER")
    add("has_rgnr_hint", "INTEGER")
    add("rgnr_hint", "TEXT")
    add("anno_hint", "INTEGER")
    add("snippet_testo", "TEXT")
    add("rgnr", "TEXT")
    add("anno", "INTEGER")
    add("tipo_documento", "TEXT")

    con.commit()


def fetch_doc_by_sha1(con: sqlite3.Connection, sha1: str) -> Optional[Tuple[Any, ...]]:
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, nome_file, tipo_documento, text_preview, snippet_testo, categoria_secondaria, rgnr, anno
        FROM documenti
        WHERE sha1=?
        LIMIT 1
        """,
        (sha1,),
    )
    return cur.fetchone()


def classify_text(text: str) -> Optional[str]:
    if not text:
        return None
    for label, rx in CATEGORIES:
        if rx.search(text):
            return label
    if RE_TRIBUNALE.search(text):
        return "ATTO_GIUDIZIARIO"
    return None


def extract_rgnr_hint(text: str) -> Tuple[int, Optional[str], Optional[int]]:
    if not text:
        return 0, None, None
    m = RE_RGNR_NUMYEAR.search(text)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return 1, f"{num}/{year}", year
    if RE_RGNR_LABEL.search(text):
        return 1, None, None
    return 0, None, None


def ocr_strong_pdf_pages(pdf_path: Path, pages: int, dpi: int) -> Tuple[str, bool]:
    try:
        import fitz  # PyMuPDF
        import numpy as np
        import easyocr
    except Exception:
        return "", False

    parts: List[str] = []
    used = False
    try:
        doc = fitz.open(str(pdf_path))
        n = min(max(1, pages), doc.page_count)
        reader = easyocr.Reader(["it", "en"], gpu=False)
        for i in range(n):
            page = doc.load_page(i)
            pix = page.get_pixmap(dpi=dpi)
            img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
            if pix.n == 4:
                img = img[:, :, :3]
            res = reader.readtext(img, detail=0, paragraph=True)
            if isinstance(res, list):
                chunk = "\n".join([r for r in res if r]).strip()
            else:
                chunk = (res or "").strip()
            if chunk:
                parts.append(chunk)
            used = True
        return "\n\n".join(parts).strip(), used
    except Exception:
        return "", used


def iter_files_in_folder(folder: Path) -> List[Path]:
    if not folder.exists():
        return []
    out = []
    for p in folder.rglob("*"):
        if p.is_file() and p.suffix.lower() in {".pdf", ".doc", ".docx", ".txt", ".rtf"}:
            out.append(p)
    return sorted(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="", help="Singolo file (path)")
    ap.add_argument("--folder", default="", help="Cartella da processare (path)")
    ap.add_argument("--pages", type=int, default=DEFAULT_PAGES)
    ap.add_argument("--dpi", type=int, default=DEFAULT_DPI)
    args = ap.parse_args()

    if not DB_PATH.exists():
        print("[ERR] DB non trovato")
        raise SystemExit(2)

    targets: List[Path] = []
    if args.file:
        targets = [Path(args.file)]
    elif args.folder:
        targets = iter_files_in_folder(Path(args.folder))
    else:
        print("[ERR] Devi passare --file oppure --folder")
        raise SystemExit(2)

    con = sqlite3.connect(DB_PATH)
    ensure_cols(con)

    processed = 0
    for f in targets:
        if not f.exists() or not f.is_file():
            continue

        sha1 = sha1_file(f)
        row = fetch_doc_by_sha1(con, sha1)
        if not row:
            print(f"[SKIP] non_in_db: {f}")
            continue

        doc_id, nome_file, tipo_doc, text_preview, snippet_db, cat2_db, rgnr_db, anno_db = row
        preview = (text_preview or "").strip()

        ocr_text = ""
        used_ocr = False
        if f.suffix.lower() == ".pdf":
            ocr_text, used_ocr = ocr_strong_pdf_pages(f, pages=args.pages, dpi=args.dpi)

        combined = (preview + "\n\n" + ocr_text).strip()
        combined = re.sub(r"\s+", " ", combined)

        is_scan = 1 if (f.suffix.lower() == ".pdf" and len(preview) < 50 and len(ocr_text) >= 120) else 0

        cat2 = classify_text(combined)
        has_rgnr_hint, rgnr_hint, anno_hint = extract_rgnr_hint(combined)

        snippet = (combined[:500] if combined else (snippet_db or "")) or ""
        hint_text = (combined[:20000] if combined else "")

        # aggiorna DB
        cur = con.cursor()
        cur.execute(
            """
            UPDATE documenti
            SET categoria_secondaria=COALESCE(categoria_secondaria, ?),
                hint_text=?,
                snippet_testo=?,
                is_scan=?,
                has_rgnr_hint=?,
                rgnr_hint=?,
                anno_hint=?
            WHERE id=?
        """,
            (cat2, hint_text, snippet, int(is_scan), int(has_rgnr_hint), rgnr_hint, anno_hint, doc_id),
        )

        # se è OCC e troviamo RGNR/ANNO con fiducia, valorizza anche i campi forti
        if (tipo_doc or "").upper() == "OCC" and rgnr_hint and anno_hint and (not rgnr_db or not anno_db):
            cur.execute("UPDATE documenti SET rgnr=?, anno=? WHERE id=?", (rgnr_hint, int(anno_hint), doc_id))

        con.commit()

        processed += 1
        print(
            f"[OK] revisione -> {nome_file} used_ocr={used_ocr} is_scan={is_scan} "
            f"cat2={cat2} has_rgnr_hint={has_rgnr_hint} rgnr_hint={rgnr_hint} anno_hint={anno_hint}"
        )

    con.close()
    print(f"[DONE] revisione completata. Processati: {processed}")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
