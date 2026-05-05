#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - WORKER: ORGANIZZATORE_ALTRO_PROBE (v1)

Probe leggero (DB only) per i NON-OCC prima di spostarli in vari_verbali:
- Se preview è vuoto e PDF -> fa OCR forte pagine 1-2 (solo per hint)
- Suggerisce categoria_secondaria
- Cerca hint RGNR (senza forzare OCC)

Nota: è “leggero” rispetto a revisione.py (che è più completa).
"""

import argparse
import hashlib
import re
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"
INPUT_DIR = PROJECT_ROOT / "input_documenti"

RE_RGNR_NUMYEAR = re.compile(
    r"(?i)\b(?:R\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R|RGNR)\s*"
    r"(?:D\s*\.?\s*D\s*\.?\s*A\s*)?"
    r"[^0-9]{0,25}"
    r"(\d{1,7})\s*[\/_\-]\s*(\d{2,4})"
)

CATEGORIES = [
    ("SENTENZA", re.compile(r"(?i)\bSENTENZA\b|\bMOTIVAZIONI?\b|\bP\.Q\.M\.?\b")),
    ("SEQUESTRO", re.compile(r"(?i)\bSEQUESTRO\b|\bPREVENTIVO\b|\bPROBATORIO\b")),
    ("DECRETO", re.compile(r"(?i)\bDECRETO\b|\bDECRETO\s+PENALE\b")),
    ("VERBALE", re.compile(r"(?i)\bVERBALE\b|\bSOMMARIE\s+INFORMAZIONI\b|\bS\.I\.T\.?\b")),
    ("GIORNALE", re.compile(r"(?i)\bRassegna\s+stampa\b|\bquotidiano\b|\bgiornale\b|\barticolo\b|\bANSA\b")),
    ("ANNOTAZIONI", re.compile(r"(?i)\bANNOTAZIONE\b|\bINFORMATIVA\b|\bRELAZIONE\b|\bSERVIZIO\b")),
]


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


def resolve_file(arg: str) -> Optional[Path]:
    p = Path(arg)
    if not p.is_absolute():
        p = (INPUT_DIR / p).resolve()
    if p.exists() and p.is_file():
        return p
    return None


def ensure_cols(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("PRAGMA table_info(documenti)")
    cols = {row[1] for row in cur.fetchall()}

    def add(name: str, coltype: str):
        if name not in cols:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {name} {coltype}")

    add("is_scan", "INTEGER")
    add("has_rgnr_hint", "INTEGER")
    add("rgnr_hint", "TEXT")
    add("anno_hint", "INTEGER")
    add("hint_text", "TEXT")
    add("categoria_secondaria", "TEXT")
    add("snippet_testo", "TEXT")
    con.commit()


def classify_text(text: str) -> Optional[str]:
    if not text:
        return None
    for label, rx in CATEGORIES:
        if rx.search(text):
            return label
    return None


def extract_rgnr_hint(text: str) -> Tuple[int, Optional[str], Optional[int]]:
    if not text:
        return 0, None, None
    m = RE_RGNR_NUMYEAR.search(text)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return 1, f"{num}/{year}", year
    return 0, None, None


def ocr_two_pages(pdf_path: Path) -> Tuple[str, bool]:
    try:
        import fitz
        import numpy as np
        import easyocr
    except Exception:
        return "", False

    try:
        doc = fitz.open(str(pdf_path))
        n = min(2, doc.page_count)
        if n <= 0:
            return "", False
        reader = easyocr.Reader(["it", "en"], gpu=False)
        parts: List[str] = []
        for i in range(n):
            page = doc.load_page(i)
            pix = page.get_pixmap(dpi=260)
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
        return "\n\n".join(parts).strip(), True
    except Exception:
        return "", False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    args = ap.parse_args()

    if not DB_PATH.exists():
        print("[ERR] DB non trovato")
        raise SystemExit(2)

    f = resolve_file(args.file)
    if not f:
        print("[ERR] file non trovato")
        raise SystemExit(2)

    sha1 = sha1_file(f)

    con = sqlite3.connect(DB_PATH)
    ensure_cols(con)
    cur = con.cursor()

    cur.execute("SELECT id, text_preview, snippet_testo FROM documenti WHERE sha1=? LIMIT 1", (sha1,))
    row = cur.fetchone()
    if not row:
        con.close()
        print("[ERR] record DB non trovato (sha1)")
        raise SystemExit(2)

    doc_id, text_preview, snippet_db = row
    preview = (text_preview or "").strip()

    ocr_text = ""
    used_ocr = False
    if f.suffix.lower() == ".pdf" and len(preview) < 120:
        ocr_text, used_ocr = ocr_two_pages(f)

    combined = (preview + "\n\n" + ocr_text).strip()
    combined = re.sub(r"\s+", " ", combined)

    is_scan = 1 if (f.suffix.lower() == ".pdf" and len(preview) < 50 and len(ocr_text) >= 120) else 0
    has_rgnr_hint, rgnr_hint, anno_hint = extract_rgnr_hint(combined)
    cat2 = classify_text(combined)

    snippet = (combined[:500] if combined else (snippet_db or "")) or ""
    hint_text = combined[:12000] if combined else ""

    cur.execute("""
        UPDATE documenti
        SET is_scan=?,
            has_rgnr_hint=?,
            rgnr_hint=?,
            anno_hint=?,
            hint_text=?,
            snippet_testo=?,
            categoria_secondaria=COALESCE(categoria_secondaria, ?)
        WHERE id=?
    """, (int(is_scan), int(has_rgnr_hint), rgnr_hint, anno_hint, hint_text, snippet, cat2, doc_id))

    con.commit()
    con.close()

    print(f"[OK] ALTRO_PROBE -> used_ocr={used_ocr} is_scan={is_scan} cat2={cat2} rgnr_hint={rgnr_hint}")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
