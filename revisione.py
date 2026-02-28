#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - revisione.py (HOME) — OCR forte + classificazione + tentativo RGNR

DEFAULT:
  python revisione.py
  -> processa <SANGIA>\libreria\revisione\occ

Opzioni:
- --folder "percorso"
- --file "percorso"
- --pages N   (default 4)
- --dpi N     (default 320)
- --limit N
- --only-status REVISIONE_OCC
- --dry-run

Nota:
- Non sposta file e non rinomina.
- Aggiorna DB con hint/categoria e, se trova un numero procedimento (anche “R.G. notizie di reato”),
  valorizza rgnr+anno (per OCC) quando mancanti.
"""

import argparse
import hashlib
import re
import sqlite3
import time
from pathlib import Path
from typing import Optional, Tuple, List, Any, Dict


# ============================
# PATHS (HOME)
# ============================

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"
DEFAULT_FOLDER = PROJECT_ROOT / "libreria" / "revisione" / "occ"

ALLOWED_EXTS = {".pdf", ".doc", ".docx", ".txt", ".rtf"}

DEFAULT_PAGES = 4
DEFAULT_DPI = 320


# ============================
# CLASSIFICAZIONE
# ============================

CATEGORIES = [
    ("SENTENZA", re.compile(r"(?i)\bSENTENZA\b|\bMOTIVAZIONI?\b|\bP\.?\s*Q\.?\s*M\.?\b")),
    ("SEQUESTRO", re.compile(r"(?i)\bSEQUESTRO\b|\bPREVENTIVO\b|\bPROBATORIO\b|\bRIESAME\b")),
    ("DECRETO", re.compile(r"(?i)\bDECRETO\b|\bDECRETO\s+PENALE\b|\bINGIUNZIONE\b")),
    ("PERQUISIZIONE", re.compile(r"(?i)\bPERQUISIZIONE\b|\bISPEZIONE\b")),
    ("VERBALE", re.compile(r"(?i)\bVERBALE\b|\bSOMMARIE\s+INFORMAZIONI\b|\bS\.?\s*I\.?\s*T\.?\b|\bINTERROGATORIO\b")),
    ("GIORNALE", re.compile(r"(?i)\bRassegna\s+stampa\b|\bquotidiano\b|\bgiornale\b|\barticolo\b|\bANSA\b")),
    ("ANNOTAZIONI", re.compile(r"(?i)\bANNOTAZIONE\b|\bINFORMATIVA\b|\bRELAZIONE\b|\bSERVIZIO\b")),
]

RE_TRIBUNALE = re.compile(r"(?i)\bTRIBUNALE\b|\bPROCURA\b|\bCORTE\b")


# ============================
# ESTRAZIONE NUMERO PROCEDIMENTO (RGNR / RG NOTIZIE REATO / PROC. PENALE)
# ============================

# RGNR classico
RE_RGNR_LABEL = re.compile(r"(?i)\bRGNR\b|R\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R\s*\.?")
RE_RGNR_NUMYEAR = re.compile(
    r"(?i)\b(?:R\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R|RGNR)\s*"
    r"(?:D\s*\.?\s*D\s*\.?\s*A\s*)?"
    r"[^0-9]{0,25}"
    r"(\d{1,7})\s*[\/_\-]\s*(\d{2,4})"
)

# Caso “N. 1389/08 RGNR” (fallback utile)
RE_N_BEFORE_RGNR = re.compile(
    r"(?i)\bN\s*\.?\s*(\d{1,7})\s*[\/_\-]\s*(\d{2,4})\s*(?:RGNR|R\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R)"
)

# ✅ Caso “N. 1389/08 R.G. notizie di reato - DDA”
RE_RG_NOTIZIE_REATO = re.compile(
    r"(?i)\bN\s*\.?\s*(\d{1,7})\s*[\/_\-]\s*(\d{2,4})\s*"
    r"R\s*\.?\s*G\s*\.?\s*(?:NOTIZIE\s+DI\s+REATO|NOTIZIE\s+REATO)\b"
)

# ✅ Caso “procedimento penale n. 1389/08 R.G. notizie di reato …”
RE_PROC_PENALE_RG = re.compile(
    r"(?i)\bprocedimento\s+penale\s*n\s*\.?\s*(\d{1,7})\s*[\/_\-]\s*(\d{2,4})"
    r"(?:[^A-Z0-9]{0,40}R\s*\.?\s*G\s*\.?\s*(?:NOTIZIE\s+DI\s+REATO|NOTIZIE\s+REATO))?"
)

# Se troviamo almeno la label “R.G. notizie di reato” ma non il numero (OCR pessimo)
RE_RG_NOTIZIE_LABEL_ONLY = re.compile(
    r"(?i)\bR\s*\.?\s*G\s*\.?\s*NOTIZIE\s+DI\s+REATO\b|\bR\s*\.?\s*G\s*\.?\s*NOTIZIE\s+REATO\b"
)


# ============================
# UTILS
# ============================

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


def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def compact_ws(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def iter_files_in_folder(folder: Path) -> List[Path]:
    if not folder.exists():
        return []
    out: List[Path] = []
    for p in folder.rglob("*"):
        if p.is_file() and p.suffix.lower() in ALLOWED_EXTS:
            out.append(p)
    return sorted(out)


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
    """
    Ritorna:
      has_hint (0/1), rgnr_hint "num/YYYY" o None, anno_hint o None

    Estrae:
    - RGNR classico
    - N. xxxx/yy RGNR
    - N. xxxx/yy R.G. notizie di reato
    - procedimento penale n. xxxx/yy (eventualmente seguito da RG notizie di reato)
    """
    if not text:
        return 0, None, None

    t = text

    # 1) più specifico: RG notizie di reato (come nel tuo esempio)
    m = RE_RG_NOTIZIE_REATO.search(t)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return 1, f"{num}/{year}", year

    # 2) procedimento penale n.
    m = RE_PROC_PENALE_RG.search(t)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return 1, f"{num}/{year}", year

    # 3) N. ... RGNR
    m = RE_N_BEFORE_RGNR.search(t)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return 1, f"{num}/{year}", year

    # 4) RGNR classico
    m = RE_RGNR_NUMYEAR.search(t)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return 1, f"{num}/{year}", year

    # 5) label senza numero (indizio comunque utile)
    if RE_RGNR_LABEL.search(t):
        return 1, None, None

    if RE_RG_NOTIZIE_LABEL_ONLY.search(t):
        return 1, None, None

    return 0, None, None


# ============================
# OCR FORTE PDF
# ============================

def ocr_strong_pdf_pages(pdf_path: Path, pages: int, dpi: int) -> Tuple[str, bool, str]:
    """
    OCR forte su pagine 1..pages.
    Ritorna: (testo, used_ocr, err)
    """
    try:
        import fitz  # PyMuPDF
        import numpy as np
        import easyocr
    except Exception as e:
        return "", False, f"deps_missing:{e}"

    parts: List[str] = []
    used = False

    try:
        doc = fitz.open(str(pdf_path))
        n = min(max(1, pages), doc.page_count)

        # Nota: easyocr stampa “Using CPU...” (è normale)
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

        return "\n\n".join(parts).strip(), used, ""
    except Exception as e:
        return "", used, str(e)


# ============================
# DB
# ============================

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
    add("status", "TEXT")
    add("last_retry_at", "TEXT")
    add("retry_count", "INTEGER")

    con.commit()


def fetch_doc_by_sha1(con: sqlite3.Connection, sha1: str) -> Optional[Tuple[Any, ...]]:
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, nome_file, tipo_documento, status, text_preview, snippet_testo,
               categoria_secondaria, rgnr, anno, retry_count
        FROM documenti
        WHERE sha1=?
        LIMIT 1
        """,
        (sha1,),
    )
    return cur.fetchone()


def update_doc(con: sqlite3.Connection, doc_id: int, updates: Dict[str, Any], dry_run: bool) -> None:
    if not updates:
        return
    if dry_run:
        return

    keys = list(updates.keys())
    set_clause = ", ".join([f"{k}=?" for k in keys])
    params = [updates[k] for k in keys] + [doc_id]

    cur = con.cursor()
    cur.execute(f"UPDATE documenti SET {set_clause} WHERE id=?", params)
    con.commit()


# ============================
# MAIN
# ============================

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="", help="Singolo file (path)")
    ap.add_argument("--folder", default="", help="Cartella da processare (path). Default: libreria/revisione/occ")
    ap.add_argument("--pages", type=int, default=DEFAULT_PAGES)
    ap.add_argument("--dpi", type=int, default=DEFAULT_DPI)
    ap.add_argument("--limit", type=int, default=0, help="Limita a N file (0=nessun limite)")
    ap.add_argument("--only-status", default="", help="Se valorizzato, processa solo record DB con questo status")
    ap.add_argument("--dry-run", action="store_true", help="Non scrive su DB")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print(f"[ERR] DB non trovato: {DB_PATH}")
        return 2

    if args.file:
        targets = [Path(args.file)]
    else:
        folder = Path(args.folder) if args.folder else DEFAULT_FOLDER
        targets = iter_files_in_folder(folder)

    if not targets:
        print("[INFO] Nessun file da processare.")
        return 0

    if args.limit and args.limit > 0:
        targets = targets[: args.limit]

    con = sqlite3.connect(DB_PATH)
    ensure_cols(con)

    print("[REV] revisione.py (AUTO)")
    print(f"[REV] start: {now_str()}")
    print(f"[REV] DB: {DB_PATH}")
    print(f"[REV] targets: {len(targets)}")
    if not args.file:
        print(f"[REV] folder: {Path(args.folder) if args.folder else DEFAULT_FOLDER}")
    if args.only_status:
        print(f"[REV] only_status: {args.only_status}")
    if args.dry_run:
        print("[REV] dry_run: True")
    print("")

    processed = 0
    skipped_not_in_db = 0
    skipped_status = 0
    errors = 0

    for i, f in enumerate(targets, start=1):
        try:
            if not f.exists() or not f.is_file():
                continue

            sha1 = sha1_file(f)
            row = fetch_doc_by_sha1(con, sha1)
            if not row:
                skipped_not_in_db += 1
                print(f"[SKIP] ({i}/{len(targets)}) not_in_db: {f.name}")
                continue

            doc_id, nome_file, tipo_doc, status_db, text_preview, snippet_db, cat2_db, rgnr_db, anno_db, retry_db = row

            if args.only_status:
                if (status_db or "").strip().upper() != args.only_status.strip().upper():
                    skipped_status += 1
                    if skipped_status <= 5 or skipped_status % 50 == 0:
                        print(f"[SKIP] ({i}/{len(targets)}) status_mismatch: id={doc_id} file='{nome_file}' status={status_db}")
                    continue

            t0 = time.time()

            preview = (text_preview or "").strip()

            ocr_text = ""
            used_ocr = False
            ocr_err = ""

            if f.suffix.lower() == ".pdf":
                ocr_text, used_ocr, ocr_err = ocr_strong_pdf_pages(f, pages=args.pages, dpi=args.dpi)

            combined = compact_ws(preview + "\n\n" + ocr_text)

            is_scan = 1 if (f.suffix.lower() == ".pdf" and len(preview) < 50 and len(ocr_text) >= 120) else 0

            cat2 = classify_text(combined)
            has_rgnr_hint, rgnr_hint, anno_hint = extract_rgnr_hint(combined)

            snippet = (combined[:500] if combined else (snippet_db or "")) or ""
            hint_text = (combined[:20000] if combined else "")

            updates: Dict[str, Any] = {
                "categoria_secondaria": (cat2_db if cat2_db else cat2),
                "hint_text": hint_text,
                "snippet_testo": snippet,
                "is_scan": int(is_scan),
                "has_rgnr_hint": int(has_rgnr_hint),
                "rgnr_hint": rgnr_hint,
                "anno_hint": int(anno_hint) if anno_hint else None,
                "last_retry_at": now_str(),
                "retry_count": int((retry_db or 0) + 1),
            }

            # OCC: se troviamo un numero procedimento affidabile e in DB mancano, valorizziamo
            if (tipo_doc or "").upper() == "OCC" and rgnr_hint and anno_hint and (not rgnr_db or not anno_db):
                updates["rgnr"] = rgnr_hint
                updates["anno"] = int(anno_hint)

            updates_clean = {k: v for k, v in updates.items() if v is not None}
            update_doc(con, doc_id, updates_clean, dry_run=args.dry_run)

            dt = int(time.time() - t0)

            msg = (
                f"[OK] ({i}/{len(targets)}) id={doc_id} file='{nome_file}' "
                f"used_ocr={used_ocr} is_scan={is_scan} cat2={updates_clean.get('categoria_secondaria')} "
                f"has_rgnr_hint={has_rgnr_hint} rgnr_hint={rgnr_hint} anno_hint={anno_hint} time={dt}s"
            )
            if ocr_err:
                msg += f" ocr_err={ocr_err}"

            print(msg)
            processed += 1

        except KeyboardInterrupt:
            print("\n[STOP] Interrotto da tastiera.")
            break
        except Exception as e:
            errors += 1
            print(f"[ERR] ({i}/{len(targets)}) file='{getattr(f, 'name', f)}' -> {e}")

    con.close()

    print("")
    print("[DONE] revisione completata.")
    print(f"[DONE] processed={processed} not_in_db={skipped_not_in_db} skipped_status={skipped_status} errors={errors}")
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
