#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - WORKER: ORGANIZZATORE_DOC_READER (v6)

Fix v6:
- Antiword mapping robusto (come v5)
- Se rgnr/anno mancano nel DB:
  - prova a estrarre da filename (pattern: 58_1996 / 58-1996 / 58 1996)
  - prova a estrarre dal testo (pattern RGNR + numero/anno, o N. 58/1996 ecc.)
- Se dopo update abbiamo rgnr+anno+procura -> status=READY
"""

import argparse
import hashlib
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"
INPUT_DIR = PROJECT_ROOT / "input_documenti"

ANTIWORD_DIR = PROJECT_ROOT / "tools" / "antiword"
ANTIWORD_EXE = ANTIWORD_DIR / "antiword.exe"
MAP_8859 = ANTIWORD_DIR / "8859-1.txt"
MAP_CP1252 = ANTIWORD_DIR / "cp1252.txt"


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


# ---- PROCURA ----
RE_TRIBUNALE_CITY = re.compile(r"(?i)\bTRIBUNALE(?:\s+ORDINARIO)?\s+DI\s+([A-ZÀ-ÖØ-Ý'\s\-]{3,80})")
RE_PROCURA_PRESSO_TRIB = re.compile(
    r"(?i)\bPROCURA\s+DELLA\s+REPUBBLICA\b.*?\bPRESSO\s+IL\s+TRIBUNALE\s+DI\s+([A-ZÀ-ÖØ-Ý'\s\-]{3,80})"
)
RE_STOPWORDS = re.compile(r"(?i)\b(SEZIONE|UFFICIO|GIUDICE|PROCURA|DIREZIONE|DISTRETTUALE|ORDINARIO)\b")

# ---- DDA ----
RE_DDA = re.compile(r"(?i)\bD\s*\.?\s*D\s*\.?\s*A\s*\.?\b|direzione\s+distrettuale\s+antimafia")

# ---- OCC ----
RE_OCC_TEXT = re.compile(
    r"(?i)ordinanza\s+di\s+applicazione\s+di\s+misura\s+cautelare|"
    r"ordinanza\s+di\s+custodia\s+cautelare|custodia\s+cautelare|misura\s+cautelare"
)

# ---- RGNR extraction ----
RE_RGNR_TEXT = re.compile(r"(?i)\bRGNR\b[^0-9]{0,20}(\d{1,7})\s*[\/_\-]\s*(\d{2,4})")
RE_NUMYEAR_FILENAME = re.compile(r"(?i)\b(\d{1,7})\s*[_\-\s]\s*(\d{4}|\d{2})\b")
RE_N_NUMYEAR_TEXT = re.compile(r"(?i)\bN\s*\.?\s*(\d{1,7})\s*[\/_\-]\s*(\d{2,4})\b")


def normalize_year(y: int) -> int:
    if y < 100:
        return 2000 + y if y <= 50 else 1900 + y
    return y


def clean_place(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    # Title Case semplice
    return " ".join([w[:1].upper() + w[1:].lower() if w else "" for w in s.split(" ")]).strip()


def extract_procura_city(text: str) -> Optional[str]:
    t = text or ""

    m = RE_PROCURA_PRESSO_TRIB.search(t)
    if m:
        chunk = RE_STOPWORDS.split(m.group(1))[0]
        chunk = re.sub(r"\s+", " ", chunk).strip()
        return clean_place(chunk) if chunk else None

    m = RE_TRIBUNALE_CITY.search(t)
    if not m:
        return None
    chunk = RE_STOPWORDS.split(m.group(1))[0]
    chunk = re.sub(r"\s+", " ", chunk).strip()
    return clean_place(chunk) if chunk else None


def extract_dda_flag(text: str) -> int:
    return 1 if RE_DDA.search(text or "") else 0


def looks_like_occ(text: str) -> bool:
    return bool(RE_OCC_TEXT.search(text or ""))


def extract_rgnr_anno_from_filename(name: str) -> Tuple[Optional[str], Optional[int]]:
    stem = Path(name).stem
    m = RE_NUMYEAR_FILENAME.search(stem)
    if not m:
        return None, None
    num = m.group(1)
    try:
        year = normalize_year(int(m.group(2)))
    except Exception:
        return None, None
    return f"{num}/{year}", year


def extract_rgnr_anno_from_text(text: str) -> Tuple[Optional[str], Optional[int]]:
    t = text or ""
    m = RE_RGNR_TEXT.search(t)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return f"{num}/{year}", year

    m = RE_N_NUMYEAR_TEXT.search(t)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return f"{num}/{year}", year

    return None, None


def _decode_best(b: bytes) -> str:
    if not b:
        return ""
    for enc in ("utf-8", "cp1252", "cp850", "latin-1"):
        try:
            return b.decode(enc, errors="ignore")
        except Exception:
            continue
    return b.decode("latin-1", errors="ignore")


def convert_doc_to_text(doc_path: Path) -> Tuple[Optional[str], str, str]:
    env = os.environ.copy()
    env["HOME"] = env.get("USERPROFILE", str(PROJECT_ROOT))
    env["ANTIWORDHOME"] = str(ANTIWORD_DIR)

    if ANTIWORD_EXE.exists():
        try:
            cmd = [str(ANTIWORD_EXE)]
            # mapping “senza spazi” + cwd=ANTIWORD_DIR
            if MAP_8859.exists():
                cmd += ["-m", "8859-1"]
            elif MAP_CP1252.exists():
                cmd += ["-m", "cp1252"]
            cmd += [str(doc_path)]

            p = subprocess.run(
                cmd,
                cwd=str(ANTIWORD_DIR),
                env=env,
                capture_output=True,
                timeout=90,
            )
            out = _decode_best(p.stdout)
            err = _decode_best(p.stderr)

            if p.returncode == 0 and out.strip():
                return out, "antiword(bundled)", err.strip()

            debug = f"antiword(bundled) rc={p.returncode} err={err.strip()[:1200]}"
        except Exception as e:
            debug = f"antiword(bundled) exception={e}"
    else:
        debug = "antiword(bundled) missing"

    soffice = shutil.which("soffice")
    if soffice:
        with tempfile.TemporaryDirectory() as td:
            outdir = Path(td)
            try:
                p = subprocess.run(
                    [
                        soffice,
                        "--headless",
                        "--nologo",
                        "--nolockcheck",
                        "--convert-to",
                        "txt:Text",
                        "--outdir",
                        str(outdir),
                        str(doc_path),
                    ],
                    capture_output=True,
                    timeout=180,
                )
                txt_path = outdir / (doc_path.stem + ".txt")
                if txt_path.exists():
                    return txt_path.read_text(encoding="utf-8", errors="ignore"), "soffice", ""
                debug = debug + f" | soffice rc={p.returncode} err={_decode_best(p.stderr)[:1200]}"
            except Exception as e:
                debug = debug + f" | soffice exception={e}"
    else:
        debug = debug + " | soffice missing"

    return None, "none", debug


def ensure_cols(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("PRAGMA table_info(documenti)")
    cols = {row[1] for row in cur.fetchall()}

    def add_col(name: str, coltype: str):
        if name not in cols:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {name} {coltype}")

    add_col("dda_flag", "INTEGER")
    add_col("procura", "TEXT")
    add_col("status", "TEXT")
    add_col("tipo_documento", "TEXT")
    add_col("text_preview", "TEXT")
    add_col("rgnr", "TEXT")
    add_col("anno", "INTEGER")

    con.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="DOC in input_documenti o path assoluto")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print("[ERR] DB non trovato.")
        sys.exit(2)

    doc_path = resolve_file(args.file)
    if not doc_path:
        print(f"[ERR] File non trovato: {args.file}")
        sys.exit(2)

    sha1 = sha1_file(doc_path)

    con = sqlite3.connect(DB_PATH)
    ensure_cols(con)
    cur = con.cursor()

    cur.execute("SELECT id, tipo_documento, rgnr, anno, procura FROM documenti WHERE sha1=? LIMIT 1", (sha1,))
    row = cur.fetchone()
    if not row:
        con.close()
        print("[ERR] Record DB non trovato per sha1")
        sys.exit(2)

    doc_id, tipo, rgnr_db, anno_db, procura_db = row

    text, method, debug_err = convert_doc_to_text(doc_path)
    if not text:
        cur.execute("UPDATE documenti SET status='NEEDS_READER' WHERE id=?", (doc_id,))
        con.commit()
        con.close()
        print("[MISS] Lettura DOC fallita. Debug:", debug_err)
        sys.exit(3)

    preview = text[:8000]
    procura = extract_procura_city(text) or (procura_db or None)
    dda_flag = extract_dda_flag(text)
    occ_ok = looks_like_occ(text)

    # rgnr/anno: prova testo, poi filename
    rgnr_new = rgnr_db
    anno_new = anno_db

    if not (rgnr_new or "").strip() or not (anno_new or 0):
        r_text, a_text = extract_rgnr_anno_from_text(text)
        if r_text and a_text:
            rgnr_new, anno_new = r_text, a_text

    if not (rgnr_new or "").strip() or not (anno_new or 0):
        r_fn, a_fn = extract_rgnr_anno_from_filename(doc_path.name)
        if r_fn and a_fn:
            rgnr_new, anno_new = r_fn, a_fn

    cur.execute("UPDATE documenti SET text_preview=? WHERE id=?", (preview, doc_id))
    if procura:
        cur.execute("UPDATE documenti SET procura=? WHERE id=?", (procura, doc_id))
    cur.execute("UPDATE documenti SET dda_flag=? WHERE id=?", (dda_flag, doc_id))

    if rgnr_new and anno_new:
        cur.execute("UPDATE documenti SET rgnr=?, anno=? WHERE id=?", (rgnr_new, int(anno_new), doc_id))

    if occ_ok and (tipo or "").upper() != "OCC":
        cur.execute("UPDATE documenti SET tipo_documento='OCC' WHERE id=?", (doc_id,))

    ready = bool((rgnr_new or "").strip() and (anno_new or 0) and (procura or "").strip())
    cur.execute("UPDATE documenti SET status=? WHERE id=?", ("READY" if ready else "NEEDS_READER", doc_id))

    con.commit()
    con.close()

    print(f"[OK] DOC reader ({method}) -> rgnr={rgnr_new} anno={anno_new} procura={procura} dda={dda_flag} occ={occ_ok} status={'READY' if ready else 'NEEDS_READER'}")
    sys.exit(0)


if __name__ == "__main__":
    main()
