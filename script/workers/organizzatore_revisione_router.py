#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - WORKER: ORGANIZZATORE_REVISIONE_ROUTER (v1)

Cosa fa:
- Scansiona libreria\revisione\ (in particolare revisione\occ\ e anche vari_verbali se vuoi estendere)
- Per ogni file in revisione:
    - legge dal DB lo stato e i campi mancanti
    - decide cosa tentare (OCR forte pagine 1–4 via revisione.py)
    - se riesce a estrarre RGNR/ANNO/PROCURA -> lancia rename + percorso (archivia)
    - se fallisce 2 volte -> marca MANUALE (manual_required=1, status=MANUALE_OCC)
- Se sembra scritto a mano:
    - euristica: troviamo "RGNR / R.G.N.R." ma nessun num/anno valido dopo OCR forte
    - oppure retry_count >=2
    -> MANUALE

Dipendenze:
- usa revisione.py (OCR forte)
- usa organizzatore_rename.py e organizzatore_percorso.py se un OCC diventa completo
"""

import hashlib
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"
LIB_DIR = PROJECT_ROOT / "libreria"
REV_DIR = LIB_DIR / "revisione"
REV_OCC_DIR = REV_DIR / "occ"

FINAL_STATUSES = {"STORED", "COMPLETED", "DONE"}

RE_RGNR_PARSE = re.compile(r"(\d{1,7})\s*/\s*(\d{4})")
RE_RGNR_LABEL = re.compile(r"(?i)\bRGNR\b|R\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R\s*\.?")


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
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def ensure_cols(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("PRAGMA table_info(documenti)")
    cols = {row[1] for row in cur.fetchall()}

    def add(name: str, coltype: str):
        if name not in cols:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {name} {coltype}")

    add("motivo_revisione", "TEXT")
    add("retry_count", "INTEGER")
    add("last_retry_at", "TEXT")
    add("manual_required", "INTEGER")
    add("manual_note", "TEXT")
    add("esito_revisione", "TEXT")

    con.commit()


def run_worker(script_rel: Path, args: List[str]) -> Tuple[bool, str]:
    script = (PROJECT_ROOT / script_rel).resolve()
    if not script.exists():
        return False, f"worker_missing:{script}"
    cmd = [sys.executable, str(script)] + args
    proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True)
    if proc.returncode != 0:
        return False, (proc.stderr or proc.stdout or "worker_failed").strip()
    return True, (proc.stdout or "").strip()


def db_get_by_sha1(con: sqlite3.Connection, sha1: str):
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, nome_file, tipo_documento, rgnr, anno, procura, status,
               percorso_file, retry_count, manual_required, motivo_revisione,
               hint_text, snippet_testo
        FROM documenti
        WHERE sha1=?
        LIMIT 1
    """,
        (sha1,),
    )
    return cur.fetchone()


def db_set_retry(con: sqlite3.Connection, doc_id: int, motivo: str):
    cur = con.cursor()
    cur.execute(
        """
        UPDATE documenti
        SET retry_count=COALESCE(retry_count,0)+1,
            last_retry_at=?,
            motivo_revisione=?
        WHERE id=?
    """,
        (now_str(), motivo, doc_id),
    )
    con.commit()


def db_mark_manual(con: sqlite3.Connection, doc_id: int, note: str):
    cur = con.cursor()
    cur.execute(
        """
        UPDATE documenti
        SET manual_required=1,
            status='MANUALE_OCC',
            esito_revisione='MANUALE_RICHIESTA',
            manual_note=?
        WHERE id=?
    """,
        (note[:2000], doc_id),
    )
    con.commit()


def is_final(status: Optional[str]) -> bool:
    return (status or "").upper().strip() in FINAL_STATUSES


def scan_revisione_files() -> List[Path]:
    if not REV_DIR.exists():
        return []
    out: List[Path] = []
    for p in REV_DIR.rglob("*"):
        if p.is_file() and p.suffix.lower() in {".pdf", ".doc", ".docx", ".txt", ".rtf"}:
            out.append(p)
    return sorted(out)


def decide_motivo(tipo: str, rgnr: Optional[str], anno: Optional[int], procura: Optional[str], hint_text: str) -> str:
    # OCC incompleto = tipico
    if (tipo or "").upper() == "OCC":
        if not rgnr or not RE_RGNR_PARSE.search(str(rgnr)):
            return "MISSING_RGNR"
        if not anno:
            return "MISSING_ANNO"
        if not (procura or "").strip():
            return "MISSING_PROCURA"
        return "OK"
    # ALTRO
    return "CLASSIFY_ALTRO"


def has_label_but_no_number(text: str) -> bool:
    t = text or ""
    if not RE_RGNR_LABEL.search(t):
        return False
    if RE_RGNR_PARSE.search(t):
        return False
    return True


def main():
    if not DB_PATH.exists():
        print("[ERR] DB non trovato")
        raise SystemExit(2)

    files = scan_revisione_files()
    if not files:
        print("Nessun file in revisione.")
        raise SystemExit(0)

    con = sqlite3.connect(DB_PATH)
    ensure_cols(con)

    auto_ok = 0
    auto_fail = 0
    manual = 0
    skipped = 0

    for f in files:
        sha1 = sha1_file(f)
        row = db_get_by_sha1(con, sha1)
        if not row:
            skipped += 1
            continue

        (
            doc_id,
            nome_file,
            tipo_doc,
            rgnr,
            anno,
            procura,
            status,
            percorso_file,
            retry_count,
            manual_required,
            motivo_revisione,
            hint_text,
            snippet,
        ) = row

        if is_final(status):
            skipped += 1
            continue

        if int(manual_required or 0) == 1:
            manual += 1
            continue

        motivo = decide_motivo(tipo_doc, rgnr, anno, procura, hint_text or "")
        if motivo == "OK":
            skipped += 1
            continue

        # anti-loop
        rc = int(retry_count or 0)
        if rc >= 2:
            db_mark_manual(con, doc_id, "Tentativi OCR forte esauriti (>=2). Probabile scritto a mano o documento degradato.")
            manual += 1
            continue

        # Tentativo: OCR forte + keyword su pagine 1-4
        db_set_retry(con, doc_id, motivo)

        ok_rev, msg_rev = run_worker(Path("script/workers/revisione.py"), ["--file", str(f), "--pages", "4", "--dpi", "320"])
        if not ok_rev:
            auto_fail += 1
            continue

        # rilegge DB dopo revisione
        row2 = db_get_by_sha1(con, sha1)
        if not row2:
            auto_fail += 1
            continue
        (_, _, tipo2, rgnr2, anno2, procura2, status2, _, retry2, manual2, _, hint_text2, snippet2) = row2

        # euristica scritto a mano: c'è label RGNR ma non num/anno
        if has_label_but_no_number((hint_text2 or "") + " " + (snippet2 or "")) and not (rgnr2 and RE_RGNR_PARSE.search(str(rgnr2))):
            db_mark_manual(con, doc_id, "Trovato riferimento a RGNR/R.G.N.R. ma numero/anno non leggibile (probabile scritto a mano).")
            manual += 1
            continue

        # se ora OCC è completo -> chiude la pratica: rename + percorso
        has_r = bool(rgnr2 and RE_RGNR_PARSE.search(str(rgnr2)))
        has_a = bool(anno2)
        has_p = bool((procura2 or "").strip())

        if (tipo2 or "").upper() == "OCC" and has_r and has_a and has_p:
            ok_rn, _ = run_worker(Path("script/workers/organizzatore_rename.py"), ["--file", str(f)])

            row3 = db_get_by_sha1(con, sha1)
            path_for_move = str(f)
            if row3 and row3[7]:
                cand = (PROJECT_ROOT / str(row3[7]).replace("\\", "/")).resolve()
                if cand.exists() and cand.is_file():
                    path_for_move = str(cand)

            ok_mv, _ = run_worker(Path("script/workers/organizzatore_percorso.py"), ["--file", path_for_move])
            if ok_rn and ok_mv:
                auto_ok += 1
            else:
                auto_fail += 1
            continue

        # ALTRO: revisione.py avrà riempito categoria_secondaria / hint_text (utile alla webapp)
        auto_ok += 1

    con.close()
    print(f"Router revisione completato: auto_ok={auto_ok} auto_fail={auto_fail} manual={manual} skipped={skipped}")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
