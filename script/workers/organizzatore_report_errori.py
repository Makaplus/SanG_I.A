#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
organizzatore_report_errori.py (V3 - single file, console-safe)
Genera un report "stabile" (sempre lo stesso file) dei record NON finali presenti nel DB.

Output:
- libreria/report/report_errori.csv   (sovrascritto a ogni run)
- libreria/report/report_errori.json  (sovrascritto a ogni run)
- libreria/report/report_errori.bak.csv (backup del precedente, 1 solo livello)

NOTE:
- CSV delimiter ';' (Excel IT)
- Writer robusto: QUOTE_ALL + doublequote + escapechar
- Console Windows: niente emoji nei print (evita UnicodeEncodeError cp1252)
"""

import csv
import json
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple


# ============================================================
# CONSOLE UTF-8 (best effort)
# ============================================================

def _force_utf8_stdout():
    """
    Evita crash su Windows (cp1252) quando appare testo non encodabile.
    Non sempre serve, ma è una cintura di sicurezza.
    """
    try:
        # Python 3.7+
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


_force_utf8_stdout()


# ============================================================
# PATHS / CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # .../SANGIA
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"

REPORT_DIR = PROJECT_ROOT / "libreria" / "report"
CSV_PATH = REPORT_DIR / "report_errori.csv"
CSV_BAK_PATH = REPORT_DIR / "report_errori.bak.csv"
JSON_PATH = REPORT_DIR / "report_errori.json"

FINAL_STATUSES = {"STORED", "COMPLETED", "DONE"}


# ============================================================
# UTIL
# ============================================================

def now_human() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def normalize_cell(v: Any) -> str:
    """Converte qualsiasi valore in stringa, gestendo None e strutture."""
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)


def table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    cur = con.cursor()
    info = cur.execute(f"PRAGMA table_info({table})").fetchall()
    # (cid, name, type, notnull, dflt_value, pk)
    return [row[1] for row in info]


# ============================================================
# CSV / JSON WRITERS
# ============================================================

def write_csv(path: Path, cols: List[str], rows: List[Tuple[Any, ...]]):
    """
    Scrittura CSV robusta:
    - separatore ';'
    - quoting totale
    - doublequote + escapechar per evitare "_csv.Error: need to escape..."
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    # backup 1 livello (solo se esiste)
    try:
        if path.exists():
            # sovrascrive sempre il bak precedente
            if CSV_BAK_PATH.exists():
                CSV_BAK_PATH.unlink(missing_ok=True)
            path.replace(CSV_BAK_PATH)
    except Exception:
        # se il file è aperto in Excel può fallire: in quel caso continuiamo senza backup
        pass

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(
            f,
            delimiter=";",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            doublequote=True,
            escapechar="\\",
            lineterminator="\n",
        )
        w.writerow(cols)
        for r in rows:
            w.writerow([normalize_cell(v) for v in r])


def write_json(path: Path, cols: List[str], rows: List[Tuple[Any, ...]], meta: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)

    out_rows: List[Dict[str, Any]] = []
    for r in rows:
        item: Dict[str, Any] = {}
        for i, c in enumerate(cols):
            item[c] = r[i] if i < len(r) else None
        out_rows.append(item)

    payload: Dict[str, Any] = {
        "generated_at": now_human(),
        "count": len(out_rows),
        "final_statuses": sorted(list(FINAL_STATUSES)),
        "rows": out_rows,
        **meta,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# ============================================================
# DB FETCH
# ============================================================

def fetch_non_final_records(db_path: Path) -> Tuple[List[str], List[Tuple[Any, ...]]]:
    """
    Estrae dal DB le righe NON finali.
    Se alcune colonne non esistono nella tabella, le rimuove dalla SELECT.
    """

    wanted_cols = [
        "eid",               # nel tuo export c'è "eid"
        "id",                # fallback (se nel DB è "id" e non "eid")
        "nome_file",
        "sha1",
        "tipo_documento",
        "rgnr",
        "anno",
        "procura",
        "dda_flag",
        "operazione_nome",
        "status",
        "percorso_file",
        "rename_evidence",
        "percorso_evidence",
        "evidence_occ",
        "conf_occ",
        "snippet_testo",
        "data_inserimento",
    ]

    con = sqlite3.connect(db_path)
    try:
        existing = set(table_columns(con, "documenti"))

        cols: List[str] = []
        if "eid" in existing:
            cols.append("eid")
        elif "id" in existing:
            cols.append("id")

        for c in wanted_cols:
            if c in ("eid", "id"):
                continue
            if c in existing:
                cols.append(c)

        if "status" not in cols:
            return cols, []

        cur = con.cursor()
        sql = "SELECT " + ", ".join(cols) + " FROM documenti"
        all_rows = cur.execute(sql).fetchall()

        status_idx = cols.index("status")
        out_rows: List[Tuple[Any, ...]] = []
        for r in all_rows:
            st = (r[status_idx] or "").upper().strip()
            if st not in FINAL_STATUSES:
                out_rows.append(r)

        return cols, out_rows

    finally:
        con.close()


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    print("[REPORT] Avvio report errori (single file)...")

    if not DB_PATH.exists():
        print(f"[REPORT] WARNING: DB non trovato: {DB_PATH}", file=sys.stderr)
        return 2

    cols, rows = fetch_non_final_records(DB_PATH)

    if not cols:
        print("[REPORT] WARNING: nessuna colonna valida trovata in tabella documenti.", file=sys.stderr)
        return 3

    # Scrivi report stabili
    write_csv(CSV_PATH, cols, rows)
    write_json(
        JSON_PATH,
        cols,
        rows,
        meta={
            "db_path": str(DB_PATH),
            "report_type": "db_non_final_single",
        },
    )

    print(f"[REPORT] OK: aggiornato {CSV_PATH} | righe={len(rows)}")
    print(f"[REPORT] OK: aggiornato {JSON_PATH} | righe={len(rows)}")
    if CSV_BAK_PATH.exists():
        print(f"[REPORT] OK: backup precedente {CSV_BAK_PATH}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
