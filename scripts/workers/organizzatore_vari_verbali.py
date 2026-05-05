#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - WORKER: ORGANIZZATORE_VARI_VERBALI (v2)

Sposta file ALTRO in:
  libreria\vari_verbali\

Aggiorna DB:
- tipo_documento = 'ALTRO'
- status = 'VARI_VERBALI'
- categoria_secondaria = NULL (se non già valorizzata)
- rename_evidence = 'non_occ_to_vari_verbali'
- percorso_file aggiornato
"""

import argparse
import hashlib
import sqlite3
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"
INPUT_DIR = PROJECT_ROOT / "input_documenti"
DEST_DIR = PROJECT_ROOT / "libreria" / "vari_verbali"


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


def unique_path(target: Path) -> Path:
    if not target.exists():
        return target
    parent = target.parent
    stem = target.stem
    suf = target.suffix
    for i in range(1, 1000):
        cand = parent / f"{stem} ({i:02d}){suf}"
        if not cand.exists():
            return cand
    raise RuntimeError("Troppi conflitti nome.")


def ensure_cols(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("PRAGMA table_info(documenti)")
    cols = {row[1] for row in cur.fetchall()}

    def add_col(name: str, coltype: str):
        if name not in cols:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {name} {coltype}")

    add_col("tipo_documento", "TEXT")
    add_col("status", "TEXT")
    add_col("rename_evidence", "TEXT")
    add_col("percorso_file", "TEXT")
    add_col("percorso_prev", "TEXT")
    add_col("nome_file_prev", "TEXT")
    add_col("nome_file", "TEXT")
    add_col("categoria_secondaria", "TEXT")

    con.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--reason", default="non_occ_to_vari_verbali")
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
    cur.execute("SELECT id FROM documenti WHERE sha1=? LIMIT 1", (sha1,))
    row = cur.fetchone()
    if not row:
        con.close()
        print("[ERR] record DB non trovato per sha1")
        raise SystemExit(2)

    doc_id = row[0]

    DEST_DIR.mkdir(parents=True, exist_ok=True)
    target = unique_path(DEST_DIR / f.name)

    try:
        f.rename(target)
    except Exception as e:
        con.close()
        print(f"[ERR] move vari_verbali fallito: {e}")
        raise SystemExit(3)

    new_rel = str(Path("libreria") / "vari_verbali" / target.name)

    cur.execute(
        """
        UPDATE documenti
        SET tipo_documento='ALTRO',
            status='VARI_VERBALI',
            categoria_secondaria=COALESCE(categoria_secondaria, NULL),
            rename_evidence=?,
            percorso_prev=percorso_file,
            percorso_file=?,
            nome_file_prev=nome_file,
            nome_file=?
        WHERE id=?
    """,
        (args.reason, new_rel, target.name, doc_id),
    )

    con.commit()
    con.close()

    print(f"[OK] VARI_VERBALI: {target.name} -> {new_rel}")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
