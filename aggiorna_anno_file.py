#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sposta fisicamente un file nella cartella del nuovo anno e aggiorna il DB."""

import argparse
import shutil
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
LIBRERIA = PROJECT_ROOT / "libreria"
DB_PATH = LIBRERIA / "documenti.db"


def find_file(nome_file: str) -> Path | None:
    for p in LIBRERIA.rglob(nome_file):
        if p.is_file():
            return p
    return None


def update_db(nome_file: str, new_year: int) -> int:
    if not DB_PATH.exists():
        return 0
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    row = cur.fetchone()
    if not row:
        con.close()
        return 0
    table = row[0]
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if "anno" not in cols or "nome_file" not in cols:
        con.close()
        return 0
    cur.execute(f"UPDATE {table} SET anno=? WHERE nome_file=?", (new_year, nome_file))
    changed = cur.rowcount
    con.commit()
    con.close()
    return changed


def main() -> None:
    parser = argparse.ArgumentParser(description="Corregge anno documento (filesystem + DB)")
    parser.add_argument("nome_file", help="Nome file presente in libreria")
    parser.add_argument("new_year", type=int, help="Nuovo anno (es: 2021)")
    args = parser.parse_args()

    src = find_file(args.nome_file)
    if not src:
        raise SystemExit(f"File non trovato in libreria: {args.nome_file}")

    dst_dir = LIBRERIA / str(args.new_year)
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / args.nome_file

    if src.resolve() != dst.resolve():
        shutil.move(str(src), str(dst))

    changed = update_db(args.nome_file, args.new_year)
    print(f"OK moved={dst} db_rows_updated={changed}")


if __name__ == "__main__":
    main()
