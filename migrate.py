#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
db_path = PROJECT_ROOT / "libreria" / "documenti.db"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_existing_columns(cursor, table_name: str) -> set[str]:
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def migrate_database() -> None:
    if not db_path.exists():
        logging.error("DB non trovato in %s", db_path)
        return
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS documenti (id INTEGER PRIMARY KEY, nome_file TEXT NOT NULL, rgnr TEXT, anno INTEGER, procura TEXT, tipo_documento TEXT, note TEXT, data_inserimento TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    existing = get_existing_columns(cur, "documenti")
    for name, typ in [("sha1", "TEXT"), ("dimensione_file", "INTEGER"), ("data_ultima_modifica", "TEXT")]:
        if name not in existing:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {name} {typ}")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_anno ON documenti(anno)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nome_file ON documenti(nome_file)")
    con.commit()
    con.close()


if __name__ == "__main__":
    migrate_database()
