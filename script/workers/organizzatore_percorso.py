#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - WORKER: ORGANIZZATORE_PERCORSO
Percorso: SANGIA/script/workers/organizzatore_percorso.py

Compito:
- Legge l'anno assegnato dal DB (per sha1 del file)
- Sposta il file nella cartella di competenza: SANGIA/libreria/YYYY/
  (solo cartella anno, niente sottocartelle)
- Aggiorna DB.percorso_file e DB.status
"""

from __future__ import annotations

import argparse
import hashlib
import sqlite3
from pathlib import Path
from typing import Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"
INPUT_DIR = PROJECT_ROOT / "input_documenti"
LIB_DIR = PROJECT_ROOT / "libreria"


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
    if p.is_absolute() and p.exists() and p.is_file():
        return p
    p2 = (INPUT_DIR / arg).resolve()
    if p2.exists() and p2.is_file():
        return p2
    for cand in LIB_DIR.rglob(Path(arg).name):
        if cand.is_file() and cand.name == Path(arg).name:
            return cand
    return None


def unique_path(target: Path) -> Path:
    if not target.exists():
        return target
    parent = target.parent
    stem = target.stem
    suf = target.suffix
    for i in range(1, 1000):
        cand = parent / f"{stem}_{i:02d}{suf}"
        if not cand.exists():
            return cand
    raise RuntimeError("Troppi conflitti nome (impossibile trovare nome unico).")


def db_get_by_sha1(sha1: str) -> Optional[Tuple[int, Optional[int], Optional[str]]]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, anno, percorso_file
        FROM documenti
        WHERE sha1=?
        LIMIT 1
    """,
        (sha1,),
    )
    row = cur.fetchone()
    con.close()
    return row


def db_update_move(doc_id: int, old_rel: str, new_rel: str, evidence: str, new_status: str) -> None:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE documenti
        SET percorso_prev=?, percorso_file=?, percorso_evidence=?, status=?
        WHERE id=?
    """,
        (old_rel, new_rel, evidence, new_status, doc_id),
    )
    con.commit()
    con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Nome file o path")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print("[ERR] DB non trovato.")
        return

    file_path = resolve_file(args.file)
    if not file_path:
        print(f"[ERR] File non trovato: {args.file}")
        return

    sha1 = sha1_file(file_path)
    row = db_get_by_sha1(sha1)
    if not row:
        print(f"[ERR] Record DB non trovato (sha1): {sha1}")
        return

    doc_id, anno, percorso_file = row
    if not anno or not (1900 <= int(anno) <= 2100):
        print(f"[SKIP] Anno non valido in DB: {anno}")
        return

    year_dir = LIB_DIR / str(int(anno))
    year_dir.mkdir(parents=True, exist_ok=True)

    target = unique_path(year_dir / file_path.name)

    if file_path.resolve().parent == year_dir.resolve():
        new_rel = str(Path("libreria") / str(int(anno)) / file_path.name)
        old_rel = percorso_file or new_rel
        if old_rel != new_rel:
            db_update_move(doc_id, old_rel, new_rel, "align_path_only", "STORED")
        print(f"[SKIP] Già in cartella anno: {file_path.name}")
        return

    try:
        file_path.rename(target)
    except Exception as e:
        print(f"[ERR] Spostamento fallito: {e}")
        return

    old_rel = percorso_file or str(Path("input_documenti") / file_path.name)
    new_rel = str(Path("libreria") / str(int(anno)) / target.name)
    evidence = f"move_by_year anno={anno}"
    db_update_move(doc_id, old_rel, new_rel, evidence, "STORED")

    print(f"[OK] MOVE: {file_path.name} -> {target}")


if __name__ == "__main__":
    main()
