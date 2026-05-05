#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Ripristina file da cartelle di errore verso la cartella input e rimuove
i relativi record nel DB SQLite, così possono essere rielaborati da organizzatore.py.

Integrazione con il sistema:
- legge da config.ini i percorsi separati di input_documenti e libreria
- usa le stesse cartelle di organizzatore.py: "Documenti_da_Smistare", "Non Smistati" e "Vari_Verbali"
"""

import argparse
import configparser
import logging
import shutil
import sqlite3
from pathlib import Path

CONFIG_INI_PATH = Path(__file__).resolve().parent / "config.ini"
DB_NAME = "documenti.db"
CARTELLE_NON_SMISTATI = "Non Smistati"
CARTELLE_VARI_VERBALI = "Vari_Verbali"


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_paths_from_config() -> tuple[Path, Path, Path, Path, Path]:
    cfg = configparser.ConfigParser()
    if not CONFIG_INI_PATH.exists():
        raise FileNotFoundError(f"File config mancante: {CONFIG_INI_PATH}")

    cfg.read(CONFIG_INI_PATH, encoding="utf-8")

    try:
        input_base = Path(cfg["PERCORSI"]["CARTELLA_INPUT_BASE"].strip())
        input_folder = cfg["PERCORSI"]["NOME_CARTELLA_INPUT"].strip()
        non_smistati_folder = cfg["PERCORSI"]["NOME_CARTELLA_NON_SMISTATI"].strip()
        libreria_base = Path(cfg["PERCORSI"]["CARTELLA_LIBRERIA_BASE"].strip())
        vari_verbali_folder = cfg["PERCORSI"]["NOME_CARTELLA_VARI_VERBALI"].strip()
    except KeyError as exc:
        raise KeyError(
            "config.ini incompleto: servono CARTELLA_INPUT_BASE, NOME_CARTELLA_INPUT, "
            "NOME_CARTELLA_NON_SMISTATI, CARTELLA_LIBRERIA_BASE, NOME_CARTELLA_VARI_VERBALI"
        ) from exc

    input_path = input_base / input_folder
    unclassified_path = input_base / non_smistati_folder
    varie_verbali_path = libreria_base / vari_verbali_folder
    db_path = libreria_base / DB_NAME

    return input_base, input_path, unclassified_path, varie_verbali_path, db_path


def reset_and_move_files(source_folder_path: Path, input_path: Path, db_path: Path) -> int:
    """Rimuove i record dal DB e sposta i file dalla sorgente alla cartella input."""
    folder_name = source_folder_path.name
    logging.info("--- Avvio rielaborazione per cartella: %s ---", folder_name)

    if not source_folder_path.exists():
        logging.warning("La cartella '%s' non esiste. Nessuna azione.", folder_name)
        return 0

    files_to_reset = [f for f in source_folder_path.iterdir() if f.is_file()]
    if not files_to_reset:
        logging.info("La cartella '%s' è vuota. Nessuna azione.", folder_name)
        return 0

    input_path.mkdir(parents=True, exist_ok=True)

    moved = 0
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        for file_path in files_to_reset:
            file_name = file_path.name
            # 1) Cancella il record DB per rielaborazione pulita
            cursor.execute("DELETE FROM documenti WHERE nome_file = ?", (file_name,))

            # 2) Sposta il file in input
            dest = input_path / file_name
            if dest.exists():
                logging.warning("File già presente in input, salto spostamento: %s", file_name)
                continue

            shutil.move(str(file_path), str(dest))
            moved += 1

        conn.commit()
        logging.info(
            "Completato '%s': %d file spostati in input e record DB rimossi.",
            folder_name,
            moved,
        )
        return moved

    except sqlite3.Error as e:
        logging.critical("Errore database durante reset '%s': %s", folder_name, e)
        if conn is not None:
            conn.rollback()
        return moved
    except Exception as e:
        logging.critical("Errore generale durante reset '%s': %s", folder_name, e)
        if conn is not None:
            conn.rollback()
        return moved
    finally:
        if conn is not None:
            conn.close()


def main_reset(include_verbali: bool = False) -> None:
    logging.info("--- Inizio Riparazione e Rielaborazione ---")

    try:
        input_base, input_path, unclassified_path, varie_verbali_path, db_path = load_paths_from_config()
    except Exception as e:
        logging.critical("Impossibile caricare configurazione: %s", e)
        return

    if not input_base.exists():
        logging.critical("CARTELLA_INPUT_BASE non trovata: %s", input_base)
        return

    if not db_path.exists():
        logging.critical("Database non trovato: %s", db_path)
        return

    total = 0
    total += reset_and_move_files(unclassified_path, input_path, db_path)

    if include_verbali:
        total += reset_and_move_files(varie_verbali_path, input_path, db_path)

    logging.info("--- Ripristino completato. File rientrati in input: %d ---", total)
    logging.info("Esegui ora: python organizzatore.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset DB + ritorno file in input per rielaborazione")
    parser.add_argument(
        "--include-verbali",
        action="store_true",
        help="Include anche i file presenti in 'Vari_Verbali'",
    )
    args = parser.parse_args()
    main_reset(include_verbali=args.include_verbali)
