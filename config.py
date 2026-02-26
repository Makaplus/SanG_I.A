# -*- coding: utf-8 -*-
"""Configurazione centrale SANGIA."""

import json
import logging
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input_documenti"
LIBRERIA_DIR = BASE_DIR / "libreria"
MEDIA_CACHE_DIR = BASE_DIR / ".media_cache"
BIBLIOTECA_DIR = INPUT_DIR / "biblioteca"
ERROR_DIR = INPUT_DIR / "errore"
DB_SQLITE_PATH = LIBRERIA_DIR / "documenti.db"
CARTELLA_DB_VETTORIALE = str(BASE_DIR / "chroma_db")


class Settings:
    SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".txt", ".png", ".jpg", ".jpeg"}
    MAX_WORKERS = os.cpu_count() if os.cpu_count() else 4
    OCR_PRIMARY_LANG = "it"
    OCR_SECONDARY_LANG = "en"


def inizializza_ambiente() -> None:
    for cartella in [INPUT_DIR, LIBRERIA_DIR, MEDIA_CACHE_DIR, BIBLIOTECA_DIR, ERROR_DIR, Path(CARTELLA_DB_VETTORIALE)]:
        try:
            cartella.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logging.error("Impossibile creare la cartella %s: %s", cartella, e)


def get_fascicolo_path(file_stem: str) -> Path:
    fascicolo_dir = LIBRERIA_DIR / file_stem
    (fascicolo_dir / "media").mkdir(parents=True, exist_ok=True)
    return fascicolo_dir


def salva_risultato_json(fascicolo_path: Path, dati: dict) -> None:
    json_path = fascicolo_path / "risultato.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(dati, f, ensure_ascii=False, indent=2, default=str)
