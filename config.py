# -*- coding: utf-8 -*-
"""Configurazione centrale SANGIA (portabile e completa)."""

from __future__ import annotations

import configparser
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
CONFIG_INI_PATH = BASE_DIR / "config.ini"


def _norm_path(value: str | Path) -> Path:
    p = Path(value)
    return p if p.is_absolute() else (BASE_DIR / p).resolve()


def _str_to_bool(value: str, default: bool = False) -> bool:
    txt = (value or "").strip().lower()
    if txt in {"1", "true", "yes", "y", "on"}:
        return True
    if txt in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _load_ini() -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    if CONFIG_INI_PATH.exists():
        cfg.read(CONFIG_INI_PATH, encoding="utf-8")
    return cfg


_CFG = _load_ini()

# --- Percorsi base (override da env o config.ini) ---
PROJECT_ROOT = _norm_path(
    os.getenv("PROJECT_ROOT")
    or _CFG.get("PERCORSI", "PROJECT_ROOT", fallback=str(BASE_DIR))
)

INPUT_DIR = _norm_path(
    os.getenv("INPUT_DIR")
    or _CFG.get("PERCORSI", "INPUT_DIR", fallback="input_documenti")
)
LIBRERIA_DIR = _norm_path(
    os.getenv("LIBRERIA_DIR")
    or _CFG.get("PERCORSI", "LIBRERIA_DIR", fallback="libreria")
)
BACKUP_DIR = _norm_path(
    os.getenv("BACKUP_DIR")
    or _CFG.get("PERCORSI", "BACKUP_DIR", fallback="Backup")
)

MEDIA_CACHE_DIR = _norm_path(
    os.getenv("MEDIA_CACHE_DIR")
    or _CFG.get("PERCORSI", "MEDIA_CACHE_DIR", fallback=".media_cache")
)
BIBLIOTECA_DIR = _norm_path(
    os.getenv("BIBLIOTECA_DIR")
    or _CFG.get("PERCORSI", "BIBLIOTECA_DIR", fallback="input_documenti/biblioteca")
)
ERROR_DIR = _norm_path(
    os.getenv("ERROR_DIR")
    or _CFG.get("PERCORSI", "ERROR_DIR", fallback="input_documenti/errore")
)

DB_SQLITE_PATH = _norm_path(
    os.getenv("DB_SQLITE_PATH")
    or _CFG.get("PERCORSI", "DB_SQLITE_PATH", fallback="libreria/documenti.db")
)
CHROMA_DIR = _norm_path(
    os.getenv("CHROMA_DIR")
    or _CFG.get("PERCORSI", "CHROMA_DIR", fallback="chroma_db")
)
KB_JSONL = _norm_path(
    os.getenv("KB_JSONL")
    or _CFG.get("PERCORSI", "KB_JSONL", fallback="libreria/knowledge_base.jsonl")
)

# Alias legacy
CARTELLA_DB_VETTORIALE = str(CHROMA_DIR)


@dataclass(frozen=True)
class Settings:
    SUPPORTED_EXTENSIONS: set[str]
    MAX_WORKERS: int
    OCR_PRIMARY_LANG: str
    OCR_SECONDARY_LANG: str
    CHROMA_COLLECTION: str
    EMBED_BATCH_SIZE: int
    EMBED_CONCURRENCY: int
    EMBED_MODEL: str
    LLM_MODEL: str
    ANONYMIZE_TELEMETRY: bool


SETTINGS = Settings(
    SUPPORTED_EXTENSIONS={
        ".pdf", ".docx", ".doc", ".txt", ".csv", ".json", ".xml", ".rtf", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".webp"
    },
    MAX_WORKERS=int(os.getenv("MAX_WORKERS") or (_CFG.get("RUNTIME", "MAX_WORKERS", fallback=str(os.cpu_count() or 4)))),
    OCR_PRIMARY_LANG=os.getenv("OCR_PRIMARY_LANG") or _CFG.get("OCR", "OCR_PRIMARY_LANG", fallback="it"),
    OCR_SECONDARY_LANG=os.getenv("OCR_SECONDARY_LANG") or _CFG.get("OCR", "OCR_SECONDARY_LANG", fallback="en"),
    CHROMA_COLLECTION=os.getenv("CHROMA_COLLECTION") or _CFG.get("CHROMA", "CHROMA_COLLECTION", fallback="kb_semantica"),
    EMBED_BATCH_SIZE=int(os.getenv("EMBED_BATCH_SIZE") or _CFG.get("EMBEDDING", "EMBED_BATCH_SIZE", fallback="64")),
    EMBED_CONCURRENCY=int(os.getenv("EMBED_CONCURRENCY") or _CFG.get("EMBEDDING", "EMBED_CONCURRENCY", fallback="4")),
    EMBED_MODEL=os.getenv("EMBED_MODEL") or _CFG.get("EMBEDDING", "EMBED_MODEL", fallback="nomic-embed-text:latest"),
    LLM_MODEL=os.getenv("LLM_MODEL") or _CFG.get("LLM", "LLM_MODEL", fallback="llama3:8b"),
    ANONYMIZE_TELEMETRY=_str_to_bool(
        os.getenv("ANONYMIZE_TELEMETRY")
        or _CFG.get("PRIVACY", "ANONYMIZE_TELEMETRY", fallback="false"),
        default=False,
    ),
)


def inizializza_ambiente() -> None:
    for cartella in [
        INPUT_DIR,
        LIBRERIA_DIR,
        BACKUP_DIR,
        MEDIA_CACHE_DIR,
        BIBLIOTECA_DIR,
        ERROR_DIR,
        CHROMA_DIR,
        DB_SQLITE_PATH.parent,
        KB_JSONL.parent,
    ]:
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
