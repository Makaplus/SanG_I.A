#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Organizzatore.py (v2 - ANNO + RGNR adiacente)
Smista documenti giudiziari per ANNO (cartelle YYYY) estraendo il "Procedimento Penale/anno"
accanto alle diciture RGNR / R.G.N.R. (o R.G.I.P., R.O.C.C., ecc.), con OCR di fallback.
"""

import os
import sys
import re
import csv
import hashlib
import logging
import sqlite3
import datetime
import shutil
from pathlib import Path

import fitz
import easyocr
import docx
from PIL import Image
import numpy as np
from chardet.universaldetector import UniversalDetector

try:
    from win32com import client as win32_client
    from pythoncom import com_error
except Exception:
    win32_client = None
    com_error = Exception


def _detect_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
        if parent.name.upper() == "SANGIA":
            return parent
    return current.parent


PROJECT_ROOT = _detect_project_root()
LIBRERIA_BASE_PATH = PROJECT_ROOT / "libreria"
INPUT_BASE_PATH = PROJECT_ROOT / "input_documenti"
BACKUP_BASE_PATH = PROJECT_ROOT / "Backup"
LOG_BASE_PATH = BACKUP_BASE_PATH / "log_smistamenti"

CARTELLE_VARI_VERBALI = "Vari_Verbali"
CARTELLA_ERRORI = "Errori"
CARTELLA_DUPLICATI = "Duplicati"
DB_NAME = "documenti.db"
SOTTOCARTELLA_SENTENZE = "Sentenze"

base_path = LIBRERIA_BASE_PATH
input_path = INPUT_BASE_PATH
error_path = INPUT_BASE_PATH / CARTELLA_ERRORI
varie_verbali_path = base_path / CARTELLE_VARI_VERBALI
duplicates_path = INPUT_BASE_PATH / CARTELLA_DUPLICATI
db_path = base_path / DB_NAME
error_report_path = error_path / "errori_report.csv"
manual_overrides_path = BACKUP_BASE_PATH / "correzioni" / "correzioni_smistamento.csv"

report_counts = {
    "totale": 0,
    "smistati_per_anno": 0,
    "smistati_verbali": 0,
    "errori": 0,
    "saltati_sha1": 0,
    "saltati_nome": 0,
    "duplicati_archiviati": 0,
}

CURRENT_YEAR = datetime.datetime.now().year
MANUAL_OVERRIDES = {}
YEAR_ONLY_MODE = True



def setup_logging():
    LOG_BASE_PATH.mkdir(parents=True, exist_ok=True)
    log_filename = datetime.datetime.now().strftime("log_smistamento_%Y%m%d_%H%M%S.log")
    log_path = LOG_BASE_PATH / log_filename
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("--- Avvio del Processo di Smistamento ---")


def init_ocr_reader():
    gpu_enabled = False
    gpu_reason = "torch non disponibile"
    try:
        import torch

        gpu_enabled = bool(torch.cuda.is_available())
        if gpu_enabled:
            gpu_reason = "CUDA disponibile"
        else:
            gpu_reason = "CUDA non disponibile (driver/GPU non rilevati)"
    except Exception as e:
        gpu_enabled = False
        gpu_reason = f"torch/cuda check fallito: {e}"

    try:
        reader = easyocr.Reader(["it"], gpu=gpu_enabled)
        if gpu_enabled:
            logging.info("OCR Reader inizializzato (GPU): %s", gpu_reason)
        else:
            logging.info("OCR Reader inizializzato (CPU): %s", gpu_reason)
        return reader
    except Exception as e:
        logging.warning("Init OCR con GPU=%s fallita (%s). Fallback CPU.", gpu_enabled, e)
        return easyocr.Reader(["it"], gpu=False)


def normalize_input_dropzone():
    return


def normalize_overrides_csv_location():
    """Mantiene un solo correzioni_smistamento.csv in Backup/correzioni e rimuove duplicati in input_documenti."""
    ensure_manual_overrides_template()
    canonical = manual_overrides_path
    migrated = 0

    for csv_path in input_path.rglob("correzioni_smistamento.csv"):
        if csv_path.resolve() == canonical.resolve():
            continue
        try:
            if csv_path.stat().st_size > 0:
                with open(csv_path, "r", encoding="utf-8-sig", newline="") as src:
                    rows = list(csv.DictReader(src, delimiter=";"))
                if rows:
                    existing_keys = set()
                    if canonical.exists() and canonical.stat().st_size > 0:
                        with open(canonical, "r", encoding="utf-8-sig", newline="") as dstf:
                            for row in csv.DictReader(dstf, delimiter=";"):
                                existing_keys.add((row.get("file") or "").strip())
                    with open(canonical, "a", encoding="utf-8", newline="") as dstf:
                        writer = csv.DictWriter(
                            dstf,
                            fieldnames=["file", "anno", "tipo_documento", "rgnr", "procura", "destinazione", "note"],
                            delimiter=";",
                        )
                        for row in rows:
                            key = (row.get("file") or "").strip()
                            if key and key not in existing_keys:
                                writer.writerow({k: row.get(k, "") for k in writer.fieldnames})
                                existing_keys.add(key)
                                migrated += 1
            csv_path.unlink(missing_ok=True)
        except Exception as e:
            logging.warning("Impossibile migrare/rimuovere %s: %s", csv_path, e)

    removed_dirs = 0
    for d in sorted([x for x in input_path.rglob("*") if x.is_dir()], reverse=True):
        if d in {input_path, error_path, duplicates_path}:
            continue
        try:
            if not any(d.iterdir()):
                d.rmdir()
                removed_dirs += 1
        except Exception:
            pass

    if migrated or removed_dirs:
        logging.info("Pulizia input_documenti completata: righe migrate CSV=%d, cartelle vuote rimosse=%d", migrated, removed_dirs)


def infer_year_fast(file_name: str, title_hints: dict, filename_info: dict, manual_override: dict | None):
    if manual_override and manual_override.get("anno"):
        return manual_override["anno"]
    if title_hints.get("year") is not None:
        return title_hints["year"]

    data_rif = filename_info.get("data_riferimento_file")
    if data_rif:
        year_str = data_rif[-4:] if len(data_rif) >= 8 else data_rif[-2:]
        yr = normalize_year(year_str)
        if yr and 1980 <= yr <= CURRENT_YEAR + 1:
            return yr

    m = re.search(r"\b(19[8-9]\d|20\d{2}|2100)\b", file_name)
    if m:
        yy = int(m.group(1))
        if 1980 <= yy <= CURRENT_YEAR + 1:
            return yy

    return infer_year_from_filename(file_name)


def load_manual_overrides() -> dict:
    overrides = {}
    if not manual_overrides_path.exists():
        return overrides

    try:
        with open(manual_overrides_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                file_name = (row.get("file") or "").strip()
                if not file_name or file_name.startswith("#"):
                    continue

                anno_val = (row.get("anno") or "").strip()
                anno = None
                if anno_val:
                    try:
                        anno = int(anno_val)
                    except ValueError:
                        anno = normalize_year(anno_val)

                overrides[file_name] = {
                    "anno": anno,
                    "tipo_documento": (row.get("tipo_documento") or "").strip() or None,
                    "rgnr": (row.get("rgnr") or "").strip() or None,
                    "procura": (row.get("procura") or "").strip() or None,
                    "destinazione": (row.get("destinazione") or "").strip().upper() or None,
                    "note": (row.get("note") or "").strip() or None,
                }
        logging.info("Override manuali caricati: %s (%d righe)", manual_overrides_path, len(overrides))
    except Exception as e:
        logging.warning("Impossibile leggere override manuali %s: %s", manual_overrides_path, e)

    return overrides


def ensure_manual_overrides_template():
    if manual_overrides_path.exists():
        return

    try:
        manual_overrides_path.parent.mkdir(parents=True, exist_ok=True)
        with open(manual_overrides_path, "w", encoding="utf-8", newline="") as f:
            f.write("file;anno;tipo_documento;rgnr;procura;destinazione;note\n")
        logging.info("Creato template override manuali: %s", manual_overrides_path)
    except Exception as e:
        logging.warning("Impossibile creare template override %s: %s", manual_overrides_path, e)


def _append_error_report(file_name: str, reason: str):
    error_path.mkdir(parents=True, exist_ok=True)
    exists = error_report_path.exists()
    with open(error_report_path, "a", encoding="utf-8") as f:
        if not exists:
            f.write("timestamp;file;reason\n")
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{ts};{file_name};{reason}\n")


def move_to_error(file_path: Path, reason: str):
    error_path.mkdir(parents=True, exist_ok=True)
    dest = error_path / file_path.name
    try:
        if dest.exists():
            stem = file_path.stem
            suffix = file_path.suffix
            dest = error_path / f"{stem}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}{suffix}"
        shutil.move(str(file_path), str(dest))
    except Exception as e:
        logging.error("Errore spostamento in cartella errori %s: %s", file_path.name, e)
    _append_error_report(file_path.name, reason)
    report_counts["errori"] += 1
    logging.warning("Errore classificazione %s: %s", file_path.name, reason)


def setup_database():
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS documenti (
            id INTEGER PRIMARY KEY,
            nome_file TEXT NOT NULL,
            rgnr TEXT,
            anno INTEGER,
            procura TEXT,
            tipo_documento TEXT,
            indagato_principale TEXT,
            num_correi INTEGER,
            operazione_nome TEXT,
            data_riferimento_file TEXT,
            modello_rgnr TEXT,
            dimensione_file INTEGER,
            data_ultima_modifica TEXT,
            sha1 TEXT,
            tipo_file TEXT,
            numero_pagine INTEGER,
            text_source TEXT,
            needs_ocr INTEGER,
            text_quality INTEGER,
            snippet_testo TEXT,
            note TEXT,
            data_inserimento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_anno ON documenti(anno)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nome_file ON documenti(nome_file)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sha1 ON documenti(sha1)")
    for col_def in [
        "tipo_file TEXT",
        "numero_pagine INTEGER",
        "text_source TEXT",
        "needs_ocr INTEGER",
        "text_quality INTEGER",
        "snippet_testo TEXT",
    ]:
        try:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {col_def}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()


def insert_document_info(
    nome_file,
    rgnr,
    anno,
    procura,
    tipo_documento,
    indagato_principale,
    num_correi,
    operazione_nome,
    data_riferimento_file,
    modello_rgnr,
    dimensione,
    data_modifica,
    sha1sum,
    tipo_file,
    numero_pagine,
    text_source,
    needs_ocr,
    text_quality,
    snippet_testo,
):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO documenti (
            nome_file, rgnr, anno, procura, tipo_documento,
            indagato_principale, num_correi, operazione_nome,
            data_riferimento_file, modello_rgnr, dimensione_file,
            data_ultima_modifica, sha1, tipo_file, numero_pagine,
            text_source, needs_ocr, text_quality, snippet_testo
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            nome_file,
            rgnr,
            anno,
            procura,
            tipo_documento,
            indagato_principale,
            num_correi,
            operazione_nome,
            data_riferimento_file,
            modello_rgnr,
            dimensione,
            data_modifica,
            sha1sum,
            tipo_file,
            numero_pagine,
            text_source,
            needs_ocr,
            text_quality,
            snippet_testo,
        ),
    )
    conn.commit()
    conn.close()


def file_is_already_processed_by_name(file_name):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM documenti WHERE nome_file = ?", (file_name,))
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def file_is_already_processed_by_hash(sha1sum):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM documenti WHERE sha1 = ?", (sha1sum,))
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def file_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_info_from_filename(filename):
    info = {
        "indagato_principale": None,
        "num_correi": None,
        "operazione_nome": None,
        "data_riferimento_file": None,
    }
    base_name = Path(filename).stem
    m = re.match(r"(.+?)\s*\+\s*(\d+)\s*(.*)", base_name, re.IGNORECASE)
    if m:
        indagato_raw = m.group(1).strip()
        info["num_correi"] = int(m.group(2))
        remainder = m.group(3).strip()
        info["indagato_principale"] = re.sub(r"(\s+\d+|\s+Olimpia)$", "", indagato_raw).strip()
        md = re.search(r"(\d{8}|\d{6}|\d{4}[._\-]\d{2}[._\-]\d{2})", remainder)
        if md:
            info["data_riferimento_file"] = re.sub(r"[._\-]", "", md.group(1))
        mo = re.search(r"\(?([A-Z][A-Za-z0-9\s]+)\)?", remainder)
        if mo:
            info["operazione_nome"] = mo.group(1).strip().replace("(", "").replace(")", "")
    return info


def classify_document_type(text):
    t = text.lower()
    keywords = {
        "Sentenza d'Appello": ["sentenza d'appello"],
        "Ordinanza di Custodia Cautelare in Carcere": [
            "ordinanza di custodia cautelare in carcere",
            "occ",
            "ordinanza di custodia cautelare",
        ],
        "Ordinanza e Sequestro Preventivo": [
            "decreto di sequestro preventivo",
            "sequestro preventivo",
        ],
        "Ordinanza": ["ordinanza"],
        "Richiesta di Misura Cautelare": [
            "richiesta di custodia cautelare",
            "richiesta di misura cautelare",
        ],
        "Annotazione di Polizia Giudiziaria": [
            "annotazione di polizia giudiziaria",
            "annotazione p.g.",
            "annotazione",
        ],
        "Relazione di Servizio": ["relazione di servizio", "relazione"],
        "Articolo di Giornale": ["articolo di giornale", "rassegna stampa", "articolo"],
        "Informativa": ["informativa"],
        "Dichiarazioni": ["dichiarazioni", "verbale"],
        "Fascicolo Personale": ["fascicolo personale", "fascicolo"],
        "Sentenza": ["sentenza", "sentenze"],
    }
    for doc_type, k_list in keywords.items():
        for k in k_list:
            if re.search(r"\b" + re.escape(k) + r"\b", t):
                return doc_type
    return "Non Classificato"


def detect_file_type(file_path: Path) -> str:
    ext = file_path.suffix.lower()
    if ext == ".pdf":
        return "pdf"
    if ext in {".docx", ".doc"}:
        return "docx" if ext == ".docx" else "doc"
    if ext in {".txt", ".csv", ".json", ".xml"}:
        return "plain"
    if ext == ".rtf":
        return "rtf"
    if ext in {".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp", ".webp"}:
        return "image"

    try:
        with open(file_path, "rb") as f:
            head = f.read(16)
        if head.startswith(b"%PDF"):
            return "pdf"
        if head.startswith(b"{\\rtf"):
            return "rtf"
        if head[:4] == b"PK\x03\x04":
            return "docx"
    except Exception:
        pass

    return "unknown"


def _text_quality_score(text: str) -> int:
    t = (text or "").strip()
    if not t:
        return 0
    alpha = sum(c.isalpha() for c in t)
    printable = sum(c.isprintable() for c in t)
    ratio_alpha = alpha / max(1, len(t))
    ratio_print = printable / max(1, len(t))
    len_score = min(60, len(t) // 30)
    return max(0, min(100, int(len_score + ratio_alpha * 30 + ratio_print * 10)))


def _read_plain_with_detection(file_path: Path, max_chars: int) -> str:
    try:
        return file_path.read_text(encoding="utf-8")[:max_chars]
    except Exception:
        det = UniversalDetector()
        with open(file_path, "rb") as f:
            for line in f:
                det.feed(line)
                if det.done:
                    break
        det.close()
        enc = det.result.get("encoding") or "latin-1"
        return file_path.read_text(encoding=enc, errors="ignore")[:max_chars]


def _rtf_to_text(rtf_text: str) -> str:
    t = re.sub(r"\\par[d]?", "\n", rtf_text)
    t = re.sub(r"\\'[0-9a-fA-F]{2}", "", t)
    t = re.sub(r"\\[a-zA-Z]+-?\d* ?", "", t)
    t = t.replace("{", " ").replace("}", " ")
    return re.sub(r"\s+", " ", t).strip()


def _ocr_image_np(img_np, ocr_reader):
    if ocr_reader is None:
        return ""
    out = []
    try:
        for (_, txt, prob) in ocr_reader.readtext(img_np):
            if prob >= 0.3:
                out.append(txt)
    except Exception:
        return ""
    return " ".join(out)


def get_text_from_file(file_path: Path, ocr_reader):
    MAX_CHARS_FOR_SEARCH = 5000
    text_content = ""
    moved_to_error = False
    file_type = detect_file_type(file_path)
    meta = {
        "tipo_file": file_type,
        "text_source": "none",
        "needs_ocr": 0,
        "text_quality": 0,
        "numero_pagine": None,
        "snippet_testo": "",
        "ocr_sample_only": 0,
    }

    if file_type == "docx":
        try:
            d = docx.Document(file_path)
            parts = [p.text for p in d.paragraphs if p.text]
            for t in d.tables:
                for row in t.rows:
                    for cell in row.cells:
                        if cell.text:
                            parts.append(cell.text)
            text_content = "\n".join(parts)[:MAX_CHARS_FOR_SEARCH]
            meta["text_source"] = "docx"
        except Exception as e:
            logging.error(f"Errore lettura DOCX: {file_path.name}. Dettaglio: {e}")

    elif file_type == "doc":
        if win32_client is None:
            logging.error(f"pywin32 non disponibile: impossibile leggere DOC {file_path.name}")
        else:
            word = None
            try:
                word = win32_client.Dispatch("Word.Application")
                doc = word.Documents.Open(str(file_path))
                text_content = (doc.Range(0, MAX_CHARS_FOR_SEARCH).Text or "")
                doc.Close(SaveChanges=0)
                meta["text_source"] = "docx"
            except com_error as e:
                logging.error(f"Errore COM DOC: {file_path.name}. Dettaglio: {e}")
                try:
                    move_to_error(file_path, "DOC corrotto o non leggibile via COM")
                    moved_to_error = True
                except Exception as move_e:
                    logging.critical(f"Impossibile spostare DOC corrotto: {move_e}")
            except Exception as e:
                logging.error(f"Errore generico DOC: {file_path.name}. Dettaglio: {e}")
            finally:
                if word:
                    try:
                        word.Quit()
                    except Exception:
                        pass

    elif file_type == "plain":
        try:
            text_content = _read_plain_with_detection(file_path, MAX_CHARS_FOR_SEARCH)
            meta["text_source"] = "plain"
        except Exception as e:
            logging.error(f"Errore lettura testo: {file_path.name}. Dettaglio: {e}")

    elif file_type == "rtf":
        try:
            raw = _read_plain_with_detection(file_path, max_chars=MAX_CHARS_FOR_SEARCH * 2)
            text_content = _rtf_to_text(raw)[:MAX_CHARS_FOR_SEARCH]
            meta["text_source"] = "plain"
        except Exception as e:
            logging.error(f"Errore lettura RTF: {file_path.name}. Dettaglio: {e}")

    elif file_type == "image":
        meta["needs_ocr"] = 1
        try:
            img = Image.open(file_path).convert("RGB")
            text_content = _ocr_image_np(np.array(img), ocr_reader)[:MAX_CHARS_FOR_SEARCH]
            meta["text_source"] = "ocr"
            meta["numero_pagine"] = 1
        except Exception as e:
            logging.error(f"Errore OCR immagine: {file_path.name}. Dettaglio: {e}")

    elif file_type == "pdf":
        try:
            with fitz.open(file_path) as doc:
                meta["numero_pagine"] = doc.page_count
                digital_parts = []
                for i in range(doc.page_count):
                    digital_parts.append(doc.load_page(i).get_text() or "")
                    if len("".join(digital_parts)) >= MAX_CHARS_FOR_SEARCH:
                        break
                digital_text = " ".join(digital_parts)[:MAX_CHARS_FOR_SEARCH]
                if _text_quality_score(digital_text) >= 20 and len(digital_text.strip()) >= 120:
                    text_content = digital_text
                    meta["text_source"] = "pdf_text"
                else:
                    meta["needs_ocr"] = 1
                    indexes = set(range(min(3, doc.page_count)))
                    if doc.page_count > 0:
                        indexes.add(doc.page_count - 1)
                    if doc.page_count > 2:
                        indexes.add(doc.page_count // 2)
                    sample_parts = []
                    for i in sorted(indexes):
                        page = doc.load_page(i)
                        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                        sample_parts.append(_ocr_image_np(np.array(img), ocr_reader))
                        if len(" ".join(sample_parts)) >= MAX_CHARS_FOR_SEARCH:
                            break
                    text_content = " ".join(sample_parts)[:MAX_CHARS_FOR_SEARCH]
                    meta["text_source"] = "ocr"
                    meta["ocr_sample_only"] = 1
        except Exception as e:
            logging.error(f"Errore lettura PDF: {file_path.name}. Dettaglio: {e}")

    else:
        try:
            text_content = _read_plain_with_detection(file_path, MAX_CHARS_FOR_SEARCH)
            meta["text_source"] = "plain"
        except Exception:
            text_content = ""
            meta["text_source"] = "none"

    text_content = (text_content or "")[:MAX_CHARS_FOR_SEARCH]
    meta["text_quality"] = _text_quality_score(text_content)
    meta["snippet_testo"] = text_content[:280]
    return text_content, moved_to_error, meta


def normalize_year(two_or_four: str):
    try:
        n = int(two_or_four)
    except ValueError:
        return None
    if 0 <= n <= 99:
        if 80 <= n <= 99:
            return 1900 + n
        pivot = CURRENT_YEAR % 100
        return 2000 + n if n <= pivot else 1900 + n
    if 1900 <= n <= CURRENT_YEAR + 1:
        return n
    return None


def find_near_pairs(text_upper: str, token_spans, window: int = 60):
    pairs = []
    pp_pat = re.compile(r"(?:N\.?\s*)?(\d{1,6})\s*[/\-]\s*(\d{2}|\d{4})")
    for s, e in token_spans:
        left = max(0, s - window)
        right = min(len(text_upper), e + window)
        for m in pp_pat.finditer(text_upper[left:right]):
            pairs.append((left + m.start(), m.group(1), m.group(2)))

    ranked = []
    for abs_start, num, yr in pairs:
        dists = [abs(abs_start - ((s + e) // 2)) for s, e in token_spans]
        ranked.append((min(dists), abs_start, num, yr))
    ranked.sort(key=lambda x: x[0])
    return [(a, b, c) for _, a, b, c in ranked]


def find_year_and_info_in_text(text: str):
    info = {
        "year": None,
        "rgnr": None,
        "procura": None,
        "modello_rgnr": None,
        "is_comune_ordinance": False,
    }

    text_upper = text.upper().replace("\n", " ")
    mp = re.search(r"(?:TRIBUNALE|PROCURA|CORTE|LEGIONE CARABINIERI|D\.?D\.?A\.?|COMUNE)\s+DI\s+([A-Z\s\(\)]+)", text_upper)
    if mp:
        info["procura"] = mp.group(1).strip()

    if re.search(r"\bCOMUNE\s+DI\b", text_upper) and not re.search(
        r"\b(TRIBUNALE|PROCURA|R\.?\s*G\.?\s*N\.?\s*R\.?|RGNR|R\.?\s*G\.?\s*I\.?\s*P\.?)\b",
        text_upper,
    ):
        info["is_comune_ordinance"] = True

    candidates = []
    weighted_patterns = [
        (1, r"\bN\.?\s*(\d{1,6})\s*[/\-]\s*(\d{2,4})\s*R\.?\s*G\.?\s*N\.?\s*R\.?\s*D\.?\s*D\.?\s*A\.?"),
        (2, r"\bN\.?\s*(\d{1,6})\s*[/\-]\s*(\d{2,4})\s*(?:R\.?\s*G\.?\s*N\.?\s*R\.?|RGNR)"),
        (3, r"\bN\.?\s*(\d{1,6})\s*[/\-]\s*(\d{2,4})\s*R\.?\s*G\.?\s*NOTIZIE\s+DI\s+REATO"),
        (4, r"\b(\d{1,6})\s*[/\-]\s*(\d{2,4})[^\n]{0,35}(?:R\.?\s*G\.?\s*N\.?\s*R\.?|RGNR)"),
        (6, r"PROC\.?\s*N\.?\s*(\d{1,6})\s*[/\-]\s*(\d{2,4})\s*(?:R\.?\s*G\.?\s*N\.?\s*R\.?|R\.?\s*G\.?\s*I\.?\s*P\.?|R\.?\s*O\.?\s*C\.?\s*C\.?)"),
        (7, r"\bN\.?\s*(\d{1,6})\s*[/\-]\s*(\d{2,4})\s*(?:R\.?\s*O\.?\s*C\.?\s*C\.?)"),
        (8, r"\b(\d{1,6})\s*[/\-]\s*(\d{2,4})\s*R\.?\s*G\.?\b"),
    ]

    for weight, pat in weighted_patterns:
        for m in re.finditer(pat, text_upper):
            yr = normalize_year(m.group(2))
            if yr:
                candidates.append((weight, m.start(), m.group(1), m.group(2), yr))

    token_regex = re.compile(
        r"(R\.?\s*G\.?\s*N\.?\s*R\.?|RGNR|R\.?\s*G\.?\s*I\.?\s*P\.?|R\.?\s*O\.?\s*C\.?\s*C\.?|R\.?\s*G\.?)"
    )
    token_spans = [m.span() for m in token_regex.finditer(text_upper)]
    for _, num_str, year_str in find_near_pairs(text_upper, token_spans, window=60):
        yr = normalize_year(year_str)
        if yr:
            near_pos = text_upper.find(f"{num_str}/{year_str}")
            candidates.append((9, near_pos if near_pos >= 0 else 999999, num_str, year_str, yr))

    if candidates:
        candidates.sort(key=lambda x: (x[0], x[1]))
        _, _, num_raw, year_raw, year_norm = candidates[0]
        info["year"] = year_norm
        info["rgnr"] = f"{num_raw}/{year_raw}"

    if info["year"] is None:
        m = re.search(r"(\d{1,6})\s*[/\-]\s*(\d{2,4})", text_upper)
        if m:
            yr = normalize_year(m.group(2))
            if yr:
                info["year"] = yr
                info["rgnr"] = f"{m.group(1)}/{m.group(2)}"

    if info["rgnr"]:
        idx = text_upper.find(info["rgnr"])
        if idx != -1:
            mm = re.search(r"MOD\.?(?:ELLO)?[\s._\-]*(\d{2})", text_upper[idx : idx + 120])
            if mm and mm.group(1) in {"21", "44"}:
                info["modello_rgnr"] = f"Mod. {mm.group(1)}"

    return info


def extract_year_from_text_dates(text: str):
    t = text.upper()

    for m in re.finditer(r"\b(\d{1,2})[./\-](\d{1,2})[./\-](\d{2,4})\b", t):
        yr = normalize_year(m.group(3))
        if yr and 1980 <= yr <= CURRENT_YEAR + 1:
            return yr

    months = "GENNAIO|FEBBRAIO|MARZO|APRILE|MAGGIO|GIUGNO|LUGLIO|AGOSTO|SETTEMBRE|OTTOBRE|NOVEMBRE|DICEMBRE"
    m = re.search(rf"\b\d{{1,2}}\s+(?:{months})\s+(\d{{4}})\b", t)
    if m:
        yr = normalize_year(m.group(1))
        if yr and 1980 <= yr <= CURRENT_YEAR + 1:
            return yr

    return None


def build_dest_dir_by_year(base: Path, anno: int) -> Path:
    return base / str(anno)


def archive_duplicate_file(file_path: Path, reason: str):
    duplicates_path.mkdir(parents=True, exist_ok=True)
    destination = duplicates_path / file_path.name
    if destination.exists():
        stem = destination.stem
        suffix = destination.suffix
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        destination = duplicates_path / f"{stem}__dup_{timestamp}{suffix}"

    try:
        shutil.move(str(file_path), str(destination))
        report_counts["duplicati_archiviati"] += 1
        logging.info("Duplicato archiviato in %s (%s): %s", duplicates_path, reason, file_path.name)
    except Exception as e:
        logging.warning("Impossibile archiviare duplicato %s (%s): %s", file_path.name, reason, e)


def infer_year_from_filename(filename: str):
    stem = Path(filename).stem
    for m in re.finditer(r"(?<!\d)(\d{6}|\d{8})(?!\d)", stem):
        token = m.group(1)
        year_token = token[0:4] if len(token) == 8 else token[0:2]
        yr = normalize_year(year_token)
        if yr and 1980 <= yr <= CURRENT_YEAR + 1:
            return yr

    sep_patterns = [
        r"(?<!\d)(\d{2})[._/-](\d{2})[._/-](\d{2}|\d{4})(?!\d)",
        r"(?<!\d)(\d{4})[._/-](\d{2})[._/-](\d{2})(?!\d)",
    ]
    for pat in sep_patterns:
        for m in re.finditer(pat, stem):
            ypart = m.group(3) if pat.startswith(r"(?<!\d)(\d{2})") else m.group(1)
            yr = normalize_year(ypart)
            if yr and 1980 <= yr <= CURRENT_YEAR + 1:
                return yr

    return None


def parse_title_hints(filename: str) -> dict:
    stem = Path(filename).stem
    t = stem.upper()

    hints = {
        "document_type": None,
        "year": None,
        "rgnr": None,
    }

    if re.search(r"\bSENTENZA\b", t):
        hints["document_type"] = "Sentenza"

    if "ANNOTAZIONE" in t:
        hints["document_type"] = "Annotazione di Polizia Giudiziaria"
    elif "RELAZIONE" in t:
        hints["document_type"] = "Relazione di Servizio"
    elif "ARTICOLO" in t:
        hints["document_type"] = "Articolo di Giornale"

    if re.search(r"\b(OCC|ORDINANZA)\b", t):
        hints["document_type"] = "Ordinanza di Custodia Cautelare in Carcere"

    if re.search(r"\bPARTE\s*[_\- ]?(I|II|III|IV|V|1|2|3|4|5)\b", t):
        hints["document_type"] = hints["document_type"] or "Ordinanza di Custodia Cautelare in Carcere"

    if re.search(r"[A-ZÀ-ÖØ-Ý][A-ZÀ-ÖØ-Ýa-zà-öø-ÿ'`]+\s+[A-ZÀ-ÖØ-Ý][A-ZÀ-ÖØ-Ýa-zà-öø-ÿ'`]+\s*\+\s*\d+", stem):
        hints["document_type"] = hints["document_type"] or "Ordinanza di Custodia Cautelare in Carcere"

    strong_patterns = [
        r"(?:SENTENZA|SENT\.?|OCC|R\.?G\.?N\.?R\.?)[^\d]{0,20}(\d{1,6})\s*[/\-]\s*(\d{2,4})",
        r"\bN\.?\s*(\d{1,6})\s*[/\-]\s*(\d{2,4})",
    ]
    for pat in strong_patterns:
        m = re.search(pat, t, re.IGNORECASE)
        if m:
            yy = normalize_year(m.group(2))
            if yy:
                hints["year"] = yy
                hints["rgnr"] = f"{m.group(1)}/{m.group(2)}"
                break

    if hints["year"] is None:
        hints["year"] = infer_year_from_filename(filename)

    return hints


def build_no_rule_reason(file_name: str, document_type: str, extracted_info: dict, filename_info: dict, title_hints: dict, final_year):
    year_from_text = extracted_info.get("year")
    rgnr = extracted_info.get("rgnr")
    procura = extracted_info.get("procura")
    is_comune = extracted_info.get("is_comune_ordinance", False)
    data_rif = filename_info.get("data_riferimento_file")
    operazione = filename_info.get("operazione_nome")

    title_year = title_hints.get("year") if title_hints else None
    title_rgnr = title_hints.get("rgnr") if title_hints else None

    parts = [
        "Nessuna regola di smistamento applicabile",
        f"tipo_documento={document_type}",
        f"anno_finale={final_year}",
        f"anno_da_testo={year_from_text}",
        f"rgnr={rgnr}",
        f"procura={procura}",
        f"comune_ordinanza={is_comune}",
        f"data_filename={data_rif}",
        f"operazione_filename={operazione}",
        f"anno_da_titolo={title_year}",
        f"rgnr_da_titolo={title_rgnr}",
    ]

    if document_type == "Non Classificato":
        parts.append("hint=nessuna keyword documento riconosciuta")
    if final_year is None:
        parts.append("hint=anno_non_rilevato")

    return " | ".join(str(x) for x in parts)


def process_file(file_path: Path, ocr_reader, year_only: bool = YEAR_ONLY_MODE):
    global report_counts
    file_name = file_path.name

    try:
        sha1sum = file_sha1(file_path)
        if file_is_already_processed_by_hash(sha1sum):
            report_counts["saltati_sha1"] += 1
            logging.info(f"File già presente (SHA1): {file_name}. Saltato.")
            archive_duplicate_file(file_path, "SHA1")
            return
    except Exception as e:
        logging.warning(f"Impossibile calcolare SHA1 per {file_name}: {e}")
        sha1sum = None

    if file_is_already_processed_by_name(file_name):
        report_counts["saltati_nome"] += 1
        logging.info(f"File già registrato per nome: {file_name}. Saltato.")
        archive_duplicate_file(file_path, "NOME")
        return

    filename_info = extract_info_from_filename(file_name)
    title_hints = parse_title_hints(file_name)
    manual_override = MANUAL_OVERRIDES.get(file_name)

    if year_only:
        dimensione = os.path.getsize(file_path)
        data_modifica = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S")
        final_year = infer_year_fast(file_name, title_hints, filename_info, manual_override)
        if not final_year:
            move_to_error(file_path, "Modalità anno: anno non rilevato da nome file/override")
            return

        dest_root = build_dest_dir_by_year(base_path, final_year)
        dest_root.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(file_path), str(dest_root / file_name))
            insert_document_info(
                nome_file=file_name,
                rgnr=(manual_override or {}).get("rgnr") or title_hints.get("rgnr"),
                anno=final_year,
                procura=(manual_override or {}).get("procura"),
                tipo_documento=(manual_override or {}).get("tipo_documento") or title_hints.get("document_type") or "Non Classificato",
                indagato_principale=filename_info.get("indagato_principale"),
                num_correi=filename_info.get("num_correi"),
                operazione_nome=filename_info.get("operazione_nome"),
                data_riferimento_file=filename_info.get("data_riferimento_file"),
                modello_rgnr=None,
                dimensione=dimensione,
                data_modifica=data_modifica,
                sha1sum=sha1sum,
                tipo_file=detect_file_type(file_path),
                numero_pagine=None,
                text_source="none",
                needs_ocr=0,
                text_quality=0,
                snippet_testo="",
            )
            report_counts["smistati_per_anno"] += 1
            logging.info("SMISTATO (solo anno): %s -> %s", file_name, dest_root)
        except Exception as e:
            logging.error("ERRORE FINALE (solo anno) %s: %s", file_name, e)
            if file_path.exists():
                move_to_error(file_path, f"Errore finale modalità anno: {e}")
        return

    text_content, moved_to_error, extraction_meta = get_text_from_file(file_path, ocr_reader)
    if moved_to_error:
        return

    dimensione = os.path.getsize(file_path)
    data_modifica = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d %H:%M:%S")

    if extraction_meta.get("ocr_sample_only") == 1 and extraction_meta.get("text_quality", 0) < 15:
        move_to_error(file_path, "PDF immagine: OCR campionato insufficiente (serve OCR completo manuale)")
        return

    if not text_content or len(text_content.strip()) < 10:
        move_to_error(file_path, f"Testo non leggibile o OCR insufficiente | chars={len((text_content or '').strip())}")
        return

    extracted_info = find_year_and_info_in_text(text_content)
    found_year = extracted_info["year"]
    document_type = classify_document_type(text_content)
    if title_hints.get("document_type") is not None:
        document_type = title_hints["document_type"]

    if manual_override and manual_override.get("tipo_documento"):
        document_type = manual_override["tipo_documento"]

    temp_year = None
    data_rif = filename_info.get("data_riferimento_file")
    if found_year is None and data_rif:
        year_str = data_rif[-4:] if len(data_rif) >= 8 else data_rif[-2:]
        yr = normalize_year(year_str)
        if yr and 1980 <= yr <= CURRENT_YEAR + 1:
            temp_year = yr

    final_year = found_year if found_year is not None else temp_year

    if final_year is None and title_hints.get("year") is not None:
        final_year = title_hints["year"]

    if final_year is None:
        m = re.search(r"\b(19[8-9]\d|20\d{2}|2100)\b", file_name)
        if m:
            yy = int(m.group(1))
            if 1980 <= yy <= CURRENT_YEAR + 1:
                final_year = yy

    if final_year is None:
        final_year = infer_year_from_filename(file_name)

    if final_year is None and extracted_info.get("rgnr"):
        mm = re.search(r"/(\d{2,4})", extracted_info.get("rgnr", ""))
        if mm:
            final_year = normalize_year(mm.group(1))

    if final_year is None:
        final_year = extract_year_from_text_dates(text_content)

    if manual_override and manual_override.get("anno"):
        final_year = manual_override["anno"]

    if manual_override and manual_override.get("rgnr"):
        extracted_info["rgnr"] = manual_override["rgnr"]
    if manual_override and manual_override.get("procura"):
        extracted_info["procura"] = manual_override["procura"]

    destination_override = (manual_override or {}).get("destinazione")

    target_path = None
    if destination_override == "ERRORI":
        move_to_error(file_path, f"Forzato da correzioni_smistamento.csv: {(manual_override or {}).get('note') or 'nessuna nota'}")
        return
    if final_year:
        dest_root = build_dest_dir_by_year(base_path, final_year)
        dest_root.mkdir(parents=True, exist_ok=True)
        target_path = dest_root
        report_counts["smistati_per_anno"] += 1
    else:
        move_to_error(file_path, build_no_rule_reason(file_name, document_type, extracted_info, filename_info, title_hints, final_year))
        return

    try:
        if manual_override:
            logging.info("Override manuale applicato a %s: %s", file_name, manual_override)
        shutil.move(str(file_path), str(target_path / file_name))
        if not extracted_info.get("rgnr") and title_hints.get("rgnr"):
            extracted_info["rgnr"] = title_hints["rgnr"]

        insert_document_info(
            nome_file=file_name,
            rgnr=extracted_info.get("rgnr"),
            anno=final_year,
            procura=extracted_info.get("procura"),
            tipo_documento=document_type,
            indagato_principale=filename_info.get("indagato_principale"),
            num_correi=filename_info.get("num_correi"),
            operazione_nome=filename_info.get("operazione_nome"),
            data_riferimento_file=filename_info.get("data_riferimento_file"),
            modello_rgnr=extracted_info.get("modello_rgnr"),
            dimensione=dimensione,
            data_modifica=data_modifica,
            sha1sum=sha1sum,
            tipo_file=extraction_meta.get("tipo_file"),
            numero_pagine=extraction_meta.get("numero_pagine"),
            text_source=extraction_meta.get("text_source"),
            needs_ocr=extraction_meta.get("needs_ocr"),
            text_quality=extraction_meta.get("text_quality"),
            snippet_testo=extraction_meta.get("snippet_testo"),
        )
        logging.info(f"SMISTATO: {file_name} -> {target_path}")
    except Exception as e:
        logging.error(f"ERRORE FINALE (DB/SPOSTAMENTO) {file_name}: {e}")
        if file_path.exists():
            move_to_error(file_path, f"Errore finale DB/Spostamento: {e}")
        report_counts["smistati_per_anno"] -= 1


def main():
    global MANUAL_OVERRIDES

    setup_logging()
    normalize_overrides_csv_location()
    ocr_reader = None if YEAR_ONLY_MODE else init_ocr_reader()

    base_path.mkdir(parents=True, exist_ok=True)
    input_path.mkdir(parents=True, exist_ok=True)
    error_path.mkdir(parents=True, exist_ok=True)
    varie_verbali_path.mkdir(parents=True, exist_ok=True)
    setup_database()
    ensure_manual_overrides_template()
    MANUAL_OVERRIDES = load_manual_overrides()

    files_to_process = [f for f in input_path.iterdir() if f.is_file()]
    report_counts["totale"] = len(files_to_process)

    if report_counts["totale"] == 0:
        logging.info(f"Nessun file trovato da processare in {input_path}")
        print(f"\n--- Report Finale ---\nNessun file trovato nella cartella {input_path}")
        print("Suggerimento: inserisci i file direttamente in input_documenti")
        return

    for file_path in files_to_process:
        if file_path.exists():
            process_file(file_path, ocr_reader, year_only=YEAR_ONLY_MODE)

    logging.info("--- Processo Completato ---")
    print("\n--- Report Finale ---")
    print(f"File Totali Considerati: {report_counts['totale']}")
    print(f"Smistati in cartelle anno: {report_counts['smistati_per_anno']}")
    print(f"File in cartella Errori: {report_counts['errori']}")
    print(f"Report errori dettagliato: {error_report_path}")
    print("Ogni riga include il motivo tecnico della mancata classificazione.")
    print(f"File saltati per duplicato SHA1: {report_counts['saltati_sha1']}")
    print(f"File saltati per duplicato nome: {report_counts['saltati_nome']}")
    print(f"File duplicati archiviati: {report_counts['duplicati_archiviati']}")
    print(f"Modalità attiva: {'solo suddivisione per anno' if YEAR_ONLY_MODE else 'analisi completa (OCR/testo)'}")


if __name__ == "__main__":
    main()
