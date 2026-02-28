#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - ORGANIZZATORE (v21.2)

Fix/novità principali:
1) Pre-process per estensione:
   - .doc/.docx/.rtf/.txt: chiama SEMPRE organizzatore_doc_reader.py (anche se non è OCC)
   - .pdf: preview pypdf per classificazione (OCR forte resta ai worker/Router revisione)

2) Classificazione rigida via “tabella regole” nel DB:
   - classi (attuali): OCC -> INFORMATIVA -> RELAZIONE -> ALTRO
   - seed DB include anche placeholder: SENTENZA, ARTICOLI, ANNOTAZIONI, RICORSI (per step successivi)

3) Smistamento NON-OCC:
   - se anno noto nel DB -> usa organizzatore_percorso.py (sposta in libreria\YYYY\) senza rename
   - se anno mancante -> usa organizzatore_vari_verbali.py

4) Output: stampa SEMPRE "tipo=..." su ogni riga.
"""

import re
import time
import json
import hashlib
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# =============================================================================
# UTILS
# =============================================================================

def keep_last_n_files(folder: Path, n: int = 5):
    folder.mkdir(parents=True, exist_ok=True)
    files = [p for p in folder.iterdir() if p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    for p in files[n:]:
        try:
            p.unlink()
        except Exception:
            pass

def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def format_mtime(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

def mark(ok: bool) -> str:
    return "✅" if ok else "⚠️"

def fmt_tipo(tipo_doc: Optional[str]) -> str:
    t = (tipo_doc or "").strip().upper() or "ALTRO"
    return f"tipo={t}"

def filename_has_occ_token(name: str) -> bool:
    # token OCC/OCCC come parola (evita match su 'OCCASIONE' ecc.)
    stem = Path(name).stem
    return bool(re.search(r"(?i)(?:^|[ \._\-])OCC{1,3}(?:$|[ \._\-])", stem))


# =============================================================================
# PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parent
INPUT_DIR = PROJECT_ROOT / "input_documenti"
DUP_DIR = INPUT_DIR / "duplicati"
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"

LIB_DIR = PROJECT_ROOT / "libreria"
REV_OCC_DIR = LIB_DIR / "revisione" / "occ"
LOG_DIR = PROJECT_ROOT / "Backup" / "log_smistamenti"

ALLOWED_EXTS = {".pdf", ".docx", ".doc", ".txt", ".rtf"}
MAX_PREVIEW_PAGES_PDF = 2
MAX_PREVIEW_CHARS = 8000
FINAL_STATUSES = {"STORED", "COMPLETED", "DONE"}

# gating OCC completo
RE_RGNR_PARSE = re.compile(r"(\d{1,7})\s*/\s*(\d{4})")

# =============================================================================
# PDF READER
# =============================================================================

def load_pdf_reader():
    try:
        from pypdf import PdfReader  # type: ignore
        return PdfReader, "pypdf"
    except Exception:
        return None, None

PdfReader, PDF_LIB = load_pdf_reader()

def pdf_preview(path: Path) -> Tuple[Dict[str, Any], Optional[int], str]:
    if not PdfReader:
        return {"_pdf_lib": "NONE"}, None, ""
    meta: Dict[str, Any] = {"_pdf_lib": PDF_LIB}
    pages = None
    parts: List[str] = []
    try:
        reader = PdfReader(str(path))
        try:
            pages = len(reader.pages)
        except Exception:
            pages = None

        n = min(MAX_PREVIEW_PAGES_PDF, pages or MAX_PREVIEW_PAGES_PDF)
        for i in range(n):
            try:
                t = reader.pages[i].extract_text() or ""
                t = t.strip()
                if t:
                    parts.append(t)
            except Exception:
                continue
    except Exception as e:
        meta["_error"] = str(e)

    preview = "\n\n".join(parts)
    if len(preview) > MAX_PREVIEW_CHARS:
        preview = preview[:MAX_PREVIEW_CHARS] + "\n...[TRUNCATED]..."
    return meta, pages, preview

def get_preview(path: Path) -> Tuple[Dict[str, Any], Optional[int], str]:
    if path.suffix.lower() == ".pdf":
        return pdf_preview(path)
    return {"_note": "no_preview"}, None, ""

# =============================================================================
# DB / RULES
# =============================================================================

def ensure_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS documenti (
        id INTEGER PRIMARY KEY,
        nome_file TEXT,
        sha1 TEXT,
        dimensione_file INTEGER,
        data_ultima_modifica TEXT,
        numero_pagine INTEGER,
        tipo_documento TEXT,
        rgnr TEXT,
        anno INTEGER,
        procura TEXT,
        status TEXT,
        evidence_occ TEXT,
        conf_occ REAL,
        snippet_testo TEXT,
        data_inserimento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("PRAGMA table_info(documenti)")
    cols = {row[1] for row in cur.fetchall()}

    def add_col(name: str, coltype: str):
        if name not in cols:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {name} {coltype}")

    add_col("tipo_file", "TEXT")
    add_col("text_preview", "TEXT")
    add_col("pdf_meta_json", "TEXT")
    add_col("nome_file_orig", "TEXT")
    add_col("nome_file_prev", "TEXT")
    add_col("rename_evidence", "TEXT")
    add_col("percorso_file", "TEXT")
    add_col("percorso_prev", "TEXT")
    add_col("percorso_evidence", "TEXT")
    add_col("dda_flag", "INTEGER")
    add_col("operazione_nome", "TEXT")
    add_col("nome_indagine", "TEXT")
    add_col("forza_polizia", "TEXT")
    add_col("categoria_secondaria", "TEXT")

    add_col("is_scan", "INTEGER")
    add_col("has_rgnr_hint", "INTEGER")
    add_col("rgnr_hint", "TEXT")
    add_col("anno_hint", "INTEGER")
    add_col("hint_text", "TEXT")

    add_col("motivo_revisione", "TEXT")
    add_col("retry_count", "INTEGER")
    add_col("last_retry_at", "TEXT")
    add_col("manual_required", "INTEGER")
    add_col("manual_note", "TEXT")
    add_col("esito_revisione", "TEXT")

    # classificazione rigida (nuovo)
    add_col("class_score", "INTEGER")
    add_col("class_trace", "TEXT")
    add_col("class_forced", "TEXT")
    add_col("note", "TEXT")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_sha1 ON documenti(sha1)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_status ON documenti(status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_tipo ON documenti(tipo_documento)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_cat2 ON documenti(categoria_secondaria)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_manual_required ON documenti(manual_required)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_doc_retry_count ON documenti(retry_count)")

    # tabella regole (minima)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS regole_classificazione (
        id INTEGER PRIMARY KEY,
        classe TEXT NOT NULL,
        marker TEXT NOT NULL,
        peso INTEGER NOT NULL DEFAULT 1,
        tipo_marker TEXT NOT NULL DEFAULT 'forte'  -- forte/debole/blacklist
    )
    """)

    con.commit()
    con.close()


def seed_rules_if_empty():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM regole_classificazione")
    n = cur.fetchone()[0]
    if n and n > 0:
        con.close()
        return

    rules = [
        # OCC (forti)
        ("OCC", "ordinanza di applicazione di misura cautelare", 6, "forte"),
        ("OCC", "ordinanza di custodia cautelare", 6, "forte"),
        ("OCC", "richiesta per l’applicazione di misure cautelari", 7, "forte"),
        ("OCC", "richiesta per l'applicazione di misure cautelari", 7, "forte"),
        ("OCC", "custodia cautelare", 4, "forte"),
        ("OCC", "misura cautelare", 3, "debole"),
        ("OCC", "giudice per le indagini preliminari", 3, "debole"),
        ("OCC", "gip", 2, "debole"),

        # INFORMATIVA (forti)
        ("INFORMATIVA", "comunicazione di notizia di reato", 7, "forte"),
        ("INFORMATIVA", "informativa di reato", 5, "forte"),
        ("INFORMATIVA", "notizia di reato", 4, "debole"),

        # RELAZIONE / ANNOTAZIONI (placeholder per step successivi)
        ("RELAZIONE", "relazione di servizio", 5, "forte"),
        ("ANNOTAZIONE", "annotazione", 4, "forte"),

        # Blacklist OCC (esempi tipici “non atto cautelare”)
        ("OCC", "camera dei deputati", -10, "blacklist"),
        ("OCC", "senato della repubblica", -10, "blacklist"),
        ("OCC", "disegni di legge", -10, "blacklist"),
        ("OCC", "rassegna stampa", -8, "blacklist"),
    ]

    cur.executemany(
        "INSERT INTO regole_classificazione (classe, marker, peso, tipo_marker) VALUES (?,?,?,?)",
        rules
    )
    con.commit()
    con.close()


def fetch_doc_by_sha1(sha1: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT id, nome_file, tipo_documento, rgnr, anno, procura, dda_flag,
               operazione_nome, nome_indagine, status, percorso_file, forza_polizia,
               text_preview, snippet_testo,
               motivo_revisione, retry_count, manual_required,
               class_score, class_trace, class_forced, note
        FROM documenti
        WHERE sha1=?
        LIMIT 1
    """, (sha1,))
    row = cur.fetchone()
    con.close()
    return row


def db_update_identity(doc_id: int, new_name: str, new_rel_path: str, file_size: int, mtime_str: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT nome_file, percorso_file FROM documenti WHERE id=? LIMIT 1", (doc_id,))
    row = cur.fetchone()
    old_name = row[0] if row else None
    old_path = row[1] if row else None

    updates = {
        "dimensione_file": file_size,
        "data_ultima_modifica": mtime_str,
    }
    if old_name != new_name:
        updates["nome_file_prev"] = old_name
        updates["nome_file"] = new_name
    if old_path != new_rel_path:
        updates["percorso_prev"] = old_path
        updates["percorso_file"] = new_rel_path

    set_clause = ", ".join([f"{k}=?" for k in updates.keys()])
    params = list(updates.values()) + [doc_id]
    cur.execute(f"UPDATE documenti SET {set_clause} WHERE id=?", params)
    con.commit()
    con.close()


def is_final_status(status: Optional[str]) -> bool:
    return (status or "").upper().strip() in FINAL_STATUSES


def is_path_in_input(percorso_file: Optional[str]) -> bool:
    if not percorso_file:
        return True
    p = percorso_file.replace("/", "\\").lower()
    return p.startswith("input_documenti\\") or p == "input_documenti"


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


def move_to_duplicati(file_path: Path) -> Path:
    DUP_DIR.mkdir(parents=True, exist_ok=True)
    target = unique_path(DUP_DIR / file_path.name)
    file_path.rename(target)
    return target


def move_occ_to_revisione(file_path: Path, sha1: str) -> Tuple[bool, str]:
    REV_OCC_DIR.mkdir(parents=True, exist_ok=True)
    target = unique_path(REV_OCC_DIR / file_path.name)
    try:
        file_path.rename(target)
    except Exception as e:
        return False, f"move_revisione_fail:{e}"

    new_rel = str(target.relative_to(PROJECT_ROOT)).replace("/", "\\")
    old_rel = str(Path("input_documenti") / file_path.name).replace("/", "\\")

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT id, nome_file, percorso_file FROM documenti WHERE sha1=? LIMIT 1", (sha1,))
    row = cur.fetchone()
    if not row:
        con.close()
        return False, "db_missing_for_revisione"

    doc_id, old_name, old_path = row

    cur.execute("""
        UPDATE documenti
        SET status='REVISIONE_OCC',
            tipo_documento='OCC',
            percorso_prev=?,
            percorso_file=?,
            nome_file_prev=?,
            nome_file=?,
            motivo_revisione=COALESCE(motivo_revisione, 'MISSING_FIELDS')
        WHERE id=?
    """, (old_path or old_rel, new_rel, old_name, target.name, doc_id))

    con.commit()
    con.close()
    return True, f"moved_to:{new_rel}"


def run_worker(script_rel: Path, args: List[str]) -> Tuple[bool, str]:
    script = (PROJECT_ROOT / script_rel).resolve()
    if not script.exists():
        return False, f"worker_missing:{script}"
    cmd = [sys.executable, str(script)] + args
    proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True)
    if proc.returncode != 0:
        return False, (proc.stderr or proc.stdout or "worker_failed").strip()
    return True, (proc.stdout or "").strip()


def iter_input_files() -> List[Path]:
    if not INPUT_DIR.exists():
        return []
    out = []
    for p in INPUT_DIR.iterdir():
        if not p.is_file():
            continue
        if p.parent.name.lower() == "duplicati":
            continue
        if p.suffix.lower() in ALLOWED_EXTS:
            out.append(p)
    return sorted(out)


def db_insert_new_file(f: Path, sha1: str, rel_path: str) -> None:
    meta, pages, preview = get_preview(f)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # default neutro, poi classifichiamo con regole
    tipo_doc = "ALTRO"
    status = "NEEDS_DEEPER_ANALYSIS"

    cur.execute("""
        INSERT INTO documenti
        (nome_file, sha1, dimensione_file, data_ultima_modifica, numero_pagine,
         tipo_documento, rgnr, anno, procura, status,
         evidence_occ, conf_occ, snippet_testo,
         tipo_file, text_preview, pdf_meta_json,
         nome_file_orig, percorso_file,
         categoria_secondaria)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        f.name, sha1, f.stat().st_size, format_mtime(f.stat().st_mtime), pages,
        tipo_doc, None, None, None, status,
        None, None,
        (preview or "")[:500],
        f.suffix.lower().lstrip("."),
        preview,
        json.dumps(meta, ensure_ascii=False),
        f.name,
        rel_path,
        None
    ))
    con.commit()
    con.close()


def db_update_classification(doc_id: int, tipo: str, status: str, score: int, trace: str, forced: Optional[str]):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        UPDATE documenti
        SET tipo_documento=?, status=?, class_score=?, class_trace=?, class_forced=?
        WHERE id=?
    """, (tipo, status, int(score), trace, forced, doc_id))
    con.commit()
    con.close()


def score_with_rules(preview_text: str, filename: str) -> Tuple[str, int, str, Optional[str]]:
    """
    Applica regole dal DB e decide una classe.
    Ritorna: (classe, score, trace, forced)
    """
    text = (preview_text or "")
    name = Path(filename).stem

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT classe, marker, peso, tipo_marker FROM regole_classificazione")
    rules = cur.fetchall()
    con.close()

    scores: Dict[str, int] = {}
    trace_parts: List[str] = []

    forced: Optional[str] = None

    def add(cl: str, val: int, why: str):
        scores[cl] = scores.get(cl, 0) + int(val)
        trace_parts.append(f"{cl}:{val}:{why}")

    for cl, marker, peso, tipo_marker in rules:
        marker_l = (marker or "").lower().strip()
        if not marker_l:
            continue

        hit = False
        if marker_l in text.lower():
            hit = True
        if marker_l in name.lower():
            hit = True

        if not hit:
            continue

        if (tipo_marker or "").lower() == "blacklist":
            add(cl, int(peso), f"BLACK:{marker_l}")
        else:
            add(cl, int(peso), f"hit:{marker_l}")

    # decisione: scegli max score
    if not scores:
        return "ALTRO", 0, "no_rules_hit", None

    best_cls = max(scores.items(), key=lambda kv: kv[1])[0]
    best_score = scores[best_cls]

    # soglia minima: se score < 3 -> ALTRO
    if best_score < 3:
        return "ALTRO", best_score, " | ".join(trace_parts), None

    return best_cls, best_score, " | ".join(trace_parts), forced


def main():
    keep_last_n_files(LOG_DIR, n=5)

    LIB_DIR.mkdir(parents=True, exist_ok=True)
    (LIB_DIR / "revisione" / "occ").mkdir(parents=True, exist_ok=True)
    (LIB_DIR / "report").mkdir(parents=True, exist_ok=True)

    ensure_db()
    seed_rules_if_empty()

    files = iter_input_files()

    print(f"[INFO] DB: {DB_PATH}")
    print(f"[INFO] Input: {INPUT_DIR}")
    print(f"[INFO] File trovati: {len(files)}")
    print(f"[INFO] PDF lib: {PDF_LIB or 'NONE'}\n")

    for f in files:
        ext = f.suffix.lower()
        ocr_state = "⏭"
        rn_state = "⏭"
        mv_state = "⏭"
        note_msg = ""

        sha1 = sha1_file(f)

        # ==========================================================
        # DUPLICATI (VERI) -> input_documenti\duplicati e STOP
        # ==========================================================
        existing = fetch_doc_by_sha1(sha1)
        if existing:
            (_id, _nome, _tipo, _rgnr, _anno, _proc, _dda, _op, _nomeind, _status, _percorso, _forza,
             _tp, _snip, _mr, _rc, _man, _cs, _ct, _cf, _note) = existing
            dup_vero = is_final_status(_status) or (not is_path_in_input(_percorso))
            if dup_vero:
                try:
                    moved = move_to_duplicati(f)
                    print(f"[DUP] {f.name} -> spostato in input_documenti\\duplicati\\{moved.name} (sha1 già presente)")
                except Exception as e:
                    print(f"[DUP] {f.name} -> sha1 già presente ma move duplicati FALLITO: {e}")
                continue

        rel_path = str(Path("input_documenti") / f.name)

        # INSERT/UPDATE identity
        if existing:
            db_update_identity(existing[0], f.name, rel_path, f.stat().st_size, format_mtime(f.stat().st_mtime))
        else:
            db_insert_new_file(f, sha1, rel_path)

        row = fetch_doc_by_sha1(sha1)
        if not row:
            print(f"{f.name} | INSERT ⚠️ | OCR ⏭ | RENAME ⏭ | MOVE ⏭ | NOTE db_missing_after_insert")
            continue

        (doc_id, db_nome_file, tipo_doc, rgnr, anno, procura, dda_flag,
         operazione_nome, nome_indagine, status, percorso_file, forza_polizia,
         text_preview, snippet_testo, motivo_rev, retry_count, manual_required,
         class_score, class_trace, class_forced, note_db) = row

        # ==========================================================
        # PRE-PROCESS PER ESTENSIONE (DOC/RTF/TXT SEMPRE)
        # ==========================================================
        if ext in (".doc", ".docx", ".rtf", ".txt"):
            ok_doc, msg_doc = run_worker(Path("script/workers/organizzatore_doc_reader.py"), ["--file", db_nome_file])
            ocr_state = mark(ok_doc)
            if not ok_doc and not note_msg:
                note_msg = msg_doc

        # ==========================================================
        # CLASSIFICAZIONE RIGIDA (tabella regole) su preview + nome file
        # ==========================================================
        # ricarica preview aggiornato dopo doc_reader
        row = fetch_doc_by_sha1(sha1)
        (doc_id, db_nome_file, tipo_doc, rgnr, anno, procura, dda_flag,
         operazione_nome, nome_indagine, status, percorso_file, forza_polizia,
         text_preview, snippet_testo, motivo_rev, retry_count, manual_required,
         class_score, class_trace, class_forced, note_db) = row

        decided_tipo, score, trace, forced = score_with_rules(text_preview or "", db_nome_file)

        # aggiorna solo se cambia
        tipo_doc_new = decided_tipo
        status_new = "CLASSIFIED" if decided_tipo == "OCC" else "NEEDS_DEEPER_ANALYSIS"

        if (tipo_doc_new != (tipo_doc or "")) or (status_new != (status or "")) or (trace != (class_trace or "")) or (int(score) != int(class_score or 0)):
            db_update_classification(doc_id, tipo_doc_new, status_new, int(score), trace, forced)

        # ricarica post-update
        row = fetch_doc_by_sha1(sha1)
        (doc_id, db_nome_file, tipo_doc, rgnr, anno, procura, dda_flag,
         operazione_nome, nome_indagine, status, percorso_file, forza_polizia,
         text_preview, snippet_testo, motivo_rev, retry_count, manual_required,
         class_score, class_trace, class_forced, note_db) = row

        tipo_up = (tipo_doc or "").upper()

        # ==========================
        # NON-OCC:
        # - se nel nome c'è OCC/OCCC -> label fix rename (v16) e poi prosegue
        # - se anno noto -> percorso (libreria\YYYY)
        # - se anno assente -> vari_verbali
        # ==========================
        if tipo_up != "OCC":
            _ok_probe, _msg_probe = run_worker(Path("script/workers/organizzatore_altro_probe.py"), ["--file", db_nome_file])

            # Se NON-OCC ma nel nome c'è "OCC/OCCC", correggiamo l'etichetta col rename (v16)
            # (es: "OCC FEHIDA.doc" -> "INFORMATIVA FEHIDA.doc")
            if filename_has_occ_token(db_nome_file):
                _ok_rn_fix, _msg_rn_fix = run_worker(Path("script/workers/organizzatore_rename.py"), ["--file", db_nome_file])
                rn_state = mark(_ok_rn_fix)
                if (not _ok_rn_fix) and (not note_msg):
                    note_msg = _msg_rn_fix
                row_fix = fetch_doc_by_sha1(sha1)
                if row_fix:
                    db_nome_file = row_fix[1]
                    tipo_doc = row_fix[2]
                    anno = row_fix[4]
                    status = row_fix[9]
                    percorso_file = row_fix[10]

            if anno:
                ok_mv, msg_mv = run_worker(Path("script/workers/organizzatore_percorso.py"), ["--file", db_nome_file])
                mv_state = mark(ok_mv)
                if not ok_mv and not note_msg:
                    note_msg = msg_mv

                row_v = fetch_doc_by_sha1(sha1)
                extra = f"{fmt_tipo(row_v[2])} anno={row_v[4]} status={row_v[9]} path={row_v[10]}"
                print(f"{f.name} | OCR {ocr_state} | RENAME {rn_state} | MOVE {mv_state} | {extra}" + (f" | NOTE {note_msg}" if note_msg else ""))
                continue

            ok_vv, msg_vv = run_worker(Path("script/workers/organizzatore_vari_verbali.py"), ["--file", db_nome_file])
            mv_state = mark(ok_vv)
            if not ok_vv and not note_msg:
                note_msg = msg_vv

            row_v = fetch_doc_by_sha1(sha1)
            extra = f"{fmt_tipo(row_v[2])} score={row_v[17] or 0} forced={row_v[19]} status={row_v[9]} path={row_v[10]}"
            print(f"{f.name} | OCR {ocr_state} | RENAME {rn_state} | MOVE {mv_state} | {extra}" + (f" | NOTE {note_msg}" if note_msg else ""))
            continue

        # ==========================
        # OCC: estrazione rapida specifica (PDF)
        # ==========================
        if ext == ".pdf":
            ok, msg = run_worker(Path("script/workers/organizzatore_occ_ocr_rgnr.py"), ["--file", db_nome_file])
            if ocr_state == "⏭":
                ocr_state = mark(ok)
            if not ok and not note_msg:
                note_msg = msg

        # ricarica per gating OCC
        row2 = fetch_doc_by_sha1(sha1)
        has_rgnr = bool(row2[3] and RE_RGNR_PARSE.search(str(row2[3])))
        has_anno = bool(row2[4])
        has_proc = bool((row2[5] or "").strip())

        # Completo -> rename + move anno
        if has_rgnr and has_anno and has_proc and (row2[9] or "").upper() not in ("NEEDS_READER", "NEEDS_DEEPER_ANALYSIS", "STANDBY"):
            ok_rn, msg_rn = run_worker(Path("script/workers/organizzatore_rename.py"), ["--file", row2[1]])
            rn_state = mark(ok_rn)
            if not ok_rn and not note_msg:
                note_msg = msg_rn

            row3 = fetch_doc_by_sha1(sha1)
            ok_mv, msg_mv = run_worker(Path("script/workers/organizzatore_percorso.py"), ["--file", row3[1]])
            mv_state = mark(ok_mv)
            if not ok_mv and not note_msg:
                note_msg = msg_mv

            final = fetch_doc_by_sha1(sha1)
            extra = f"{fmt_tipo(final[2])} rgnr={final[3]} anno={final[4]} dda={final[6]} procura={final[5]} status={final[9]} path={final[10]}"
            print(f"{f.name} | OCR {ocr_state} | RENAME {rn_state} | MOVE {mv_state} | {extra}" + (f" | NOTE {note_msg}" if note_msg else ""))
            continue

        # Incompleto -> revisione OCC
        ok_rev, msg_rev = move_occ_to_revisione(f, sha1)
        mv_state = mark(ok_rev)
        if not ok_rev and not note_msg:
            note_msg = msg_rev

        rowf = fetch_doc_by_sha1(sha1)
        extra = f"{fmt_tipo(rowf[2])} rgnr={rowf[3]} anno={rowf[4]} dda={rowf[6]} procura={rowf[5]} status={rowf[9]} path={rowf[10]}"
        print(f"{f.name} | OCR {ocr_state} | RENAME {rn_state} | MOVE {mv_state} | {extra}" + (f" | NOTE {note_msg}" if note_msg else ""))

    # report
    ok_rep, msg_rep = run_worker(Path("script/workers/organizzatore_report_errori.py"), [])
    if ok_rep and msg_rep:
        print("\n[REPORT] " + msg_rep)
    elif not ok_rep:
        print("\n[REPORT] ⚠️ " + msg_rep)

    # ROUTER REVISIONE
    ok_router, msg_router = run_worker(Path("script/workers/organizzatore_revisione_router.py"), [])
    if ok_router and msg_router:
        print("\n[REVISIONE] " + msg_router)
    elif not ok_router:
        print("\n[REVISIONE] ⚠️ " + msg_router)

    print("\n[NOTE] Pipeline completata.\n")


if __name__ == "__main__":
    main()
