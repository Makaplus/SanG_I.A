#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - WORKER: ORGANIZZATORE_RENAME (v17)

v17 FIX CRITICO:
- Ripristinata estrazione corretta del nome indagine/operazione dal filename:
  - Caso: "OCC <OPERAZIONE> RGNR ..." => prende <OPERAZIONE>
  - Fallback: prende il testo PRIMA del primo marker (RGNR/PROCURA/DDA/...)
- Mantiene fix RANGO-ZINGARI: forza (REP.OP. CS) NON deve finire dentro operazione_nome
- NON-OCC: label-fix (se nel nome c'è OCC/OCCC, sostituisce solo con tipo reale)

Resta invariato (OCC):
- Nome canonico: "OCC <OPERAZIONE> RGNR <NUM_ANNO> (DDA) <PROCURA> - <FORZA>"
- DB-aware path
"""

import argparse
import hashlib
import re
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"
INPUT_DIR = PROJECT_ROOT / "input_documenti"
REV_OCC_DIR = PROJECT_ROOT / "libreria" / "revisione" / "occ"


# ----------------------------
# UTIL
# ----------------------------
def sha1_file(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def sanitize_component_keep_spaces(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r'[<>:"/\\|?*\x00-\x1F]', " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


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


def parse_rgnr(rgnr: str) -> Optional[Tuple[str, int]]:
    if not rgnr:
        return None
    m = re.search(r"(\d{1,7})\s*/\s*(\d{4})", rgnr)
    if not m:
        return None
    return m.group(1), int(m.group(2))


# ----------------------------
# REGEX / NORMALIZZAZIONI
# ----------------------------
RE_ONLY_DIGITS = re.compile(r"^\d{1,10}$")
RE_RGNRTOKEN = re.compile(r"^\d{1,7}\s*[!/_\-]\s*\d{2,4}$", re.IGNORECASE)

RE_DATE_PREFIX = re.compile(r"^\s*\d{2}\.\d{2}\.\d{4}\s+", re.IGNORECASE)
RE_OCC_TOKEN = re.compile(r"(?i)(?:^|[ \._\-])OCC{1,3}(?:$|[ \._\-])")
RE_OCC_PREFIX = re.compile(r"(?i)^\s*OCC{1,3}\s+")
RE_MARKERS = re.compile(
    r"(?i)\bRGNR\b|\bR\.?\s*G\.?\s*N\.?\s*R\.?\b|\bRGIP\b|\bR\.?\s*G\.?\s*I\.?\s*P\.?\b|\bROCC\b|\bR\.?\s*O\.?\s*C\.?\s*C\.?\b|\bPROCURA\b|\bTRIBUNALE\b|\bDDA\b"
)
RE_NUMYEAR_ANY = re.compile(r"(?i)\b\d{1,7}\s*[!/_\-]\s*\d{2,4}\b")

RE_PLUS = re.compile(r"\s*\+\s*")
RE_PARENS = re.compile(r"[()\[\]{}]")

# se in coda all'operazione rimane roba tipo "REP OP CS", tagliala
RE_TRAILING_FORZA_GENERIC = re.compile(r"(?i)\bREP\s*\.?\s*OP\s*\.?\s*[A-Z]{1,3}\b\s*$")


def replace_occ_token_in_stem(stem: str, new_label: str) -> str:
    lab = sanitize_component_keep_spaces(new_label).upper()
    s = stem

    def _repl(m):
        pre = m.group(1) or ""
        post = m.group(2) or ""
        return f"{pre}{lab}{post}"

    s2 = re.sub(r"(?i)(^|[ \._\-])OCC{1,3}([ \._\-]|$)", _repl, s)
    s2 = re.sub(r"\s+", " ", s2).strip(" _.-")
    return s2


def _norm_forza(s: str) -> str:
    s = sanitize_component_keep_spaces(s).upper()
    s = s.replace("REPARTO OPERATIVO", "REP.OP.")
    s = re.sub(r"\bREP\s*\.?\s*OP\s*\.?\b", "REP.OP.", s)
    s = re.sub(r"\.{2,}", ".", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_dotted_acronyms(s: str) -> str:
    t = " " + sanitize_component_keep_spaces(s).upper() + " "
    repl = {
        r"\bR\s*\.\s*O\s*\.\s*S\s*\.?\b": " ROS ",
        r"\bG\s*\.\s*I\s*\.\s*C\s*\.\s*O\s*\.?\b": " GICO ",
        r"\bR\s*\.\s*O\s*\.\s*N\s*\.\s*I\s*\.?\b": " RONI ",
        r"\bG\s*\.\s*D\s*\.\s*F\s*\.?\b": " GDF ",
        r"\bD\s*\.\s*I\s*\.\s*A\s*\.?\b": " DIA ",
        r"\bS\s*\.\s*C\s*\.\s*O\s*\.?\b": " SCO ",
        r"\bD\s*\.\s*I\s*\.\s*G\s*\.\s*O\s*\.\s*S\s*\.?\b": " DIGOS ",
        r"\bC\s*\.\s*C\s*\.?\b": " CC ",
        r"\bP\s*\.\s*S\s*\.?\b": " PS ",
    }
    for pat, rep in repl.items():
        t = re.sub(pat, rep, t, flags=re.IGNORECASE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_forza_from_anywhere(stem: str) -> Optional[str]:
    s = normalize_dotted_acronyms(stem)
    s = s.replace("_", " ")
    s = re.sub(r"[\-]+", " ", s)
    s = " " + s.upper() + " "

    m = re.search(r"\bREP\.?\s*OP\.?\s*([A-Z]{1,3})\b", s)
    if m:
        return _norm_forza(f"REP.OP. {m.group(1)}")

    m = re.search(r"\bROS\s+([A-Z]{1,3})\b", s)
    if m:
        return _norm_forza(f"ROS {m.group(1)}")

    m = re.search(r"\bGICO\s+([A-Z]{1,3})\b", s)
    if m:
        return _norm_forza(f"GICO {m.group(1)}")

    m = re.search(r"\bGDF\s+([A-Z]{1,3})\b", s)
    if m:
        return _norm_forza(f"GDF {m.group(1)}")

    if re.search(r"\bRONI\b", s):
        return "RONI"

    m = re.search(r"\b(DIA|SCO|DIGOS|CC|PS|POLIZIA)\b(?:\s+([A-Z]{1,3}))?", s)
    if m:
        base = m.group(1)
        extra = (m.group(2) or "").strip()
        return _norm_forza(f"{base} {extra}".strip())

    return None


def _dedup_words_upper(s: str) -> str:
    toks = [t for t in re.split(r"\s+", s.strip()) if t]
    out: List[str] = []
    seen = set()
    prev = None
    for t in toks:
        if prev == t:
            continue
        if len(t) >= 4 and t in seen:
            continue
        out.append(t)
        if len(t) >= 4:
            seen.add(t)
        prev = t
    return " ".join(out).strip()


def _canon_for_match(s: str) -> str:
    t = (s or "").upper()
    t = re.sub(r"[^A-Z0-9]+", "", t)
    return t


def _remove_forza_robusta(text: str, forza: Optional[str]) -> str:
    if not text:
        return text
    t = text

    if forza:
        t = re.sub(rf"\b{re.escape(forza)}\b", " ", t, flags=re.IGNORECASE)

        f_can = _canon_for_match(forza)
        if f_can:
            chars = list(f_can)
            pat = r"\b" + r"\W*".join(map(re.escape, chars)) + r"\b"
            t = re.sub(pat, " ", t, flags=re.IGNORECASE)

    t = re.sub(RE_TRAILING_FORZA_GENERIC, " ", t)
    t = re.sub(r"\s+", " ", t).strip(" -_.,")
    return t


def clean_operazione(op: Optional[str], procura: str, forza: Optional[str]) -> Optional[str]:
    if not op:
        return None

    s = normalize_dotted_acronyms(op)
    s = sanitize_component_keep_spaces(s).upper()
    s = s.replace("_", " ")
    s = RE_PLUS.sub(" ", s)
    s = RE_PARENS.sub(" ", s)
    s = re.sub(r"\s*-\s*", "-", s)
    s = re.sub(r"\s+", " ", s).strip()

    if s.startswith("OCC-"):
        s = s[4:].strip()

    s = _remove_forza_robusta(s, forza)

    pr = sanitize_component_keep_spaces(procura).upper()
    if pr:
        s = re.sub(rf"\bDDA\s+{re.escape(pr)}\b", " ", s, flags=re.IGNORECASE)
        s = re.sub(rf"\b{re.escape(pr)}\b", " ", s, flags=re.IGNORECASE)

    s = re.sub(r"\b(PROCURA|TRIBUNALE)\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bDDA\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bRGNR\b", " ", s, flags=re.IGNORECASE)

    s = s.replace("--", "-")
    s = re.sub(r"-{2,}", "-", s)
    s = s.strip(" -")

    s = sanitize_component_keep_spaces(s).upper()
    s = _dedup_words_upper(s)

    s = s.strip(" -")
    s = re.sub(r"-{2,}", "-", s).strip(" -")

    if not s or RE_ONLY_DIGITS.match(s) or RE_RGNRTOKEN.match(s):
        return None

    return s


# ✅ v17: estrazione corretta operazione
def extract_operazione_from_filename(stem: str, forza: Optional[str]) -> Optional[str]:
    """
    1) Se trova "OCC <...> RGNR" prende <...> (è il caso CAMALEONTE / SAGGEZZA / RANGO-ZINGARI)
    2) Fallback: prende il testo prima del primo marker (RGNR/DDA/PROCURA/TRIBUNALE...)
    3) Pulisce date, numeri, forza, ecc.
    """
    s0 = stem.strip()
    s0 = normalize_dotted_acronyms(s0)
    s0 = RE_DATE_PREFIX.sub("", s0)
    s0 = re.sub(r"\s+", " ", s0).strip()

    up = " " + s0.upper().replace('_', ' ') + " "

    # 1) OCC <OPERAZIONE> RGNR
    m = re.search(r"(?i)\bOCC{1,3}\b\s+(.{2,80}?)\s+\bR\.?\s*G\.?\s*N\.?\s*R\b", up)
    if m:
        cand = m.group(1).strip(" -_.,")
        cand = re.sub(r"\s+", " ", cand).strip()
        cand = _remove_forza_robusta(cand, forza)
        cand = RE_NUMYEAR_ANY.sub(" ", cand)
        cand = sanitize_component_keep_spaces(cand).strip(" -")
        if cand and (not RE_ONLY_DIGITS.match(cand)) and (not RE_RGNRTOKEN.match(cand)):
            return cand.upper()

    # 2) fallback: prima del primo marker
    s = s0
    m2 = RE_MARKERS.search(s)
    if m2:
        before = s[:m2.start()].strip(" -_.,")
        before = re.sub(r"(?i)^\s*OCC{1,3}\s+", "", before).strip()
        before = _remove_forza_robusta(before, forza)
        before = RE_NUMYEAR_ANY.sub(" ", before)
        before = sanitize_component_keep_spaces(before).strip(" -")
        if before and (not RE_ONLY_DIGITS.match(before)) and (not RE_RGNRTOKEN.match(before)):
            return before.upper()

    # 3) ultimo fallback: togli prefissi OCC e pulisci
    cleaned = re.sub(r"(?i)^\s*OCC{1,3}\s+", "", s0).strip()
    cleaned = _remove_forza_robusta(cleaned, forza)
    cleaned = RE_NUMYEAR_ANY.sub(" ", cleaned)
    cleaned = sanitize_component_keep_spaces(cleaned).strip(" -")
    return cleaned.upper() if cleaned else None


# ----------------------------
# DB
# ----------------------------
def ensure_cols(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("PRAGMA table_info(documenti)")
    cols = {row[1] for row in cur.fetchall()}

    def add_col(name: str, coltype: str):
        if name not in cols:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {name} {coltype}")

    add_col("dda_flag", "INTEGER")
    add_col("operazione_nome", "TEXT")
    add_col("rename_evidence", "TEXT")
    add_col("nome_file_prev", "TEXT")
    add_col("nome_file_orig", "TEXT")
    add_col("percorso_file", "TEXT")
    add_col("percorso_prev", "TEXT")
    add_col("forza_polizia", "TEXT")
    add_col("procura", "TEXT")
    add_col("status", "TEXT")
    add_col("rgnr", "TEXT")
    add_col("anno", "INTEGER")
    add_col("tipo_documento", "TEXT")
    add_col("note", "TEXT")
    con.commit()


def db_get_by_sha1(sha1: str):
    con = sqlite3.connect(DB_PATH)
    ensure_cols(con)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, nome_file, nome_file_orig, tipo_documento, rgnr, anno, procura, dda_flag,
               operazione_nome, percorso_file, status, forza_polizia, note
        FROM documenti
        WHERE sha1=?
        LIMIT 1
        """,
        (sha1,),
    )
    row = cur.fetchone()
    con.close()
    return row


def db_set_standby(doc_id: int, reason: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE documenti SET status='STANDBY', rename_evidence=? WHERE id=?", (reason, doc_id))
    con.commit()
    con.close()


def db_update_rename(
    doc_id: int,
    old_name: str,
    new_name: str,
    old_rel: str,
    new_rel: str,
    evidence: str,
    forza_new: Optional[str],
    operazione_new: Optional[str],
):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("SELECT forza_polizia, operazione_nome FROM documenti WHERE id=? LIMIT 1", (doc_id,))
    row = cur.fetchone()
    forza_old = (row[0] or "").strip() if row else ""
    op_old = (row[1] or "").strip() if row else ""

    forza_final = forza_old
    if forza_new and forza_new.strip():
        forza_final = forza_new.strip()

    op_final = op_old
    if operazione_new and operazione_new.strip():
        op_final = operazione_new.strip()

    cur.execute(
        """
        UPDATE documenti
        SET nome_file_prev=?, nome_file=?, rename_evidence=?,
            percorso_prev=?, percorso_file=?,
            forza_polizia=?,
            operazione_nome=?
        WHERE id=?
        """,
        (old_name, new_name, evidence, old_rel, new_rel, (forza_final or None), (op_final or None), doc_id),
    )
    con.commit()
    con.close()


# ----------------------------
# PATH resolving (DB-aware)
# ----------------------------
def resolve_file_db_aware(arg: str, percorso_file_db: Optional[str]) -> Optional[Path]:
    p = Path(arg)
    if p.is_absolute() and p.exists() and p.is_file():
        return p

    candidates: List[Path] = []

    if percorso_file_db:
        try:
            candidates.append((PROJECT_ROOT / Path(percorso_file_db)).resolve())
        except Exception:
            pass

    name = p.name
    candidates.append((REV_OCC_DIR / name).resolve())
    candidates.append((INPUT_DIR / name).resolve())
    candidates.append((PROJECT_ROOT / name).resolve())

    for c in candidates:
        if c.exists() and c.is_file():
            return c
    return None


# ----------------------------
# NAME builder (OCC)
# ----------------------------
def build_occ_name(ext: str, operazione: Optional[str], num: str, year: int, dda_flag: int, procura: str, forza: Optional[str]) -> str:
    parts: List[str] = ["OCC"]
    if operazione:
        parts.append(sanitize_component_keep_spaces(operazione).upper())

    parts.append("RGNR")
    parts.append(f"{num}_{year}")

    if int(dda_flag) == 1:
        parts.append("DDA")

    parts.append(sanitize_component_keep_spaces(procura))

    base = sanitize_component_keep_spaces(" ".join([p for p in parts if p and p.strip()]))

    if forza:
        base = f"{base} - {sanitize_component_keep_spaces(forza).upper()}"

    if len(base) > 185:
        base = base[:185].rstrip()

    return f"{base}{ext}"


# ----------------------------
# MAIN
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, help="Nome file (come in DB) o path")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print("[ERR] DB non trovato.")
        raise SystemExit(2)

    raw_p = Path(args.file)
    file_path_guess = None
    if raw_p.is_absolute() and raw_p.exists():
        file_path_guess = raw_p
    else:
        p_in = (INPUT_DIR / raw_p.name).resolve()
        if p_in.exists():
            file_path_guess = p_in
        else:
            p_rev = (REV_OCC_DIR / raw_p.name).resolve()
            if p_rev.exists():
                file_path_guess = p_rev

    if not file_path_guess:
        con = sqlite3.connect(DB_PATH)
        ensure_cols(con)
        cur = con.cursor()
        cur.execute(
            """
            SELECT id, nome_file, nome_file_orig, tipo_documento, rgnr, anno, procura, dda_flag,
                   operazione_nome, percorso_file, status, forza_polizia, note
            FROM documenti
            WHERE nome_file=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (raw_p.name,),
        )
        row_name = cur.fetchone()
        con.close()
        if not row_name:
            print(f"[ERR] File non trovato: {args.file}")
            raise SystemExit(2)

        (doc_id, nome_file_db, nome_file_orig, tipo, rgnr, anno, procura, dda_flag,
         operazione_db, percorso_file, status, forza_db, note_db) = row_name

        file_path = resolve_file_db_aware(nome_file_db, percorso_file)
        if not file_path:
            print(f"[ERR] File non trovato su disco (DB-aware): {nome_file_db}")
            raise SystemExit(2)

        sha1 = sha1_file(file_path)
        row = db_get_by_sha1(sha1)
        if not row:
            print("[ERR] Record DB non trovato (sha1)")
            raise SystemExit(2)
    else:
        file_path = file_path_guess
        sha1 = sha1_file(file_path)
        row = db_get_by_sha1(sha1)
        if not row:
            print("[ERR] Record DB non trovato (sha1)")
            raise SystemExit(2)

    (doc_id, nome_file_db, nome_file_orig, tipo, rgnr, anno, procura, dda_flag,
     operazione_db, percorso_file, status, forza_db, note_db) = row

    file_path = resolve_file_db_aware(str(file_path), percorso_file) or file_path
    tipo_up = (tipo or "").upper()

    # NON-OCC: label-fix se nel nome c'è OCC/OCCC
    if tipo_up != "OCC":
        if not RE_OCC_TOKEN.search(file_path.stem):
            print("[SKIP] Non OCC (no token OCC/OCCC nel nome)")
            raise SystemExit(0)

        old_name = file_path.name
        old_rel = percorso_file or str(Path("input_documenti") / old_name)

        new_stem = replace_occ_token_in_stem(file_path.stem, tipo_up)
        new_name = f"{new_stem}{file_path.suffix.lower()}"

        if new_name == old_name:
            print("[SKIP] Non OCC (label già corretta)")
            raise SystemExit(0)

        target = unique_path(file_path.with_name(new_name))
        try:
            file_path.rename(target)
        except Exception as e:
            db_set_standby(doc_id, f"rename_error_LABELFIX: {e}")
            print(f"[ERR] Rename FS (LABELFIX): {e}")
            raise SystemExit(4)

        try:
            rel = target.relative_to(PROJECT_ROOT)
            new_rel = str(rel).replace("/", "\\")
        except Exception:
            new_rel = old_rel

        evidence = f"rename_LABELFIX_v17 tipo={tipo_up} old='{old_name}'"
        db_update_rename(doc_id, old_name, target.name, old_rel, new_rel, evidence, None, None)

        print(f"[OK] RENAME LABELFIX: {old_name} -> {target.name}")
        raise SystemExit(0)

    # OCC canonico completo
    parsed = parse_rgnr(rgnr or "")
    missing = []
    if not parsed:
        missing.append("RGNR_PARSE")
    if not anno:
        missing.append("ANNO")
    if dda_flag is None:
        missing.append("DDA_FLAG")
    if not (procura or "").strip():
        missing.append("PROCURA")

    if missing:
        db_set_standby(doc_id, f"rename_standby_OCC: missing {', '.join(missing)}")
        print(f"[STANDBY] missing {', '.join(missing)}")
        raise SystemExit(3)

    num, _ = parsed
    year = int(anno)
    dda = int(dda_flag)
    pr = (procura or "").strip()

    stem_cur = file_path.stem
    stem_orig = Path(nome_file_orig).stem if nome_file_orig else stem_cur

    forza = (forza_db or "").strip()
    if not forza:
        forza = extract_forza_from_anywhere(stem_orig) or extract_forza_from_anywhere(stem_cur)
    if forza:
        forza = _norm_forza(forza)

    op = (operazione_db or "").strip()
    if op:
        op = sanitize_component_keep_spaces(op).upper()

    if (not op) or RE_ONLY_DIGITS.match(op) or op == num or RE_RGNRTOKEN.match(op):
        op = extract_operazione_from_filename(stem_orig, forza) or extract_operazione_from_filename(stem_cur, forza)

    op = clean_operazione(op, pr, forza)

    old_name = file_path.name
    old_rel = percorso_file or str(Path("input_documenti") / old_name)

    new_name = build_occ_name(file_path.suffix.lower(), op, num, year, dda, pr, forza)
    if new_name == old_name:
        print("[SKIP] Nome già conforme")
        raise SystemExit(0)

    target = unique_path(file_path.with_name(new_name))
    try:
        file_path.rename(target)
    except Exception as e:
        db_set_standby(doc_id, f"rename_error_OCC: {e}")
        print(f"[ERR] Rename FS: {e}")
        raise SystemExit(4)

    try:
        rel = target.relative_to(PROJECT_ROOT)
        new_rel = str(rel).replace("/", "\\")
    except Exception:
        new_rel = old_rel

    evidence = f"rename_OCC_v17 rgnr={rgnr} anno={year} dda={dda} procura={pr} forza={forza or ''} op={op or ''}"
    db_update_rename(doc_id, old_name, target.name, old_rel, new_rel, evidence, forza, op)

    print(f"[OK] RENAME: {old_name} -> {target.name}")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
