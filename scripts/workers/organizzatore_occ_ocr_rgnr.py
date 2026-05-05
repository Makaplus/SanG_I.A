#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SANGIA - WORKER: ORGANIZZATORE_OCC_OCR_RGNR (v2.1) — DB-aware + logica v1 (Fix v7) + RG NOTIZIE REATO

Integra:
✅ Tutta la logica “v1 Fix v7” (procura pulita da rumore/timbri, correzioni OCR, normalizzazione)
✅ Supporto DB-aware: trova sempre il file usando documenti.percorso_file (se presente)
✅ Supporto argomenti:
   - --doc-id <ID>   (consigliato: lavora sempre anche in revisione/occ)
   - --file <NOME>   (fallback; prova revisione/occ + input_documenti + path DB se trovato)
✅ Mantiene logica status:
   - se manca RGNR => NEEDS_DEEPER_ANALYSIS (ok per revisione)
   - se completo (rgnr+anno+procura) => READY

✅ NEW:
- Estrazione numero procedimento anche da:
  - “N. 1389/08 R.G. notizie di reato - DDA”
  - “procedimento penale n. 1389/08 ... (R.G. notizie di reato ...)”
"""

import argparse
import hashlib
import re
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, List


# ----------------------------
# PATHS
# ----------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"
INPUT_DIR = PROJECT_ROOT / "input_documenti"
REV_OCC_DIR = PROJECT_ROOT / "libreria" / "revisione" / "occ"

# ----------------------------
# REGEX / FIX
# ----------------------------
RE_DDA = re.compile(r"(?i)\bD\s*\.?\s*D\s*\.?\s*A\s*\.?\b|direzione\s+distrettuale\s+antimafia")

RE_TRIBUNALE_CITY = re.compile(r"(?i)\bTRIBUNALE(?:\s+ORDINARIO)?\s+DI\s+([A-ZÀ-ÖØ-Ý'\s\-]{3,80})")
RE_PROCURA_PRESSO_TRIB = re.compile(
    r"(?i)\bPROCURA\s+DELLA\s+REPUBBLICA\b.*?\bPRESSO\s+IL\s+TRIBUNALE\s+DI\s+([A-ZÀ-ÖØ-Ý'\s\-]{3,80})"
)
RE_STOPWORDS = re.compile(r"(?i)\b(SEZIONE|UFFICIO|GIUDICE|PROCURA|DIREZIONE|DISTRETTUALE|ORDINARIO)\b")

RE_OCC_HINT = re.compile(
    r"(?i)ordinanza|misura\s+cautelare|misure|custodia\s+cautelare|sequestro\s+preventivo|applicazione\s+di\s+misura"
)

RE_LABEL_RGNR = re.compile(r"(?i)\bR\s*G\s*N\s*R\b")
RE_LABEL_DOTTED_RGNR = re.compile(r"(?i)\bR\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R\s*\.?\b")

RE_N_BEFORE_RGNR = re.compile(
    r"(?i)\bN\s*\.?\s*(\d{1,7})\s*[\/_\-]\s*(\d{2,4})\s*"
    r"(?:R\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R\s*\.?)"
)

RE_RGNR_BEFORE_NUM = re.compile(
    r"(?i)(?:R\s*\.?\s*G\s*\.?\s*N\s*\.?\s*R\s*\.?)"
    r".{0,40}?"
    r"(\d{1,7})\s*[\/_\-]\s*(\d{2,4})"
)

RE_FALLBACK_N = re.compile(r"(?i)\b(?:MISURE?\s*)?N\s*\.?\s*(\d{1,7})\s*[\/_\-]\s*(\d{2,4})\b")

# ✅ NEW: RG notizie di reato
RE_RG_NOTIZIE_REATO = re.compile(
    r"(?i)\bN\s*\.?\s*(\d{1,7})\s*[\/_\-]\s*(\d{2,4})\s*"
    r"R\s*\.?\s*G\s*\.?\s*(?:NOTIZIE\s+DI\s+REATO|NOTIZIE\s+REATO)\b"
)

# ✅ NEW: procedimento penale n.
RE_PROC_PENALE_RG = re.compile(
    r"(?i)\bprocedimento\s+penale\s*n\s*\.?\s*(\d{1,7})\s*[\/_\-]\s*(\d{2,4})"
    r"(?:.{0,60}?R\s*\.?\s*G\s*\.?\s*(?:NOTIZIE\s+DI\s+REATO|NOTIZIE\s+REATO))?"
)

PROCURA_FIX = {
    "ÌVFILANO": "Milano",
    "IVFILANO": "Milano",
    "MILANO": "Milano",
    "L'AQUTLA": "L'Aquila",
    "LAQUTLA": "L'Aquila",
    "L AQUTLA": "L'Aquila",
    "L AQUILA": "L'Aquila",
    "L'AQULA": "L'Aquila",
    "L AQULA": "L'Aquila",
    "LAQULA": "L'Aquila",
}

# parole da timbro/rumore: tronca la procura prima di questi marker
RE_PROCURA_NOISE = re.compile(
    r"(?i)\b(PERVENUTO|PERVENUTA|RICEVUTO|RICEVUTA|PROTOCOLLO|PROT\.?|ARRIVATO|ARRIVATA|DEPOSITATO|DEPOSITATA|ENTRATA|ENTRATO)\b"
)


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


def normalize_year(y: int) -> int:
    if y < 100:
        return 2000 + y if y <= 50 else 1900 + y
    return y


def clean_place(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return " ".join([w[:1].upper() + w[1:].lower() if w else "" for w in s.split(" ")]).strip()


def clean_procura_noise(s: str) -> str:
    """
    Rimuove rumore OCR/timbri che si attaccano alla città (es: "L'aqula Pervenuio").
    """
    if not s:
        return s
    t = re.sub(r"\s+", " ", s).strip()

    m = RE_PROCURA_NOISE.search(t)
    if m:
        t = t[:m.start()].strip()

    # se restano due parole e la seconda sembra rumore, meglio troncare
    parts = t.split()
    if len(parts) >= 2:
        last = parts[-1]
        if len(last) >= 7 and not any(ch.isdigit() for ch in last) and last.lower() not in ("calabria", "abruzzo", "sicilia"):
            t = " ".join(parts[:-1]).strip()

    return t


def apply_procura_fix(s: str) -> str:
    raw = re.sub(r"\s+", " ", (s or "").strip()).upper()
    raw = raw.replace("’", "'").replace("`", "'").replace("´", "'").strip()

    if raw in PROCURA_FIX:
        return PROCURA_FIX[raw]

    if "MILANO" in raw and len(raw) <= 16:
        return "Milano"
    if "AQUILA" in raw:
        return "L'Aquila"

    return clean_place(s)


def extract_procura_city(text: str) -> Optional[str]:
    t = text or ""

    m = RE_PROCURA_PRESSO_TRIB.search(t)
    if m:
        chunk = RE_STOPWORDS.split(m.group(1))[0]
        chunk = re.sub(r"\s+", " ", chunk).strip()
        chunk = clean_procura_noise(chunk)
        return apply_procura_fix(chunk) if chunk else None

    m = RE_TRIBUNALE_CITY.search(t)
    if not m:
        return None
    chunk = RE_STOPWORDS.split(m.group(1))[0]
    chunk = re.sub(r"\s+", " ", chunk).strip()
    chunk = clean_procura_noise(chunk)
    return apply_procura_fix(chunk) if chunk else None


def normalize_text_for_rgnr(s: str) -> str:
    t = (s or "")
    t = t.replace("\u00a0", " ")
    t = t.replace("’", "'").replace("`", "'").replace("´", "'")
    t = t.replace("!", "/")
    t = t.replace("|", "1")
    t = t.upper()

    # OCR: I/L confusi con 1
    t = re.sub(r"(?<=\d)[IL](?=\d)", "1", t)
    t = re.sub(r"(?<=[\/_\-])[\s]*[IL](?=\d)", "1", t)
    t = re.sub(r"(?<=\d)[IL](?=[\/_\-])", "1", t)

    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_rgnr(text_raw: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Estrae rgnr/anno anche da varianti:
    - N. 1389/08 R.G. notizie di reato - DDA
    - procedimento penale n. 1389/08 ...
    - N. ... RGNR
    - RGNR ... num/anno
    - fallback N ... se hint OCC
    """
    t = normalize_text_for_rgnr(text_raw)

    # ✅ 1) RG NOTIZIE DI REATO (come nel tuo caso)
    m = RE_RG_NOTIZIE_REATO.search(t)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return f"{num}/{year}", year

    # ✅ 2) procedimento penale n.
    m = RE_PROC_PENALE_RG.search(t)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return f"{num}/{year}", year

    # 3) N. xxxx/yy RGNR
    m = RE_N_BEFORE_RGNR.search(t)
    if m:
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return f"{num}/{year}", year

    # 4) RGNR label prima e poi numero
    if RE_LABEL_DOTTED_RGNR.search(t) or RE_LABEL_RGNR.search(t):
        m = RE_RGNR_BEFORE_NUM.search(t)
        if m:
            num = m.group(1)
            year = normalize_year(int(m.group(2)))
            return f"{num}/{year}", year

    # 5) fallback: N xxxx/yy con hint OCC
    m = RE_FALLBACK_N.search(t)
    if m and RE_OCC_HINT.search(t):
        num = m.group(1)
        year = normalize_year(int(m.group(2)))
        return f"{num}/{year}", year

    return None, None


def ocr_lite_first_page(pdf_path: Path) -> str:
    try:
        import fitz  # PyMuPDF
        import numpy as np
        import easyocr
    except Exception:
        return ""

    try:
        doc = fitz.open(str(pdf_path))
        if doc.page_count < 1:
            return ""
        page = doc.load_page(0)
        pix = page.get_pixmap(dpi=240)
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
        if pix.n == 4:
            img = img[:, :, :3]
        reader = easyocr.Reader(["it", "en"], gpu=False)
        res = reader.readtext(img, detail=0, paragraph=True)
        if isinstance(res, list):
            return "\n".join([r for r in res if r]).strip()
        return (res or "").strip()
    except Exception:
        return ""


def ensure_cols(con: sqlite3.Connection):
    cur = con.cursor()
    cur.execute("PRAGMA table_info(documenti)")
    cols = {r[1] for r in cur.fetchall()}

    def add_col(name: str, coltype: str):
        if name not in cols:
            cur.execute(f"ALTER TABLE documenti ADD COLUMN {name} {coltype}")

    add_col("text_preview", "TEXT")
    add_col("snippet_testo", "TEXT")
    add_col("tipo_documento", "TEXT")
    add_col("rgnr", "TEXT")
    add_col("anno", "INTEGER")
    add_col("procura", "TEXT")
    add_col("dda_flag", "INTEGER")
    add_col("status", "TEXT")
    add_col("percorso_file", "TEXT")

    con.commit()


# ----------------------------
# DB-aware path resolving
# ----------------------------
def db_fetch_by_id(con: sqlite3.Connection, doc_id: int) -> Optional[tuple]:
    cur = con.cursor()
    cur.execute("""
        SELECT id, nome_file, sha1, percorso_file, text_preview, snippet_testo, rgnr, anno, procura
        FROM documenti
        WHERE id=?
        LIMIT 1
    """, (doc_id,))
    return cur.fetchone()


def db_fetch_by_filename(con: sqlite3.Connection, nome_file: str) -> Optional[tuple]:
    cur = con.cursor()
    cur.execute("""
        SELECT id, nome_file, sha1, percorso_file, text_preview, snippet_testo, rgnr, anno, procura
        FROM documenti
        WHERE nome_file=?
        ORDER BY id DESC
        LIMIT 1
    """, (nome_file,))
    return cur.fetchone()


def resolve_path(nome_file: str, percorso_file: Optional[str]) -> Optional[Path]:
    candidates: List[Path] = []

    # 1) percorso_file dal DB (relativo a PROJECT_ROOT)
    if percorso_file:
        try:
            p = (PROJECT_ROOT / Path(percorso_file)).resolve()
            candidates.append(p)
        except Exception:
            pass

    # 2) revisione OCC
    candidates.append((REV_OCC_DIR / nome_file).resolve())

    # 3) input_documenti
    candidates.append((INPUT_DIR / nome_file).resolve())

    # 4) nome già assoluto/relativo
    candidates.append((PROJECT_ROOT / nome_file).resolve())

    for c in candidates:
        if c.exists() and c.is_file():
            return c
    return None


# ----------------------------
# MAIN
# ----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--doc-id", type=int, default=None, help="ID documento nel DB (consigliato)")
    ap.add_argument("--file", default=None, help="Nome file (fallback). Cerca in revisione/occ o input_documenti.")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print("[ERR] DB non trovato")
        raise SystemExit(2)

    con = sqlite3.connect(DB_PATH)
    ensure_cols(con)

    row = None
    if args.doc_id is not None:
        row = db_fetch_by_id(con, int(args.doc_id))
        if not row:
            con.close()
            print("[ERR] record DB non trovato (doc-id)")
            raise SystemExit(2)
    else:
        if not args.file:
            con.close()
            print("[ERR] serve --doc-id oppure --file")
            raise SystemExit(2)
        row = db_fetch_by_filename(con, str(args.file))

        # se non trovato nel DB, prova file fisico e poi lookup per sha1 (compat)
        if not row:
            f_try = Path(args.file)
            if not f_try.is_absolute():
                c1 = (REV_OCC_DIR / f_try.name).resolve()
                c2 = (INPUT_DIR / f_try.name).resolve()
                f_try = c1 if c1.exists() else c2
            if not f_try.exists():
                con.close()
                print("[ERR] file non trovato")
                raise SystemExit(2)

            sha1 = sha1_file(f_try)
            cur = con.cursor()
            cur.execute("""
                SELECT id, nome_file, sha1, percorso_file, text_preview, snippet_testo, rgnr, anno, procura
                FROM documenti
                WHERE sha1=? LIMIT 1
            """, (sha1,))
            row = cur.fetchone()
            if not row:
                con.close()
                print("[ERR] record DB non trovato")
                raise SystemExit(2)

    # row schema:
    # (id, nome_file, sha1, percorso_file, text_preview, snippet_testo, rgnr, anno, procura)
    doc_id, nome_file, sha1_db, percorso_file, text_preview, snippet, rgnr_db, anno_db, procura_db = row

    # risolvi path fisico
    fpath = resolve_path(nome_file, percorso_file)
    if not fpath:
        con.close()
        print("[ERR] file non trovato")
        raise SystemExit(2)

    used_ocr = False
    preview = (text_preview or "").strip()

    # Estrazioni da preview
    rgnr, anno = extract_rgnr(preview)
    procura = extract_procura_city(preview) or procura_db
    dda_flag = 1 if RE_DDA.search(preview) else 0

    # OCR lite se PDF e manca qualcosa
    if fpath.suffix.lower() == ".pdf" and (not rgnr or not anno or not procura):
        o = ocr_lite_first_page(fpath)
        if o:
            used_ocr = True
            r2, a2 = extract_rgnr(o)
            p2 = extract_procura_city(o)
            d2 = 1 if RE_DDA.search(o) else 0

            if r2 and a2:
                rgnr, anno = r2, a2
            if p2:
                procura = p2
            if d2:
                dda_flag = 1

            if not preview:
                preview = o

    # snippet
    snip = (preview[:500] if preview else (snippet or "")) or ""

    # pulizia procura finale
    if procura:
        procura = apply_procura_fix(clean_procura_noise(procura))

    cur = con.cursor()

    # aggiorna sempre campi utili
    if procura:
        cur.execute("UPDATE documenti SET procura=? WHERE id=?", (procura, doc_id))

    cur.execute("UPDATE documenti SET dda_flag=? WHERE id=?", (int(dda_flag), doc_id))
    cur.execute("UPDATE documenti SET snippet_testo=? WHERE id=?", (snip, doc_id))
    cur.execute("UPDATE documenti SET tipo_documento='OCC' WHERE id=?", (doc_id,))

    if rgnr and anno:
        cur.execute("UPDATE documenti SET rgnr=?, anno=? WHERE id=?", (rgnr, int(anno), doc_id))

    # status
    status = "READY" if (rgnr and anno and procura) else "NEEDS_DEEPER_ANALYSIS"
    cur.execute("UPDATE documenti SET status=? WHERE id=?", (status, doc_id))

    con.commit()
    con.close()

    print(
        f"[OK] OCC_OCR_RGNR_v2.1(DB-aware) -> "
        f"id={doc_id} file='{nome_file}' path='{str(fpath)}' "
        f"rgnr={rgnr} anno={anno} procura={procura} dda={dda_flag} status={status} ocr_lite={used_ocr}"
    )
    raise SystemExit(0)


if __name__ == "__main__":
    main()
