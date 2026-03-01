#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import tkinter as tk
from tkinter import filedialog


def detect_project_root() -> Path:
codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
    """Root progetto portabile: directory che contiene questo script."""
    return Path(__file__).resolve().parent
=======
    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
        if parent.name.upper() == "SANGIA":
            return parent
    return current.parent
main


PROJECT_ROOT = detect_project_root()

codex/iniziare-progetto-libreria-atti-di-polizia-sri8es

def display_path(path: Path) -> str:
    try:
        rel = path.resolve().relative_to(PROJECT_ROOT.resolve())
        return f"{PROJECT_ROOT.name}/{rel.as_posix()}"
    except Exception:
        return str(path)

=======
main
DB_PATH = PROJECT_ROOT / "libreria" / "documenti.db"
BACKUP_DIR = PROJECT_ROOT / "Backup"
INPUT_DIR = PROJECT_ROOT / "input_documenti"
OVERRIDES_PATH = BACKUP_DIR / "correzioni" / "correzioni_smistamento.csv"

HTML_DIR = PROJECT_ROOT / "HTML"
ADMIN_HTML_PATH = HTML_DIR / "smistamento_web.html"
ORGANIZZATORE_HTML_PATH = HTML_DIR / "organizzatore.html"
READER_HTML_PATH = HTML_DIR / "reader.html"

START_SMISTAMENTO_BAT = PROJECT_ROOT / "start_smistamento.bat"

_jobs_lock = threading.Lock()
_jobs = {}


def _new_job(name: str) -> str:
    job_id = str(uuid.uuid4())
    with _jobs_lock:
        _jobs[job_id] = {
            "job_id": job_id,
            "name": name,
            "status": "queued",
            "started_at": None,
            "ended_at": None,
            "returncode": None,
            "events": [],
            "error": None,
        }
    return job_id


def _append_event(job_id: str, stream: str, line: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ev = {"ts": ts, "stream": stream, "line": line}
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job["events"].append(ev)
        if len(job["events"]) > 2500:
            job["events"] = job["events"][-2500:]


def _set_job_status(job_id: str, **kwargs):
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job.update(kwargs)


def get_job(job_id: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return None
        return dict(job)


def run_bat_job(bat_path: Path, args=None, name="smistamento (BAT)") -> str:
    job_id = _new_job(name)
    args = args or []

    def worker():
        try:
            _set_job_status(job_id, status="running", started_at=datetime.now().isoformat(timespec="seconds"))

            if not bat_path.exists():
                raise FileNotFoundError(f"BAT non trovato: {bat_path}")

            cmd = ["cmd.exe", "/c", str(bat_path), *args]

            proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )

            def pump(stream, stream_name):
                try:
                    for line in iter(stream.readline, ""):
                        if line == "":
                            break
                        _append_event(job_id, stream_name, line.rstrip("\n"))
                except Exception as e:
                    _append_event(job_id, "system", f"[pump error {stream_name}] {e}")
                finally:
                    try:
                        stream.close()
                    except Exception:
                        pass

            t_out = threading.Thread(target=pump, args=(proc.stdout, "stdout"), daemon=True)
            t_err = threading.Thread(target=pump, args=(proc.stderr, "stderr"), daemon=True)
            t_out.start()
            t_err.start()

            rc = proc.wait()

            for _ in range(30):
                if not (t_out.is_alive() or t_err.is_alive()):
                    break
                time.sleep(0.05)

            _set_job_status(
                job_id,
                status="completed" if rc == 0 else "failed",
                returncode=rc,
                ended_at=datetime.now().isoformat(timespec="seconds"),
            )
        except Exception as e:
            _set_job_status(
                job_id,
                status="failed",
                error=str(e),
                ended_at=datetime.now().isoformat(timespec="seconds"),
            )
            _append_event(job_id, "system", f"[exception] {e}")

    threading.Thread(target=worker, daemon=True).start()
    return job_id


def get_cuda_status() -> dict:
    status = {
        "ok": True,
        "cuda_available": False,
        "device_count": 0,
        "device_name": None,
        "note": None,
    }
    try:
        import torch  # type: ignore

        status["cuda_available"] = bool(torch.cuda.is_available())
        status["device_count"] = int(torch.cuda.device_count()) if status["cuda_available"] else 0
        if status["cuda_available"] and status["device_count"] > 0:
            status["device_name"] = torch.cuda.get_device_name(0)
        status["torch_version"] = getattr(torch, "__version__", None)
        status["cuda_runtime_version"] = getattr(getattr(torch, "version", None), "cuda", None)
    except Exception as e:
        status["ok"] = False
        status["note"] = f"torch non disponibile o errore: {e}"
    return status


def ensure_override_csv():
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    if not OVERRIDES_PATH.exists():
        OVERRIDES_PATH.write_text(
            "file;anno;tipo_documento;rgnr;procura;destinazione;note\n",
            encoding="utf-8-sig",
        )


def _to_title(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    return " ".join([w[:1].upper() + w[1:].lower() if w else "" for w in s.split()])


def _to_upper(s: str) -> str:
    return (s or "").strip().upper()


def _sanitize_year(s: str) -> str:
    return (s or "").strip()


def normalize_fields(payload: dict) -> dict:
    out = dict(payload or {})
    out["file"] = (out.get("file") or "").strip()
    out["anno"] = _sanitize_year(out.get("anno") or "")
    out["tipo_documento"] = _to_title(out.get("tipo_documento") or "")
    out["rgnr"] = _to_upper(out.get("rgnr") or "")
    out["procura"] = _to_title(out.get("procura") or "")
    out["destinazione"] = _to_upper(out.get("destinazione") or "")
    out["note"] = (out.get("note") or "").strip()
    if "id" in out:
        out["id"] = (str(out.get("id")) or "").strip()
    return out


def read_db_rows(limit: int = 200):
    if not DB_PATH.exists():
        return []
    limit = max(1, min(int(limit), 1000))
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, nome_file, anno, rgnr, procura, tipo_documento, note, data_inserimento
        FROM documenti
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def backup_db() -> str:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB non trovato: {DB_PATH}")
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = BACKUP_DIR / f"documenti_{ts}.db"
    shutil.copy2(DB_PATH, dst)
    return str(dst)


def restore_db_from_backup(backup_path: str) -> dict:
    bp = Path(backup_path).expanduser()
    if not bp.exists():
        raise FileNotFoundError(f"Backup non trovato: {bp}")
    if bp.suffix.lower() != ".db":
        raise ValueError("Seleziona un file .db valido")

    auto_backup = backup_db()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(bp, DB_PATH)

    return {"auto_backup": auto_backup, "restored_from": str(bp), "restored_to": str(DB_PATH)}


def update_db_row_safe(payload: dict) -> dict:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB non trovato: {DB_PATH}")

    p = normalize_fields(payload)
    row_id = p.get("id")
    if row_id in (None, "", "0"):
        raise ValueError("Campo 'id' obbligatorio per aggiornare il DB")

    allowed = ["anno", "rgnr", "procura", "tipo_documento", "note"]
    updates = {}
    for k in allowed:
        val = (p.get(k) or "")
        if str(val).strip() != "":
            updates[k] = val

    if not updates:
        return {"changed": 0, "updated_fields": []}

    sets = ", ".join([f"{k} = ?" for k in updates.keys()])
    params = list(updates.values()) + [int(row_id)]

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        f"""
        UPDATE documenti
        SET {sets}
        WHERE id = ?
        """,
        params,
    )
    conn.commit()
    changed = cur.rowcount
    conn.close()

    return {"changed": changed, "updated_fields": list(updates.keys())}


def find_db_candidates_by_filename(file_value: str):
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB non trovato: {DB_PATH}")

    basename = Path(file_value).name if file_value else ""
    if not basename:
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, nome_file, anno, rgnr, procura, tipo_documento, note, data_inserimento
        FROM documenti
        WHERE nome_file = ?
        """,
        (basename,),
    )
    rows = [dict(r) for r in cur.fetchall()]

    if not rows:
        like = f"%{basename}%"
        cur.execute(
            """
            SELECT id, nome_file, anno, rgnr, procura, tipo_documento, note, data_inserimento
            FROM documenti
            WHERE nome_file LIKE ?
            """,
            (like,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    conn.close()
    return rows


def read_override_rows():
    ensure_override_csv()
    rows = []
    fields = ["file", "anno", "tipo_documento", "rgnr", "procura", "destinazione", "note"]

    with open(OVERRIDES_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for idx, row in enumerate(reader):
            r = {k: (row.get(k) or "").strip() for k in fields}
            r = normalize_fields(r)
            r["_idx"] = idx
            r["_basename"] = Path(r.get("file") or "").name
            rows.append(r)

    return rows


def upsert_override(payload: dict):
    ensure_override_csv()
    p = normalize_fields(payload)

    file_name = p.get("file", "").strip()
    if not file_name:
        raise ValueError("Campo 'file' obbligatorio")

    fields = ["file", "anno", "tipo_documento", "rgnr", "procura", "destinazione", "note"]

    rows = []
    with open(OVERRIDES_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            row = {k: (row.get(k) or "") for k in fields}
            rows.append(row)

    updated = False
    for row in rows:
        if (row.get("file", "") or "").strip() == file_name:
            for k in fields:
                val = (p.get(k) or "").strip()
                if val != "":
                    row[k] = val
            updated = True
            break

    if not updated:
        rows.append({k: p.get(k, "") for k in fields})

    with open(OVERRIDES_PATH, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


def apply_override_to_db(payload: dict) -> dict:
    p = normalize_fields(payload)
    file_value = (p.get("file") or "").strip()
    if not file_value:
        raise ValueError("Campo 'file' obbligatorio")

    target_id = (payload.get("target_id") or "")
    target_id = str(target_id).strip() if target_id is not None else ""

    overrides = read_override_rows()
    base = Path(file_value).name

    chosen = None
    for r in overrides:
        if (r.get("file") or "").strip() == file_value:
            chosen = r
            break
    if chosen is None:
        for r in overrides:
            if (r.get("_basename") or "") == base:
                chosen = r
                break

    if chosen is None:
        return {"ok": False, "error": f"Nessuna correzione trovata in CSV per: {file_value}", "candidates": []}

    db_payload = {
        "anno": chosen.get("anno", ""),
        "tipo_documento": chosen.get("tipo_documento", ""),
        "rgnr": chosen.get("rgnr", ""),
        "procura": chosen.get("procura", ""),
        "note": chosen.get("note", ""),
    }

    if target_id:
        backup_path = backup_db()
        result = update_db_row_safe({"id": target_id, **db_payload})
        return {
            "ok": True,
            "mode": "by_id",
            "backup_path": backup_path,
            "target_id": int(target_id),
            **result,
        }

    candidates = find_db_candidates_by_filename(chosen.get("file", "") or base)
    if not candidates:
        return {
            "ok": False,
            "error": f"Nessun documento DB trovato per basename: {base}",
            "candidates": [],
        }

    if len(candidates) > 1:
        return {
            "ok": False,
            "error": f"Trovati {len(candidates)} candidati DB per '{base}'. Seleziona un ID e riprova.",
            "candidates": candidates[:50],
            "hint": "Ripeti chiamata con target_id",
        }

    doc = candidates[0]
    backup_path = backup_db()
    result = update_db_row_safe({"id": doc["id"], **db_payload})

    return {
        "ok": True,
        "mode": "auto_single_match",
        "backup_path": backup_path,
        "target_id": doc["id"],
        "matched_nome_file": doc.get("nome_file"),
        **result,
    }


def pick_file_dialog(initial_dir: Path, title: str, filetypes):
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(
        title=title,
        initialdir=str(initial_dir),
        filetypes=filetypes,
    )
    root.destroy()
    return path or ""


def pick_folder_dialog(initial_dir: Path, title: str) -> str:
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askdirectory(
        title=title,
        initialdir=str(initial_dir),
        mustexist=True,
    )
    root.destroy()
    return path or ""


def pick_import_file_dialog() -> str:
    return pick_file_dialog(
        initial_dir=PROJECT_ROOT,
        title="Seleziona file da importare in input_documenti",
        filetypes=[("PDF e tutti", "*.pdf;*.*"), ("Tutti i file", "*.*")],
    )


def pick_backup_db_dialog() -> str:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    return pick_file_dialog(
        initial_dir=BACKUP_DIR,
        title="Seleziona un backup DB da ripristinare",
        filetypes=[("Database SQLite", "*.db"), ("Tutti i file", "*.*")],
    )


def pick_file_for_override_dialog() -> str:
    return pick_file_dialog(
        initial_dir=PROJECT_ROOT,
        title="Seleziona un file (override)",
        filetypes=[("PDF e tutti", "*.pdf;*.*"), ("Tutti i file", "*.*")],
    )


def pick_dest_folder_dialog() -> str:
    return pick_folder_dialog(
        initial_dir=PROJECT_ROOT,
        title="Seleziona cartella di destinazione",
    )


def import_file_to_input(src_path: str) -> dict:
    if not src_path:
        raise ValueError("Nessun file selezionato")
    src = Path(src_path)
    if not src.exists():
        raise FileNotFoundError(f"File non trovato: {src}")

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    dst = INPUT_DIR / src.name
    if dst.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = INPUT_DIR / f"{src.stem}_{ts}{src.suffix}"

    shutil.copy2(src, dst)
    return {"imported_from": str(src), "imported_to": str(dst)}


class Handler(BaseHTTPRequestHandler):
    def _json(self, status: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_html(self, path: Path):
        if not path.exists():
            self._json(404, {"ok": False, "error": f"HTML non trovato: {path}"})
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/":
            return self._serve_html(ADMIN_HTML_PATH)

        if parsed.path == "/organizzatore":
            return self._serve_html(ORGANIZZATORE_HTML_PATH)

        if parsed.path == "/reader":
            return self._serve_html(READER_HTML_PATH)

        if parsed.path == "/api/documenti":
            q = parse_qs(parsed.query)
            limit = int((q.get("limit", ["200"])[0] or "200"))
            self._json(200, {"ok": True, "rows": read_db_rows(limit)})
            return

        if parsed.path == "/api/overrides":
            rows = read_override_rows()
 codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
            self._json(200, {"ok": True, "rows": rows, "path": display_path(OVERRIDES_PATH)})
=======
            self._json(200, {"ok": True, "rows": rows, "path": str(OVERRIDES_PATH)})
 main
            return

        if parsed.path == "/api/job":
            q = parse_qs(parsed.query)
            job_id = (q.get("id", [""])[0] or "").strip()
            job = get_job(job_id)
            if not job:
                self._json(404, {"ok": False, "error": "Job non trovato"})
                return
            self._json(200, {"ok": True, "job": job})
            return

        if parsed.path == "/api/cuda-status":
            self._json(200, get_cuda_status())
            return

        self._json(404, {"ok": False, "error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)

        if parsed.path == "/api/upload-to-input":
            try:
                import cgi

                fs = cgi.FieldStorage(
                    fp=self.rfile,
                    headers=self.headers,
                    environ={
                        "REQUEST_METHOD": "POST",
                        "CONTENT_TYPE": self.headers.get("Content-Type"),
                    },
                )

                if "file" not in fs:
                    self._json(400, {"ok": False, "error": "Campo 'file' mancante"})
                    return

                file_item = fs["file"]
                filename = getattr(file_item, "filename", "") or ""
                if not filename:
                    self._json(400, {"ok": False, "error": "Nessun file selezionato"})
                    return

                INPUT_DIR.mkdir(parents=True, exist_ok=True)

                safe_name = Path(filename).name
                dst = INPUT_DIR / safe_name
                if dst.exists():
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    dst = INPUT_DIR / f"{dst.stem}_{ts}{dst.suffix}"

                with open(dst, "wb") as out:
                    shutil.copyfileobj(file_item.file, out)

 codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
                self._json(200, {"ok": True, "imported_to": display_path(dst)})
=======
                self._json(200, {"ok": True, "imported_to": str(dst)})
 main
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        content_len = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(content_len) if content_len else b"{}"

        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            payload = {}

        if parsed.path == "/api/backup-db":
            try:
                path = backup_db()
                self._json(200, {"ok": True, "backup_path": path})
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        if parsed.path == "/api/restore-db":
            try:
                chosen = pick_backup_db_dialog()
                if not chosen:
                    self._json(200, {"ok": False, "cancelled": True})
                    return
                info = restore_db_from_backup(chosen)
                self._json(200, {"ok": True, **info})
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        if parsed.path == "/api/override":
            try:
                upsert_override(payload)
                self._json(200, {"ok": True})
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        if parsed.path == "/api/update-db":
            try:
                backup_path = backup_db()
                result = update_db_row_safe(payload)
                self._json(200, {"ok": True, "backup_path": backup_path, **result})
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        if parsed.path == "/api/apply-override-to-db":
            try:
                result = apply_override_to_db(payload)
                self._json(200, result)
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        if parsed.path == "/api/pick-file":
            try:
                path = pick_file_for_override_dialog()
                self._json(200, {"ok": True, "path": path})
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        if parsed.path == "/api/pick-dest":
            try:
                path = pick_dest_folder_dialog()
                self._json(200, {"ok": True, "path": path})
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        if parsed.path == "/api/pick-import-file":
            try:
                path = pick_import_file_dialog()
                self._json(200, {"ok": True, "path": path})
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        if parsed.path == "/api/import-to-input":
            try:
                src = (payload.get("path") or "").strip()
                info = import_file_to_input(src)
                self._json(200, {"ok": True, **info})
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        if parsed.path == "/api/run-smistamento-job":
            try:
                job_id = run_bat_job(
                    START_SMISTAMENTO_BAT,
                    args=["--web"],
                    name="smistamento (start_smistamento.bat)",
                )
                self._json(200, {"ok": True, "job_id": job_id})
            except Exception as e:
                self._json(400, {"ok": False, "error": str(e)})
            return

        self._json(404, {"ok": False, "error": "Not found"})


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 8765), Handler)

    print("SANG_I.A. Admin avviato: http://127.0.0.1:8765")
 codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
    print(f"PROJECT_ROOT: {display_path(PROJECT_ROOT)}")
    print(f"HTML_DIR: {display_path(HTML_DIR)}")
    print(f"DB: {display_path(DB_PATH)}")
    print(f"Backup dir: {display_path(BACKUP_DIR)}")
    print(f"Input dir: {display_path(INPUT_DIR)}")
    print(f"Override CSV: {display_path(OVERRIDES_PATH)}")
    print(f"BAT: {display_path(START_SMISTAMENTO_BAT)}")
=======
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"HTML_DIR: {HTML_DIR}")
    print(f"DB: {DB_PATH}")
    print(f"Backup dir: {BACKUP_DIR}")
    print(f"Input dir: {INPUT_DIR}")
    print(f"Override CSV: {OVERRIDES_PATH}")
    print(f"BAT: {START_SMISTAMENTO_BAT}")
    main

    try:
        import webbrowser

        webbrowser.open("http://127.0.0.1:8765", new=1, autoraise=True)
    except Exception:
        pass

    server.serve_forever()


if __name__ == "__main__":
    main()
