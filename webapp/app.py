import os
import csv
import uuid
import time
import shutil
import sqlite3
import logging
import threading
import subprocess
import sys
from functools import wraps
from datetime import datetime

from flask import (
    Flask, request, session, redirect, url_for,
    render_template, abort, jsonify
)
from werkzeug.security import generate_password_hash, check_password_hash

HERE = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(HERE, ".."))

DOCUMENTI_DB = os.path.join(PROJECT_ROOT, "libreria", "documenti.db")
USERS_DB = os.path.join(HERE, "users.db")

INPUT_DIR = os.path.join(PROJECT_ROOT, "input_documenti")
BACKUP_DIR = os.path.join(PROJECT_ROOT, "Backup")
LOGDIR = os.path.join(BACKUP_DIR, "log_smistamenti")

CORREZIONI_CSV = os.path.join(BACKUP_DIR, "correzioni", "correzioni_smistamento.csv")

HOST = "127.0.0.1"
PORT = 8765

app = Flask(
    __name__,
    template_folder=(os.path.join(HERE, "template") if os.path.isdir(os.path.join(HERE, "template")) else os.path.join(HERE, "templates")),
    static_folder=os.path.join(HERE, "static"),
)
app.secret_key = os.environ.get("SANGIA_SECRET_KEY", "CAMBIA_QUESTA_SECRET_KEY_LUNGA")
logging.getLogger("werkzeug").setLevel(logging.WARNING)


def ensure_dirs():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    os.makedirs(LOGDIR, exist_ok=True)


def ts_now():
    return datetime.now().isoformat(timespec="seconds")


def safe_copy(src: str, dst_dir: str) -> str:
    os.makedirs(dst_dir, exist_ok=True)
    base = os.path.basename(src)
    dst = os.path.join(dst_dir, base)

    if os.path.abspath(src) == os.path.abspath(dst):
        return dst

    if os.path.exists(dst):
        name, ext = os.path.splitext(base)
        dst = os.path.join(dst_dir, f"{name}_{int(time.time())}{ext}")

    shutil.copy2(src, dst)
    return dst


def backup_file(path: str) -> str:
    os.makedirs(BACKUP_DIR, exist_ok=True)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.basename(path)
    dst = os.path.join(BACKUP_DIR, f"backup_{base}_{stamp}")
    shutil.copy2(path, dst)
    return dst


def json_ok(**kwargs):
    d = {"ok": True}
    d.update(kwargs)
    return jsonify(d)


def json_err(msg, code=400, **kwargs):
    d = {"ok": False, "error": msg}
    d.update(kwargs)
    return jsonify(d), code


def users_conn():
    return sqlite3.connect(USERS_DB)


def _ensure_column(con, table, col_name, col_type):
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if col_name not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
        con.commit()


def init_users_db():
    con = users_conn()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('admin','user')),
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
    """)
    con.commit()

    _ensure_column(con, "users", "last_login", "TEXT")

    cur.execute("SELECT 1 FROM users WHERE username=?", ("admin",))
    if not cur.fetchone():
        cur.execute(
            "INSERT INTO users (username, password_hash, role, is_active, created_at, last_login) VALUES (?,?,?,?,?,?)",
            ("admin", generate_password_hash("admin123!"), "admin", 1, ts_now(), None)
        )
        con.commit()

    con.close()


def get_user_by_username(username: str):
    con = users_conn()
    cur = con.cursor()
    cur.execute("SELECT id, username, password_hash, role, is_active, created_at, last_login FROM users WHERE username=?", (username,))
    row = cur.fetchone()
    con.close()
    return row


def get_user_by_id(user_id: int):
    con = users_conn()
    cur = con.cursor()
    cur.execute("SELECT id, username, role, is_active, created_at, last_login FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    con.close()
    return row


def set_last_login(user_id: int):
    con = users_conn()
    cur = con.cursor()
    cur.execute("UPDATE users SET last_login=? WHERE id=?", (ts_now(), user_id))
    con.commit()
    con.close()


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login_page"))
        u = get_user_by_id(session["user_id"])
        if not u or u[3] != 1:
            session.clear()
            return redirect(url_for("login_page"))
        return fn(*args, **kwargs)
    return wrapper


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login_page"))
        u = get_user_by_id(session["user_id"])
        if not u or u[3] != 1:
            session.clear()
            return redirect(url_for("login_page"))
        if session.get("role") != "admin":
            abort(403)
        return fn(*args, **kwargs)
    return wrapper


def documenti_conn():
    return sqlite3.connect(DOCUMENTI_DB)


def detect_documenti_table(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    if not tables:
        return None, []
    for candidate in ("documenti", "docs", "DOCUMENTI"):
        if candidate in tables:
            return candidate, tables
    return tables[0], tables


def get_table_columns(cur, table):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return cols


@app.route("/", methods=["GET"])
def login_page():
    if "user_id" in session:
        return redirect(url_for("admin_page") if session.get("role") == "admin" else url_for("consulta_page"))
    return render_template("login.html")


@app.route("/login", methods=["POST"])
def do_login():
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""

    row = get_user_by_username(username)
    if not row:
        return render_template("login.html", error="Credenziali non valide")

    user_id, uname, pwd_hash, role, is_active, _created, _last_login = row
    if is_active != 1:
        return render_template("login.html", error="Utente disabilitato")
    if not check_password_hash(pwd_hash, password):
        return render_template("login.html", error="Credenziali non valide")

    session["user_id"] = user_id
    session["username"] = uname
    session["role"] = role

    try:
        set_last_login(user_id)
    except Exception:
        pass

    return redirect(url_for("consulta_page"))


@app.route("/logout", methods=["GET"])
def logout():
    session.clear()
    return redirect(url_for("login_page"))


@app.route("/admin", methods=["GET"])
@admin_required
def admin_page():
    return render_template("admin.html", username=session.get("username"), role=session.get("role"))


@app.route("/consulta", methods=["GET"])
@login_required
def consulta_page():
    return render_template("consulta.html", username=session.get("username"), role=session.get("role"))


@app.route("/chatbot", methods=["GET"])
@login_required
def chatbot_page():
    return render_template("chatbot.html", username=session.get("username"), role=session.get("role"))


@app.route("/organizzatore", methods=["GET"])
@admin_required
def organizzatore_page():
    return render_template("organizzatore.html", username=session.get("username"), role=session.get("role"))


@app.route("/reader", methods=["GET"])
@admin_required
def reader_page():
    return render_template("reader.html", username=session.get("username"), role=session.get("role"))


@app.route("/smistamento_web", methods=["GET"])
@admin_required
def smistamento_web_page():
    return render_template("smistamento_web.html", username=session.get("username"), role=session.get("role"))


@app.route("/api/me", methods=["GET"])
@login_required
def api_me():
    return jsonify({"ok": True, "username": session.get("username"), "role": session.get("role")})


@app.route("/api/cuda-status", methods=["GET"])
@login_required
def api_cuda_status():
    try:
        import torch  # type: ignore
        torch_version = getattr(torch, "__version__", "n/d")
        cuda_rt = getattr(torch.version, "cuda", None)
        cuda_available = bool(torch.cuda.is_available())
        device_count = int(torch.cuda.device_count()) if cuda_available else 0
        device_name = torch.cuda.get_device_name(0) if cuda_available and device_count > 0 else None
        note = "CUDA disponibile" if cuda_available else "torch vede solo CPU"
        return jsonify({
            "ok": True,
            "torch_version": torch_version,
            "cuda_runtime_version": cuda_rt,
            "cuda_available": cuda_available,
            "device_count": device_count,
            "device_name": device_name,
            "note": note
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "note": str(e),
            "torch_version": "n/d",
            "cuda_runtime_version": "n/d",
            "cuda_available": False,
            "device_count": 0,
            "device_name": None
        })


@app.route("/api/users", methods=["GET"])
@admin_required
def api_users_list():
    con = users_conn()
    cur = con.cursor()
    cur.execute("SELECT id, username, role, is_active, created_at, last_login FROM users ORDER BY id ASC")
    rows = cur.fetchall()
    con.close()
    return json_ok(rows=rows)


@app.route("/api/users", methods=["POST"])
@admin_required
def api_users_create():
    data = request.get_json(force=True)
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = data.get("role") or "user"
    if not username or not password:
        return json_err("username e password obbligatori", 400)
    if role not in ("admin", "user"):
        return json_err("role deve essere admin o user", 400)

    con = users_conn()
    cur = con.cursor()
    try:
        cur.execute(
            "INSERT INTO users (username, password_hash, role, is_active, created_at, last_login) VALUES (?,?,?,?,?,?)",
            (username, generate_password_hash(password), role, 1, ts_now(), None)
        )
        con.commit()
    except sqlite3.IntegrityError:
        con.close()
        return json_err("username già esistente", 409)
    con.close()
    return json_ok()


@app.route("/api/users/<int:user_id>/disable", methods=["POST"])
@admin_required
def api_users_disable(user_id: int):
    con = users_conn()
    cur = con.cursor()
    cur.execute("UPDATE users SET is_active=0 WHERE id=?", (user_id,))
    con.commit()
    con.close()
    return json_ok()


@app.route("/api/users/<int:user_id>/enable", methods=["POST"])
@admin_required
def api_users_enable(user_id: int):
    con = users_conn()
    cur = con.cursor()
    cur.execute("UPDATE users SET is_active=1 WHERE id=?", (user_id,))
    con.commit()
    con.close()
    return json_ok()


@app.route("/api/users/<int:user_id>/password", methods=["POST"])
@admin_required
def api_users_set_password(user_id: int):
    data = request.get_json(force=True)
    password = data.get("password") or ""
    if not password:
        return json_err("password obbligatoria", 400)
    con = users_conn()
    cur = con.cursor()
    cur.execute("UPDATE users SET password_hash=? WHERE id=?", (generate_password_hash(password), user_id))
    con.commit()
    con.close()
    return json_ok()


@app.route("/api/users/<int:user_id>/delete", methods=["POST"])
@admin_required
def api_users_delete(user_id: int):
    if session.get("user_id") == user_id:
        return json_err("Non puoi eliminare l'utente con cui sei loggato", 400)

    con = users_conn()
    cur = con.cursor()
    cur.execute("SELECT username FROM users WHERE id=?", (user_id,))
    row = cur.fetchone()
    if not row:
        con.close()
        return json_err("Utente non trovato", 404)

    cur.execute("DELETE FROM users WHERE id=?", (user_id,))
    con.commit()
    con.close()
    return json_ok()


@app.route("/api/documenti", methods=["GET"])
@login_required
def api_documenti():
    if not os.path.exists(DOCUMENTI_DB):
        return json_err(f"DB documenti non trovato: {DOCUMENTI_DB}", 500)

    limit = int(request.args.get("limit", "50"))
    offset = int(request.args.get("offset", "0"))
    q = (request.args.get("q") or "").strip()

    con = documenti_conn()
    cur = con.cursor()

    table, tables = detect_documenti_table(cur)
    if not table:
        con.close()
        return json_err("Nessuna tabella trovata nel DB documenti", 500)

    cols = get_table_columns(cur, table)
    base_select = f"SELECT rowid AS _rowid, * FROM {table}"

    if q:
        like = f"%{q}%"
        where_parts = []
        params = []
        for c in cols:
            where_parts.append(f"CAST({c} AS TEXT) LIKE ?")
            params.append(like)
        where_sql = " OR ".join(where_parts)
        sql = f"{base_select} WHERE {where_sql} LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        cur.execute(sql, params)
    else:
        cur.execute(f"{base_select} LIMIT ? OFFSET ?", (limit, offset))

    rows = cur.fetchall()
    con.close()

    return json_ok(
        table=table,
        tables=tables,
        columns=["_rowid"] + cols,
        rows=rows,
        limit=limit,
        offset=offset
    )


@app.route("/api/documenti/update", methods=["POST"])
@admin_required
def api_documenti_update():
    data = request.get_json(force=True)
    table = data.get("table")
    rowid = data.get("rowid")
    updates = data.get("updates", {})

    if not table or rowid is None or not isinstance(updates, dict) or not updates:
        return json_err("Parametri mancanti (table, rowid, updates)", 400)

    con = documenti_conn()
    cur = con.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    if not cur.fetchone():
        con.close()
        return json_err("Tabella non valida", 400)

    valid_cols = set(get_table_columns(cur, table))
    bad = [k for k in updates.keys() if k not in valid_cols]
    if bad:
        con.close()
        return json_err(f"Colonne non valide: {bad}", 400)

    bkp = backup_file(DOCUMENTI_DB)

    set_sql = ", ".join([f"{k}=?" for k in updates.keys()])
    params = list(updates.values())
    params.append(rowid)

    cur.execute(f"UPDATE {table} SET {set_sql} WHERE rowid=?", params)
    con.commit()
    con.close()
    return json_ok(backup_path=bkp)


@app.route("/api/documenti/fix-year", methods=["POST"])
@admin_required
def api_documenti_fix_year():
    data = request.get_json(force=True)
    nome_file = (data.get("nome_file") or "").strip()
    new_year = (data.get("new_year") or "").strip()
    if not nome_file or not new_year:
        return json_err("nome_file e new_year obbligatori", 400)
    try:
        int(new_year)
    except Exception:
        return json_err("new_year non valido", 400)

    source_path = None
    for root, _dirs, files in os.walk(os.path.join(PROJECT_ROOT, "libreria")):
        if nome_file in files:
            source_path = os.path.join(root, nome_file)
            break
    if not source_path:
        return json_err("File non trovato in libreria", 404)

    target_dir = os.path.join(PROJECT_ROOT, "libreria", new_year)
    os.makedirs(target_dir, exist_ok=True)
    target_path = os.path.join(target_dir, nome_file)
    if os.path.abspath(source_path) != os.path.abspath(target_path):
        shutil.move(source_path, target_path)

    if os.path.exists(DOCUMENTI_DB):
        con = documenti_conn()
        cur = con.cursor()
        table, _ = detect_documenti_table(cur)
        if table:
            cols = get_table_columns(cur, table)
            if "anno" in cols and "nome_file" in cols:
                bkp = backup_file(DOCUMENTI_DB)
                cur.execute(f"UPDATE {table} SET anno=? WHERE nome_file=?", (new_year, nome_file))
                con.commit()
                con.close()
                return json_ok(updated=True, moved_to=target_path, backup_path=bkp)
        con.close()

    return json_ok(updated=False, moved_to=target_path)


_jobs = {}
_jobs_lock = threading.Lock()


def _job_add_event(job_id: str, stream: str, line: str):
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return
        job["events"].append({"ts": ts_now(), "stream": stream, "line": line.rstrip("\n")})


def _run_subprocess_job(job_id: str, cmd: list, cwd: str):
    with _jobs_lock:
        _jobs[job_id]["status"] = "running"
        _jobs[job_id]["started_at"] = ts_now()

    try:
        p = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        def pump(pipe, stream_name):
            try:
                for line in iter(pipe.readline, ""):
                    if not line:
                        break
                    _job_add_event(job_id, stream_name, line)
            finally:
                try:
                    pipe.close()
                except Exception:
                    pass

        t1 = threading.Thread(target=pump, args=(p.stdout, "stdout"), daemon=True)
        t2 = threading.Thread(target=pump, args=(p.stderr, "stderr"), daemon=True)
        t1.start()
        t2.start()

        rc = p.wait()
        t1.join(timeout=0.2)
        t2.join(timeout=0.2)

        with _jobs_lock:
            job = _jobs[job_id]
            job["returncode"] = rc
            job["ended_at"] = ts_now()
            job["status"] = "completed" if rc == 0 else "failed"

    except Exception as e:
        _job_add_event(job_id, "stderr", f"EXCEPTION: {e}")
        with _jobs_lock:
            job = _jobs[job_id]
            job["returncode"] = 1
            job["ended_at"] = ts_now()
            job["status"] = "failed"


@app.route("/api/run-smistamento-job", methods=["POST"])
@admin_required
def api_run_smistamento_job():
    py = os.path.join(PROJECT_ROOT, "venv", "Scripts", "python.exe")
    if not os.path.exists(py):
        py = os.environ.get("PYTHON", "python3")

    organizzatore = os.path.join(PROJECT_ROOT, "organizzatore.py")
    if not os.path.exists(organizzatore):
        return json_err("organizzatore.py non trovato", 500)

    job_id = str(uuid.uuid4())
    job = {
        "job_id": job_id,
        "name": "smistamento",
        "status": "queued",
        "started_at": None,
        "ended_at": None,
        "returncode": None,
        "events": []
    }
    with _jobs_lock:
        _jobs[job_id] = job

    t = threading.Thread(
        target=_run_subprocess_job,
        args=(job_id, [py, organizzatore, "--watch"], PROJECT_ROOT),
        daemon=True
    )
    t.start()

    return json_ok(job_id=job_id)


@app.route("/api/job", methods=["GET"])
@admin_required
def api_job():
    job_id = request.args.get("id") or ""
    with _jobs_lock:
        job = _jobs.get(job_id)
        if not job:
            return json_err("job non trovato", 404)
        return json_ok(job=job)


codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
@app.route("/api/run-revisione-job", methods=["POST"])
@admin_required
def api_run_revisione_job():
    py = os.path.join(PROJECT_ROOT, "venv", "Scripts", "python.exe")
    if not os.path.exists(py):
        py = os.environ.get("PYTHON", "python3")

    revisione_script = os.path.join(PROJECT_ROOT, "revisione.py")
    if not os.path.exists(revisione_script):
        return json_err("revisione.py non trovato", 500)

    data = request.get_json(silent=True) or {}
    mode = str(data.get("mode") or "occ").strip().lower()

    cmd = [py, revisione_script]
    job_name = "revisione_occ"

    if mode == "db":
        cmd += ["--only-status", "REVISIONE_OCC"]
        job_name = "revisione_db"
    elif mode == "revisione":
        cmd += ["--folder", os.path.join(PROJECT_ROOT, "libreria", "revisione")]
        job_name = "revisione_folder"
    elif mode == "occ":
        cmd += ["--folder", os.path.join(PROJECT_ROOT, "libreria", "revisione", "occ")]
        job_name = "revisione_occ"
    else:
        return json_err("mode non valido (usa: db, revisione, occ)", 400)

    job_id = str(uuid.uuid4())
    job = {
        "job_id": job_id,
        "name": job_name,
        "status": "queued",
        "started_at": None,
        "ended_at": None,
        "returncode": None,
        "events": []
    }
    with _jobs_lock:
        _jobs[job_id] = job

    t = threading.Thread(
        target=_run_subprocess_job,
        args=(job_id, cmd, PROJECT_ROOT),
        daemon=True
    )
    t.start()

    return json_ok(job_id=job_id, mode=mode)


=======
main
def _tk_pick_file():
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        path = filedialog.askopenfilename()
        root.destroy()
        return path
    except Exception:
        return ""


def _tk_pick_dir():
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        path = filedialog.askdirectory()
        root.destroy()
        return path
    except Exception:
        return ""




@app.route("/api/upload-to-input", methods=["POST"])
@admin_required
def api_upload_to_input():
    if "file" not in request.files:
        return json_err("Campo 'file' mancante", 400)
    f = request.files["file"]
    if not f or not f.filename:
        return json_err("Nessun file selezionato", 400)
    os.makedirs(INPUT_DIR, exist_ok=True)
    filename = os.path.basename(f.filename)
    dst = os.path.join(INPUT_DIR, filename)
    if os.path.exists(dst):
        name, ext = os.path.splitext(filename)
        dst = os.path.join(INPUT_DIR, f"{name}_{int(time.time())}{ext}")
    f.save(dst)
    return json_ok(imported_to=dst)

@app.route("/api/pick-import-file", methods=["POST"])
@admin_required
def api_pick_import_file():
    path = _tk_pick_file()
    if not path:
        return json_ok(path=None)
    return json_ok(path=path)


@app.route("/api/import-to-input", methods=["POST"])
@admin_required
def api_import_to_input():
    data = request.get_json(force=True)
    path = (data.get("path") or "").strip()
    if not path:
        return json_err("path mancante", 400)
    if not os.path.exists(path):
        return json_err("file non trovato", 404)
    try:
        dst = safe_copy(path, INPUT_DIR)
        return json_ok(imported_to=dst)
    except Exception as e:
        return json_err(str(e), 500)


@app.route("/api/backup-db", methods=["POST"])
@admin_required
def api_backup_db():
    if not os.path.exists(DOCUMENTI_DB):
        return json_err("documenti.db non trovato", 500)
    try:
        bkp = backup_file(DOCUMENTI_DB)
        return json_ok(backup_path=bkp)
    except Exception as e:
        return json_err(str(e), 500)


@app.route("/api/restore-db", methods=["POST"])
@admin_required
def api_restore_db():
    if not os.path.exists(BACKUP_DIR):
        return json_err("Backup dir non trovato", 500)

    backups = [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.startswith("backup_documenti.db_")]
    backups.sort(reverse=True)

    if not backups:
        return json_err("Nessun backup disponibile", 404)

    src = backups[0]
    try:
        auto = backup_file(DOCUMENTI_DB) if os.path.exists(DOCUMENTI_DB) else None
        shutil.copy2(src, DOCUMENTI_DB)
        return json_ok(restored_from=src, auto_backup=auto)
    except Exception as e:
        return json_err(str(e), 500)


def _ensure_csv_header(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return
    header = ["file", "anno", "tipo_documento", "rgnr", "procura", "destinazione", "note"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()


def _read_overrides(path: str):
    _ensure_csv_header(path)
    rows = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for idx, row in enumerate(r):
            row = dict(row)
            row["_idx"] = idx + 1
            row["_basename"] = os.path.basename(row.get("file") or "")
            rows.append(row)
    return rows


def _append_override(path: str, payload: dict):
    _ensure_csv_header(path)
    header = ["file", "anno", "tipo_documento", "rgnr", "procura", "destinazione", "note"]
    row = {k: (payload.get(k) or "") for k in header}
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writerow(row)


@app.route("/api/overrides", methods=["GET"])
@admin_required
def api_overrides():
    rows = _read_overrides(CORREZIONI_CSV)
    return json_ok(path=CORREZIONI_CSV, rows=rows)


@app.route("/api/override", methods=["POST"])
@admin_required
def api_override_add():
    payload = request.get_json(force=True)
    if payload.get("rgnr"):
        payload["rgnr"] = str(payload["rgnr"]).upper()
    if payload.get("destinazione"):
        payload["destinazione"] = str(payload["destinazione"]).upper()
    _append_override(CORREZIONI_CSV, payload)
    return json_ok()


@app.route("/api/pick-file", methods=["POST"])
@admin_required
def api_pick_file():
    path = _tk_pick_file()
    return json_ok(path=path if path else None)


@app.route("/api/pick-dest", methods=["POST"])
@admin_required
def api_pick_dest():
    path = _tk_pick_dir()
    return json_ok(path=path if path else None)


def _find_document_candidates_by_filename(cur, table, file_value: str):
    cols = get_table_columns(cur, table)
    candidates = []
    like = f"%{file_value}%"

    possible_cols = []
    for c in cols:
        lc = c.lower()
        if lc in ("nome_file", "file", "file_path", "filepath", "path") or "nome" in lc:
            possible_cols.append(c)

    if not possible_cols:
        possible_cols = cols

    where = " OR ".join([f"CAST({c} AS TEXT) LIKE ?" for c in possible_cols])
    params = [like] * len(possible_cols)

    cur.execute(f"SELECT rowid AS _rowid, * FROM {table} WHERE {where} LIMIT 50", params)
    rows = cur.fetchall()

    idx_nome = None
    for k in ("nome_file", "file", "file_path"):
        if k in cols:
            idx_nome = cols.index(k) + 1
            break

    for r in rows:
        rid = r[0]
        nome = r[idx_nome] if idx_nome is not None and idx_nome < len(r) else ""
        candidates.append({"id": rid, "nome_file": nome})
    return candidates


@app.route("/api/apply-override-to-db", methods=["POST"])
@admin_required
def api_apply_override_to_db():
    data = request.get_json(force=True)
    file_value = (data.get("file") or "").strip()
    target_id = data.get("target_id")

    if not file_value:
        return json_err("file mancante", 400)

    overrides = _read_overrides(CORREZIONI_CSV)
    base = os.path.basename(file_value)
    ov = None
    for r in overrides:
        if (r.get("file") or "").strip() == file_value or os.path.basename((r.get("file") or "").strip()) == base:
            ov = r
            break
    if not ov:
        return json_err("Override non trovato nel CSV", 404)

    if not os.path.exists(DOCUMENTI_DB):
        return json_err("documenti.db non trovato", 500)

    con = documenti_conn()
    cur = con.cursor()
    table, _tables = detect_documenti_table(cur)
    if not table:
        con.close()
        return json_err("Tabella documenti non trovata", 500)

    cols = get_table_columns(cur, table)

    if target_id is None:
        candidates = _find_document_candidates_by_filename(cur, table, base)
        if len(candidates) == 0:
            con.close()
            return json_err("Nessun record candidato trovato per questo file", 404)
        if len(candidates) > 1:
            con.close()
            return json_err("Conflitto: più record candidati", 409, candidates=candidates)
        target_id = candidates[0]["id"]

    try:
        target_id = int(str(target_id).strip())
    except Exception:
        con.close()
        return json_err("target_id non valido", 400)

    updates = {}
    mapping = {
        "anno": "anno",
        "tipo_documento": "tipo_documento",
        "rgnr": "rgnr",
        "procura": "procura",
        "note": "note",
        "destinazione": "destinazione",
    }
    for src, dst in mapping.items():
        if dst in cols:
            val = (ov.get(src) or "").strip()
            if val != "":
                updates[dst] = val

    if not updates:
        con.close()
        return json_err("Nessun campo valido da applicare", 400)

    bkp = backup_file(DOCUMENTI_DB)

    set_sql = ", ".join([f"{k}=?" for k in updates.keys()])
    params = list(updates.values())
    params.append(target_id)
    cur.execute(f"UPDATE {table} SET {set_sql} WHERE rowid=?", params)
    con.commit()
    con.close()

    return json_ok(backup_path=bkp, target_id=target_id, updated_fields=list(updates.keys()))


@app.route("/api/update-db", methods=["POST"])
@admin_required
def api_update_db():
    data = request.get_json(force=True)
    rid = data.get("id")
    if rid is None:
        return json_err("id mancante", 400)
    try:
        rid = int(str(rid).strip())
    except Exception:
        return json_err("id non valido", 400)

    if not os.path.exists(DOCUMENTI_DB):
        return json_err("documenti.db non trovato", 500)

    con = documenti_conn()
    cur = con.cursor()
    table, _tables = detect_documenti_table(cur)
    if not table:
        con.close()
        return json_err("Tabella documenti non trovata", 500)

    cols = get_table_columns(cur, table)
    allowed = set(cols)

    updates = {}
    for k, v in data.items():
        if k == "id":
            continue
        if k in allowed:
            if v is None:
                continue
            sv = str(v)
            if sv.strip() == "":
                continue
            updates[k] = sv

    if not updates:
        con.close()
        return json_err("Nessun campo da aggiornare", 400)

codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
    old_nome = None
    nome_col = "nome_file" if "nome_file" in allowed else None
    if nome_col:
        cur.execute(f"SELECT {nome_col} FROM {table} WHERE rowid=?", (rid,))
        row = cur.fetchone()
        old_nome = row[0] if row else None

    bkp = backup_file(DOCUMENTI_DB)

    renamed_to = None
    if nome_col and "nome_file" in updates and old_nome and updates["nome_file"] != old_nome:
        libreria_root = os.path.join(PROJECT_ROOT, "libreria")
        src_path = None
        for root, _dirs, files in os.walk(libreria_root):
            if old_nome in files:
                src_path = os.path.join(root, old_nome)
                break
        if src_path:
            dst_path = os.path.join(os.path.dirname(src_path), updates["nome_file"])
            if os.path.exists(dst_path):
                con.close()
                return json_err("Esiste già un file con questo nome nella stessa cartella", 400)
            os.replace(src_path, dst_path)
            renamed_to = dst_path

=======
    bkp = backup_file(DOCUMENTI_DB)

main
    set_sql = ", ".join([f"{k}=?" for k in updates.keys()])
    params = list(updates.values())
    params.append(rid)
    cur.execute(f"UPDATE {table} SET {set_sql} WHERE rowid=?", params)
    con.commit()
    con.close()

codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
    return json_ok(backup_path=bkp, updated_fields=list(updates.keys()), renamed_to=renamed_to)
=======
    return json_ok(backup_path=bkp, updated_fields=list(updates.keys()))
main


@app.route("/api/documenti/search", methods=["GET"])
@login_required
def api_documenti_search():
    q = (request.args.get("q") or "").strip()
    if not q:
        return json_ok(rows=[])
    if not os.path.exists(DOCUMENTI_DB):
        return json_err("documenti.db non trovato", 500)

    con = documenti_conn()
    cur = con.cursor()
    table, _ = detect_documenti_table(cur)
    if not table:
        con.close()
        return json_err("Tabella documenti non trovata", 500)
    cols = get_table_columns(cur, table)

    target_col = None
    for c in ("nome_file", "file", "file_path", "path"):
        if c in cols:
            target_col = c
            break
    if target_col is None:
        target_col = cols[0]

    like = f"%{q}%"
    cur.execute(f"SELECT rowid AS _rowid, * FROM {table} WHERE CAST({target_col} AS TEXT) LIKE ? ORDER BY _rowid DESC LIMIT 50", (like,))
    rows = cur.fetchall()
    con.close()
    return json_ok(table=table, columns=["_rowid"] + cols, rows=rows)


@app.route("/api/update-anno-and-move", methods=["POST"])
@admin_required
def api_update_anno_and_move():
    data = request.get_json(force=True)
    nome_file = (data.get("nome_file") or "").strip()
    anno = (data.get("anno") or "").strip()
    if not nome_file or not anno:
        return json_err("nome_file e anno obbligatori", 400)

    try:
        int(anno)
    except Exception:
        return json_err("anno non valido", 400)

    script_path = os.path.join(PROJECT_ROOT, "aggiorna_anno_file.py")
    if not os.path.exists(script_path):
        return json_err("Script aggiorna_anno_file.py non trovato", 500)

    cmd = [sys.executable, script_path, nome_file, anno]
    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        return json_err("Errore durante aggiornamento anno", 500, details=(proc.stderr or proc.stdout))

    return json_ok(message="Anno aggiornato e file spostato", output=proc.stdout.strip())


@app.route("/api/run-reset-errori", methods=["POST"])
@admin_required
def api_run_reset_errori():
    script_path = os.path.join(PROJECT_ROOT, "reset_errori_db.py")
    if not os.path.exists(script_path):
        return json_err("reset_errori_db.py non trovato", 500)

    data = request.get_json(silent=True) or {}
    cmd = [sys.executable, script_path]
    if bool(data.get("include_verbali")):
        cmd.append("--include-verbali")

    proc = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
    if proc.returncode != 0:
        return json_err("Errore durante reset errori", 500, details=(proc.stderr or proc.stdout))
    return json_ok(output=(proc.stdout or "").strip())


@app.route("/api/delete-db-row-and-requeue", methods=["POST"])
@admin_required
def api_delete_db_row_and_requeue():
    data = request.get_json(force=True)
    rid = data.get("id")
    if rid is None:
        return json_err("id mancante", 400)
    try:
        rid = int(str(rid).strip())
    except Exception:
        return json_err("id non valido", 400)

    if not os.path.exists(DOCUMENTI_DB):
        return json_err("documenti.db non trovato", 500)

    con = documenti_conn()
    cur = con.cursor()
    table, _ = detect_documenti_table(cur)
    if not table:
        con.close()
        return json_err("Tabella documenti non trovata", 500)

    cols = get_table_columns(cur, table)
    name_col = "nome_file" if "nome_file" in cols else ("file" if "file" in cols else None)
    if not name_col:
        con.close()
        return json_err("Colonna nome file non trovata", 500)

    cur.execute(f"SELECT {name_col} FROM {table} WHERE rowid=?", (rid,))
    row = cur.fetchone()
    if not row:
        con.close()
        return json_err("Riga non trovata", 404)
    nome_file = row[0]

    bkp = backup_file(DOCUMENTI_DB)
    cur.execute(f"DELETE FROM {table} WHERE rowid=?", (rid,))
    con.commit()
    con.close()

    moved_to = None
    if nome_file:
        src_path = None
        libreria_root = os.path.join(PROJECT_ROOT, "libreria")
        for root, _dirs, files in os.walk(libreria_root):
            if nome_file in files:
                src_path = os.path.join(root, nome_file)
                break
        if src_path:
            os.makedirs(INPUT_DIR, exist_ok=True)
            dst = os.path.join(INPUT_DIR, nome_file)
            if os.path.exists(dst):
                base, ext = os.path.splitext(nome_file)
                dst = os.path.join(INPUT_DIR, f"{base}_{int(time.time())}{ext}")
            shutil.move(src_path, dst)
            moved_to = dst

    return json_ok(deleted_id=rid, backup_path=bkp, requeued_to=moved_to, nome_file=nome_file)


if __name__ == "__main__":
    ensure_dirs()
    init_users_db()
    print(f"SANG_I.A. Login avviato: http://{HOST}:{PORT}")
    print(f"PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"DB utenti: {USERS_DB}")
    app.run(host=HOST, port=PORT, debug=False)
