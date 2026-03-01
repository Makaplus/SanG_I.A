# SANGIA — Libreria Atti PG e Informative di Reato

Progetto operativo per:

- ingestione documenti giudiziari (atti PG, informative, verbali, allegati),
- smistamento automatico per anno e metadati,
- OCR + normalizzazione del testo,
- costruzione di una base interrogabile (SQLite + Vector DB),
codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
- consultazione via web/admin.
=======
- consultazione via web/admin e, successivamente, via agent AI.
main

---

## Stato attuale

codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
La codebase è in **fase operativa**: il flusso base ingestione → classificazione → rename/move → revisione è già utilizzabile.

---

## Struttura reale attuale (come da tua installazione)

```text
SANGIA/
├─ input_documenti/
├─ libreria/
├─ chroma_db/
├─ Backup/
├─ script/
│  ├─ 01_reader.py
│  ├─ 02_preparatore.py
│  ├─ 03_embeder.py
│  ├─ start.sh
│  └─ workers/
│     ├─ organizzatore_altro_probe.py
│     ├─ organizzatore_doc_reader.py
│     ├─ organizzatore_occ_ocr_rgnr.py
│     ├─ organizzatore_percorso.py
│     ├─ organizzatore_rename.py
│     ├─ organizzatore_report_errori.py
│     ├─ organizzatore_revisione_router.py
│     ├─ organizzatore_vari_verbali.py
│     ├─ parser_targhe.py
│     ├─ reader_dates.py
│     ├─ reader_dialogues.py
│     ├─ reader_images.py
│     ├─ reader_notes.py
│     ├─ reader_ocr.py
│     ├─ reader_ocr2.py
│     ├─ reader_ocr3.py
│     ├─ reader_pdf.py
│     ├─ reader_persone.py
│     └─ reader_sdi.py
├─ tools/
├─ webapp/
├─ start.bat
├─ organizzatore.py
├─ organizzatore_old.py
├─ revisione.py
├─ smistamento_web.py
├─ migrate.py
├─ reset_errori_db.py
├─ aggiorna_anno_file.py
├─ config.py
├─ config.ini
├─ requirements.txt
└─ README.md
```

=======
La codebase è in **fase embrionale ma già utilizzabile** per:

1. avvio ambiente locale;
2. import documenti in cartella input;
3. smistamento automatico in libreria;
4. tracciamento su database;
5. utility di backup/ripristino e correzione errori.

---

## Struttura progetto (allineata all'installazione Windows)

Struttura prevista sotto `SANGIA/`:

```text
SANGIA/
├─ input_documenti/              # ingresso file da processare
├─ libreria/                     # output organizzato + documenti.db
├─ chroma_db/                    # database vettoriale
├─ Backup/                       # backup DB/log/correzioni
├─ webapp/                       # app Flask (login, admin, consulta)
│  ├─ app.py                     # server web + API utenti/documenti/job
│  ├─ users.db                   # DB utenti (creato al primo avvio)
│  ├─ template/                  # pagine HTML
│  └─ static/                    # css + immagini UI
├─ script/                       # script di supporto
├─ venv/                         # ambiente virtuale locale
├─ start.bat                     # avvio orchestrato su Windows
├─ organizzatore.py              # motore principale di smistamento
├─ smistamento_web.py            # server admin/API locale per smistamento
├─ reset_errori_db.py            # ripristino file+record per rielaborazione
├─ aggiorna_anno_file.py         # corregge anno: sposta file + aggiorna DB
├─ migrate.py                    # migrazione/idempotenza schema SQLite
├─ config.py                     # costanti e utility globali
├─ config.ini                    # percorsi operativi locali
└─ .env                          # impostazioni runtime (LLM/embedding/chroma)
```

> Nota: in repository possono esserci file aggiuntivi di sviluppo (script POSIX), ma il flusso operativo reale resta quello sopra.

main
---

## Flusso operativo consigliato

codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
1. **Ingest**: copia file in `input_documenti/`.
2. **Smistamento principale**: esegui `organizzatore.py`.
3. **Pipeline OCC**:
   - estrazione OCR/RGNR/procura,
   - rename canonico,
   - spostamento in `libreria/<ANNO>/`.
4. **Pipeline NON-OCC**:
   - probe leggero,
   - move in `vari_verbali` o cartella anno (se disponibile).
5. **Revisione**:
   - i casi incompleti vanno in `libreria/revisione/occ`,
   - seconda passata con router revisione.

---

## Script principali

- `organizzatore.py`: orchestratore principale, DB-aware.
- `revisione.py`: revisione OCR forte + classificazione + hint RGNR.
- `script/workers/organizzatore_occ_ocr_rgnr.py`: estrazione OCC specifica.
- `script/workers/organizzatore_rename.py`: naming canonico OCC e label-fix NON-OCC.
- `script/workers/organizzatore_percorso.py`: move in cartelle anno.
- `script/workers/organizzatore_vari_verbali.py`: fallback NON-OCC.
- `script/workers/organizzatore_revisione_router.py`: retry/reinstradamento revisione.
- `script/01_reader.py`, `02_preparatore.py`, `03_embeder.py`: pipeline dati/testo/embedding.

---

## Idee per assemblarlo meglio (pratiche e immediate)

### 1) Uniforma gli entrypoint
- Mantieni **un solo entrypoint ufficiale** (`start.bat` su Windows, `script/start.sh` su Linux).
- Fai chiamare sempre lo stesso ordine: migrate → organizzatore → report → revisione-router.

### 2) Separa chiaramente i layer
- `orchestrator/`: logica di flusso (`organizzatore.py`, `revisione.py`).
- `workers/`: task atomici DB-aware (rename, OCR, move, probe).
- `readers/`: parser semantici (`reader_*`, `parser_targhe.py`).
- `web/`: Flask e template.

### 3) Convenzioni stato DB (un dizionario unico)
- Centralizza status (`CLASSIFIED`, `READY`, `NEEDS_DEEPER_ANALYSIS`, `REVISIONE_OCC`, `STORED`, ...)
  in un modulo comune per evitare drift tra script.

### 4) Tracciamento job unico
- Aggiungi tabella `job_runs` (job_id, started_at, ended_at, ok, errors, note).
- Ogni worker riceve `--job-id` e scrive eventi: debug molto più facile.

### 5) Riduci duplicazioni regex/OCR
- Estrai in `script/workers/common_patterns.py`:
  - regex RGNR,
  - normalizzazione anno,
  - utilità OCR.
- Così eviti divergenze tra `revisione.py`, `altro_probe.py`, `occ_ocr_rgnr.py`.

### 6) README operativo “giornaliero”
- Mantieni nel README solo:
  - struttura,
  - comandi avvio,
  - troubleshooting tipico.
- Sposta dettagli tecnici lunghi in `docs/ARCHITETTURA.md` e `docs/WORKERS.md`.

---

## Comandi base

```bash
# setup
pip install -r requirements.txt

# avvio orchestratore
python organizzatore.py

# revisione manuale/assistita
python revisione.py --only-status REVISIONE_OCC
```
=======
1. **Avvio**
   - usare `start.bat` (Windows) dalla root progetto;
   - il batch avvia il web server locale e poi lancia lo smistamento.

2. **Import documenti**
   - via browser/API (`smistamento_web.py`) oppure copia diretta in `input_documenti`.

3. **Smistamento**
   - `organizzatore.py` legge i file, estrae testo (digitale/OCR), rileva anno/RGNR e sposta in `libreria/<anno>/...`;
   - aggiorna SQLite (`libreria/documenti.db`) con metadati e segnali qualità testo.

4. **Gestione anomalie**
   - file non classificabili in cartelle errore;
   - `reset_errori_db.py` consente rientro in input + pulizia record DB;
   - override manuali via CSV per correzioni guidate.

5. **Indicizzazione semantica (step successivo)**
   - uso di `chroma_db` + embedding model per interrogazione semantica;
   - integrazione con agente conversazionale su dominio giudiziario.

---

## Configurazione

### 1) Python e virtualenv

È consigliato Python **3.10+**.

### 2) Dipendenze unificate

Da ora il progetto usa **un solo file**:

```bash
pip install -r requirements.txt
```

Questo sostituisce la gestione frammentata precedente (`requirements.txt`, `requirements_embed.txt`, `requirements_ocr.txt`).

### 3) File di ambiente

Verificare in `.env` i percorsi e i modelli:

- `LIBRERIA_DIR`
- `KB_JSONL`
- `CHROMA_DIR`
- `CHROMA_COLLECTION`
- `EMBED_MODEL`
- `LLM_MODEL`

### 4) Config locale path

`config.ini` governa i percorsi di lavoro reali su macchina Windows (input, libreria, cartelle tecniche).

---

## Script principali

- `start.bat`: bootstrap operativo completo (web + smistamento).
- `organizzatore.py`: smistatore con modalità singola o `--watch` (in ascolto).
- `smistamento_web.py`: compat entry-point che avvia la webapp Flask principale.
- `aggiorna_anno_file.py`: sposta fisicamente file nella cartella anno corretta e aggiorna `documenti.db`.
- `reset_errori_db.py`: rimette in input i file da cartelle errore e resetta record DB.
- `migrate.py`: migrazione schema DB in modalità idempotente.

---

## Roadmap breve

- consolidare modulo OCR worker-based (queue + parallelismo controllato);
- migliorare estrazione metadati processuali (RGNR, procura, tipo atto, qualità OCR);
- pipeline stabile per vectorization incrementale;
- endpoint interrogazione unificato per agente AI con filtri giuridici.
main

---

## Note operative

codex/iniziare-progetto-libreria-atti-di-polizia-sri8es
- Prima di run massivi: backup di `libreria/documenti.db`.
- Le regex sono volutamente ridondanti per robustezza su OCR sporco.
- In caso di mismatch file/DB, privilegiare sempre lookup SHA1 + `percorso_file`.
=======
- Priorità: robustezza su documenti reali, non perfezione accademica del parser.
- Le regole di fallback (titolo file, date, pattern RGNR) sono intenzionalmente ridondanti.
- Prima di ogni update massivo al DB è consigliato backup automatico/manuale.
main
