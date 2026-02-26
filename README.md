# SANGIA — Libreria Atti PG e Informative di Reato

Progetto operativo per:

- ingestione documenti giudiziari (atti PG, informative, verbali, allegati),
- smistamento automatico per anno e metadati,
- OCR + normalizzazione del testo,
- costruzione di una base interrogabile (SQLite + Vector DB),
- consultazione via web/admin e, successivamente, via agent AI.

---

## Stato attuale

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

---

## Flusso operativo consigliato

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

---

## Note operative

- Priorità: robustezza su documenti reali, non perfezione accademica del parser.
- Le regole di fallback (titolo file, date, pattern RGNR) sono intenzionalmente ridondanti.
- Prima di ogni update massivo al DB è consigliato backup automatico/manuale.
