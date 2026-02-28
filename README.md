# SANGIA — Libreria Atti PG e Informative di Reato

Progetto operativo per:

- ingestione documenti giudiziari (atti PG, informative, verbali, allegati),
- smistamento automatico per anno e metadati,
- OCR + normalizzazione del testo,
- costruzione di una base interrogabile (SQLite + Vector DB),
- consultazione via web/admin.

---

## Stato attuale

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

---

## Flusso operativo consigliato

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

---

## Note operative

- Prima di run massivi: backup di `libreria/documenti.db`.
- Le regex sono volutamente ridondanti per robustezza su OCR sporco.
- In caso di mismatch file/DB, privilegiare sempre lookup SHA1 + `percorso_file`.
