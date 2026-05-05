$ErrorActionPreference = "Stop"
Write-Host "=== SANGIA INSTALLER ==="

if (!(Test-Path ".\requirements.txt")) { throw "requirements.txt NON trovato in root." }

function HasPy311 {
  try { py -3.11 -c "import sys; print(sys.version)" | Out-Null; return $true }
  catch { return $false }
}

if (!(Get-Command py -ErrorAction SilentlyContinue)) {
  throw "Il launcher 'py' non è disponibile. Reinstalla Python spuntando 'Install launcher'."
}

if (-not (HasPy311)) {
  Write-Host "Python 3.11 non trovato. Provo installazione con winget..."
  if (!(Get-Command winget -ErrorAction SilentlyContinue)) {
    throw "winget non disponibile. Installa Python 3.11 manualmente e rilancia."
  }
  winget install -e --id Python.Python.3.11 --accept-source-agreements --accept-package-agreements
  Start-Sleep -Seconds 2
  if (-not (HasPy311)) {
    throw "Python 3.11 installato ma 'py -3.11' non risponde ancora. Chiudi e riapri il terminale e rilancia."
  }
}

if (!(Test-Path ".\venv\Scripts\python.exe")) {
  Write-Host "Creo venv..."
  py -3.11 -m venv venv
}

$venvPy = ".\venv\Scripts\python.exe"
if (!(Test-Path $venvPy)) { throw "venv non creato correttamente (python.exe mancante)." }

Write-Host "Aggiorno pip..."
& $venvPy -m pip install --upgrade pip setuptools wheel

Write-Host "Installo requirements..."
& $venvPy -m pip install -r requirements.txt


Write-Host "Verifico sintassi file principali..."
& $venvPy -m py_compile organizzatore.py webapp\app.py

Write-Host "Fatto. Per avviare:"
Write-Host "  venv\Scripts\activate"
Write-Host "  python organizzatore.py"
