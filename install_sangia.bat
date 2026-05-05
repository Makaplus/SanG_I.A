@echo off
setlocal EnableExtensions

cd /d "%~dp0"
echo [SANGIA] Cartella: %CD%

REM 1) Verifica winget
where winget >nul 2>nul
if errorlevel 1 (
  echo [ERRORE] winget non disponibile. Installa "App Installer" e riprova.
  pause
  exit /b 1
)

REM 2) Verifica presenza Python 3.11 (py launcher)
py -3.11 -c "import sys; print(sys.version)" >nul 2>nul
if errorlevel 1 (
  echo [SANGIA] Python 3.11 non trovato. Installazione con winget...
  winget install -e --id Python.Python.3.11 --accept-source-agreements --accept-package-agreements
  if errorlevel 1 (
    echo [ERRORE] Installazione Python 3.11 fallita.
    pause
    exit /b 1
  )
) else (
  echo [SANGIA] Python 3.11 presente.
)

REM 3) Crea venv se manca
if not exist "venv\Scripts\python.exe" (
  echo [SANGIA] Creo venv...
  py -3.11 -m venv venv
  if errorlevel 1 (
    echo [ERRORE] Creazione venv fallita.
    pause
    exit /b 1
  )
) else (
  echo [SANGIA] venv gia' presente.
)

REM 4) Installa requirements
if not exist "requirements.txt" (
  echo [ERRORE] requirements.txt non trovato in %CD%
  pause
  exit /b 1
)

echo [SANGIA] Aggiorno pip...
"venv\Scripts\python.exe" -m pip install --upgrade pip setuptools wheel

echo [SANGIA] Installo requirements...
"venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
  echo [ERRORE] Installazione requirements fallita.
  pause
  exit /b 1
)

echo.


echo [SANGIA] Verifico sintassi file principali...
"venv\Scripts\python.exe" -m py_compile organizzatore.py webapp\app.py
if errorlevel 1 (
  echo [ERRORE] Sintassi non valida in uno script Python principale.
  pause
  exit /b 1
)

echo [SANGIA] INSTALLAZIONE COMPLETATA.
echo Per avviare:
echo   call venv\Scripts\activate
echo   python organizzatore.py
echo.
pause
endlocal
