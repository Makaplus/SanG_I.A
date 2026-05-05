@echo off
setlocal EnableExtensions

title SANGIA - START

cd /d "%~dp0"
set "PROJECT_ROOT=%CD%"

chcp 65001 >nul
set PYTHONUTF8=1

set "PY=%PROJECT_ROOT%\venv\Scripts\python.exe"
set "WEBAPP=%PROJECT_ROOT%\webapp\app.py"

echo ==========================================
echo SANGIA AVVIO
echo ROOT: %PROJECT_ROOT%
echo PY:   %PY%
echo ==========================================
echo.

if not exist "%PY%" (
    echo [ERRORE] VENV non trovato:
    echo %PY%
    echo Esegui prima install_sangia.bat
    pause
    exit /b 1
)

if not exist "%WEBAPP%" (
    echo [ERRORE] Non trovo webapp\app.py
    pause
    exit /b 1
)

if not exist "%PROJECT_ROOT%\organizzatore.py" (
    echo [ERRORE] Non trovo organizzatore.py
    pause
    exit /b 1
)

echo [INFO] Verifica sintassi Python (organizzatore.py + webapp\app.py)...
"%PY%" -m py_compile "%PROJECT_ROOT%\organizzatore.py" "%WEBAPP%"
if errorlevel 1 (
    echo.
    echo [ERRORE] File Python con errore di sintassi.
    echo [SUGGERIMENTO] Se usi Git: ripristina il file con
    echo   git checkout -- organizzatore.py
    echo poi rilancia start.bat
    pause
    exit /b 1
)

echo [INFO] Apro browser su http://127.0.0.1:8765
start "" "http://127.0.0.1:8765"

echo [INFO] Avvio Web Server (finestra corrente)
"%PY%" "%WEBAPP%"

exit /b %errorlevel%
