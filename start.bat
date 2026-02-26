@echo off
setlocal EnableExtensions

title SANGIA - START

cd /d "%~dp0"
set "PROJECT_ROOT=%CD%"

chcp 65001 >nul
set PYTHONUTF8=1

set "PY=%PROJECT_ROOT%\venv\Scripts\python.exe"

echo ==========================================
echo SANGIA AVVIO
echo ROOT: %PROJECT_ROOT%
echo PY:   %PY%
echo ==========================================
echo.

if not exist "%PY%" (
    echo [ERRORE] VENV non trovato:
    echo %PY%
    pause
    exit /b 1
)

set "WEBAPP=%PROJECT_ROOT%\webapp\app.py"
if not exist "%WEBAPP%" (
    echo [ERRORE] Non trovo webapp\app.py
    pause
    exit /b 1
)

echo [INFO] Apro browser su http://127.0.0.1:8765
start "" "http://127.0.0.1:8765"

echo [INFO] Avvio Web Server (finestra corrente)
"%PY%" "%WEBAPP%"

exit /b %errorlevel%
