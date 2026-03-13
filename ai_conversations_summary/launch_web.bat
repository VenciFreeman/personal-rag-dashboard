@echo off
setlocal

rem Web UI quick launcher (double-click friendly, no Python console window).
rem Pick a working Python first, then open browser only after health check.

cd /d "%~dp0"

set "PY_LAUNCH="
set "PY_CHECK="
set "TOPLEVEL_PY=..\.venv\Scripts\python.exe"

if exist "%TOPLEVEL_PY%" (
    "%TOPLEVEL_PY%" -c "import fastapi,uvicorn,jinja2; import web.main" >nul 2>nul
    if %errorlevel%==0 (
        set "PY_LAUNCH=%TOPLEVEL_PY%"
    )
)

if not defined PY_LAUNCH if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" -c "import fastapi,uvicorn,jinja2; import web.main" >nul 2>nul
    if %errorlevel%==0 (
        set "PY_LAUNCH=.venv\Scripts\python.exe"
    )
)

if not defined PY_LAUNCH (
    where py >nul 2>nul
    if %errorlevel%==0 (
        py -3 -c "import fastapi,uvicorn,jinja2; import web.main" >nul 2>nul
        if %errorlevel%==0 (
            set "PY_LAUNCH=py"
            set "PY_CHECK=1"
        )
    )
)

if not defined PY_LAUNCH (
    echo [ERROR] No working Python environment found for Web UI.
    echo         Please install dependencies in .venv or system Python:
    echo         python -m pip install fastapi uvicorn jinja2
    echo Press any key to close...
    pause >nul
    exit /b 1
)

if defined PY_CHECK (
    start "AI Summary Web" /min cmd /c "py -3 launch_web.py"
) else (
    start "AI Summary Web" /min "%PY_LAUNCH%" "launch_web.py"
)

rem Browser auto-open is handled by launch_web.py to avoid duplicate windows.

exit /b 0
