@echo off
setlocal

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
    echo [ERROR] No working Python environment found for Library Tracker Web UI.
    echo         Please install dependencies in .venv or system Python:
    echo         python -m pip install fastapi uvicorn jinja2
    pause >nul
    exit /b 1
)

if defined PY_CHECK (
    start "Library Tracker Web" /min cmd /c "py -3 launch_web.py"
) else (
    start "Library Tracker Web" /min "%PY_LAUNCH%" "launch_web.py"
)

exit /b 0
