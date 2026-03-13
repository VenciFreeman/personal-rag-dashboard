@echo off
setlocal

rem Web UI quick launcher (double-click friendly, no Python console window).
rem Pick a working Python first, then open browser only after health check.

cd /d "%~dp0"

set "PY_LAUNCH="
set "PY_CHECK="
set "TOPLEVEL_PY=..\.venv\Scripts\python.exe"

if exist "%TOPLEVEL_PY%" (
    "%TOPLEVEL_PY%" -c "import fastapi,uvicorn,jinja2,openai; import web.main" >nul 2>nul
    if %errorlevel%==0 (
        set "PY_LAUNCH=%TOPLEVEL_PY%"
    )
)

if not defined PY_LAUNCH if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" -c "import fastapi,uvicorn,jinja2,openai; import web.main" >nul 2>nul
    if %errorlevel%==0 (
        set "PY_LAUNCH=.venv\Scripts\python.exe"
    )
)

if not defined PY_LAUNCH (
    where py >nul 2>nul
    if %errorlevel%==0 (
        py -3 -c "import fastapi,uvicorn,jinja2,openai; import web.main" >nul 2>nul
        if %errorlevel%==0 (
            set "PY_LAUNCH=py"
            set "PY_CHECK=1"
        )
    )
)

if not defined PY_LAUNCH (
    if exist "%TOPLEVEL_PY%" (
        echo [Nav Dashboard] Missing dependency detected, installing requirements with %TOPLEVEL_PY% ...
        "%TOPLEVEL_PY%" -m pip install -r requirements.txt
        "%TOPLEVEL_PY%" -c "import fastapi,uvicorn,jinja2,openai; import web.main" >nul 2>nul
        if %errorlevel%==0 (
            set "PY_LAUNCH=%TOPLEVEL_PY%"
        )
    )
)

if not defined PY_LAUNCH (
    echo [ERROR] No working Python environment found for Nav Dashboard Web UI.
    echo         Please install dependencies in .venv or system Python:
    echo         python -m pip install -r requirements.txt
    pause >nul
    exit /b 1
)

if defined PY_CHECK (
    start "Nav Dashboard Web" /min cmd /c "py -3 launch_web.py"
) else (
    start "Nav Dashboard Web" /min "%PY_LAUNCH%" "launch_web.py"
)

rem Browser auto-open is handled by launch_web.py to avoid duplicate windows.

exit /b 0
