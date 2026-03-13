@echo off
setlocal

rem Remove stale vector entries whose source markdown files no longer exist.
rem Usage:
rem   .\scripts\prune_stale_vectors.bat
rem   .\scripts\prune_stale_vectors.bat --dry-run

set "SCRIPT_DIR=%~dp0"
set "WORKSPACE_DIR=%SCRIPT_DIR%.."
set "PY_SCRIPT=%SCRIPT_DIR%prune_stale_vectors.py"
set "TOPLEVEL_VENV_PY=%WORKSPACE_DIR%\..\.venv\Scripts\python.exe"
set "LOCAL_VENV_PY=%WORKSPACE_DIR%\.venv\Scripts\python.exe"
set "VENV_PY="

if exist "%TOPLEVEL_VENV_PY%" (
    set "VENV_PY=%TOPLEVEL_VENV_PY%"
) else if exist "%LOCAL_VENV_PY%" (
    set "VENV_PY=%LOCAL_VENV_PY%"
)

rem Fail fast if the python entry script is missing.
if not exist "%PY_SCRIPT%" (
    echo ERROR: script not found: "%PY_SCRIPT%"
    exit /b 1
)

rem Prefer project venv for deterministic dependency resolution.
if defined VENV_PY (
    "%VENV_PY%" "%PY_SCRIPT%" %*
) else (
    rem Fallback for environments without local .venv.
    py -3 "%PY_SCRIPT%" %*
)

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
