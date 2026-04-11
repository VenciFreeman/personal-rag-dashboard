@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

set "PY_BOOTSTRAP="
if exist ".venv\Scripts\python.exe" goto have_venv

where py >nul 2>nul
if %errorlevel%==0 (
  set "PY_BOOTSTRAP=py -3"
) else (
  where python >nul 2>nul
  if %errorlevel% neq 0 (
    echo [error] Python launcher ^(`py`^) and `python` are both unavailable.
    exit /b 1
  )
  set "PY_BOOTSTRAP=python"
)

echo [setup] Creating root virtual environment...
call %PY_BOOTSTRAP% -m venv ".venv"
if %errorlevel% neq 0 exit /b %errorlevel%

:have_venv
set "PY_EXE=%CD%\.venv\Scripts\python.exe"
if not exist "%PY_EXE%" (
  echo [error] Root virtual environment is missing: %PY_EXE%
  exit /b 1
)

echo [setup] Upgrading pip...
call "%PY_EXE%" -m pip install --upgrade pip
if %errorlevel% neq 0 exit /b %errorlevel%

echo [setup] Installing workspace package...
call "%PY_EXE%" -m pip install -e .
if %errorlevel% neq 0 exit /b %errorlevel%

for %%R in (
  "ai_conversations_summary\requirements.txt"
  "library_tracker\requirements.txt"
  "nav_dashboard\requirements.txt"
  "property\requirements.txt"
  "journey\requirements.txt"
) do (
  if exist %%~R (
    echo [setup] Installing %%~R ...
    call "%PY_EXE%" -m pip install -r %%~R
    if !errorlevel! neq 0 exit /b !errorlevel!
  )
)

echo [done] Workspace setup complete.
echo [done] Launch with nav_dashboard\launch_web.bat or any app-specific launch_web.bat.
endlocal