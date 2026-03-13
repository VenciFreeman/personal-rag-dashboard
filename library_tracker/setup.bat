@echo off
setlocal
cd /d "%~dp0"

if "%1"=="" (
  python scripts\setup_env.py
) else (
  python scripts\setup_env.py %*
)

endlocal
