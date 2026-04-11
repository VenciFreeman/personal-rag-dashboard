@echo off
setlocal
cd /d "%~dp0"

if exist "..\setup_workspace.bat" (
  call "..\setup_workspace.bat" %*
) else if "%1"=="" (
  python scripts\setup\setup_env.py
) else (
  python scripts\setup\setup_env.py %*
)

endlocal
