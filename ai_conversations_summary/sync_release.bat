@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "SYNC_PS1=%SCRIPT_DIR%scripts\sync_to_release.ps1"

if not exist "%SYNC_PS1%" (
    echo ERROR: sync script not found: "%SYNC_PS1%"
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%SYNC_PS1%" %*
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%
