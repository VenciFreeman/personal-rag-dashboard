@echo off
setlocal
cd /d "%~dp0.."
call "setup_workspace.bat" %*
endlocal