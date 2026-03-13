@echo off
setlocal
cd /d "%~dp0"

for /f "delims=" %%b in ('git branch --show-current') do set "CUR_BRANCH=%%b"

if "%CUR_BRANCH%"=="" (
  echo [ERROR] Cannot determine current branch.
  exit /b 1
)

echo [sync] super repo current branch: %CUR_BRANCH%
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0switch_linked_branch.ps1" -Branch "%CUR_BRANCH%"
if errorlevel 1 exit /b %errorlevel%

git submodule update --init --recursive --remote
if errorlevel 1 exit /b %errorlevel%

echo [done] synced submodules to branch: %CUR_BRANCH%
exit /b 0
