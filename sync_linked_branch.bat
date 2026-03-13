@echo off
setlocal
cd /d "%~dp0"

set "TARGET_BRANCH=%~1"
if "%TARGET_BRANCH%"=="" (
  echo Usage: sync_linked_branch.bat ^<branch^>
  echo Example: sync_linked_branch.bat private-data
  exit /b 1
)

echo [sync] target branch: %TARGET_BRANCH%
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0switch_linked_branch.ps1" -Branch "%TARGET_BRANCH%"
if errorlevel 1 exit /b %errorlevel%

git submodule update --init --recursive --remote
if errorlevel 1 exit /b %errorlevel%

echo [done] synced submodules to branch: %TARGET_BRANCH%
exit /b 0
