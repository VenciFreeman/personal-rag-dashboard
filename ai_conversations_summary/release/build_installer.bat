@echo off
setlocal EnableExtensions
cd /d %~dp0

set "ROOT=%cd%"
set "TOPLEVEL_VENV_PY=%ROOT%\..\.venv\Scripts\python.exe"
set "TOPLEVEL_VENV_SITE=%ROOT%\..\.venv\Lib\site-packages"
set "LOCAL_VENV_PY=%ROOT%\.venv\Scripts\python.exe"
set "LOCAL_VENV_SITE=%ROOT%\.venv\Lib\site-packages"
set "VENV_PY="
set "VENV_SITE="
set "STAGING=%ROOT%\installer\staging"
set "ISS_FILE=%ROOT%\installer\AI-Summary-GUI.iss"

if exist "%TOPLEVEL_VENV_PY%" (
  set "VENV_PY=%TOPLEVEL_VENV_PY%"
  set "VENV_SITE=%TOPLEVEL_VENV_SITE%"
) else if exist "%LOCAL_VENV_PY%" (
  set "VENV_PY=%LOCAL_VENV_PY%"
  set "VENV_SITE=%LOCAL_VENV_SITE%"
)

if not defined VENV_PY (
  echo [ERROR] Missing venv Python.
  echo Checked:
  echo   %TOPLEVEL_VENV_PY%
  echo   %LOCAL_VENV_PY%
  echo Please create .venv and install dependencies first.
  pause
  exit /b 1
)

if not exist "%VENV_SITE%" (
  echo [ERROR] Missing venv site-packages: %VENV_SITE%
  pause
  exit /b 1
)

if not exist "%ISS_FILE%" (
  echo [ERROR] Missing Inno Setup script: %ISS_FILE%
  pause
  exit /b 1
)

echo [1/5] Resolve base Python installation path...
set "PY_BASE="
for /f "usebackq delims=" %%I in (`"%VENV_PY%" -c "import sys; print(sys.base_prefix)"`) do set "PY_BASE=%%I"
if "%PY_BASE%"=="" (
  echo [ERROR] Failed to resolve sys.base_prefix from venv.
  pause
  exit /b 1
)
if not exist "%PY_BASE%\pythonw.exe" (
  echo [ERROR] pythonw.exe not found in base Python: %PY_BASE%
  pause
  exit /b 1
)

echo [2/5] Prepare staging folder...
if exist "%STAGING%" rmdir /s /q "%STAGING%"
mkdir "%STAGING%\app" >nul 2>nul
mkdir "%STAGING%\python" >nul 2>nul

echo [3/5] Copy release app snapshot...
robocopy "%ROOT%\app" "%STAGING%\app" /MIR /R:1 /W:1 /NFL /NDL /NJH /NJS /NP
if errorlevel 8 (
  echo [ERROR] Failed to copy release\app to staging.
  pause
  exit /b 1
)

echo [4/5] Copy Python runtime and installed packages...
robocopy "%PY_BASE%" "%STAGING%\python" /MIR /R:1 /W:1 /NFL /NDL /NJH /NJS /NP
if errorlevel 8 (
  echo [ERROR] Failed to copy base Python runtime.
  pause
  exit /b 1
)

robocopy "%VENV_SITE%" "%STAGING%\python\Lib\site-packages" /MIR /R:1 /W:1 /NFL /NDL /NJH /NJS /NP
if errorlevel 8 (
  echo [ERROR] Failed to copy venv site-packages.
  pause
  exit /b 1
)

echo [5/5] Build installer with Inno Setup...
set "ISCC=%ProgramFiles(x86)%\Inno Setup 6\ISCC.exe"
if not exist "%ISCC%" set "ISCC=%ProgramFiles%\Inno Setup 6\ISCC.exe"
if not exist "%ISCC%" (
  echo [ERROR] Inno Setup 6 not found.
  echo Install from https://jrsoftware.org/isinfo.php
  pause
  exit /b 1
)

"%ISCC%" "%ISS_FILE%"
if errorlevel 1 (
  echo [ERROR] Installer build failed.
  pause
  exit /b 1
)

echo.
echo Installer build finished.
echo Output: %ROOT%\dist_installer\AI-Conversations-Summary-Setup.exe
pause
endlocal
