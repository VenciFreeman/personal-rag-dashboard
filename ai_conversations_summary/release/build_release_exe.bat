@echo off
setlocal
cd /d %~dp0

set "PY_CMD=python"
if exist "..\.venv\Scripts\python.exe" set "PY_CMD=..\.venv\Scripts\python.exe"
if "%PY_CMD%"=="python" if exist ".venv\Scripts\python.exe" set "PY_CMD=.venv\Scripts\python.exe"

"%PY_CMD%" -m PyInstaller --version >nul 2>nul
if errorlevel 1 (
  echo PyInstaller not found. Installing...
  "%PY_CMD%" -m pip install pyinstaller
  if errorlevel 1 (
    echo Failed to install PyInstaller.
    pause
    exit /b 1
  )
)

set "WORK_DIR=%cd%\.pyi_build"
set "DIST_DIR=%cd%\dist"
if exist "%WORK_DIR%" rmdir /s /q "%WORK_DIR%"
if exist "%DIST_DIR%\AI-Summary-GUI" rmdir /s /q "%DIST_DIR%\AI-Summary-GUI"

"%PY_CMD%" -m PyInstaller --noconfirm --clean --windowed --name AI-Summary-GUI --workpath "%WORK_DIR%" --distpath "%DIST_DIR%" "%cd%\app\scripts\gui_launcher.py"
if errorlevel 1 (
  echo Build failed.
  pause
  exit /b 1
)

echo.
echo Build finished.
echo Run: %DIST_DIR%\AI-Summary-GUI\AI-Summary-GUI.exe
start "" "%DIST_DIR%\AI-Summary-GUI"
pause
