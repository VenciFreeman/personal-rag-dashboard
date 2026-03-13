@echo off
setlocal
cd /d %~dp0

REM Always build with project venv Python first (fallback to system python).
set "PY_CMD=python"
if exist "..\.venv\Scripts\python.exe" set "PY_CMD=..\.venv\Scripts\python.exe"
if "%PY_CMD%"=="python" if exist ".venv\Scripts\python.exe" set "PY_CMD=.venv\Scripts\python.exe"

set "WORK_DIR=.pyi_build"
set "DIST_DIR=release"

REM Ensure PyInstaller exists in selected Python environment.
"%PY_CMD%" -m PyInstaller --version >nul 2>nul
if errorlevel 1 (
	echo PyInstaller not found. Installing into current environment...
	"%PY_CMD%" -m pip install pyinstaller
	if errorlevel 1 (
		echo Failed to install PyInstaller.
		echo Please run manually: "%PY_CMD%" -m pip install pyinstaller
		pause
		exit /b 1
	)
)

REM Clean old artifacts to avoid mixing stale build outputs.
if exist "%WORK_DIR%" rmdir /s /q "%WORK_DIR%"
if exist "%DIST_DIR%\ai_summary_gui" rmdir /s /q "%DIST_DIR%\ai_summary_gui"

REM Build GUI executable (window mode, no console).
"%PY_CMD%" -m PyInstaller --noconfirm --clean --windowed --name ai_summary_gui --workpath "%WORK_DIR%" --distpath "%DIST_DIR%" gui_launcher.py
if errorlevel 1 (
	echo Build failed.
	pause
	exit /b 1
)

echo.
echo Build finished.
echo Run ONLY: %DIST_DIR%\ai_summary_gui\ai_summary_gui.exe
if exist "build\ai_summary_gui\ai_summary_gui.exe" (
	echo Do NOT run legacy file: build\ai_summary_gui\ai_summary_gui.exe
)
start "" "%cd%\%DIST_DIR%\ai_summary_gui"
pause
