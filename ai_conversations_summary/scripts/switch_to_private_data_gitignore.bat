@echo off
setlocal

rem Apply private-data branch gitignore policy from template file.
rem It also saves current .gitignore as .gitignore.main.backup.
rem Usage:
rem   .\scripts\switch_to_private_data_gitignore.bat
rem   .\scripts\switch_to_private_data_gitignore.bat --dry-run

set "SCRIPT_DIR=%~dp0"
set "WORKSPACE_DIR=%SCRIPT_DIR%.."
set "EXAMPLE_FILE=%WORKSPACE_DIR%\.gitignore.private-data-branch.example"
set "TARGET_FILE=%WORKSPACE_DIR%\.gitignore"
set "BACKUP_FILE=%WORKSPACE_DIR%\.gitignore.main.backup"
set "DRY_RUN=0"

rem Support a lightweight preview mode for safer branch operations.
if /I "%~1"=="--dry-run" set "DRY_RUN=1"
if /I "%~1"=="-n" set "DRY_RUN=1"

if not exist "%EXAMPLE_FILE%" (
    echo ERROR: example file not found: "%EXAMPLE_FILE%"
    exit /b 1
)

if "%DRY_RUN%"=="1" (
    echo [DRY-RUN] Would backup "%TARGET_FILE%" to "%BACKUP_FILE%" if target exists.
    echo [DRY-RUN] Would copy "%EXAMPLE_FILE%" to "%TARGET_FILE%".
    echo [DRY-RUN] Done.
    exit /b 0
)

rem Keep a backup of the previous policy before overwriting .gitignore.
if exist "%TARGET_FILE%" (
    copy /Y "%TARGET_FILE%" "%BACKUP_FILE%" >nul
    if errorlevel 1 (
        echo ERROR: failed to backup current .gitignore to "%BACKUP_FILE%"
        exit /b 1
    )
)

copy /Y "%EXAMPLE_FILE%" "%TARGET_FILE%" >nul
if errorlevel 1 (
    echo ERROR: failed to apply private-data gitignore.
    exit /b 1
)

echo Applied private-data gitignore policy.
echo Backup saved at: "%BACKUP_FILE%"
echo Next step: git add .gitignore ^&^& git commit -m "Use private-data branch gitignore policy"

endlocal & exit /b 0
