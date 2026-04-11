@echo off
setlocal

rem Full-stack launcher (double-click friendly).
rem Behavior:
rem 1) Ensure root .venv exists and has all workspace dependencies.
rem 2) Start ai_conversations_summary / library_tracker / property / journey in background.
rem 3) Start nav_dashboard and let launch_web.py handle browser auto-open.

cd /d "%~dp0"

set "WORKSPACE_SETUP=..\setup_workspace.bat"
set "TOPLEVEL_PY=%CD%\..\.venv\Scripts\python.exe"

if not exist "%TOPLEVEL_PY%" goto bootstrap_workspace
call :check_env
if %errorlevel% neq 0 goto bootstrap_workspace
goto start_services

:bootstrap_workspace
if not exist "%WORKSPACE_SETUP%" (
    echo [ERROR] Missing workspace setup script: %WORKSPACE_SETUP%
    pause >nul
    exit /b 1
)

echo [Nav Dashboard] Bootstrapping workspace environment...
call "%WORKSPACE_SETUP%"
if %errorlevel% neq 0 (
    echo [ERROR] Workspace setup failed.
    pause >nul
    exit /b %errorlevel%
)

if not exist "%TOPLEVEL_PY%" (
    echo [ERROR] Root .venv Python not found after setup: %TOPLEVEL_PY%
    pause >nul
    exit /b 1
)

call :check_env
if %errorlevel% neq 0 (
    echo [ERROR] Dependency validation failed after setup.
    pause >nul
    exit /b 1
)

:start_services
echo [Nav Dashboard] Starting full workspace services...
set "PERSONAL_AI_STACK_DISABLE_BROWSER_OPEN=1"
start "AI Summary Web" /min /d "..\ai_conversations_summary" "%TOPLEVEL_PY%" "launch_web.py"
start "Library Tracker Web" /min /d "..\library_tracker" "%TOPLEVEL_PY%" "launch_web.py"
start "Property Web" /min /d "..\property" "%TOPLEVEL_PY%" "launch_web.py"
start "Journey Web" /min /d "..\journey" "%TOPLEVEL_PY%" "launch_web.py"
set "PERSONAL_AI_STACK_DISABLE_BROWSER_OPEN="
start "Nav Dashboard Web" /min "%TOPLEVEL_PY%" "launch_web.py"

rem Browser auto-open is handled by launch_web.py to avoid duplicate windows.

exit /b 0

:check_env
"%TOPLEVEL_PY%" -c "import fastapi,uvicorn,jinja2,openai; import web.main" >nul 2>nul
exit /b %errorlevel%
