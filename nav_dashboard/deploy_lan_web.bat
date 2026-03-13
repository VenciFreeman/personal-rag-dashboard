@echo off
setlocal

rem LAN deployment launcher for Navigation Dashboard.
rem Behavior:
rem 1) Ensure admin rights and open firewall inbound rules.
rem 2) Start AI Summary + Library Tracker in background (no browser auto-open).
rem 3) Start Nav Dashboard in foreground and auto-open only Nav Dashboard page.

cd /d "%~dp0"

set "NAV_DASHBOARD_WEB_HOST=0.0.0.0"
set "NAV_DASHBOARD_WEB_PORT=8092"
set "AI_SUMMARY_WEB_HOST=0.0.0.0"
set "AI_SUMMARY_WEB_PORT=8000"
set "LIBRARY_WEB_HOST=0.0.0.0"
set "LIBRARY_WEB_PORT=8091"

set "TOPLEVEL_PY=..\.venv\Scripts\python.exe"
set "LOCAL_PY=.venv\Scripts\python.exe"
set "PY_FOR_FIREWALL="
set "PY_FOR_RUN="
set "PY_FOR_RUN_ABS="

if exist "%TOPLEVEL_PY%" (
    set "PY_FOR_FIREWALL=%CD%\..\.venv\Scripts\python.exe"
    set "PY_FOR_RUN=%TOPLEVEL_PY%"
    set "PY_FOR_RUN_ABS=%CD%\..\.venv\Scripts\python.exe"
) else if exist "%LOCAL_PY%" (
    set "PY_FOR_FIREWALL=%CD%\.venv\Scripts\python.exe"
    set "PY_FOR_RUN=%LOCAL_PY%"
    set "PY_FOR_RUN_ABS=%CD%\.venv\Scripts\python.exe"
)

echo [Nav Dashboard] Preparing LAN deployment ...
echo   Nav Dashboard: %NAV_DASHBOARD_WEB_HOST%:%NAV_DASHBOARD_WEB_PORT%
echo   AI Summary:    %AI_SUMMARY_WEB_HOST%:%AI_SUMMARY_WEB_PORT%
echo   Library:       %LIBRARY_WEB_HOST%:%LIBRARY_WEB_PORT%

set "IS_ADMIN=0"
rem Check admin rights. If missing, continue startup but skip firewall rule changes.
net session >nul 2>nul
if %errorlevel%==0 set "IS_ADMIN=1"

if "%IS_ADMIN%"=="0" (
    echo [WARN] Not running as Administrator.
    echo [WARN] Firewall rules will be skipped. Local machine can still open the page.
    echo [WARN] For LAN access from other devices, right-click this file and choose "Run as administrator".
)

if "%IS_ADMIN%"=="1" goto FIREWALL_SETUP
echo [Nav Dashboard] Skipping firewall rule setup (non-admin mode).
goto FIREWALL_DONE

:FIREWALL_SETUP
rem Add firewall inbound rules (safe to run repeatedly).
netsh advfirewall firewall delete rule name="Nav Dashboard Web %NAV_DASHBOARD_WEB_PORT%" >nul 2>nul
netsh advfirewall firewall add rule name="Nav Dashboard Web %NAV_DASHBOARD_WEB_PORT%" dir=in action=allow protocol=TCP localport=%NAV_DASHBOARD_WEB_PORT% profile=any >nul 2>nul
netsh advfirewall firewall delete rule name="AI Summary Web %AI_SUMMARY_WEB_PORT%" >nul 2>nul
netsh advfirewall firewall add rule name="AI Summary Web %AI_SUMMARY_WEB_PORT%" dir=in action=allow protocol=TCP localport=%AI_SUMMARY_WEB_PORT% profile=any >nul 2>nul
netsh advfirewall firewall delete rule name="Library Tracker Web %LIBRARY_WEB_PORT%" >nul 2>nul
netsh advfirewall firewall add rule name="Library Tracker Web %LIBRARY_WEB_PORT%" dir=in action=allow protocol=TCP localport=%LIBRARY_WEB_PORT% profile=any >nul 2>nul
if defined PY_FOR_FIREWALL (
    netsh advfirewall firewall delete rule name="Nav Dashboard Python Inbound" >nul 2>nul
    netsh advfirewall firewall add rule name="Nav Dashboard Python Inbound" dir=in action=allow program="%PY_FOR_FIREWALL%" profile=any >nul 2>nul
)

:FIREWALL_DONE

echo [Nav Dashboard] Stopping previous deployment instances on ports %AI_SUMMARY_WEB_PORT%, %LIBRARY_WEB_PORT%, %NAV_DASHBOARD_WEB_PORT% ...
rem Attempt graceful shutdown of Nav Dashboard first (prevents Chrome SSE crash on active pages)
powershell -NoProfile -Command "try { Invoke-RestMethod -Uri 'http://127.0.0.1:%NAV_DASHBOARD_WEB_PORT%/api/shutdown' -Method POST -TimeoutSec 1 2>$null | Out-Null } catch {}" >nul 2>nul
timeout /t 2 /nobreak >nul
call :STOP_PORT_LISTENERS %AI_SUMMARY_WEB_PORT%
call :STOP_PORT_LISTENERS %LIBRARY_WEB_PORT%
call :STOP_PORT_LISTENERS %NAV_DASHBOARD_WEB_PORT%
echo [Nav Dashboard] Previous instances cleanup complete.
echo.

echo [Nav Dashboard] LAN access URLs (same subnet):
set "HAS_RECOMMENDED=0"
setlocal EnableDelayedExpansion
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /R /C:"IPv4 Address" /C:"IPv4.*:"') do (
    set "ip=%%a"
    set "ip=!ip: =!"
    if not "!ip!"=="" (
        if "!ip:~0,8!"=="192.168." (
            if "!HAS_RECOMMENDED!"=="0" echo   Recommended:
            echo   Dashboard: http://!ip!:%NAV_DASHBOARD_WEB_PORT%/
            echo   AI Summary: http://!ip!:%AI_SUMMARY_WEB_PORT%/
            echo   Library:    http://!ip!:%LIBRARY_WEB_PORT%/
            set "HAS_RECOMMENDED=1"
        )
    )
)
endlocal

echo   All local IPv4 (Dashboard):
setlocal EnableDelayedExpansion
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /R /C:"IPv4 Address" /C:"IPv4.*:"') do (
    set "ip=%%a"
    set "ip=!ip: =!"
    if not "!ip!"=="" echo   http://!ip!:%NAV_DASHBOARD_WEB_PORT%/
)
endlocal
echo.

if defined PY_FOR_RUN (
    "%PY_FOR_RUN%" -c "import fastapi,uvicorn,jinja2,openai; import web.main" >nul 2>nul
    if not %errorlevel%==0 (
        echo [Nav Dashboard] Missing dependency detected, installing requirements ...
        "%PY_FOR_RUN%" -m pip install -r requirements.txt
        "%PY_FOR_RUN%" -c "import fastapi,uvicorn,jinja2,openai; import web.main" >nul 2>nul
        if not %errorlevel%==0 (
            echo [ERROR] Dependency check still failed after install. Please run:
            echo         %PY_FOR_RUN% -m pip install -r requirements.txt
            exit /b 1
        )
    )

    rem Silent mode: use Start-Process hidden with python.exe to avoid extra windows
    rem and keep uvicorn startup reliable on environments where pythonw -m uvicorn is unstable.
    if not "%AI_SUMMARY_WEB_PORT%"=="" powershell -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -Command "Start-Process -WindowStyle Hidden -FilePath '%PY_FOR_RUN_ABS%' -WorkingDirectory '%CD%\..\ai_conversations_summary' -ArgumentList '-m','uvicorn','web.main:app','--host','%AI_SUMMARY_WEB_HOST%','--port','%AI_SUMMARY_WEB_PORT%'"
    if not "%LIBRARY_WEB_PORT%"=="" (
        powershell -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -Command "Start-Process -WindowStyle Hidden -FilePath '%PY_FOR_RUN_ABS%' -WorkingDirectory '%CD%\..\library_tracker' -ArgumentList '-m','uvicorn','web.main:app','--host','%LIBRARY_WEB_HOST%','--port','%LIBRARY_WEB_PORT%'"
    )
    powershell -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -Command "Start-Process -WindowStyle Hidden -FilePath '%PY_FOR_RUN_ABS%' -WorkingDirectory '%CD%' -ArgumentList 'launch_web.py'"
    echo [Nav Dashboard] Started services in hidden mode.
    exit /b 0

    if not "%AI_SUMMARY_WEB_PORT%"=="" start "AI Summary Service" /min /d "..\ai_conversations_summary" "%PY_FOR_RUN%" -m uvicorn web.main:app --host %AI_SUMMARY_WEB_HOST% --port %AI_SUMMARY_WEB_PORT%
    if not "%LIBRARY_WEB_PORT%"=="" start "Library Tracker Service" /min /d "..\library_tracker" "%PY_FOR_RUN%" -m uvicorn web.main:app --host %LIBRARY_WEB_HOST% --port %LIBRARY_WEB_PORT%

    echo [Nav Dashboard] Started AI Summary and Library Tracker in background.
    echo [Nav Dashboard] Opening only Nav Dashboard in browser...
    "%PY_FOR_RUN%" "launch_web.py"
    exit /b %errorlevel%
)

where py >nul 2>nul
if %errorlevel%==0 (
    start "AI Summary Service" /min /d "..\ai_conversations_summary" py -3 -m uvicorn web.main:app --host %AI_SUMMARY_WEB_HOST% --port %AI_SUMMARY_WEB_PORT%
    start "Library Tracker Service" /min /d "..\library_tracker" py -3 -m uvicorn web.main:app --host %LIBRARY_WEB_HOST% --port %LIBRARY_WEB_PORT%

    echo [Nav Dashboard] Started AI Summary and Library Tracker in background.
    echo [Nav Dashboard] Opening only Nav Dashboard in browser...
    py -3 "launch_web.py"
    exit /b %errorlevel%
)

echo [ERROR] Python not found. Please install Python or create .venv first.
echo Press any key to close...
pause >nul
exit /b 1

:STOP_PORT_LISTENERS
set "TARGET_PORT=%~1"
if "%TARGET_PORT%"=="" exit /b 0
for /f "tokens=5" %%P in ('netstat -ano -p tcp ^| findstr /R /C:":%TARGET_PORT% .*LISTENING"') do (
    if not "%%P"=="0" (
        echo [Nav Dashboard] Stopping PID %%P on port %TARGET_PORT% ...
        taskkill /PID %%P /F >nul 2>nul
    )
)
timeout /t 2 >nul
exit /b 0
