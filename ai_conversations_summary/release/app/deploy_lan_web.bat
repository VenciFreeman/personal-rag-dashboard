@echo off
setlocal

rem LAN deployment launcher for Web UI.
rem Behavior:
rem 1) Ensure admin rights and open firewall inbound rules.
rem 2) Print local LAN URLs for peer machines.
rem 3) Start launch_web.py and keep this window open.

cd /d "%~dp0"

set "AI_SUMMARY_WEB_HOST=0.0.0.0"
set "AI_SUMMARY_WEB_PORT=8000"
set "TOPLEVEL_PY=..\.venv\Scripts\python.exe"
set "LOCAL_PY=.venv\Scripts\python.exe"
set "PY_FOR_FIREWALL="
set "PY_FOR_RUN="

if exist "%TOPLEVEL_PY%" (
    set "PY_FOR_FIREWALL=%CD%\..\.venv\Scripts\python.exe"
    set "PY_FOR_RUN=%TOPLEVEL_PY%"
) else if exist "%LOCAL_PY%" (
    set "PY_FOR_FIREWALL=%CD%\.venv\Scripts\python.exe"
    set "PY_FOR_RUN=%LOCAL_PY%"
)

echo [AI Summary] Preparing LAN deployment on %AI_SUMMARY_WEB_HOST%:%AI_SUMMARY_WEB_PORT% ...

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
echo [AI Summary] Skipping firewall rule setup (non-admin mode).
goto FIREWALL_DONE

:FIREWALL_SETUP
rem Add firewall inbound rules (safe to run repeatedly).
netsh advfirewall firewall delete rule name="AI Summary Web %AI_SUMMARY_WEB_PORT%" >nul 2>nul
netsh advfirewall firewall add rule name="AI Summary Web %AI_SUMMARY_WEB_PORT%" dir=in action=allow protocol=TCP localport=%AI_SUMMARY_WEB_PORT% profile=any >nul 2>nul
if defined PY_FOR_FIREWALL (
    netsh advfirewall firewall delete rule name="AI Summary Python Inbound" >nul 2>nul
    netsh advfirewall firewall add rule name="AI Summary Python Inbound" dir=in action=allow program="%PY_FOR_FIREWALL%" profile=any >nul 2>nul
)
netsh advfirewall firewall show rule name="AI Summary Web %AI_SUMMARY_WEB_PORT%" >nul 2>nul
if not %errorlevel%==0 (
    echo [WARN] Firewall rule was not created successfully.
    echo [WARN] Other computers may not be able to connect.
) else (
    echo [AI Summary] Firewall rule ready: AI Summary Web %AI_SUMMARY_WEB_PORT%
    netsh advfirewall firewall show rule name="AI Summary Web %AI_SUMMARY_WEB_PORT%"
)
if defined PY_FOR_FIREWALL (
    netsh advfirewall firewall show rule name="AI Summary Python Inbound" >nul 2>nul
    if %errorlevel%==0 (
        echo [AI Summary] Firewall rule ready: AI Summary Python Inbound
        netsh advfirewall firewall show rule name="AI Summary Python Inbound"
    )
)

:FIREWALL_DONE

echo [AI Summary] LAN access URLs (same subnet):
set "HAS_RECOMMENDED=0"
setlocal EnableDelayedExpansion
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /R /C:"IPv4 Address" /C:"IPv4.*:"') do (
    set "ip=%%a"
    set "ip=!ip: =!"
    if not "!ip!"=="" (
        if "!ip:~0,8!"=="192.168." (
            if "!HAS_RECOMMENDED!"=="0" echo   Recommended:
            echo   http://!ip!:%AI_SUMMARY_WEB_PORT%/
            set "HAS_RECOMMENDED=1"
        )
    )
)
endlocal

echo   All local IPv4:
setlocal EnableDelayedExpansion
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /R /C:"IPv4 Address" /C:"IPv4.*:"') do (
    set "ip=%%a"
    set "ip=!ip: =!"
    if not "!ip!"=="" echo   http://!ip!:%AI_SUMMARY_WEB_PORT%/
)
endlocal
echo.
echo [AI Summary] Keep this window open while serving. Press Ctrl+C to stop.
echo [AI Summary] If another computer still cannot access:
echo   1) Ensure both devices are on the same subnet/VLAN.
echo   2) Try the 192.168.x.x address first (usually home/office LAN).
echo   3) Check whether endpoint security software blocks python.exe inbound traffic.
echo   4) On another PC run: Test-NetConnection 192.168.0.102 -Port %AI_SUMMARY_WEB_PORT%
echo   5) If PingSucceeded=False or TcpTestSucceeded=False, this is network/security isolation.
echo   6) Some enterprise networks forbid peer-to-peer even on same subnet; use VPN overlay/port forwarding if needed.
echo.

if defined PY_FOR_RUN (
    "%PY_FOR_RUN%" "launch_web.py"
    exit /b %errorlevel%
)

where py >nul 2>nul
if %errorlevel%==0 (
    py -3 "launch_web.py"
    exit /b %errorlevel%
)

echo [ERROR] Python not found. Please install Python or create .venv first.
echo Press any key to close...
pause >nul
exit /b 1
