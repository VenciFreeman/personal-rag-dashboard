@echo off
REM Prune expired web search cache entries (older than 7 days by default).
REM Schedule this with Windows Task Scheduler for automatic cleanup.
cd /d "%~dp0"
python prune_web_cache.py %*
