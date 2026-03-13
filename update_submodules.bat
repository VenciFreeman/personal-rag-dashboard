@echo off
setlocal
cd /d "%~dp0"

git submodule sync --recursive
git submodule update --init --recursive --remote

echo [done] submodules updated for current branch mapping (.gitmodules branch=.)
endlocal
