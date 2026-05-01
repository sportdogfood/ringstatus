@echo off
setlocal

set REPO_PATH=%~dp0
if "%REPO_PATH:~-1%"=="\" set REPO_PATH=%REPO_PATH:~0,-1%
set SCRIPT_PATH=%REPO_PATH%\run_tagger_heavy_lane.ps1
set POWERSHELL_EXE=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe

cd /d "%REPO_PATH%" || exit /b 1

if not exist "%POWERSHELL_EXE%" set POWERSHELL_EXE=powershell
if not exist "%SCRIPT_PATH%" (
  echo run_tagger_heavy_lane.ps1 not found at "%SCRIPT_PATH%"
  exit /b 1
)

"%POWERSHELL_EXE%" -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_PATH%"

exit /b %ERRORLEVEL%
