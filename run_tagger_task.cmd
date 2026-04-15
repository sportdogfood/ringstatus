@echo off
setlocal

set DRY_RUN=0
set CALC_MODE=promote
set WATCH_VIEW=heartbeat
set REPO_PATH=%~dp0
if "%REPO_PATH:~-1%"=="\" set REPO_PATH=%REPO_PATH:~0,-1%
set NODE_PATH=C:\Program Files\nodejs\node.exe
if not exist "%NODE_PATH%" set NODE_PATH=node

cd /d "%REPO_PATH%" || exit /b 1

echo [%DATE% %TIME%] TAGGER RUN >> "C:\actions-runner\ringstatus\epoch-tagger.log"
"%NODE_PATH%" "%REPO_PATH%\tagger.js" >> "C:\actions-runner\ringstatus\epoch-tagger.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] HEARTBEAT_PATTERNS RUN >> "C:\actions-runner\ringstatus\epoch-tagger.log"
"%NODE_PATH%" "%REPO_PATH%\heartbeat_patterns.js" >> "C:\actions-runner\ringstatus\epoch-tagger.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] SCHEDULES_DAILYV2 RUN >> "C:\actions-runner\ringstatus\schedules-dailyv2.log"
"%NODE_PATH%" "%REPO_PATH%\schedules_dailyv2.js" >> "C:\actions-runner\ringstatus\schedules-dailyv2.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] SCHEDULES_CALCULATORV2 RUN >> "C:\actions-runner\ringstatus\schedules-calculatorv2.log"
"%NODE_PATH%" "%REPO_PATH%\schedules_calculatorv2.js" >> "C:\actions-runner\ringstatus\schedules-calculatorv2.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] TRIPS_DAILYV2 RUN >> "C:\actions-runner\ringstatus\trips-dailyv2.log"
"%NODE_PATH%" "%REPO_PATH%\trips_dailyv2.js" >> "C:\actions-runner\ringstatus\trips-dailyv2.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] TRIPS_TAGGER RUN >> "C:\actions-runner\ringstatus\trips-tagger.log"
"%NODE_PATH%" "%REPO_PATH%\trips_tagger.js" >> "C:\actions-runner\ringstatus\trips-tagger.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] TRIPS_CALCULATORV2 RUN >> "C:\actions-runner\ringstatus\trips-calculatorv2.log"
"%NODE_PATH%" "%REPO_PATH%\trips_calculatorv2.js" >> "C:\actions-runner\ringstatus\trips-calculatorv2.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

timeout /t 30 /nobreak >nul

echo [%DATE% %TIME%] PUBLISH RUN >> "C:\actions-runner\ringstatus\publisher.log"
"%NODE_PATH%" "%REPO_PATH%\publisher.js" >> "C:\actions-runner\ringstatus\publisher.log" 2>&1

exit /b %ERRORLEVEL%
