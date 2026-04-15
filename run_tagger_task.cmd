@echo off
setlocal

set DRY_RUN=0
set CALC_MODE=promote
set WATCH_VIEW=heartbeat

cd /d "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus" || exit /b 1

echo [%DATE% %TIME%] TAGGER RUN >> "C:\actions-runner\ringstatus\epoch-tagger.log"
"C:\Program Files\nodejs\node.exe" tagger.js >> "C:\actions-runner\ringstatus\epoch-tagger.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] HEARTBEAT_PATTERNS RUN >> "C:\actions-runner\ringstatus\epoch-tagger.log"
"C:\Program Files\nodejs\node.exe" heartbeat_patterns.js >> "C:\actions-runner\ringstatus\epoch-tagger.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] SCHEDULES_DAILYV2 RUN >> "C:\actions-runner\ringstatus\schedules-dailyv2.log"
"C:\Program Files\nodejs\node.exe" schedules_dailyv2.js >> "C:\actions-runner\ringstatus\schedules-dailyv2.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] SCHEDULES_CALCULATORV2 RUN >> "C:\actions-runner\ringstatus\schedules-calculatorv2.log"
"C:\Program Files\nodejs\node.exe" schedules_calculatorv2.js >> "C:\actions-runner\ringstatus\schedules-calculatorv2.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] TRIPS_DAILYV2 RUN >> "C:\actions-runner\ringstatus\trips-dailyv2.log"
"C:\Program Files\nodejs\node.exe" trips_dailyv2.js >> "C:\actions-runner\ringstatus\trips-dailyv2.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] TRIPS_TAGGER RUN >> "C:\actions-runner\ringstatus\trips-tagger.log"
"C:\Program Files\nodejs\node.exe" trips_tagger.js >> "C:\actions-runner\ringstatus\trips-tagger.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] TRIPS_CALCULATORV2 RUN >> "C:\actions-runner\ringstatus\trips-calculatorv2.log"
"C:\Program Files\nodejs\node.exe" trips_calculatorv2.js >> "C:\actions-runner\ringstatus\trips-calculatorv2.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

timeout /t 30 /nobreak >nul

echo [%DATE% %TIME%] PUBLISH RUN >> "C:\actions-runner\ringstatus\publisher.log"
"C:\Program Files\nodejs\node.exe" publisher.js >> "C:\actions-runner\ringstatus\publisher.log" 2>&1

exit /b %ERRORLEVEL%
