@echo off
setlocal

set DRY_RUN=0
set CALC_MODE=promote

cd /d "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus" || exit /b 1

echo [%DATE% %TIME%] TAGGER RUN >> "C:\actions-runner\ringstatus\epoch-tagger.log"
"C:\Program Files\nodejs\node.exe" tagger.js >> "C:\actions-runner\ringstatus\epoch-tagger.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] HEARTBEAT_PATTERNS RUN >> "C:\actions-runner\ringstatus\epoch-tagger.log"
"C:\Program Files\nodejs\node.exe" heartbeat_patterns.js >> "C:\actions-runner\ringstatus\epoch-tagger.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] TRIPS_TAGGER RUN >> "C:\actions-runner\ringstatus\trips-tagger.log"
"C:\Program Files\nodejs\node.exe" trips_tagger.js >> "C:\actions-runner\ringstatus\trips-tagger.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

echo [%DATE% %TIME%] TRIPS_CALCULATOR RUN >> "C:\actions-runner\ringstatus\trips-calculator.log"
"C:\Program Files\nodejs\node.exe" trips_calculator.js >> "C:\actions-runner\ringstatus\trips-calculator.log" 2>&1
if errorlevel 1 exit /b %ERRORLEVEL%

timeout /t 30 /nobreak >nul

echo [%DATE% %TIME%] PUBLISH RUN >> "C:\actions-runner\ringstatus\publisher.log"
"C:\Program Files\nodejs\node.exe" publisher.js >> "C:\actions-runner\ringstatus\publisher.log" 2>&1

exit /b %ERRORLEVEL%
