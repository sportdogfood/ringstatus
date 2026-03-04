@echo off
setlocal

set DRY_RUN=0

cd /d "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus" || exit /b 1

echo [%DATE% %TIME%] TAGGER RUN >> "C:\actions-runner\ringstatus\epoch-tagger.log"
"C:\Program Files\nodejs\node.exe" tagger.js >> "C:\actions-runner\ringstatus\epoch-tagger.log" 2>&1

echo [%DATE% %TIME%] PUBLISH RUN >> "C:\actions-runner\ringstatus\publisher.log"
"C:\Program Files\nodejs\node.exe" "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\publisher.js" >> "C:\actions-runner\ringstatus\publisher.log" 2>&1
exit /b %ERRORLEVEL%
