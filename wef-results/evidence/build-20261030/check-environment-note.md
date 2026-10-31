# Check harness environment recovery

Initial run-checks.py executed unit/integration, retained-source-shape and JavaScript syntax checks successfully and saved their exact outputs. Its PowerShell parser step could not start the assumed C:/Program Files/PowerShell/7/pwsh.exe (WinError2; tool call chunk a44fe4). No launcher/business code was executed or failed.

Read-only process inspection located the existing bundled shell under C:/Users/gombc/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/powershell/pwsh.exe. Corrected only the evidence harness path lookup, then used --remaining to preserve the three successful executions and execute only the missing parser check. All four commands now have exit0 in execution-log.json. This does not repair or retry a business workflow.
