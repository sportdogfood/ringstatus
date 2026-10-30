$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$stateDir = Join-Path $PSScriptRoot 'runtime'
New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
$logPath = Join-Path $stateDir ('collector-' + (Get-Date -Format 'yyyy-MM-dd') + '.log')
$envPath = Join-Path $stateDir 'collector.env'
$attemptId = [guid]::NewGuid().ToString()
$attemptStarted = [DateTime]::UtcNow.ToString('o')
$collectorExit = 1
$attemptError = $null
$receiptDir = Join-Path $stateDir 'attempt-receipts'
New-Item -ItemType Directory -Path $receiptDir -Force | Out-Null
$receiptPath = Join-Path $receiptDir ($attemptId + '.jsonl')
function Write-AttemptReceipt([string]$state, [string]$completed) {
    $receipt = @{version=1;runId=$attemptId;started=$attemptStarted;state=$state;pid=$PID}
    if ($state -eq 'finished') {
        $receipt.completed = $completed
        $receipt.exitCode = $collectorExit
        if ($attemptError) { $receipt.error = $attemptError }
    }
    # Base64 preserves receipt bytes amid the existing UTF-16 Node output.
    $body = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($receipt | ConvertTo-Json -Compress)))
    Add-Content -LiteralPath $logPath -Encoding UTF8 -Value ('WEF_SCHEDULES_ATTEMPT ' + $body)
    Add-Content -LiteralPath $receiptPath -Encoding UTF8 -Value ($receipt | ConvertTo-Json -Compress)
}
Add-Content -LiteralPath $logPath -Value ('START ' + (Get-Date -Format o))
Write-AttemptReceipt 'started' ''
$env:WEF_SCHEDULES_ATTEMPT_ID = $attemptId
$env:WEF_SCHEDULES_ATTEMPT_STARTED = $attemptStarted
$env:WEF_SCHEDULES_ATTEMPT_LOG = $logPath
try {
    if (-not (Test-Path -LiteralPath $envPath)) { throw 'Missing private runtime/collector.env' }
    # Load the private environment inside the logging entry before product imports.
    # Preserve the existing native stderr capture and exit-code behavior.
    $ErrorActionPreference = 'Continue'
    $LASTEXITCODE = $null
    & node (Join-Path $PSScriptRoot 'attempt-entry.js') >> $logPath 2>&1
    if ($null -ne $LASTEXITCODE) { $collectorExit = $LASTEXITCODE }
    else { $attemptError = 'Collector process did not return a native exit code' }
}
catch {
    $attemptError = $_.Exception.Message
    Add-Content -LiteralPath $logPath -Value ('WEF_SCHEDULES_WRAPPER_FAILED: ' + $attemptError)
}
finally {
    $ErrorActionPreference = 'Stop'
    Write-AttemptReceipt 'finished' ([DateTime]::UtcNow.ToString('o'))
    Add-Content -LiteralPath $logPath -Value ('END ' + (Get-Date -Format o) + ' exit=' + $collectorExit)
    Remove-Item Env:WEF_SCHEDULES_ATTEMPT_ID -ErrorAction SilentlyContinue
    Remove-Item Env:WEF_SCHEDULES_ATTEMPT_STARTED -ErrorAction SilentlyContinue
    Remove-Item Env:WEF_SCHEDULES_ATTEMPT_LOG -ErrorAction SilentlyContinue
}
exit $collectorExit
