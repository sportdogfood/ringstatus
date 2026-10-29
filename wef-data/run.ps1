$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$stateDir = Join-Path $PSScriptRoot 'runtime'
New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
$logPath = Join-Path $stateDir ('collector-' + (Get-Date -Format 'yyyy-MM-dd') + '.log')
$envPath = Join-Path $stateDir 'collector.env'
. (Join-Path $PSScriptRoot 'attempt-log.ps1')
$attemptRoot = Join-Path $stateDir 'logging/wef-data'
$env:WEF_LOG_ATTEMPT_ID = Start-WefAttempt $attemptRoot 'wef-data' 'appdl16HJ5ETm8N69'
Add-Content -LiteralPath $logPath -Value ('START ' + (Get-Date -Format o))
$collectorExit = 1
try {
    if (-not (Test-Path -LiteralPath $envPath)) { throw 'Missing private runtime/collector.env' }
    $ErrorActionPreference = 'Continue'
    $LASTEXITCODE = $null
    & node --env-file=$envPath (Join-Path $PSScriptRoot 'entry.js') >> $logPath 2>&1
    if ($null -ne $LASTEXITCODE) { $collectorExit = $LASTEXITCODE }
}
finally {
    $ErrorActionPreference = 'Stop'
    Complete-WefFallback $attemptRoot $env:WEF_LOG_ATTEMPT_ID $collectorExit
    Add-Content -LiteralPath $logPath -Value ('END ' + (Get-Date -Format o) + ' exit=' + $collectorExit)
}
exit $collectorExit
