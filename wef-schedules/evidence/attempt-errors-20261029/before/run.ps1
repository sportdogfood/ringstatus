$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$stateDir = Join-Path $PSScriptRoot 'runtime'
New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
$logPath = Join-Path $stateDir ('collector-' + (Get-Date -Format 'yyyy-MM-dd') + '.log')
$envPath = Join-Path $stateDir 'collector.env'
if (-not (Test-Path -LiteralPath $envPath)) { throw 'Missing private runtime/collector.env' }
Add-Content -LiteralPath $logPath -Value ('START ' + (Get-Date -Format o))
$collectorExit = 1
try {
    # Windows PowerShell treats native stderr as an ErrorRecord. Capture it
    # without turning it into a wrapper exception or losing Node's exit code.
    $ErrorActionPreference = 'Continue'
    $LASTEXITCODE = $null
    & node --env-file=$envPath (Join-Path $PSScriptRoot 'collect.js') >> $logPath 2>&1
    if ($null -ne $LASTEXITCODE) { $collectorExit = $LASTEXITCODE }
}
finally {
    $ErrorActionPreference = 'Stop'
    Add-Content -LiteralPath $logPath -Value ('END ' + (Get-Date -Format o) + ' exit=' + $collectorExit)
}
exit $collectorExit
