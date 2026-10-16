$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$stateDir = Join-Path $PSScriptRoot 'runtime'
New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
$logPath = Join-Path $stateDir ('collector-' + (Get-Date -Format 'yyyy-MM-dd') + '.log')
$envPath = Join-Path $stateDir 'collector.env'
if (-not (Test-Path -LiteralPath $envPath)) { throw 'Missing private runtime/collector.env' }
Add-Content -LiteralPath $logPath -Value ('START ' + (Get-Date -Format o))
& node --env-file=$envPath (Join-Path $PSScriptRoot 'collect.js') >> $logPath 2>&1
$collectorExit = $LASTEXITCODE
Add-Content -LiteralPath $logPath -Value ('END ' + (Get-Date -Format o) + ' exit=' + $collectorExit)
exit $collectorExit
