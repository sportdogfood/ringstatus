param([Parameter(Mandatory=$true)][string]$EnvFile,[Parameter(Mandatory=$true)][string]$StateDirectory)
$ErrorActionPreference='Stop'
& node ('--env-file=' + $EnvFile) (Join-Path $PSScriptRoot 'schedule-feed-cadence.mjs') $StateDirectory
exit $LASTEXITCODE
