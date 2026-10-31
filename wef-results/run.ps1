$ErrorActionPreference = 'Stop'
# Prepared launcher only. No task is registered or activated by this file.
$resultsEnvFile = Join-Path $PSScriptRoot 'runtime/collector.env'
if (-not (Test-Path -LiteralPath $resultsEnvFile)) { throw 'Missing private Results runtime/collector.env' }
& 'C:\Program Files\nodejs\node.exe' "--env-file=$resultsEnvFile" (Join-Path $PSScriptRoot 'entry.js')
exit $LASTEXITCODE
