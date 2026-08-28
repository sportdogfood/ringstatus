param([switch]$ScheduleOnly, [switch]$OogOnly, [switch]$DryRun)
$ErrorActionPreference = 'Stop'
if ($DryRun) { $env:DRY_RUN = '1' }
$args = @(); if ($ScheduleOnly) { $args += '--schedule-only' }; if ($OogOnly) { $args += '--oog-only' }
& 'C:\Program Files\nodejs\node.exe' (Join-Path $PSScriptRoot 'sgl_browser_enrichment.js') @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
