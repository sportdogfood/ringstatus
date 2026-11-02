# Prepared rollback only. Does not start/stop any collector or repair production data.
$ErrorActionPreference='Stop'
$moduleRoot=(Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Runner active; no rollback performed'}
$installed=Get-Content (Join-Path $PSScriptRoot 'deployed.json') -Raw | ConvertFrom-Json
foreach($f in $installed.hashes){if((Get-FileHash (Join-Path $moduleRoot $f.file)).Hash.ToLowerInvariant() -ne $f.sha256){throw "Intervening change $($f.file); no rollback performed"}}
foreach($name in @('airtable.js','cleanup.js','lanes.js')){Copy-Item -LiteralPath (Join-Path $PSScriptRoot "baseline/$name") -Destination (Join-Path $moduleRoot $name) -Force}
Register-ScheduledTask -TaskName wef-live-collector -Xml (Get-Content (Join-Path $PSScriptRoot 'task-frozen-before.xml') -Raw) -Force | Out-Null
$added=(Resolve-Path (Join-Path $moduleRoot 'live-cadence.js')).Path
if($added -ne (Join-Path $moduleRoot 'live-cadence.js')){throw 'Unexpected rollback path'}
Remove-Item -LiteralPath $added
foreach($name in @('airtable.js','cleanup.js','lanes.js')){if((Get-FileHash (Join-Path $moduleRoot $name)).Hash -ne (Get-FileHash (Join-Path $PSScriptRoot "baseline/$name")).Hash){throw 'Rollback hash mismatch'}}
@{at=[DateTime]::UtcNow.ToString('o');status='Baseline restored; no manual run';warning='Restores old archive-based cleanup/cadence, so use only under applicable owner rollback authority'} | ConvertTo-Json | Set-Content (Join-Path $PSScriptRoot 'rollback-receipt.json')
