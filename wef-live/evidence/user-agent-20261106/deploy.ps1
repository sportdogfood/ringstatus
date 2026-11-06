$ErrorActionPreference='Stop'
$r=$PSScriptRoot
$module=(Resolve-Path (Join-Path $r '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Normal runner active; installation not started'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 20){throw 'Too near normal tick; installation not started'}
foreach($f in Get-Content "$r/runtime-before.json" -Raw | ConvertFrom-Json){if((Get-FileHash (Join-Path $module $f.file)).Hash.ToLowerInvariant() -ne $f.sha256){throw "Intervening change $($f.file)"}}
if((Get-FileHash "$r/baseline/lib/wef-source-read.js").Hash -ne (Get-FileHash (Join-Path $module '../lib/wef-source-read.js')).Hash){throw 'Shared reader changed'}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-before.xml" -Raw)).OuterXml){throw 'Task changed'}
foreach($name in @('activity.js','source.js')){Copy-Item -LiteralPath "$r/candidate/wef-live/$name" -Destination (Join-Path $module "$name.deploy-tmp");Move-Item -LiteralPath (Join-Path $module "$name.deploy-tmp") -Destination (Join-Path $module $name) -Force}
foreach($name in @('activity.js','source.js')){if((Get-FileHash (Join-Path $module $name)).Hash -ne (Get-FileHash "$r/candidate/wef-live/$name").Hash){throw 'Installed target mismatch'}}
$hashes=@(Get-ChildItem $module -File -Filter '*.js' | ForEach-Object {@{file=$_.Name;sha256=(Get-FileHash $_.FullName).Hash.ToLowerInvariant()}})
$old=Get-Content "$r/runtime-before.json" -Raw | ConvertFrom-Json
foreach($f in $old | Where-Object file -notin @('activity.js','source.js')){if(($hashes | Where-Object file -eq $f.file).sha256 -ne $f.sha256){throw 'Unrelated runtime changed'}}
if((Get-FileHash "$r/baseline/lib/wef-source-read.js").Hash -ne (Get-FileHash (Join-Path $module '../lib/wef-source-read.js')).Hash){throw 'Shared reader changed during installation'}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-before.xml" -Raw)).OuterXml){throw 'Task changed during installation'}
@{deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed=@('activity.js','source.js');unchanged_js=23;task_unchanged=$true;shared_reader_unchanged=$true;status='Installed/readback PASS; scheduled proof pending';manual_run=$false} | ConvertTo-Json -Depth 7 | Set-Content "$r/deployed.json"
Write-Output 'Installed2files;23other JS/shared reader/task unchanged'
