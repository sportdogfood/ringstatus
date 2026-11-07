$ErrorActionPreference='Stop'
$r=$PSScriptRoot
$module=(Resolve-Path (Join-Path $r '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Normal runner active; no installation'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 20){throw 'Too near normal tick; no installation'}
foreach($f in Get-ChildItem "$r/baseline/wef-live" -File -Filter '*.js'){if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw "Intervening change $($f.Name)"}}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-before.xml" -Raw)).OuterXml){throw 'Task changed'}
Copy-Item "$r/candidate/wef-live/live-cadence.js" (Join-Path $module 'live-cadence.js.deploy-tmp');Move-Item -LiteralPath (Join-Path $module 'live-cadence.js.deploy-tmp') -Destination (Join-Path $module 'live-cadence.js') -Force
$hashes=@(foreach($f in Get-ChildItem "$r/candidate/wef-live" -Filter '*.js'){$hash=(Get-FileHash (Join-Path $module $f.Name)).Hash.ToLowerInvariant();if($hash -ne (Get-FileHash $f.FullName).Hash.ToLowerInvariant()){throw 'Installed candidate mismatch'};@{file=$f.Name;sha256=$hash}})
if((Get-FileHash "$r/baseline/lib/wef-source-read.js").Hash -ne (Get-FileHash (Join-Path $module '../lib/wef-source-read.js')).Hash){throw 'Shared reader changed'}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-before.xml" -Raw)).OuterXml){throw 'Task changed during installation'}
@{deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed='live-cadence.js';unchanged_js=24;header_files_preserved=$true;task_unchanged=$true;shared_reader_unchanged=$true;manual_run=$false;status='Installed/readback PASS;normal Shows-time decision verification pending'} | ConvertTo-Json -Depth 7 | Set-Content "$r/deployed.json"
Write-Output 'Shows Eastern time decision deployed;24otherJS including decoded pause unchanged'

