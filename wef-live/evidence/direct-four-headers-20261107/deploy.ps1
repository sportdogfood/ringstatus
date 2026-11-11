$ErrorActionPreference='Stop'
$r=$PSScriptRoot
$module=(Resolve-Path (Join-Path $r '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Normal runner active; no installation'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 15){throw 'Too near next normal tick; no installation'}
foreach($f in Get-ChildItem "$r/baseline/wef-live" -File -Filter '*.js'){if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw "Intervening change $($f.Name)"}}
if((Get-FileHash "$r/baseline/lib/wef-source-read.js").Hash -ne (Get-FileHash (Join-Path $module '../lib/wef-source-read.js')).Hash){throw 'Shared reader changed'}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-before.xml" -Raw)).OuterXml){throw 'Task changed'}
foreach($name in @('activity.js','source.js')){Copy-Item (Join-Path "$r/candidate/wef-live" $name) (Join-Path $module ($name+'.deploy-tmp'));Move-Item -LiteralPath (Join-Path $module ($name+'.deploy-tmp')) -Destination (Join-Path $module $name) -Force}
$hashes=@(foreach($f in Get-ChildItem "$r/candidate/wef-live" -Filter '*.js'){$hash=(Get-FileHash (Join-Path $module $f.Name)).Hash.ToLowerInvariant();if($hash -ne (Get-FileHash $f.FullName).Hash.ToLowerInvariant()){throw 'Installed candidate mismatch'};@{file=$f.Name;sha256=$hash}})
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-before.xml" -Raw)).OuterXml){throw 'Task changed during installation'}
@{deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed=@('activity.js','source.js');unchanged_js=24;decoded_paused=$true;task_unchanged=$true;manual_run=$false;status='Installed/readback PASS;normal scheduled feed verification pending'} | ConvertTo-Json -Depth 7 | Set-Content "$r/deployed.json"
Write-Output 'Four direct feeds/four headers installed;task and all collection/link/cleanup logic unchanged'
