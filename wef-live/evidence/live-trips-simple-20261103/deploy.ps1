$ErrorActionPreference='Stop'
$r=$PSScriptRoot
$module=(Resolve-Path (Join-Path $r '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Runner active; no installation'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 25){throw 'Too near normal tick; no installation'}
if(Test-Path (Join-Path $module 'runtime/cleanup-stop.json')){throw 'Existing cleanup stop; no installation'}
foreach($f in Get-ChildItem "$r/baseline" -Filter '*.js'){if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw "Intervening change $($f.Name)"}}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-frozen.xml" -Raw)).OuterXml){throw 'Task changed'}
foreach($name in @('lanes.js','live-trips.js','schema.js','cleanup.js')){Copy-Item "$r/candidate/$name" (Join-Path $module "$name.deploy-tmp");Move-Item -LiteralPath (Join-Path $module "$name.deploy-tmp") -Destination (Join-Path $module $name) -Force}
$hashes=@(foreach($f in Get-ChildItem "$r/candidate" -Filter '*.js'){$hash=(Get-FileHash (Join-Path $module $f.Name)).Hash.ToLowerInvariant();if($hash -ne (Get-FileHash $f.FullName).Hash.ToLowerInvariant()){throw 'Installed hash mismatch'};@{file=$f.Name;sha256=$hash}})
@{deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed=@('lanes.js','live-trips.js','schema.js','cleanup.js');unchanged_js=21;task_unchanged=$true;manual_run=$false;status='Installed hashes PASS; remove empty obsolete schema next, then normal verification'} | ConvertTo-Json -Depth 7 | Set-Content "$r/deployed.json"
Write-Output '25installed hashes verified;21JS/task unchanged'
