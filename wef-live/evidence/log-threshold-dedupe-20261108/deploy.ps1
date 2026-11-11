$ErrorActionPreference='Stop'
$r=$PSScriptRoot
$module=(Resolve-Path (Join-Path $r '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Normal runner active;no installation'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 15){throw 'Too near normal tick;no installation'}
foreach($f in Get-ChildItem "$r/baseline/wef-live" -File -Filter '*.js'){if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw "Intervening change $($f.Name)"}}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-before.xml" -Raw)).OuterXml){throw 'Task changed'}
foreach($name in @('cleanup.js','airtable.js','linker.js','lanes.js','live-cadence.js')){Copy-Item (Join-Path "$r/candidate/wef-live" $name) (Join-Path $module ($name+'.deploy-tmp'));Move-Item -LiteralPath (Join-Path $module ($name+'.deploy-tmp')) -Destination (Join-Path $module $name) -Force}
$hashes=@(foreach($f in Get-ChildItem "$r/candidate/wef-live" -Filter '*.js'){$hash=(Get-FileHash (Join-Path $module $f.Name)).Hash.ToLowerInvariant();if($hash -ne (Get-FileHash $f.FullName).Hash.ToLowerInvariant()){throw 'Installed mismatch'};@{file=$f.Name;sha256=$hash}})
@{deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed=@('cleanup.js','airtable.js','linker.js','lanes.js','live-cadence.js');unchanged_js=21;task_unchanged=$true;collection_unchanged=$true;decoded_paused=$true;manual_run=$false;status='Installed/readback PASS;normal logging-stop/threshold cleanup proof pending'} | ConvertTo-Json -Depth 7 | Set-Content "$r/deployed.json"
Write-Output 'errs-log writes disabled;threshold cleanup installed;task unchanged'
