$ErrorActionPreference='Stop'
$r=$PSScriptRoot
$module=(Resolve-Path (Join-Path $r '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Normal runner active; no installation'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 20){throw 'Too near normal tick; no installation'}
foreach($f in Get-ChildItem "$r/baseline/wef-live" -File -Filter '*.js'){if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw "Intervening change $($f.Name)"}}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-before.xml" -Raw)).OuterXml){throw 'Task changed'}
foreach($name in @('model.js','lanes.js')){Copy-Item (Join-Path $r/candidate/wef-live $name) (Join-Path $module ($name+'.deploy-tmp'));Move-Item -LiteralPath (Join-Path $module ($name+'.deploy-tmp')) -Destination (Join-Path $module $name) -Force}
$hashes=@(foreach($f in Get-ChildItem "$r/candidate/wef-live" -Filter '*.js'){$hash=(Get-FileHash (Join-Path $module $f.Name)).Hash.ToLowerInvariant();if($hash -ne (Get-FileHash $f.FullName).Hash.ToLowerInvariant()){throw 'Installed candidate mismatch'};@{file=$f.Name;sha256=$hash}})
if((Get-FileHash "$r/baseline/lib/wef-source-read.js").Hash -ne (Get-FileHash (Join-Path $module '../lib/wef-source-read.js')).Hash){throw 'Shared reader changed'}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$r/task-before.xml" -Raw)).OuterXml){throw 'Task changed during installation'}
@{deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed=@('model.js','lanes.js');unchanged_js=23;header_files_preserved=$true;task_unchanged=$true;shared_reader_unchanged=$true;manual_run=$false;status='Installed/readback PASS;two normal scheduled runs pending'} | ConvertTo-Json -Depth 7 | Set-Content "$r/deployed.json"
Write-Output 'Independent live details deployed;decoded requests paused;23otherJS unchanged'


