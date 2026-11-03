$ErrorActionPreference='Stop'
$release=$PSScriptRoot
$module=(Resolve-Path (Join-Path $release '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Runner active; no deployment'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 25){throw 'Too near scheduled tick; no deployment'}
if(Test-Path (Join-Path $module 'runtime/cleanup-stop.json')){throw 'Existing failed cleanup stop; no deployment'}
foreach($f in Get-ChildItem "$release/baseline" -Filter '*.js'){if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw "Intervening change: $($f.Name)"}}
$beforeTask=Export-ScheduledTask wef-live-collector
$beforeTask | Set-Content "$release/task-frozen.xml" -Encoding Unicode
$started=[DateTime]::UtcNow.ToString('o')
$beforeState=Get-Content (Join-Path $module 'runtime/cleanup-state.json') -Raw | ConvertFrom-Json
$historyBefore=@(Get-ChildItem (Join-Path $module 'runtime/history') -File -Recurse | Select-Object FullName,Length,LastWriteTimeUtc)
$historyBefore | ConvertTo-Json -Depth 3 | Set-Content "$release/history-before.json"
foreach($name in @('airtable.js','cleanup.js','lanes.js')){
 $dest=Join-Path $module $name
 Copy-Item -LiteralPath "$release/candidate/$name" -Destination "$dest.deploy-tmp"
 Move-Item -LiteralPath "$dest.deploy-tmp" -Destination $dest -Force
}
$hashes=@(foreach($f in Get-ChildItem "$release/candidate" -Filter '*.js'){
 $h=(Get-FileHash (Join-Path $module $f.Name)).Hash.ToLowerInvariant()
 if($h -ne (Get-FileHash $f.FullName).Hash.ToLowerInvariant()){throw "Installed hash mismatch: $($f.Name)"}
 @{file=$f.Name;sha256=$h}
})
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml]$beforeTask).OuterXml){throw 'Task changed unexpectedly'}
@{started=$started;deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed=@('airtable.js','cleanup.js','lanes.js');unchanged_js=21;task_unchanged=$true;state_before=$beforeState;manual_run=$false;status='Installed/readback PASS; scheduled proof pending'} | ConvertTo-Json -Depth 7 | Set-Content "$release/deployed.json"
Write-Output 'Installed/readback PASS: 3 changed JS,21 unchanged JS; existing task unchanged'
