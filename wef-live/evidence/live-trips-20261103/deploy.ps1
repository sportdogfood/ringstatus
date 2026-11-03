$ErrorActionPreference='Stop'
$release=$PSScriptRoot
$module=(Resolve-Path (Join-Path $release '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Runner active; no deployment'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 25){throw 'Too near normal tick; no deployment'}
if(Test-Path (Join-Path $module 'runtime/cleanup-stop.json')){throw 'Existing cleanup stop; no deployment'}
foreach($f in Get-ChildItem "$release/baseline" -Filter '*.js'){if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw "Intervening change: $($f.Name)"}}
if(Test-Path (Join-Path $module 'live-trips.js')){throw 'Unexpected existing new module'}
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$release/task-frozen.xml" -Raw)).OuterXml){throw 'Task changed before deployment'}
$started=[DateTime]::UtcNow.ToString('o')
foreach($name in @('live-trips.js','schema.js','cleanup.js','lanes.js')){
 $dest=Join-Path $module $name
 Copy-Item -LiteralPath "$release/candidate/$name" -Destination "$dest.deploy-tmp"
 Move-Item -LiteralPath "$dest.deploy-tmp" -Destination $dest -Force
}
$hashes=@(foreach($f in Get-ChildItem "$release/candidate" -Filter '*.js'){
 $hash=(Get-FileHash (Join-Path $module $f.Name)).Hash.ToLowerInvariant()
 if($hash -ne (Get-FileHash $f.FullName).Hash.ToLowerInvariant()){throw "Installed hash mismatch: $($f.Name)"}
 @{file=$f.Name;sha256=$hash}
})
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne ([xml](Get-Content "$release/task-frozen.xml" -Raw)).OuterXml){throw 'Task changed during deployment'}
@{started=$started;deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed=@('schema.js','cleanup.js','lanes.js');added='live-trips.js';unchanged_js=21;task_unchanged=$true;manual_run=$false;status='Installed/hash/task readback PASS; normal scheduled proof pending'} | ConvertTo-Json -Depth 7 | Set-Content "$release/deployed.json"
Write-Output 'Installed25JS hashes PASS;21JS and existing task unchanged'
