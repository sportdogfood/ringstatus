$ErrorActionPreference='Stop'
$e=$PSScriptRoot
$module=(Resolve-Path (Join-Path $e '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Normal runner active; no installation'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 15){throw 'Too near normal tick; no installation'}
$before=[xml](Get-Content "$e/task-before.xml" -Raw)
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne $before.OuterXml){throw 'Task definition changed'}
foreach($f in Get-ChildItem "$e/baseline/wef-live" -Filter '*.js'){
 if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw "Intervening source change $($f.Name)"}
}
$markerPath=Join-Path $module 'runtime/cleanup-stop.json'
$marker=Get-Content -LiteralPath $markerPath -Raw | ConvertFrom-Json
$failed=Get-Content "$e/before-cleanup-634f3191-18ac-4eab-95b4-04bf771b0596.json" -Raw | ConvertFrom-Json
if($marker.run_id -ne '634f3191-18ac-4eab-95b4-04bf771b0596' -or $marker.error -ne 'fetch failed' -or @($failed.actions).Count -ne 0 -or $failed.show){throw 'Recovery marker is not the verified zero-action failure'}
if((Get-FileHash $markerPath).Hash -ne (Get-FileHash "$e/before-cleanup-stop.json").Hash){throw 'Stop marker changed'}
Copy-Item "$e/candidate/wef-live/cleanup.js" (Join-Path $module 'cleanup.js.deploy-tmp')
Move-Item -LiteralPath (Join-Path $module 'cleanup.js.deploy-tmp') -Destination (Join-Path $module 'cleanup.js') -Force
$hashes=@(foreach($f in Get-ChildItem "$e/candidate/wef-live" -Filter '*.js'){
 $hash=(Get-FileHash (Join-Path $module $f.Name)).Hash.ToLowerInvariant()
 if($hash -ne (Get-FileHash $f.FullName).Hash.ToLowerInvariant()){throw 'Installed hash mismatch'}
 @{file=$f.Name;sha256=$hash}
})
Copy-Item -LiteralPath $markerPath -Destination "$e/released-stop.json"
Remove-Item -LiteralPath $markerPath
@{deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed=@('cleanup.js');unchanged_js=26;task_unchanged=$true;released_stop_run_id=$marker.run_id;manual_run=$false;record_repairs=$false;status='Installed and verified zero-action stop released; two normal runs pending'} | ConvertTo-Json -Depth 7 | Set-Content "$e/deployed.json"
'Cleanup read recovery installed; existing scheduled task will resume'
