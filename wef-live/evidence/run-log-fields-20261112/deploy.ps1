$ErrorActionPreference='Stop'
$e=$PSScriptRoot
$module=(Resolve-Path (Join-Path $e '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Normal runner active; no installation'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 15){throw 'Too near scheduled tick; no installation'}
$before=[xml](Get-Content "$e/task-before.xml" -Raw)
if(([xml](Export-ScheduledTask wef-live-collector)).OuterXml -ne $before.OuterXml){throw 'Task definition changed'}
foreach($f in Get-ChildItem "$e/baseline/wef-live" -Filter '*.js'){
 if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw "Intervening source change $($f.Name)"}
}
$manifest=Get-Content "$e/review-manifest.json" -Raw | ConvertFrom-Json
foreach($f in $manifest.candidate){if((Get-FileHash $f.path).Hash.ToLowerInvariant() -ne $f.sha256){throw 'Reviewed candidate changed'}}
$changed=@('run-log-fields.js','schema.js','live-cadence.js','linker.js','lanes.js')
foreach($name in $changed){
 Copy-Item "$e/candidate/wef-live/$name" (Join-Path $module ($name+'.deploy-tmp'))
 Move-Item -LiteralPath (Join-Path $module ($name+'.deploy-tmp')) -Destination (Join-Path $module $name) -Force
}
$hashes=@(foreach($f in Get-ChildItem "$e/candidate/wef-live" -Filter '*.js'){
 $hash=(Get-FileHash (Join-Path $module $f.Name)).Hash.ToLowerInvariant()
 if($hash -ne (Get-FileHash $f.FullName).Hash.ToLowerInvariant()){throw 'Installed hash mismatch'}
 @{file=$f.Name;sha256=$hash}
})
@{deployed_at=[DateTime]::UtcNow.ToString('o');hashes=$hashes;changed=$changed;unchanged_js=23;task_unchanged=$true;manual_run=$false;record_repairs=$false;status='Installed; normal scheduled proof pending'} | ConvertTo-Json -Depth 7 | Set-Content "$e/deployed.json"
'Installed six future run-log fields; normal scheduled acceptance pending'
