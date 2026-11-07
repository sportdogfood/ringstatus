$ErrorActionPreference='Stop'
$r=$PSScriptRoot
$module=(Resolve-Path (Join-Path $r '../..')).Path
if((Get-ScheduledTask wef-live-collector).State -ne 'Ready'){throw 'Wait for normal task to finish; do not interrupt'}
if(((Get-ScheduledTaskInfo wef-live-collector).NextRunTime-(Get-Date)).TotalSeconds -lt 20){throw 'Too near normal tick; no source restore'}
foreach($name in @('lanes.js','schema.js','airtable.js','linker.js')){
 $target=Join-Path $module $name
 if($name -eq 'linker.js' -and -not (Test-Path $target)){continue}
 $allowed=@((Get-FileHash (Join-Path "$r/candidate/wef-live" $name)).Hash)
 if($name -ne 'linker.js'){$allowed+=(Get-FileHash (Join-Path "$r/baseline/wef-live" $name)).Hash}
 if($allowed -notcontains (Get-FileHash $target).Hash){throw "Intervening owner change $name; not overwritten"}
}
foreach($name in @('lanes.js','schema.js','airtable.js')){Copy-Item (Join-Path "$r/baseline/wef-live" $name) (Join-Path $module ($name+'.revert-tmp'));Move-Item -LiteralPath (Join-Path $module ($name+'.revert-tmp')) -Destination (Join-Path $module $name) -Force}
if(Test-Path (Join-Path $module 'linker.js')){Remove-Item -LiteralPath (Join-Path $module 'linker.js')}
foreach($f in Get-ChildItem "$r/baseline/wef-live" -File -Filter '*.js'){if((Get-FileHash $f.FullName).Hash -ne (Get-FileHash (Join-Path $module $f.Name)).Hash){throw 'Restored baseline mismatch'}}
@{reverted_at=[DateTime]::UtcNow.ToString('o');reason='Owner-authorized revert on failed linker acceptance';baseline_restored=$true;records_repaired=0;schema_or_links_deleted=0;manual_runs=0} | ConvertTo-Json | Set-Content "$r/reverted.json"
Write-Output 'Prior code restored;records,links and schema retained;no manual run'
