$ErrorActionPreference='Stop'
$releaseRoot=$PSScriptRoot
$moduleRoot=(Resolve-Path (Join-Path $releaseRoot '../..')).Path
$taskName='wef-live-collector'
$task=Get-ScheduledTask -TaskName $taskName
if($task.State -ne 'Ready'){throw 'Runner is active or unavailable; deployment not started'}
$info=Get-ScheduledTaskInfo -TaskName $taskName
if(($info.NextRunTime-(Get-Date)).TotalSeconds -lt 25){throw 'Too close to normal tick; deployment not started'}
if(Test-Path (Join-Path $moduleRoot 'runtime/cleanup-stop.json')){throw 'Existing cleanup stop; deployment not started'}
foreach($file in Get-ChildItem (Join-Path $releaseRoot 'baseline') -Filter '*.js'){
 if((Get-FileHash $file.FullName).Hash -ne (Get-FileHash (Join-Path $moduleRoot $file.Name)).Hash){throw "Intervening source change: $($file.Name)"}
}
if(Test-Path (Join-Path $moduleRoot 'live-cadence.js')){throw 'Unexpected existing live-cadence.js'}
$actualXml=Export-ScheduledTask -TaskName $taskName
$expectedXml=[xml](Get-Content (Join-Path $releaseRoot 'task-before.xml') -Raw)
if(([xml]$actualXml).OuterXml -ne $expectedXml.OuterXml){throw 'Intervening task change'}
$actualXml | Set-Content (Join-Path $releaseRoot 'task-frozen-before.xml') -Encoding Unicode
$started=[DateTime]::UtcNow.ToString('o')
foreach($name in @('live-cadence.js','airtable.js','cleanup.js','lanes.js')){
 $target=Join-Path $moduleRoot $name
 Copy-Item -LiteralPath (Join-Path $releaseRoot "candidate/$name") -Destination "$target.deploy-tmp"
 Move-Item -LiteralPath "$target.deploy-tmp" -Destination $target -Force
}
# Registration changes the existing definition only; never invoke the task.
Register-ScheduledTask -TaskName $taskName -Xml (Get-Content (Join-Path $releaseRoot 'task-prepared.xml') -Raw) -Force | Out-Null
$readback=Export-ScheduledTask -TaskName $taskName
$readback | Set-Content (Join-Path $releaseRoot 'task-deployed.xml') -Encoding Unicode
$normalizedExpected=[xml](Get-Content (Join-Path $releaseRoot 'task-prepared.xml') -Raw)
# Task Scheduler serializes a24h duration as P1D and omits false defaults.
$normalizedExpected.Task.Triggers.CalendarTrigger.Repetition.Duration='P1D'
$repeat=$normalizedExpected.Task.Triggers.CalendarTrigger.Repetition
[void]$repeat.RemoveChild($repeat.SelectSingleNode('*[local-name()="StopAtDurationEnd"]'))
if(([xml]$readback).OuterXml -ne $normalizedExpected.OuterXml){throw 'Task semantic readback mismatch'}
$hashes=@(foreach($file in Get-ChildItem (Join-Path $releaseRoot 'candidate') -Filter '*.js'){
 $target=Join-Path $moduleRoot $file.Name
 $hash=(Get-FileHash $target).Hash.ToLowerInvariant()
 if($hash -ne (Get-FileHash $file.FullName).Hash.ToLowerInvariant()){throw "Installed hash mismatch $($file.Name)"}
 @{file=$file.Name;sha256=$hash}
})
@{started=$started;deployed_at=[DateTime]::UtcNow.ToString('o');task=$taskName;manual_run=$false;hashes=$hashes;unchanged_js=20;changed_js=@('airtable.js','cleanup.js','lanes.js');added_js='live-cadence.js';next_run=(Get-ScheduledTaskInfo $taskName).NextRunTime.ToString('o');status='Installed and read back; scheduled verification pending'} | ConvertTo-Json -Depth 8 | Set-Content (Join-Path $releaseRoot 'deployed.json')
Get-Content (Join-Path $releaseRoot 'deployed.json') -Raw
