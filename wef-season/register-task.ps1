param([switch]$Enable)
$ErrorActionPreference='Stop'
$taskName='wef-season-collector'
if(Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue){throw 'Season task already exists; inspect before replacement'}
if(-not(Test-Path (Join-Path $PSScriptRoot 'schema.json'))){throw 'Verified Season schema missing'}
if(-not(Test-Path (Join-Path $PSScriptRoot '../wef-data/runtime/collector.env'))){throw 'Existing host environment missing'}
$data=Get-ScheduledTask -TaskName wef-data-collector
if($data.Principal.LogonType -ne 'Interactive'){throw 'Existing runner identity differs'}
$action=New-ScheduledTaskAction -Execute (Join-Path $env:WINDIR 'System32/wscript.exe') -Argument ('//B //Nologo "'+(Join-Path $PSScriptRoot 'run-hidden.vbs')+'"') -WorkingDirectory $PSScriptRoot
$trigger=New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2) -RepetitionInterval (New-TimeSpan -Minutes 5)
$settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -Hidden
$principal=New-ScheduledTaskPrincipal -UserId $data.Principal.UserId -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $taskName -InputObject (New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description 'Consume verified retained Data into four Season bases; no source acquisition, no deletes.') | Out-Null
if(-not$Enable){Disable-ScheduledTask -TaskName $taskName|Out-Null}
# Registration never starts a collector. Only the ordinary trigger may run it.
Get-ScheduledTask -TaskName $taskName|Select-Object TaskName,State
