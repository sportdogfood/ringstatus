param([switch]$Enable)
$ErrorActionPreference='Stop'
$taskName='wef-people-join'
if(Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue){throw 'People task exists; inspect before replacement'}
if(-not(Test-Path (Join-Path $PSScriptRoot 'schema.json'))){throw 'Verified People schema missing'}
if(-not(Test-Path (Join-Path $PSScriptRoot '../wef-data/runtime/collector.env'))){throw 'Existing host environment missing'}
$reference=Get-ScheduledTask -TaskName wef-season-collector
if($reference.Principal.LogonType -ne 'Interactive'){throw 'Existing runner identity differs'}
$action=New-ScheduledTaskAction -Execute (Join-Path $env:WINDIR 'System32/wscript.exe') -Argument ('//B //Nologo "'+(Join-Path $PSScriptRoot 'run-hidden.vbs')+'"') -WorkingDirectory $PSScriptRoot
$trigger=New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2) -RepetitionInterval (New-TimeSpan -Hours 4)
$settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -Hidden
$principal=New-ScheduledTaskPrincipal -UserId $reference.Principal.UserId -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $taskName -InputObject (New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description 'Derived People join from existing shared inputs; four hours; no acquisition or deletes.') | Out-Null
if(-not$Enable){Disable-ScheduledTask -TaskName $taskName|Out-Null}
# Registration never manually starts the workflow.
Get-ScheduledTask -TaskName $taskName|Select-Object TaskName,State
