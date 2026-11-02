$ErrorActionPreference = 'Stop'
$taskName = 'wef-data-cleanup'
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) { throw 'Cleanup task already exists; inspect before replacement' }
$existing = Get-ScheduledTask -TaskName 'wef-data-collector'
$action = New-ScheduledTaskAction -Execute (Join-Path $env:WINDIR 'System32\wscript.exe') -Argument ('//B //Nologo "' + (Join-Path $PSScriptRoot 'cleanup-hidden.vbs') + '"') -WorkingDirectory $PSScriptRoot
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -Hidden
$task = New-ScheduledTask -Action $action -Settings $settings -Principal $existing.Principal -Description 'Independent Data cleanup; on verified new-show/day-change alerts only. No periodic trigger. Day change checks1000/500logs.'
Register-ScheduledTask -TaskName $taskName -InputObject $task | Out-Null
# Registration never starts cleanup. No collector/linking task or stop is changed.
Get-ScheduledTask -TaskName $taskName | Select-Object TaskName,State
