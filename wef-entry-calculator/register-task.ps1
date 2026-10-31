$ErrorActionPreference = 'Stop'
# Deployment only, after independent review. Registration never starts a manual run.
$entryTaskName = 'wef-entry-calculator'
if (Get-ScheduledTask -TaskName $entryTaskName -ErrorAction SilentlyContinue) { throw 'Entry task already exists; inspect before replacing' }
$entryLauncher = Join-Path $PSScriptRoot 'run-hidden.vbs'
$entryAction = New-ScheduledTaskAction -Execute "$env:WINDIR\System32\wscript.exe" -Argument ('//B //Nologo "' + $entryLauncher + '"') -WorkingDirectory $PSScriptRoot
$entryTrigger = New-ScheduledTaskTrigger -Once -At ((Get-Date).AddMinutes(3)) -RepetitionInterval (New-TimeSpan -Minutes 3)
$entryPrincipal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
$entrySettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Seconds 175) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $entryTaskName -Action $entryAction -Trigger $entryTrigger -Principal $entryPrincipal -Settings $entrySettings -Description 'Entry-only TODAY calculator; read-only Data/Live/Timing; three-minute normal schedule.'
