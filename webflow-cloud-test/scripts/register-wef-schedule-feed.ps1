param([Parameter(Mandatory=$true)][string]$EnvFile,[Parameter(Mandatory=$true)][string]$StateDirectory)
$ErrorActionPreference='Stop'
if(-not (Test-Path -LiteralPath $EnvFile -PathType Leaf)){throw 'Existing Airtable environment file is missing'}
if(Get-ScheduledTask -TaskName 'wef-schedule-app' -ErrorAction SilentlyContinue){throw 'Schedule task already exists; inspect before replacing'}
$identity=Get-ScheduledTask -TaskName 'wef-data-collector'
if($identity.Principal.LogonType -ne 'Interactive'){throw 'Existing scheduled identity changed'}
$script=Join-Path $PSScriptRoot 'run-wef-schedule-feed.ps1'
$argument='-NoProfile -NonInteractive -WindowStyle Hidden -File "'+$script+'" -EnvFile "'+(Resolve-Path -LiteralPath $EnvFile).Path+'" -StateDirectory "'+[IO.Path]::GetFullPath($StateDirectory)+'"'
$action=New-ScheduledTaskAction -Execute (Join-Path $env:WINDIR 'System32/WindowsPowerShell/v1.0/powershell.exe') -Argument $argument -WorkingDirectory $PSScriptRoot
$next=[DateTimeOffset]::FromUnixTimeSeconds(([math]::Floor([DateTimeOffset]::UtcNow.ToUnixTimeSeconds()/180)+1)*180).LocalDateTime
$trigger=New-ScheduledTaskTrigger -Once -At $next -RepetitionInterval (New-TimeSpan -Minutes 3)
$settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Minutes 10) -Hidden
$principal=New-ScheduledTaskPrincipal -UserId $identity.Principal.UserId -LogonType Interactive -RunLevel Limited
$task=New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description 'Schedule-app sole writer: Day 0 diffs every 3 minutes; full shared Data every 15; durable halt on failure. Normal time trigger only.'
Register-ScheduledTask -TaskName 'wef-schedule-app' -InputObject $task | Out-Null
Get-ScheduledTask -TaskName 'wef-schedule-app' | Select-Object TaskName,State
Get-ScheduledTaskInfo -TaskName 'wef-schedule-app' | Select-Object NextRunTime
