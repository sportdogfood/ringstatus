param([switch]$Enable)
$ErrorActionPreference = 'Stop'
$taskName = 'wef-data-collector'
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) { throw 'Data task already exists; inspect before replacement' }
$runtime = Join-Path $PSScriptRoot 'runtime'
if (-not (Test-Path -LiteralPath (Join-Path $runtime 'collector.env'))) { throw 'Private Data environment missing' }
if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot '.venv\Scripts\python.exe'))) { throw 'Private Python environment missing' }
$existing = Get-ScheduledTask -TaskName 'wef-shows-collector'
if ($existing.Principal.LogonType -ne 'Interactive') { throw 'Existing scheduled identity changed; inspect before installation' }
$action = New-ScheduledTaskAction -Execute (Join-Path $env:WINDIR 'System32\wscript.exe') -Argument ('//B //Nologo "' + (Join-Path $PSScriptRoot 'run-hidden.vbs') + '"') -WorkingDirectory $PSScriptRoot
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2) -RepetitionInterval (New-TimeSpan -Minutes 1)
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -Hidden
$principal = New-ScheduledTaskPrincipal -UserId $existing.Principal.UserId -LogonType Interactive -RunLevel Limited
$task = New-ScheduledTask -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Description 'Data-owned retained collection; 60-second due-work wake, no overlap, durable stop on first failure. Requires logged-in existing owner account.'
Register-ScheduledTask -TaskName $taskName -InputObject $task | Out-Null
if (-not $Enable) { Disable-ScheduledTask -TaskName $taskName | Out-Null }
# Installation never starts the collector. Only its normal time trigger may do so.
Get-ScheduledTask -TaskName $taskName | Select-Object TaskName,State
