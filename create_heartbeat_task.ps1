param(
    [string]$TaskName = 'ringstatus-heartbeat',
    [string]$NodePath = 'C:\Program Files\nodejs\node.exe'
)

$ErrorActionPreference = 'Stop'

$repoPath = Split-Path -Parent $PSCommandPath
$heartbeatLanePath = Join-Path $repoPath 'run_tagger_heartbeat_lane.ps1'

if (-not (Test-Path $heartbeatLanePath)) {
    throw "run_tagger_heartbeat_lane.ps1 not found at $heartbeatLanePath"
}

$powershellPath = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
if (-not (Test-Path $powershellPath)) { $powershellPath = 'powershell.exe' }

$action = New-ScheduledTaskAction `
    -Execute $powershellPath `
    -Argument ('-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "{0}"' -f $heartbeatLanePath) `
    -WorkingDirectory $repoPath

$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date `
    -RepetitionInterval (New-TimeSpan -Minutes 5) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description 'Runs ringstatus heartbeat lane every 5 minutes.' `
    -Force | Out-Null

Enable-ScheduledTask -TaskName $TaskName | Out-Null

Get-ScheduledTask -TaskName $TaskName |
    Select-Object TaskName, State, @{n='Interval';e={$_.Triggers[0].Repetition.Interval}}, @{n='Command';e={$_.Actions[0].Execute}}, @{n='Args';e={$_.Actions[0].Arguments}} |
    Format-Table -AutoSize
