param(
    [string]$TaskName = 'ringstatus-heartbeat',
    [string]$NodePath = 'C:\Program Files\nodejs\node.exe'
)

$ErrorActionPreference = 'Stop'

$repoPath = Split-Path -Parent $PSCommandPath
$taggerPath = Join-Path $repoPath 'tagger.js'

if (-not (Test-Path $taggerPath)) {
    throw "tagger.js not found at $taggerPath"
}

if (-not (Test-Path $NodePath)) {
    $NodePath = 'node'
}

$action = New-ScheduledTaskAction `
    -Execute $NodePath `
    -Argument ('"{0}"' -f $taggerPath) `
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
    -Description 'Runs ringstatus tagger.js heartbeat every 5 minutes.' `
    -Force | Out-Null

Enable-ScheduledTask -TaskName $TaskName | Out-Null

Get-ScheduledTask -TaskName $TaskName |
    Select-Object TaskName, State, @{n='Interval';e={$_.Triggers[0].Repetition.Interval}}, @{n='Command';e={$_.Actions[0].Execute}}, @{n='Args';e={$_.Actions[0].Arguments}} |
    Format-Table -AutoSize
