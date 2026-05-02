param(
    [string]$TaskName = "ringstatus-pipeline-local"
)

$ErrorActionPreference = "Stop"

$repoPath = Split-Path -Parent $PSCommandPath
$runnerCmd = Join-Path $repoPath "run_tagger_task.cmd"

if (-not (Test-Path $runnerCmd)) {
    throw "run_tagger_task.cmd not found at: $runnerCmd"
}

$action = New-ScheduledTaskAction `
    -Execute "C:\Windows\System32\cmd.exe" `
    -Argument ("/d /c `"{0}`"" -f $runnerCmd) `
    -WorkingDirectory $repoPath

$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date `
    -RepetitionInterval (New-TimeSpan -Minutes 5) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances Queue

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Runs run_tagger_task.cmd every 5 minutes for ringstatus pipeline." `
    -Force | Out-Null

Enable-ScheduledTask -TaskName $TaskName | Out-Null
Start-ScheduledTask -TaskName $TaskName

Get-ScheduledTask -TaskName $TaskName |
    Select-Object TaskName, State, @{n="Interval";e={$_.Triggers[0].Repetition.Interval}}, @{n="Command";e={$_.Actions[0].Execute}}, @{n="Args";e={$_.Actions[0].Arguments}} |
    Format-Table -AutoSize
