$ErrorActionPreference = 'Stop'

$sourceRoot = Split-Path -Parent $PSCommandPath
$targetRoot = 'C:\ringstatus-task'
$logRoot = 'C:\actions-runner\ringstatus'
$reportPath = Join-Path $sourceRoot 'runner_hotfix_report.txt'
$syncScript = Join-Path $sourceRoot 'sync_runner_hotfix.ps1'
$runScript = Join-Path $targetRoot 'run_tagger_task.ps1'
$taskName = 'epoch-tagger-local'

function Write-Section {
    param(
        [string]$Title,
        [string[]]$Lines = @()
    )

    Add-Content -Path $reportPath -Value ("`r`n=== {0} ===`r`n" -f $Title)
    foreach ($line in $Lines) {
        Add-Content -Path $reportPath -Value $line
    }
}

function Write-CommandOutputSection {
    param(
        [string]$Title,
        [scriptblock]$Command
    )

    try {
        $output = & $Command 2>&1 | Out-String
        Write-Section -Title $Title -Lines @($output.TrimEnd())
    } catch {
        $message = $_ | Out-String
        Write-Section -Title $Title -Lines @($message.TrimEnd())
    }
}

function Write-FileTailSection {
    param(
        [string]$Title,
        [string]$Path,
        [int]$Tail = 80
    )

    if (-not (Test-Path $Path)) {
        Write-Section -Title $Title -Lines @("MISSING: $Path")
        return
    }

    try {
        $content = Get-Content -Path $Path -Tail $Tail -ErrorAction Stop | Out-String
        Write-Section -Title $Title -Lines @($content.TrimEnd())
    } catch {
        $message = $_ | Out-String
        Write-Section -Title $Title -Lines @($message.TrimEnd())
    }
}

Set-Content -Path $reportPath -Value ("runner hotfix report | generated_at={0}" -f (Get-Date).ToString('o'))

Write-Section -Title 'Context' -Lines @(
    "source_root=$sourceRoot",
    "target_root=$targetRoot",
    "log_root=$logRoot",
    "task_name=$taskName"
)

Write-CommandOutputSection -Title 'Sync Hotfix' -Command { & $syncScript }
Write-CommandOutputSection -Title 'Direct Run' -Command { powershell -NoProfile -ExecutionPolicy Bypass -File $runScript }
Write-CommandOutputSection -Title 'Scheduled Task' -Command {
    $task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $taskName -ErrorAction Stop
    [pscustomobject]@{
        TaskName = $task.TaskName
        State = $task.State
        LastRunTime = $info.LastRunTime
        LastTaskResult = $info.LastTaskResult
        NextRunTime = $info.NextRunTime
        NumberOfMissedRuns = $info.NumberOfMissedRuns
    } | Format-List | Out-String
}

Write-FileTailSection -Title 'Runner Pipeline Tail' -Path (Join-Path $logRoot 'runner-pipeline.log') -Tail 160
Write-FileTailSection -Title 'Schedules Dailyv2 Tail' -Path (Join-Path $logRoot 'schedules-dailyv2.log') -Tail 120
Write-FileTailSection -Title 'Schedules Calculatorv2 Tail' -Path (Join-Path $logRoot 'schedules-calculatorv2.log') -Tail 80
Write-FileTailSection -Title 'Trips Dailyv2 Tail' -Path (Join-Path $logRoot 'trips-dailyv2.log') -Tail 100
Write-FileTailSection -Title 'Trips Tagger Tail' -Path (Join-Path $logRoot 'trips-tagger.log') -Tail 80
Write-FileTailSection -Title 'Trips Calculatorv2 Tail' -Path (Join-Path $logRoot 'trips-calculatorv2.log') -Tail 80
Write-FileTailSection -Title 'Publisher Tail' -Path (Join-Path $logRoot 'publisher.log') -Tail 80

Write-Output $reportPath
