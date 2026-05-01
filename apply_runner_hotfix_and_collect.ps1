$ErrorActionPreference = 'Stop'

$sourceRoot = Split-Path -Parent $PSCommandPath
$targetRoot = 'C:\ringstatus-task'
$logRoot = 'C:\actions-runner\ringstatus'
$reportPath = Join-Path $sourceRoot 'runner_hotfix_report.txt'
$syncScript = Join-Path $sourceRoot 'sync_runner_hotfix.ps1'
$runScript = Join-Path $targetRoot 'run_tagger_task.ps1'
$taskName = 'epoch-tagger-local'
$logPaths = @{
    RunnerPipeline = Join-Path $logRoot 'runner-pipeline.log'
    SchedulesDaily = Join-Path $logRoot 'schedules-dailyv2.log'
    SchedulesCalc = Join-Path $logRoot 'schedules-calculatorv2.log'
    TripsDaily = Join-Path $logRoot 'trips-dailyv2.log'
    TripsTagger = Join-Path $logRoot 'trips-tagger.log'
    TripsCalc = Join-Path $logRoot 'trips-calculatorv2.log'
    Publisher = Join-Path $logRoot 'publisher.log'
}

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

function Get-FileLengthOrZero {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return [int64]0
    }

    return (Get-Item $Path).Length
}

function Get-AppendedUtf8Text {
    param(
        [string]$Path,
        [int64]$StartByte
    )

    if (-not (Test-Path $Path)) {
        return ''
    }

    $bytes = [System.IO.File]::ReadAllBytes($Path)
    if ($StartByte -ge $bytes.Length) {
        return ''
    }

    $slice = $bytes[$StartByte..($bytes.Length - 1)]
    return [System.Text.UTF8Encoding]::new($false).GetString($slice)
}

Set-Content -Path $reportPath -Value ("runner hotfix report | generated_at={0}" -f (Get-Date).ToString('o'))

Write-Section -Title 'Context' -Lines @(
    "source_root=$sourceRoot",
    "target_root=$targetRoot",
    "log_root=$logRoot",
    "task_name=$taskName"
)

$logSnapshotBefore = @{}
foreach ($entry in $logPaths.GetEnumerator()) {
    $logSnapshotBefore[$entry.Key] = Get-FileLengthOrZero -Path $entry.Value
}

Write-CommandOutputSection -Title 'Sync Hotfix' -Command { & $syncScript }

$directRunOutput = powershell -NoProfile -ExecutionPolicy Bypass -File $runScript 2>&1 | Out-String
$directRunExitCode = $LASTEXITCODE
Write-Section -Title 'Direct Run' -Lines @(
    "exit_code=$directRunExitCode",
    $directRunOutput.TrimEnd()
)

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

Write-FileTailSection -Title 'Runner Pipeline Tail' -Path $logPaths.RunnerPipeline -Tail 160
Write-FileTailSection -Title 'Schedules Dailyv2 Tail' -Path $logPaths.SchedulesDaily -Tail 120
Write-FileTailSection -Title 'Schedules Calculatorv2 Tail' -Path $logPaths.SchedulesCalc -Tail 80
Write-FileTailSection -Title 'Trips Dailyv2 Tail' -Path $logPaths.TripsDaily -Tail 100
Write-FileTailSection -Title 'Trips Tagger Tail' -Path $logPaths.TripsTagger -Tail 80
Write-FileTailSection -Title 'Trips Calculatorv2 Tail' -Path $logPaths.TripsCalc -Tail 80
Write-FileTailSection -Title 'Publisher Tail' -Path $logPaths.Publisher -Tail 80

$runnerChunk = Get-AppendedUtf8Text -Path $logPaths.RunnerPipeline -StartByte $logSnapshotBefore.RunnerPipeline
$schedulesChunk = Get-AppendedUtf8Text -Path $logPaths.SchedulesDaily -StartByte $logSnapshotBefore.SchedulesDaily
$tripsChunk = Get-AppendedUtf8Text -Path $logPaths.TripsDaily -StartByte $logSnapshotBefore.TripsDaily

$summary = [pscustomobject]@{
    report_path = $reportPath
    direct_run_exit_code = $directRunExitCode
    pipeline_started_logged = ($runnerChunk -match '"event":"pipeline_started"')
    tracked_files_logged = ($runnerChunk -match '"tracked_files":\[')
    pipeline_completed_logged = ($runnerChunk -match '"event":"pipeline_completed"')
    pipeline_deferred_failures_logged = ($runnerChunk -match '"event":"pipeline_completed_with_deferred_failures"')
    schedules_daily_logged = ($runnerChunk -match '"label":"SCHEDULES_DAILYV2"')
    schedules_calc_logged = ($runnerChunk -match '"label":"SCHEDULES_CALCULATORV2"')
    trips_daily_logged = ($runnerChunk -match '"label":"TRIPS_DAILYV2"')
    trips_calc_logged = ($runnerChunk -match '"label":"TRIPS_CALCULATORV2"')
    class_endpoint_422_detected = ($tripsChunk -match 'Field \\"class_endpoint\\" cannot accept a value because the field is computed')
    zero_row_schedule_guard_detected = ($schedulesChunk -match 'Refusing destructive zero-row schedule sync')
    default_day_scope_error_detected = ($schedulesChunk -match 'Missing required heartbeat field: default_app_sql_date_is')
}

Write-Section -Title 'Summary' -Lines @(($summary | ConvertTo-Json -Depth 4))
$summary
