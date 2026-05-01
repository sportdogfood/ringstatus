$ErrorActionPreference = 'Stop'

$sourceRoot = Split-Path -Parent $PSCommandPath
$targetRoot = 'C:\ringstatus-task'
$logRoot = 'C:\actions-runner\ringstatus'
$reportPath = Join-Path $sourceRoot 'runner_hotfix_report.txt'
$syncScript = Join-Path $sourceRoot 'sync_runner_hotfix.ps1'
$runScript = Join-Path $targetRoot 'run_tagger_task.ps1'
$taskName = 'epoch-tagger-local'
$directRunTimeoutSeconds = 900
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

function Write-ProgressMessage {
    param([string]$Message)

    $line = "[{0}] {1}" -f (Get-Date).ToString('yyyy-MM-dd HH:mm:ss'), $Message
    Write-Output $line
    Add-Content -Path $reportPath -Value $line
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

function Invoke-DirectRunWithTimeout {
    param(
        [string]$ScriptPath,
        [int]$TimeoutSeconds
    )

    $stdoutPath = [System.IO.Path]::GetTempFileName()
    $stderrPath = [System.IO.Path]::GetTempFileName()
    $timedOut = $false

    try {
        $process = Start-Process -FilePath 'powershell' `
            -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $ScriptPath) `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath `
            -PassThru `
            -WindowStyle Hidden

        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        Write-ProgressMessage "Direct run started | pid=$($process.Id) timeout_seconds=$TimeoutSeconds"

        while (-not $process.HasExited) {
            if ($stopwatch.Elapsed.TotalSeconds -ge $TimeoutSeconds) {
                $timedOut = $true
                try { $process.Kill() } catch {}
                break
            }

            Start-Sleep -Seconds 5
            if (-not $process.HasExited) {
                Write-ProgressMessage "Direct run still running | elapsed_seconds=$([int][Math]::Round($stopwatch.Elapsed.TotalSeconds))"
            }
        }

        if (-not $process.HasExited) {
            $process.WaitForExit()
        }

        $stopwatch.Stop()

        return [pscustomobject]@{
            ExitCode = if ($timedOut) { -1 } else { $process.ExitCode }
            TimedOut = $timedOut
            ElapsedSeconds = [int][Math]::Round($stopwatch.Elapsed.TotalSeconds)
            StdOut = if (Test-Path $stdoutPath) { Get-Content -Path $stdoutPath -Raw -ErrorAction SilentlyContinue } else { '' }
            StdErr = if (Test-Path $stderrPath) { Get-Content -Path $stderrPath -Raw -ErrorAction SilentlyContinue } else { '' }
        }
    }
    finally {
        Remove-Item -Path $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
    }
}

Set-Content -Path $reportPath -Value ("runner hotfix report | generated_at={0}" -f (Get-Date).ToString('o'))

Write-Section -Title 'Context' -Lines @(
    "source_root=$sourceRoot",
    "target_root=$targetRoot",
    "log_root=$logRoot",
    "task_name=$taskName",
    "direct_run_timeout_seconds=$directRunTimeoutSeconds"
)

$logSnapshotBefore = @{}
foreach ($entry in $logPaths.GetEnumerator()) {
    $logSnapshotBefore[$entry.Key] = Get-FileLengthOrZero -Path $entry.Value
}

Write-ProgressMessage "Sync hotfix starting"
Write-CommandOutputSection -Title 'Sync Hotfix' -Command { & $syncScript }
Write-ProgressMessage "Sync hotfix completed"

$directRunResult = Invoke-DirectRunWithTimeout -ScriptPath $runScript -TimeoutSeconds $directRunTimeoutSeconds
$directRunExitCode = $directRunResult.ExitCode
Write-Section -Title 'Direct Run' -Lines @(
    "exit_code=$directRunExitCode",
    "timed_out=$($directRunResult.TimedOut)",
    "elapsed_seconds=$($directRunResult.ElapsedSeconds)",
    ($directRunResult.StdOut.TrimEnd()),
    ($directRunResult.StdErr.TrimEnd())
)
Write-ProgressMessage "Direct run finished | exit_code=$directRunExitCode timed_out=$($directRunResult.TimedOut) elapsed_seconds=$($directRunResult.ElapsedSeconds)"

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
    direct_run_timed_out = $directRunResult.TimedOut
    direct_run_elapsed_seconds = $directRunResult.ElapsedSeconds
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
Write-ProgressMessage "Wrapper complete | report=$reportPath"
$summary
