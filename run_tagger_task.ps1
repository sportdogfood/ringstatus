$ErrorActionPreference = 'Stop'

$env:DRY_RUN          = '0'
$env:CALC_MODE        = 'promote'
$env:WATCH_VIEW       = 'heartbeat'
$env:AIRTABLE_BASE_ID  = 'apptdhhNzduxm5gjn'
$env:AIRTABLE_TABLE    = 'tblCnHDB4IVtxqulo'
$env:AIRTABLE_VIEW_HOT = 'viwATt1y2RKpn2FSZ'
$env:CUSTOMER_ID       = '15'
. (Join-Path (Split-Path -Parent $PSCommandPath) 'runner_pipeline_common.ps1')
$targetShow = Resolve-HeartbeatTargetShow -BaseId $env:AIRTABLE_BASE_ID
if ($targetShow) {
    $env:HEARTBEAT_TARGET_SHOW_RECORD_ID = $targetShow.RecordId
    $env:HEARTBEAT_TARGET_APP_SHOW_ID = $targetShow.ShowId
    $env:HEARTBEAT_TARGET_SQL_DATES = ($targetShow.SqlDates -join ',')
    if ($targetShow.ShowDates -contains $targetShow.FocusDay) {
        $env:CUSTOMER_ID = $targetShow.CustomerId
    }
}
if (-not $env:PUBLISHER_DELAY_SECONDS) {
    $env:PUBLISHER_DELAY_SECONDS = '0'
}

$repoPath = Split-Path -Parent $PSCommandPath
$nodePath = 'C:\Program Files\nodejs\node.exe'
$logDir   = 'C:\actions-runner\ringstatus'

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

if (-not (Test-Path $repoPath)) {
    throw "Repo path not found: $repoPath"
}

if (-not (Test-Path $nodePath)) {
    $nodePath = 'node'
}

Set-Location $repoPath

$script:DeferredStepFailures = @()
$summaryPath = Join-Path $logDir 'runner-pipeline.log'

function Get-LogEncodingIssue {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return $null
    }

    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        $sampleLength = [int][Math]::Min([int64]256, $stream.Length)
        if ($sampleLength -le 0) {
            return $null
        }

        $buffer = New-Object byte[] $sampleLength
        [void]$stream.Read($buffer, 0, $sampleLength)
    }
    finally {
        $stream.Dispose()
    }

    if ($sampleLength -ge 2) {
        if (($buffer[0] -eq 0xFF -and $buffer[1] -eq 0xFE) -or ($buffer[0] -eq 0xFE -and $buffer[1] -eq 0xFF)) {
            return 'utf16_bom'
        }
    }

    $nullByteCount = ($buffer | Where-Object { $_ -eq 0 }).Count
    if ($nullByteCount -ge 8) {
        return 'utf16_null_bytes'
    }

    return $null
}

function Ensure-Utf8LogFile {
    param([string]$Path)

    $issue = Get-LogEncodingIssue -Path $Path
    if (-not $issue) {
        return
    }

    $directory = Split-Path -Parent $Path
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($Path)
    $extension = [System.IO.Path]::GetExtension($Path)
    $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $archiveName = "{0}.{1}.{2}{3}" -f $baseName, $issue, $stamp, $extension
    $archivePath = Join-Path $directory $archiveName
    Move-Item -Path $Path -Destination $archivePath -Force
}

function Append-Utf8Text {
    param(
        [string]$Path,
        [string]$Text
    )

    Ensure-Utf8LogFile -Path $Path
    [System.IO.File]::AppendAllText(
        $Path,
        $Text,
        [System.Text.UTF8Encoding]::new($false)
    )
}

function Write-PipelineEvent {
    param([hashtable]$EventData)

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $json = $EventData | ConvertTo-Json -Compress -Depth 6
    Append-Utf8Text -Path $summaryPath -Text "[$timestamp] RUNNER PIPELINE`r`n$json`r`n"
}

function Get-TrackedFileState {
    param([string[]]$RelativePaths)

    $items = @()
    foreach ($relativePath in $RelativePaths) {
        $fullPath = Join-Path $repoPath $relativePath
        if (-not (Test-Path $fullPath)) {
            $items += [pscustomobject]@{
                path = $relativePath
                exists = $false
            }
            continue
        }

        $item = Get-Item $fullPath
        $hash = (Get-FileHash -Algorithm SHA256 -Path $fullPath).Hash
        $items += [pscustomobject]@{
            path = $relativePath
            exists = $true
            length = $item.Length
            last_write_time = $item.LastWriteTime.ToString('o')
            sha256 = $hash
        }
    }

    return $items
}

function Run-Step {
    param(
        [string]$Label,
        [string]$ScriptName,
        [string]$LogPath,
        [switch]$ContinueOnError
    )

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $allText = "[$timestamp] $Label RUN`r`n"
    $scriptPath = Join-Path $repoPath $ScriptName
    $startedAt = Get-Date

    $nodeOutput = & $nodePath $scriptPath 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
    $durationMs = [int][Math]::Round(((Get-Date) - $startedAt).TotalMilliseconds)
    $allText += $nodeOutput
    $allText += ("{0}`r`n" -f (@{
        ok = ($exitCode -eq 0)
        event = 'step_completed'
        label = $Label
        script = $ScriptName
        exit_code = $exitCode
        duration_ms = $durationMs
    } | ConvertTo-Json -Compress))

    Append-Utf8Text -Path $LogPath -Text $allText
    Write-PipelineEvent @{
        ok = ($exitCode -eq 0)
        event = 'step_completed'
        label = $Label
        script = $ScriptName
        log_path = $LogPath
        exit_code = $exitCode
        duration_ms = $durationMs
        continue_on_error = [bool]$ContinueOnError
    }

    if ($exitCode -ne 0) {
        if ($ContinueOnError) {
            $script:DeferredStepFailures += [pscustomobject]@{
                Label = $Label
                ScriptName = $ScriptName
                ExitCode = $exitCode
                LogPath = $LogPath
                Timestamp = $timestamp
                DurationMs = $durationMs
            }
            return
        }

        exit $exitCode
    }
}

Write-PipelineEvent @{
    ok = $true
    event = 'pipeline_started'
    publisher_delay_seconds = [int]$env:PUBLISHER_DELAY_SECONDS
    repo_path = $repoPath
    tracked_files = Get-TrackedFileState @(
        'run_tagger_task.cmd',
        'run_tagger_task.ps1',
        'schedules_dailyv2.js',
        'schedules_calculatorv2.js',
        'trips_dailyv2.js',
        'trips_tagger.js',
        'trips_calculator.js',
        'publisher.js'
    )
}

Run-Step -Label 'TAGGER'             -ScriptName 'tagger.js'             -LogPath "$logDir\epoch-tagger.log"
Run-Step -Label 'HEARTBEAT_PATTERNS' -ScriptName 'heartbeat_patterns.js' -LogPath "$logDir\epoch-tagger.log"
Run-Step -Label 'SCHEDULES_DAILYV2'      -ScriptName 'schedules_dailyv2.js'      -LogPath "$logDir\schedules-dailyv2.log"
Run-Step -Label 'SCHEDULES_CALCULATORV2' -ScriptName 'schedules_calculatorv2.js' -LogPath "$logDir\schedules-calculatorv2.log" -ContinueOnError
Run-Step -Label 'TRIPS_DAILYV2'          -ScriptName 'trips_dailyv2.js'          -LogPath "$logDir\trips-dailyv2.log"
Run-Step -Label 'TRIPS_TAGGER'           -ScriptName 'trips_tagger.js'           -LogPath "$logDir\trips-tagger.log"
Run-Step -Label 'TRIPS_CALCULATORV2'     -ScriptName 'trips_calculatorv2.js'     -LogPath "$logDir\trips-calculatorv2.log" -ContinueOnError

if ($script:DeferredStepFailures.Count -gt 0) {
    Write-PipelineEvent @{
        ok = $false
        event = 'publish_skipped_due_to_deferred_failures'
        failures = $script:DeferredStepFailures
    }

    exit 1
}

$publisherDelaySeconds = [int]($env:PUBLISHER_DELAY_SECONDS)
if ($publisherDelaySeconds -gt 0) {
    Write-PipelineEvent @{
        ok = $true
        event = 'publisher_delay_started'
        seconds = $publisherDelaySeconds
    }
    Start-Sleep -Seconds $publisherDelaySeconds
}

Run-Step -Label 'PUBLISH'            -ScriptName 'publisher.js'          -LogPath "$logDir\publisher.log"

if ($script:DeferredStepFailures.Count -gt 0) {
    Write-PipelineEvent @{
        ok = $false
        event = 'pipeline_completed_with_deferred_failures'
        failures = $script:DeferredStepFailures
    }

    exit 1
}

Write-PipelineEvent @{
    ok = $true
    event = 'pipeline_completed'
    deferred_failures = 0
}

exit $LASTEXITCODE
