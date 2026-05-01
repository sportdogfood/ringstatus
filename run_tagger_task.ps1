﻿$ErrorActionPreference = 'Stop'

$env:DRY_RUN          = '0'
$env:CALC_MODE        = 'promote'
$env:WATCH_VIEW       = 'heartbeat'
$env:AIRTABLE_BASE_ID  = 'apptdhhNzduxm5gjn'
$env:AIRTABLE_TABLE    = 'tblCnHDB4IVtxqulo'
$env:AIRTABLE_VIEW_HOT = 'viwATt1y2RKpn2FSZ'
$env:CUSTOMER_ID       = '15'

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

    $nodeOutput = & $nodePath $scriptPath 2>&1 | Out-String
    $exitCode = $LASTEXITCODE
    $allText += $nodeOutput

    [System.IO.File]::AppendAllText(
        $LogPath,
        $allText,
        [System.Text.UTF8Encoding]::new($false)
    )

    if ($exitCode -ne 0) {
        if ($ContinueOnError) {
            $script:DeferredStepFailures += [pscustomobject]@{
                Label = $Label
                ScriptName = $ScriptName
                ExitCode = $exitCode
                LogPath = $LogPath
                Timestamp = $timestamp
            }
            return
        }

        exit $exitCode
    }
}

Run-Step -Label 'TAGGER'             -ScriptName 'tagger.js'             -LogPath "$logDir\epoch-tagger.log"
Run-Step -Label 'HEARTBEAT_PATTERNS' -ScriptName 'heartbeat_patterns.js' -LogPath "$logDir\epoch-tagger.log"
Run-Step -Label 'SCHEDULES_DAILYV2'      -ScriptName 'schedules_dailyv2.js'      -LogPath "$logDir\schedules-dailyv2.log"
Run-Step -Label 'SCHEDULES_CALCULATORV2' -ScriptName 'schedules_calculatorv2.js' -LogPath "$logDir\schedules-calculatorv2.log" -ContinueOnError
Run-Step -Label 'TRIPS_DAILYV2'          -ScriptName 'trips_dailyv2.js'          -LogPath "$logDir\trips-dailyv2.log"
Run-Step -Label 'TRIPS_TAGGER'           -ScriptName 'trips_tagger.js'           -LogPath "$logDir\trips-tagger.log"
Run-Step -Label 'TRIPS_CALCULATORV2'     -ScriptName 'trips_calculatorv2.js'     -LogPath "$logDir\trips-calculatorv2.log" -ContinueOnError

Start-Sleep -Seconds 30

Run-Step -Label 'PUBLISH'            -ScriptName 'publisher.js'          -LogPath "$logDir\publisher.log"

if ($script:DeferredStepFailures.Count -gt 0) {
    $summaryPath = Join-Path $logDir 'runner-pipeline.log'
    $summary = [pscustomobject]@{
        ok = $false
        event = 'pipeline_completed_with_deferred_failures'
        failures = $script:DeferredStepFailures
    } | ConvertTo-Json -Depth 4

    [System.IO.File]::AppendAllText(
        $summaryPath,
        "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] RUNNER SUMMARY`r`n$summary`r`n",
        [System.Text.UTF8Encoding]::new($false)
    )

    exit 1
}

exit $LASTEXITCODE
