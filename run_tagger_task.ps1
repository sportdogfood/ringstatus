﻿$ErrorActionPreference = 'Stop'

$env:DRY_RUN          = '0'
$env:CALC_MODE        = 'promote'
$env:WATCH_VIEW       = 'heartbeat'
$env:AIRTABLE_BASE_ID  = 'apptdhhNzduxm5gjn'
$env:AIRTABLE_TABLE    = 'tblCnHDB4IVtxqulo'
$env:AIRTABLE_VIEW_HOT = 'viwATt1y2RKpn2FSZ'
$env:CUSTOMER_ID       = '15'

$repoPath = 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus'
$nodePath = 'C:\Program Files\nodejs\node.exe'
$logDir   = 'C:\actions-runner\ringstatus'

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

if (-not (Test-Path $repoPath)) {
    throw "Repo path not found: $repoPath"
}

if (-not (Test-Path $nodePath)) {
    throw "Node path not found: $nodePath"
}

Set-Location $repoPath

function Run-Step {
    param(
        [string]$Label,
        [string]$ScriptName,
        [string]$LogPath
    )

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $allText = "[$timestamp] $Label RUN`r`n"
    $scriptPath = Join-Path $repoPath $ScriptName

    $nodeOutput = & $nodePath $scriptPath 2>&1 | Out-String
    $allText += $nodeOutput

    [System.IO.File]::AppendAllText(
        $LogPath,
        $allText,
        [System.Text.UTF8Encoding]::new($false)
    )

    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

Run-Step -Label 'TAGGER'             -ScriptName 'tagger.js'             -LogPath "$logDir\epoch-tagger.log"
Run-Step -Label 'HEARTBEAT_PATTERNS' -ScriptName 'heartbeat_patterns.js' -LogPath "$logDir\epoch-tagger.log"
Run-Step -Label 'SCHEDULES_DAILYV2'      -ScriptName 'schedules_dailyv2.js'      -LogPath "$logDir\schedules-dailyv2.log"
Run-Step -Label 'SCHEDULES_CALCULATORV2' -ScriptName 'schedules_calculatorv2.js' -LogPath "$logDir\schedules-calculatorv2.log"
Run-Step -Label 'TRIPS_DAILYV2'          -ScriptName 'trips_dailyv2.js'          -LogPath "$logDir\trips-dailyv2.log"
Run-Step -Label 'TRIPS_TAGGER'           -ScriptName 'trips_tagger.js'           -LogPath "$logDir\trips-tagger.log"
Run-Step -Label 'TRIPS_CALCULATORV2'     -ScriptName 'trips_calculatorv2.js'     -LogPath "$logDir\trips-calculatorv2.log"

Start-Sleep -Seconds 30

Run-Step -Label 'PUBLISH'            -ScriptName 'publisher.js'          -LogPath "$logDir\publisher.log"

exit $LASTEXITCODE
