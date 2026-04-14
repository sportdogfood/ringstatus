$ErrorActionPreference = 'Stop'

$env:DRY_RUN          = '0'
$env:CALC_MODE        = 'promote'
$env:WATCH_VIEW       = 'hb_targets_active'
$env:AIRTABLE_TOKEN    = ''
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
    $header = "[$timestamp] $Label RUN`r`n"

    [System.IO.File]::AppendAllText(
        $LogPath,
        $header,
        [System.Text.UTF8Encoding]::new($false)
    )

    & $nodePath $ScriptName 2>&1 | Out-File -FilePath $LogPath -Append -Encoding utf8

    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

Run-Step -Label 'TAGGER'             -ScriptName 'tagger.js'             -LogPath "$logDir\epoch-tagger.log"
Run-Step -Label 'HEARTBEAT_PATTERNS' -ScriptName 'heartbeat_patterns.js' -LogPath "$logDir\epoch-tagger.log"
Run-Step -Label 'TRIPS_TAGGER'       -ScriptName 'trips_tagger.js'       -LogPath "$logDir\trips-tagger.log"
Run-Step -Label 'TRIPS_CALCULATOR'   -ScriptName 'trips_calculator.js'   -LogPath "$logDir\trips-calculator.log"

Start-Sleep -Seconds 30

Run-Step -Label 'PUBLISH'            -ScriptName 'publisher.js'          -LogPath "$logDir\publisher.log"

exit $LASTEXITCODE
