$ErrorActionPreference = 'Stop'

$env:DRY_RUN   = '0'
$env:CALC_MODE = 'promote'
$env:WATCH_VIEW = 'hb_targets_active'

$repoPath = 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus'
$nodePath = 'C:\Program Files\nodejs\node.exe'

Set-Location $repoPath

function Run-Step {
    param(
        [string]$Label,
        [string]$ScriptName,
        [string]$LogPath
    )

    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Add-Content -Path $LogPath -Value "[$timestamp] $Label RUN"

    & $nodePath $ScriptName *>> $LogPath

    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

Run-Step -Label 'TAGGER'              -ScriptName 'tagger.js'              -LogPath 'C:\actions-runner\ringstatus\epoch-tagger.log'
Run-Step -Label 'HEARTBEAT_PATTERNS'  -ScriptName 'heartbeat_patterns.js'  -LogPath 'C:\actions-runner\ringstatus\epoch-tagger.log'
Run-Step -Label 'TRIPS_TAGGER'        -ScriptName 'trips_tagger.js'        -LogPath 'C:\actions-runner\ringstatus\trips-tagger.log'
Run-Step -Label 'TRIPS_CALCULATOR'    -ScriptName 'trips_calculator.js'    -LogPath 'C:\actions-runner\ringstatus\trips-calculator.log'

Start-Sleep -Seconds 30

Run-Step -Label 'PUBLISH'             -ScriptName 'publisher.js'           -LogPath 'C:\actions-runner\ringstatus\publisher.log'

exit $LASTEXITCODE
