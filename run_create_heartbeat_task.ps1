param(
    [string]$TaskName = 'ringstatus-heartbeat',
    [string]$NodePath = 'C:\Program Files\nodejs\node.exe'
)

$ErrorActionPreference = 'Stop'

$repoPath = Split-Path -Parent $PSCommandPath
$targetScript = Join-Path $repoPath 'create_heartbeat_task.ps1'

if (-not (Test-Path $targetScript)) {
    throw "create_heartbeat_task.ps1 not found at $targetScript"
}

& powershell -NoProfile -ExecutionPolicy Bypass -File $targetScript -TaskName $TaskName -NodePath $NodePath
exit $LASTEXITCODE
