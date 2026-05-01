$ErrorActionPreference = 'Stop'

$sourceRoot = Split-Path -Parent $PSCommandPath
$targetRoot = 'C:\ringstatus-task'

$files = @(
    'run_tagger_task.cmd',
    'run_tagger_task.ps1',
    'run_tagger_task_cmd.ps1',
    'schedules_dailyv2.js',
    'schedules_calculatorv2.js',
    'trips_dailyv2.js',
    'trips_calculator.js'
)

if (-not (Test-Path $sourceRoot)) {
    throw "Source root not found: $sourceRoot"
}

if (-not (Test-Path $targetRoot)) {
    New-Item -ItemType Directory -Path $targetRoot -Force | Out-Null
}

foreach ($file in $files) {
    $sourcePath = Join-Path $sourceRoot $file
    if (-not (Test-Path $sourcePath)) {
        throw "Missing source file: $sourcePath"
    }

    Copy-Item -Path $sourcePath -Destination (Join-Path $targetRoot $file) -Force
}

$report = foreach ($file in $files) {
    $sourcePath = Join-Path $sourceRoot $file
    $targetPath = Join-Path $targetRoot $file
    $sourceItem = Get-Item $sourcePath
    $targetItem = Get-Item $targetPath
    $sourceHash = (Get-FileHash -Algorithm SHA256 -Path $sourcePath).Hash
    $targetHash = (Get-FileHash -Algorithm SHA256 -Path $targetPath).Hash

    [pscustomobject]@{
        File = $file
        SourceLength = $sourceItem.Length
        TargetLength = $targetItem.Length
        SourceLastWriteTime = $sourceItem.LastWriteTime.ToString('s')
        TargetLastWriteTime = $targetItem.LastWriteTime.ToString('s')
        HashMatch = ($sourceHash -eq $targetHash)
        SHA256 = $targetHash
    }
}

$report | Format-Table -AutoSize
