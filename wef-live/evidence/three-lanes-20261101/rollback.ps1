$ErrorActionPreference = 'Stop'
$releaseDir = $PSScriptRoot
$liveDir = [IO.Path]::GetFullPath((Join-Path $releaseDir '../..'))
if ((Get-ScheduledTask -TaskName 'wef-live-collector').State -eq 'Running') {
    throw 'Collector is running. Do not replace its files mid-run; wait for normal completion.'
}
$candidate = Get-Content -LiteralPath (Join-Path $releaseDir 'candidate-hashes.json') -Raw | ConvertFrom-Json
$baseline = Get-Content -LiteralPath (Join-Path $releaseDir 'baseline.json') -Raw | ConvertFrom-Json
# Check every affected target before replacing any. Never absorb another owner's edits.
foreach ($item in $candidate.PSObject.Properties) {
    $target = Join-Path $liveDir $item.Name
    if (Test-Path -LiteralPath $target) {
        $hash = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant()
        $old = $baseline | Where-Object name -EQ $item.Name
        if ($hash -ne $item.Value -and (!$old -or $hash -ne $old.sha256.ToLowerInvariant())) {
            throw "Protected file changed since this release: $target"
        }
    }
}
foreach ($name in @('entry.js','airtable.js')) {
    Copy-Item -LiteralPath (Join-Path $releaseDir "baseline/$name") -Destination (Join-Path $liveDir $name) -Force
}
foreach ($name in @('lanes.js','lane-store.js')) {
    $target = Join-Path $liveDir $name
    if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target }
}
# Same task identity/action/account; restoring its saved XML does not invoke the collector.
Register-ScheduledTask -TaskName 'wef-live-collector' -Xml (Get-Content -LiteralPath (Join-Path $releaseDir 'baseline/task.xml') -Raw) -Force | Out-Null
Write-Output 'Baseline files and task restored. No records changed and no collector invoked. Verify on normal scheduled runs.'
