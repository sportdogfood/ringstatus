$ErrorActionPreference = 'Stop'
$repoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
Set-Location -LiteralPath $repoRoot
$clockRuntime = Join-Path $repoRoot 'wef-data/runtime/show-time-health'
New-Item -ItemType Directory -Path $clockRuntime -Force | Out-Null
$clockLog = Join-Path $clockRuntime ('check-' + (Get-Date -Format 'yyyy-MM-dd') + '.log')
$clockEnv = Join-Path $repoRoot 'wef-shows/runtime/collector.env'
$clockScript = Join-Path $PSScriptRoot 'show-time-health.mjs'
$clockExit = 1
Add-Content -LiteralPath $clockLog -Encoding UTF8 -Value ('START ' + [datetime]::UtcNow.ToString('o'))
try {
    $ErrorActionPreference = 'Continue'
    & 'C:\Program Files\nodejs\node.exe' --env-file=$clockEnv $clockScript --scheduled 2>&1 | Out-File -LiteralPath $clockLog -Encoding UTF8 -Append
    if ($null -ne $LASTEXITCODE) { $clockExit = $LASTEXITCODE }
}
finally {
    $ErrorActionPreference = 'Stop'
    Add-Content -LiteralPath $clockLog -Encoding UTF8 -Value ('END ' + [datetime]::UtcNow.ToString('o') + ' exit=' + $clockExit)
}
exit $clockExit
