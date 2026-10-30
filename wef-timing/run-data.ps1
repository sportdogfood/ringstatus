$ErrorActionPreference = 'Stop'
$paceExit = 1
$paceLog = $null
try {
    Set-Location -LiteralPath $PSScriptRoot
    $paceRuntime = Join-Path $PSScriptRoot 'runtime/data'
    New-Item -ItemType Directory -Path $paceRuntime -Force | Out-Null
    $paceLog = Join-Path $paceRuntime 'runner.log'
    Add-Content -LiteralPath $paceLog -Value ('START ' + (Get-Date -Format o))
    $paceEnvFile = Join-Path $paceRuntime 'runner.env'
    $paceEntryFile = Join-Path $PSScriptRoot 'entry-data.js'
    & 'C:\Program Files\nodejs\node.exe' "--env-file=$paceEnvFile" $paceEntryFile >> $paceLog 2>&1
    $paceExit = $LASTEXITCODE
} catch {
    $paceExit = 1
    [Console]::Error.WriteLine('Timing launcher: ' + $_.Exception.Message)
    if ($paceLog) {
        try { Add-Content -LiteralPath $paceLog -Value ('ERROR ' + $_.Exception.Message) } catch { }
    }
} finally {
    if ($paceLog) {
        try { Add-Content -LiteralPath $paceLog -Value ('END ' + (Get-Date -Format o) + ' exit=' + $paceExit) }
        catch { $paceExit = 1; [Console]::Error.WriteLine('Timing launcher log write failed') }
    }
    # Keep the normal schedule enabled; report failure through the receipt and exit code.
}
exit $paceExit
