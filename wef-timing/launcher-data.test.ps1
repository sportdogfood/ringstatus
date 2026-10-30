$ErrorActionPreference = 'Stop'
$paceTestRoot = Join-Path ([IO.Path]::GetTempPath()) ('ring pace launcher ' + [guid]::NewGuid().ToString())
[IO.Directory]::CreateDirectory($paceTestRoot) | Out-Null
$paceBody = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'run-data.ps1'))
$paceVbs = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'run-hidden-data.vbs'))
$paceCases = @('success','node_failure','initialization_failure','logging_failure','final_logging_failure')
$paceResults = @()
foreach ($paceCase in $paceCases) {
    $paceCaseRoot = Join-Path $paceTestRoot $paceCase
    [IO.Directory]::CreateDirectory((Join-Path $paceCaseRoot 'runtime/data')) | Out-Null
    [IO.File]::WriteAllText((Join-Path $paceCaseRoot 'runtime/data/runner.env'), "PACE_FIXTURE_ONLY=loaded`n")
    $paceDummy = "const fs=require('node:fs'),path=require('node:path');fs.writeFileSync(path.join(__dirname,'node-probe.json'),JSON.stringify({env:process.env.PACE_FIXTURE_ONLY,entry:process.argv[1]}));process.exit(" + $(if ($paceCase -eq 'node_failure') {'7'} else {'0'}) + ");"
    [IO.File]::WriteAllText((Join-Path $paceCaseRoot 'entry-data.js'), $paceDummy)
    $paceHeader = @'
# INERT FIXTURE: fake entry-data.js only; task disabling is intercepted in this child process.
[IO.File]::WriteAllText((Join-Path $PSScriptRoot 'shell-version.txt'), $PSVersionTable.PSVersion.ToString())
function Disable-ScheduledTask {
    param([string]$TaskName, [string]$ErrorAction)
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot 'disable-probe.txt'), $TaskName)
}
'@
    if ($paceCase -eq 'initialization_failure') { $paceHeader += "`nfunction New-Item { throw 'Fixture initialization failure' }`n" }
    if ($paceCase -eq 'logging_failure') { $paceHeader += "`nfunction Add-Content { throw 'Fixture logging failure' }`n" }
    if ($paceCase -eq 'final_logging_failure') {
        $paceHeader += @'

$script:paceFixtureLogCalls = 0
function Add-Content {
    param($LiteralPath, $Value)
    $script:paceFixtureLogCalls++
    if ($script:paceFixtureLogCalls -gt 1) { throw 'Fixture final logging failure' }
    Microsoft.PowerShell.Management\Add-Content -LiteralPath $LiteralPath -Value $Value
}
'@
    }
    [IO.File]::WriteAllText((Join-Path $paceCaseRoot 'run-data.ps1'), $paceHeader + "`n" + $paceBody)
    [IO.File]::WriteAllText((Join-Path $paceCaseRoot 'run-hidden-data.vbs'), $paceVbs)
    $paceProcess = Start-Process -FilePath "$env:SystemRoot\System32\wscript.exe" -ArgumentList @('//B','//Nologo',('"' + (Join-Path $paceCaseRoot 'run-hidden-data.vbs') + '"')) -WindowStyle Hidden -Wait -PassThru
    $paceVersion = [IO.File]::ReadAllText((Join-Path $paceCaseRoot 'shell-version.txt'))
    if (-not $paceVersion.StartsWith('5.1.')) { throw "Wrong fixture shell: $paceVersion" }
    $paceDisabled = Test-Path -LiteralPath (Join-Path $paceCaseRoot 'disable-probe.txt')
    if ($paceCase -eq 'success') {
        if ($paceProcess.ExitCode -ne 0 -or $paceDisabled) { throw 'Success fixture did not propagate zero without disabling' }
        $paceProbe = [IO.File]::ReadAllText((Join-Path $paceCaseRoot 'node-probe.json')) | ConvertFrom-Json
        if ($paceProbe.env -ne 'loaded' -or $paceProbe.entry -ne (Join-Path $paceCaseRoot 'entry-data.js')) { throw 'Quoted environment/entry arguments failed' }
    } else {
        if ($paceProcess.ExitCode -eq 0 -or -not $paceDisabled) { throw "Failure fixture bypassed disable/exit: $paceCase" }
        if ([IO.File]::ReadAllText((Join-Path $paceCaseRoot 'disable-probe.txt')) -ne 'wef-timing-data') { throw 'Wrong disable target' }
        if ($paceCase -eq 'node_failure' -and $paceProcess.ExitCode -ne 7) { throw 'Native exit code not propagated' }
    }
    $paceResults += @{case=$paceCase;pass=$true;shell=$paceVersion;exit=$paceProcess.ExitCode;disable_intercepted=$paceDisabled}
}
@{inert=$true;production_runner_invoked=$false;production_task_touched=$false;fixture_root=$paceTestRoot;results=$paceResults} | ConvertTo-Json -Depth 5
