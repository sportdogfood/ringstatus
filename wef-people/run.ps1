$ErrorActionPreference='Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot '../wef-data/attempt-log.ps1')
$root=Join-Path $PSScriptRoot 'runtime/logging'
$attempt=Start-WefAttempt $root 'wef-people' 'app6uHgUYCCKTVlJE'
$env:WEF_PEOPLE_ATTEMPT=$attempt
$envPath=Join-Path $PSScriptRoot '../wef-data/runtime/collector.env'
$exitCode=1
try {
    if(-not(Test-Path -LiteralPath $envPath)){throw 'Missing host-managed environment'}
    & node --env-file=$envPath (Join-Path $PSScriptRoot 'entry.js')
    $exitCode=$LASTEXITCODE
} finally {
    Complete-WefFallback $root $attempt $exitCode
    $row=Get-Content (Join-Path $root ('run-logs/'+$attempt+'.json')) -Raw|ConvertFrom-Json
    if($row.status -eq 'Failed'){
        $stop=Join-Path $PSScriptRoot 'runtime/stopped.json'
        if(-not(Test-Path -LiteralPath $stop)){Write-WefLogFile $stop @{at=[DateTime]::UtcNow.ToString('o');reason='Failed scheduled attempt';attempt=$attempt}}
    }
}
exit $exitCode
