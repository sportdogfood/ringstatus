$ErrorActionPreference='Stop'
Set-Location -LiteralPath $PSScriptRoot
. (Join-Path $PSScriptRoot '../wef-data/attempt-log.ps1')
$config=Get-Content (Join-Path $PSScriptRoot '../wef-data/logging-schema.json') -Raw|ConvertFrom-Json
$attempts=@{}
foreach($entry in $config.workflows.PSObject.Properties){if($entry.Name -notlike 'wef-season-*'){continue};$root=Join-Path $PSScriptRoot ('runtime/logging/'+$entry.Name);$attempts[$entry.Name]=Start-WefAttempt $root $entry.Name $entry.Value.base}
$env:WEF_SEASON_ATTEMPTS=$attempts|ConvertTo-Json -Compress
$envPath=Join-Path $PSScriptRoot '../wef-data/runtime/collector.env'
$exitCode=1
try{if(-not(Test-Path -LiteralPath $envPath)){throw 'Missing host-managed environment'};& node --env-file=$envPath (Join-Path $PSScriptRoot 'entry.js');$exitCode=$LASTEXITCODE}
finally{foreach($entry in $attempts.GetEnumerator()){Complete-WefFallback (Join-Path $PSScriptRoot ('runtime/logging/'+$entry.Key)) $entry.Value $exitCode}}
exit $exitCode
