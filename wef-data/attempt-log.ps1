# Local durable start is deliberately before credential and process checks.
function Start-WefAttempt {
    param([string]$Root, [string]$Workflow, [string]$BaseId)
    $attemptId = [Guid]::NewGuid().ToString()
    $folder = Join-Path $Root 'run-logs'
    New-Item -ItemType Directory -Path $folder -Force | Out-Null
    $record = @{ id=$attemptId; workflow=$Workflow; base=$BaseId; started=[DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ'); status='Started'; stage='Startup'; 'owner-pid'=$PID }
    Write-WefLogFile (Join-Path $folder ($attemptId+'.json')) $record
    return $attemptId
}
function Write-WefLogFile {
    param([string]$Path, [object]$Value)
    $temporary = $Path+'.tmp'
    $bytes = [Text.Encoding]::UTF8.GetBytes(($Value | ConvertTo-Json -Compress -Depth 8))
    $stream = [IO.File]::Open($temporary,[IO.FileMode]::Create,[IO.FileAccess]::Write,[IO.FileShare]::None)
    try { $stream.Write($bytes,0,$bytes.Length); $stream.Flush($true) } finally { $stream.Dispose() }
    Move-Item -LiteralPath $temporary -Destination $Path -Force
}
function Complete-WefFallback {
    param([string]$Root,[string]$AttemptId,[int]$ExitCode)
    $file=Join-Path (Join-Path $Root 'run-logs') ($AttemptId+'.json')
    $record=Get-Content -LiteralPath $file -Raw | ConvertFrom-Json
    if($record.status -ne 'Started'){ return }
    $now=[DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ss.fffZ')
    $record.status='Failed'; $record.stage='ProcessStartup'
    $record | Add-Member -NotePropertyName completed -NotePropertyValue $now
    $record | Add-Member -NotePropertyName 'elapsed-seconds' -NotePropertyValue ([Math]::Max(0,([DateTime]::Parse($now)-[DateTime]::Parse($record.started)).TotalSeconds))
    $errorDir=Join-Path $Root 'err-logs'; New-Item -ItemType Directory -Path $errorDir -Force | Out-Null
    $ordinal=@(Get-ChildItem -LiteralPath $errorDir -Filter ($AttemptId+'-*.json')).Count+1
    $errorId=$AttemptId+'-'+$ordinal.ToString('0000')
    Write-WefLogFile (Join-Path $errorDir ($errorId+'.json')) @{id=$errorId;attempt=$AttemptId;at=$now;stage='ProcessStartup';category='Process';operation='Start';target='Local runner';message=('Runner returned no terminal attempt receipt; exit '+$ExitCode);'write-outcome'='Unknown'}
    Write-WefLogFile $file $record
}
