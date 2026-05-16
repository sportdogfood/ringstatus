$ErrorActionPreference = 'Stop'

function ConvertTo-SqlDateList {
    param(
        [string]$StartDate,
        [string]$EndDate
    )

    $start = [datetime]::ParseExact($StartDate, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
    $end = [datetime]::ParseExact($EndDate, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
    if ($end -lt $start) {
        throw "Show end_date is before start_date: $StartDate -> $EndDate"
    }

    $dates = @()
    for ($day = $start; $day -le $end; $day = $day.AddDays(1)) {
        $dates += $day.ToString('yyyy-MM-dd')
    }
    return $dates
}

function Resolve-HeartbeatTargetShow {
    param(
        [string]$BaseId,
        [string]$ShowId,
        [string]$TableName = 'show',
        [string]$ViewName = 'heartbeat'
    )

    $token = [Environment]::GetEnvironmentVariable('AIRTABLE_TOKEN', 'Process')
    if ([string]::IsNullOrWhiteSpace($token)) {
        $token = [Environment]::GetEnvironmentVariable('AIRTABLE_TOKEN', 'User')
    }
    if ([string]::IsNullOrWhiteSpace($token)) {
        throw "AIRTABLE_TOKEN is required to resolve HEARTBEAT_TARGET_APP_SHOW_ID from shows"
    }

    $encodedBase = [uri]::EscapeDataString($BaseId)
    $encodedTable = [uri]::EscapeDataString($TableName)
    $url = "https://api.airtable.com/v0/$encodedBase/$encodedTable"
    $queryParts = @()
    if (-not [string]::IsNullOrWhiteSpace($ViewName)) {
        $queryParts += "view=$([uri]::EscapeDataString($ViewName))"
    }
    foreach ($field in @('show_id', 'customer_id', 'start_date', 'end_date', 'show_name', 'heartbeat')) {
        $queryParts += "fields%5B%5D=$([uri]::EscapeDataString($field))"
    }
    $requestUri = "$url`?$($queryParts -join '&')"

    $headers = @{ Authorization = "Bearer $token" }
    try {
        $response = Invoke-RestMethod -Method Get -Uri $requestUri -Headers $headers
        $records = @($response.records)
        if ([string]::IsNullOrWhiteSpace($ShowId)) {
            $heartbeatRecords = @($records | Where-Object { $_.fields.heartbeat -eq $true })
            if ($heartbeatRecords.Count -gt 0) {
                $records = $heartbeatRecords
            }
        }
    }
    catch {
        $fallbackParts = @()
        foreach ($field in @('show_id', 'customer_id', 'start_date', 'end_date', 'show_name', 'heartbeat')) {
            $fallbackParts += "fields%5B%5D=$([uri]::EscapeDataString($field))"
        }
        $fallbackUri = "$url`?$($fallbackParts -join '&')"
        $response = Invoke-RestMethod -Method Get -Uri $fallbackUri -Headers $headers
        $records = @($response.records | Where-Object { $_.fields.heartbeat -eq $true })
    }

    if (-not [string]::IsNullOrWhiteSpace($ShowId)) {
        $records = @($records | Where-Object { [string]$_.fields.show_id -eq $ShowId })
    }

    if ($records.Count -lt 1) {
        throw "No focused show record found in $TableName/$ViewName"
    }
    if ($records.Count -gt 1) {
        throw "Multiple focused show records found in $TableName/$ViewName"
    }

    $fields = $records[0].fields
    $customerId = $fields.customer_id
    if ([string]::IsNullOrWhiteSpace([string]$customerId)) {
        throw "focused show record has no customer_id"
    }

    $startDate = [string]$fields.start_date
    $endDate = [string]$fields.end_date
    if ([string]::IsNullOrWhiteSpace($startDate) -or [string]::IsNullOrWhiteSpace($endDate)) {
        throw "focused show record must have start_date and end_date"
    }

    $dateList = ConvertTo-SqlDateList -StartDate $startDate -EndDate $endDate
    return [pscustomobject]@{
        RecordId = $records[0].id
        ShowId = [string]$fields.show_id
        CustomerId = [string]$customerId
        StartDate = $startDate
        EndDate = $endDate
        SqlDates = $dateList
        ShowName = [string]$fields.show_name
    }
}

function Initialize-RunnerDefaults {
    param([string]$ScriptRoot)

    $env:DRY_RUN           = '0'
    $env:CALC_MODE         = 'promote'
    $env:WATCH_VIEW        = 'heartbeat'
    $env:AIRTABLE_BASE_ID  = 'apptdhhNzduxm5gjn'
    $env:AIRTABLE_TABLE    = 'tblCnHDB4IVtxqulo'
    $env:AIRTABLE_VIEW_HOT = 'viwATt1y2RKpn2FSZ'
    $env:CUSTOMER_ID       = '15'

    $targetShow = Resolve-HeartbeatTargetShow -BaseId $env:AIRTABLE_BASE_ID
    if ($targetShow) {
        $env:HEARTBEAT_TARGET_APP_SHOW_ID = $targetShow.ShowId
        $env:HEARTBEAT_TARGET_SQL_DATES = ($targetShow.SqlDates -join ',')
        $todaySqlDate = Get-Date -Format 'yyyy-MM-dd'
        if ($targetShow.SqlDates -contains $todaySqlDate) {
            $env:CUSTOMER_ID = $targetShow.CustomerId
        }
    }

    $sglEnvNames = @(
        'SGL_AUTHORIZATION',
        'SGL_BEARER_TOKEN',
        'SGL_COOKIE_HEADER',
        'SGL_FETCH_SESSION_JSON',
        'SGL_USER_AGENT',
        'SGL_SEC_CH_UA',
        'SGL_SEC_CH_UA_MOBILE',
        'SGL_SEC_CH_UA_PLATFORM',
        'SGL_RECAPTCHA_TOKEN',
        'SGL_X_RECAPTCHA_TOKEN'
    )
    foreach ($name in $sglEnvNames) {
        if (-not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name, 'Process'))) {
            continue
        }

        $userValue = [Environment]::GetEnvironmentVariable($name, 'User')
        if (-not [string]::IsNullOrWhiteSpace($userValue)) {
            Set-Item -LiteralPath "Env:$name" -Value $userValue
        }
    }

    if (-not $env:PUBLISHER_DELAY_SECONDS) {
        $env:PUBLISHER_DELAY_SECONDS = '0'
    }

    $repoPath = $ScriptRoot
    $nodePath = 'C:\Program Files\nodejs\node.exe'
    $logDir = 'C:\actions-runner\ringstatus'

    if (-not (Test-Path $logDir)) {
        New-Item -ItemType Directory -Path $logDir -Force | Out-Null
    }

    if (-not (Test-Path $repoPath)) {
        throw "Repo path not found: $repoPath"
    }

    if (-not (Test-Path $nodePath)) {
        $nodePath = 'node'
    }

    Set-Location $repoPath

    return @{
        RepoPath = $repoPath
        NodePath = $nodePath
        LogDir = $logDir
    }
}

function Get-LogEncodingIssue {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return $null
    }

    $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        $sampleLength = [int][Math]::Min([int64]256, $stream.Length)
        if ($sampleLength -le 0) {
            return $null
        }

        $buffer = New-Object byte[] $sampleLength
        [void]$stream.Read($buffer, 0, $sampleLength)
    }
    finally {
        $stream.Dispose()
    }

    if ($sampleLength -ge 2) {
        if (($buffer[0] -eq 0xFF -and $buffer[1] -eq 0xFE) -or ($buffer[0] -eq 0xFE -and $buffer[1] -eq 0xFF)) {
            return 'utf16_bom'
        }
    }

    $nullByteCount = ($buffer | Where-Object { $_ -eq 0 }).Count
    if ($nullByteCount -ge 8) {
        return 'utf16_null_bytes'
    }

    return $null
}

function Ensure-Utf8LogFile {
    param([string]$Path)

    $issue = Get-LogEncodingIssue -Path $Path
    if (-not $issue) {
        return
    }

    $directory = Split-Path -Parent $Path
    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($Path)
    $extension = [System.IO.Path]::GetExtension($Path)
    $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $archiveName = "{0}.{1}.{2}{3}" -f $baseName, $issue, $stamp, $extension
    $archivePath = Join-Path $directory $archiveName
    Move-Item -Path $Path -Destination $archivePath -Force
}

function Append-Utf8Text {
    param(
        [string]$Path,
        [string]$Text
    )

    Ensure-Utf8LogFile -Path $Path
    [System.IO.File]::AppendAllText(
        $Path,
        $Text,
        [System.Text.UTF8Encoding]::new($false)
    )
}

function Get-TrackedFileState {
    param(
        [string]$RepoPath,
        [string[]]$RelativePaths
    )

    $items = @()
    foreach ($relativePath in $RelativePaths) {
        $fullPath = Join-Path $RepoPath $relativePath
        if (-not (Test-Path $fullPath)) {
            $items += [pscustomobject]@{
                path = $relativePath
                exists = $false
            }
            continue
        }

        $item = Get-Item $fullPath
        $hash = (Get-FileHash -Algorithm SHA256 -Path $fullPath).Hash
        $items += [pscustomobject]@{
            path = $relativePath
            exists = $true
            length = $item.Length
            last_write_time = $item.LastWriteTime.ToString('o')
            sha256 = $hash
        }
    }

    return $items
}

function Invoke-RunnerPipeline {
    param(
        [string]$ScriptRoot,
        [string]$PipelineName,
        [string]$SummaryFileName,
        [string[]]$TrackedFiles,
        [object[]]$Steps,
        [switch]$IncludePublisherDelay
    )

    $context = Initialize-RunnerDefaults -ScriptRoot $ScriptRoot
    $repoPath = $context.RepoPath
    $nodePath = $context.NodePath
    $logDir = $context.LogDir
    $summaryPath = Join-Path $logDir $SummaryFileName
    $deferredFailures = @()

    function Write-PipelineEvent {
        param([hashtable]$EventData)

        $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
        $json = $EventData | ConvertTo-Json -Compress -Depth 6
        Append-Utf8Text -Path $summaryPath -Text "[$timestamp] RUNNER PIPELINE`r`n$json`r`n"
    }

    function Invoke-RunnerStep {
        param(
            [string]$Label,
            [string]$ScriptName,
            [string]$LogPath,
            [bool]$ContinueOnError
        )

        $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
        $allText = "[$timestamp] $Label RUN`r`n"
        $scriptPath = Join-Path $repoPath $ScriptName
        $startedAt = Get-Date

        $nodeOutput = & $nodePath $scriptPath 2>&1 | Out-String
        $exitCode = $LASTEXITCODE
        $durationMs = [int][Math]::Round(((Get-Date) - $startedAt).TotalMilliseconds)
        $allText += $nodeOutput
        $allText += ("{0}`r`n" -f (@{
            ok = ($exitCode -eq 0)
            event = 'step_completed'
            label = $Label
            script = $ScriptName
            exit_code = $exitCode
            duration_ms = $durationMs
            pipeline = $PipelineName
        } | ConvertTo-Json -Compress))

        Append-Utf8Text -Path $LogPath -Text $allText
        Write-PipelineEvent @{
            ok = ($exitCode -eq 0)
            event = 'step_completed'
            label = $Label
            script = $ScriptName
            log_path = $LogPath
            exit_code = $exitCode
            duration_ms = $durationMs
            pipeline = $PipelineName
            continue_on_error = $ContinueOnError
        }

        if ($exitCode -ne 0) {
            if ($ContinueOnError) {
                $script:deferredFailures += [pscustomobject]@{
                    Label = $Label
                    ScriptName = $ScriptName
                    ExitCode = $exitCode
                    LogPath = $LogPath
                    Timestamp = $timestamp
                    DurationMs = $durationMs
                    Pipeline = $PipelineName
                }
                return
            }

            exit $exitCode
        }
    }

    Write-PipelineEvent @{
        ok = $true
        event = 'pipeline_started'
        pipeline = $PipelineName
        publisher_delay_seconds = [int]$env:PUBLISHER_DELAY_SECONDS
        repo_path = $repoPath
        tracked_files = Get-TrackedFileState -RepoPath $repoPath -RelativePaths $TrackedFiles
    }

    foreach ($step in $Steps) {
        $logPath = Join-Path $logDir $step.LogFileName
        Invoke-RunnerStep `
            -Label $step.Label `
            -ScriptName $step.ScriptName `
            -LogPath $logPath `
            -ContinueOnError ([bool]$step.ContinueOnError)
    }

    if ($IncludePublisherDelay) {
        $publisherDelaySeconds = [int]($env:PUBLISHER_DELAY_SECONDS)
        if ($publisherDelaySeconds -gt 0) {
            Write-PipelineEvent @{
                ok = $true
                event = 'publisher_delay_started'
                pipeline = $PipelineName
                seconds = $publisherDelaySeconds
            }
            Start-Sleep -Seconds $publisherDelaySeconds
        }
    }

    if ($deferredFailures.Count -gt 0) {
        Write-PipelineEvent @{
            ok = $false
            event = 'pipeline_completed_with_deferred_failures'
            pipeline = $PipelineName
            failures = $deferredFailures
        }

        exit 1
    }

    Write-PipelineEvent @{
        ok = $true
        event = 'pipeline_completed'
        pipeline = $PipelineName
        deferred_failures = 0
    }

    exit $LASTEXITCODE
}
