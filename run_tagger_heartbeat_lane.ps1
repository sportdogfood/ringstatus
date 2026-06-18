$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot 'runner_pipeline_common.ps1')

function Get-WecEnvValue {
    param([string]$Name)

    $value = [Environment]::GetEnvironmentVariable($Name, 'Process')
    if ([string]::IsNullOrWhiteSpace($value)) {
        $value = [Environment]::GetEnvironmentVariable($Name, 'User')
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            Set-Item -LiteralPath "Env:$Name" -Value $value
        }
    }
    return $value
}

function Get-WecActiveFocusKey {
    param(
        [string]$BaseId,
        [string]$Token
    )

    $encodedBase = [uri]::EscapeDataString($BaseId)
    $encodedTable = [uri]::EscapeDataString('focus_show')
    $filter = [uri]::EscapeDataString('{active}=1')
    $fields = @('show_no', 'focus_day') | ForEach-Object {
        "fields%5B%5D=$([uri]::EscapeDataString($_))"
    }
    $url = "https://api.airtable.com/v0/$encodedBase/$encodedTable`?maxRecords=1&filterByFormula=$filter&$($fields -join '&')"
    $headers = @{ Authorization = "Bearer $Token" }
    $response = Invoke-RestMethod -Method Get -Uri $url -Headers $headers
    $records = @($response.records)
    if ($records.Count -lt 1) {
        throw "active WEC focus_show not found"
    }

    $fields = $records[0].fields
    $showNo = [string]$fields.show_no
    $focusDayRaw = [string]$fields.focus_day
    if ([string]::IsNullOrWhiteSpace($showNo) -or [string]::IsNullOrWhiteSpace($focusDayRaw)) {
        throw "active WEC focus_show missing show_no or focus_day"
    }

    $focusDay = ([datetime]$focusDayRaw).ToString('yyyy-MM-dd')
    return @{
        ShowNo = $showNo
        FocusDay = $focusDay
        Key = "$showNo|$focusDay"
    }
}

function Get-WecDateKey {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ''
    }

    try {
        return ([datetime]$Value).ToUniversalTime().ToString('yyyy-MM-dd')
    }
    catch {
        return ''
    }
}

function Get-WecSha256 {
    param([string]$Text)

    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    $hashBytes = [System.Security.Cryptography.SHA256]::Create().ComputeHash($bytes)
    return ([BitConverter]::ToString($hashBytes) -replace '-', '').ToLowerInvariant()
}

function Get-WecStage1SourceSignature {
    param(
        [string]$ShowNo,
        [string]$FocusDay
    )

    $syncUrl = Get-WecEnvValue -Name 'WEC_SYNC_URL'
    if ([string]::IsNullOrWhiteSpace($syncUrl)) {
        $syncUrl = 'https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/'
    }

    $rows = @()
    for ($offset = 0; ; $offset += 200) {
        $uri = [System.UriBuilder]::new($syncUrl)
        $query = [System.Web.HttpUtility]::ParseQueryString('')
        $query['action'] = 'export-mirror-table'
        $query['show_no'] = $ShowNo
        $query['table'] = 'rings'
        $query['limit'] = '200'
        $query['offset'] = [string]$offset
        $uri.Query = $query.ToString()

        $payload = Invoke-RestMethod -Method Get -Uri $uri.Uri.AbsoluteUri
        $page = @($payload.data)
        $rows += $page
        if (-not $payload.has_more -or $page.Count -lt 200) {
            break
        }
    }

    $selectedRows = @(
        $rows |
            Where-Object {
                [string]$_.show_no -eq $ShowNo -and
                -not [string]::IsNullOrWhiteSpace([string]$_.ring_day_no) -and
                (Get-WecDateKey -Value ([string]$_.day_label)) -eq $FocusDay
            } |
            Sort-Object @{ Expression = { [string]$_.ring_no } }, @{ Expression = { [string]$_.ring_day_no } }
    )

    $ringDayNoSet = @($selectedRows | ForEach-Object { [string]$_.ring_day_no })
    $signatureSource = @($selectedRows | ForEach-Object {
        "{0}|{1}|{2}|{3}" -f ([string]$_.ring_no), ([string]$_.ring_day_no), ([string]$_.ring_name), (Get-WecDateKey -Value ([string]$_.day_label))
    }) -join "`n"

    return @{
        RowCount = $selectedRows.Count
        RingDayNoSet = $ringDayNoSet
        RingDayNoSetText = ($ringDayNoSet -join ',')
        SourceSignature = Get-WecSha256 -Text $signatureSource
    }
}

function Invoke-WecStage2HeartbeatBinding {
    $stage2WrapperPath = 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\catalyst-workspaces\horseshowing\runners\sync_focus_update_schedule_to_staging.js'
    $stage2Root = Split-Path -Parent $stage2WrapperPath
    $stage2LogDir = Join-Path $stage2Root 'logs'
    $stage2LogPath = Join-Path $stage2LogDir 'sync_focus_update_schedule_to_staging.log'
    $stage2LastSuccessPath = Join-Path $stage2LogDir 'sync_focus_update_schedule_to_staging.last_success.json'

    if (-not (Test-Path $stage2WrapperPath)) {
        throw "Stage 2 wrapper not found: $stage2WrapperPath"
    }

    if (-not (Test-Path $stage2LogDir)) {
        New-Item -ItemType Directory -Path $stage2LogDir -Force | Out-Null
    }

    $airtableToken = Get-WecEnvValue -Name 'AIRTABLE_TOKEN'
    if ([string]::IsNullOrWhiteSpace($airtableToken)) {
        $airtableToken = Get-WecEnvValue -Name 'AIRTABLE_WEC_TOKEN'
    }
    if ([string]::IsNullOrWhiteSpace($airtableToken)) {
        throw "AIRTABLE_TOKEN or AIRTABLE_WEC_TOKEN is required for WEC Stage 2 heartbeat binding"
    }

    $baseId = Get-WecEnvValue -Name 'WEC_AIRTABLE_BASE_ID'
    if ([string]::IsNullOrWhiteSpace($baseId)) {
        $baseId = 'app6XS1RvsPNRT6os'
        $env:WEC_AIRTABLE_BASE_ID = $baseId
    }

    $focus = Get-WecActiveFocusKey -BaseId $baseId -Token $airtableToken
    $stage1Signature = Get-WecStage1SourceSignature -ShowNo $focus.ShowNo -FocusDay $focus.FocusDay
    $guardKeySafe = $focus.Key -replace '[^A-Za-z0-9_.-]', '_'
    $stage2LockPath = Join-Path $stage2LogDir "sync_focus_update_schedule_to_staging.$guardKeySafe.lock"

    if (Test-Path $stage2LockPath) {
        throw "Stage 2 duplicate-run guard active for $($focus.Key): $stage2LockPath"
    }

        Set-Content -LiteralPath $stage2LockPath -Value (@{
            key = $focus.Key
            show_no = $focus.ShowNo
            focus_day = $focus.FocusDay
            stage1_row_count = $stage1Signature.RowCount
            ring_day_no_set = $stage1Signature.RingDayNoSetText
            stage1_source_signature = $stage1Signature.SourceSignature
            started_at = (Get-Date).ToString('o')
        } | ConvertTo-Json -Depth 4) -Encoding UTF8

    try {
        $nodePath = 'C:\Program Files\nodejs\node.exe'
        if (-not (Test-Path $nodePath)) {
            $nodePath = 'node'
        }

        Append-Utf8Text -Path $stage2LogPath -Text "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] RUN Stage 2 $($focus.Key)`r`n"
        Push-Location $stage2Root
        try {
            $output = & $nodePath $stage2WrapperPath 2>&1 | Out-String
            $exitCode = $LASTEXITCODE
        }
        finally {
            Pop-Location
        }

        Append-Utf8Text -Path $stage2LogPath -Text $output
        Append-Utf8Text -Path $stage2LogPath -Text "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] EXIT $exitCode Stage 2 $($focus.Key)`r`n"

        if ($exitCode -ne 0) {
            throw "Stage 2 wrapper failed for $($focus.Key); see $stage2LogPath"
        }

        $summary = $output | ConvertFrom-Json
        $summaryKey = "$($summary.show_no)|$($summary.focus_day)"
        if ($summaryKey -ne $focus.Key) {
            throw "Stage 2 summary key mismatch: expected $($focus.Key), got $summaryKey"
        }

        Set-Content -LiteralPath $stage2LastSuccessPath -Value (@{
            key = $focus.Key
            show_no = $focus.ShowNo
            focus_day = $focus.FocusDay
            stage1_row_count = $stage1Signature.RowCount
            ring_day_no_set = $stage1Signature.RingDayNoSetText
            stage1_source_signature = $stage1Signature.SourceSignature
            completed_at = (Get-Date).ToString('o')
            log_path = $stage2LogPath
        } | ConvertTo-Json -Depth 4) -Encoding UTF8
    }
    finally {
        Remove-Item -LiteralPath $stage2LockPath -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-WecStage2HeartbeatBindingIfEnabled {
    $enabled = Get-WecEnvValue -Name 'WEC_STAGE2_HEARTBEAT_ENABLED'
    if ($enabled -ne '1') {
        return
    }

    $stage2WrapperPath = 'C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus-data\catalyst-workspaces\horseshowing\runners\sync_focus_update_schedule_to_staging.js'
    $stage2LogDir = Join-Path (Split-Path -Parent $stage2WrapperPath) 'logs'
    $stage2LogPath = Join-Path $stage2LogDir 'sync_focus_update_schedule_to_staging.log'
    if (-not (Test-Path $stage2LogDir)) {
        New-Item -ItemType Directory -Path $stage2LogDir -Force | Out-Null
    }

    try {
        Invoke-WecStage2HeartbeatBinding
        Append-Utf8Text -Path $stage2LogPath -Text "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] WEC Stage 2 heartbeat PASS`r`n"
    }
    catch {
        Append-Utf8Text -Path $stage2LogPath -Text "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] WEC Stage 2 heartbeat FAIL: $($_.Exception.Message)`r`n"
    }
}

Invoke-WecStage2HeartbeatBindingIfEnabled

$steps = @(
    @{
        Label = 'TAGGER'
        ScriptName = 'tagger.js'
        LogFileName = 'epoch-tagger.log'
        ContinueOnError = $false
    },
    @{
        Label = 'HEARTBEAT_PATTERNS'
        ScriptName = 'heartbeat_patterns.js'
        LogFileName = 'epoch-tagger.log'
        ContinueOnError = $false
    },
    @{
        Label = 'HEARTBEAT_TASK_CADENCE'
        ScriptName = 'heartbeat_task_cadence.js'
        LogFileName = 'heartbeat-task-cadence.log'
        ContinueOnError = $true
    },
    @{
        Label = 'SLOT_ORCHESTRATOR'
        ScriptName = 'heartbeat_slot_orchestrator.js'
        LogFileName = 'heartbeat-slot-orchestrator.log'
        ContinueOnError = $true
    }
)

Invoke-RunnerPipeline `
    -ScriptRoot $PSScriptRoot `
    -PipelineName 'heartbeat_lane' `
    -SummaryFileName 'runner-pipeline-heartbeat.log' `
    -TrackedFiles @(
        'run_tagger_heartbeat_lane.cmd',
        'run_tagger_heartbeat_lane.ps1',
        'runner_pipeline_common.ps1',
        'tagger.js',
        'heartbeat_patterns.js',
        'heartbeat_task_cadence.js',
        'lib/heartbeat_mode.js',
        'lib/default_show_date_guard.js',
        'lib/watch_schedule_scope_relink.js',
        'lib/watch_trips_scope_relink.js',
        'lib/watch_trips_scope.js',
        'heartbeat_slot_orchestrator.js',
        'live_groups_daily.js',
        'live_rings_daily.js',
        'live_class_detail.js'
    ) `
    -Steps $steps
