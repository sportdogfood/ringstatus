$ErrorActionPreference = 'Stop'

$repoPath = Split-Path -Parent $PSCommandPath
$runTaskPath = Join-Path $repoPath 'run_tagger_task.ps1'

if (-not (Test-Path $runTaskPath)) {
    throw "run_tagger_task.ps1 not found at $runTaskPath"
}

$baseId = if ($env:AIRTABLE_BASE_ID) { $env:AIRTABLE_BASE_ID } else { 'apptdhhNzduxm5gjn' }
$tableName = if ($env:TABLE_SHOW_TARGET) { $env:TABLE_SHOW_TARGET } elseif ($env:TABLE_SHOW) { $env:TABLE_SHOW } else { 'show' }
$viewName = if ($env:VIEW_SHOW_TARGET) { $env:VIEW_SHOW_TARGET } else { 'heartbeat' }

$token = [Environment]::GetEnvironmentVariable('AIRTABLE_TOKEN', 'Process')
if ([string]::IsNullOrWhiteSpace($token)) {
    $token = [Environment]::GetEnvironmentVariable('AIRTABLE_TOKEN', 'User')
}
if ([string]::IsNullOrWhiteSpace($token)) {
    throw 'AIRTABLE_TOKEN is required to resolve focused show records'
}

$encodedBase = [uri]::EscapeDataString($baseId)
$encodedTable = [uri]::EscapeDataString($tableName)
$queryParts = @("view=$([uri]::EscapeDataString($viewName))")
foreach ($field in @('show_id', 'customer_id', 'focus_day', 'start_date', 'end_date', 'heartbeat')) {
    $queryParts += "fields%5B%5D=$([uri]::EscapeDataString($field))"
}
$uri = "https://api.airtable.com/v0/$encodedBase/$encodedTable`?$($queryParts -join '&')"
$headers = @{ Authorization = "Bearer $token" }
$todaySqlDate = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([datetime]::UtcNow, 'Eastern Standard Time').ToString('yyyy-MM-dd')

function Test-ShowRecordInActiveWindow {
    param($Record)
    $startDate = [string]$Record.fields.start_date
    $endDate = [string]$Record.fields.end_date
    if ([string]::IsNullOrWhiteSpace($startDate) -or [string]::IsNullOrWhiteSpace($endDate)) {
        return $true
    }
    return ($todaySqlDate -ge $startDate -and $todaySqlDate -le $endDate)
}

$records = @()
do {
    $response = Invoke-RestMethod -Method Get -Uri $uri -Headers $headers
    $records += @($response.records | Where-Object { $_.fields.heartbeat -eq $true -and (Test-ShowRecordInActiveWindow $_) })
    if ($response.offset) {
        $uri = "https://api.airtable.com/v0/$encodedBase/$encodedTable`?$($queryParts -join '&')&offset=$([uri]::EscapeDataString($response.offset))"
    } else {
        $uri = $null
    }
} while ($uri)

if ($records.Count -lt 1) {
    Write-Host "No focused show records found in $tableName/$viewName; writing no-active-feeds heartbeat."
    $env:HEARTBEAT_NO_ACTIVE_FEEDS = 'true'
    Remove-Item Env:HEARTBEAT_TARGET_SHOW_RECORD_ID -ErrorAction SilentlyContinue
    Remove-Item Env:HEARTBEAT_TARGET_APP_SHOW_ID -ErrorAction SilentlyContinue
    Remove-Item Env:HEARTBEAT_TARGET_SQL_DATES -ErrorAction SilentlyContinue
    Remove-Item Env:HEARTBEAT_TARGET_CUSTOMER_ID -ErrorAction SilentlyContinue
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runTaskPath
    exit $LASTEXITCODE
}

$failures = @()
foreach ($record in $records) {
    $env:HEARTBEAT_TARGET_SHOW_RECORD_ID = $record.id
    $env:HEARTBEAT_TARGET_APP_SHOW_ID = [string]$record.fields.show_id
    $env:HEARTBEAT_TARGET_CUSTOMER_ID = [string]$record.fields.customer_id
    $env:CUSTOMER_ID = [string]$record.fields.customer_id
    Remove-Item Env:HEARTBEAT_NO_ACTIVE_FEEDS -ErrorAction SilentlyContinue

    Write-Host "Running focused show $($record.id) show_id=$($record.fields.show_id) customer_id=$($record.fields.customer_id) focus_day=$($record.fields.focus_day)"
    & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runTaskPath
    if ($LASTEXITCODE -ne 0) {
        $failures += [pscustomobject]@{
            record_id = $record.id
            show_id = $record.fields.show_id
            customer_id = $record.fields.customer_id
            exit_code = $LASTEXITCODE
        }
    }
}

Remove-Item Env:HEARTBEAT_TARGET_SHOW_RECORD_ID -ErrorAction SilentlyContinue
Remove-Item Env:HEARTBEAT_TARGET_CUSTOMER_ID -ErrorAction SilentlyContinue
Remove-Item Env:HEARTBEAT_NO_ACTIVE_FEEDS -ErrorAction SilentlyContinue

if ($failures.Count -gt 0) {
    $failures | ConvertTo-Json -Depth 4 | Write-Error
    exit 1
}
