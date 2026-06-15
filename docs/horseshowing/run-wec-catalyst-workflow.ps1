param(
  [string]$ShowNo = "14906",
  [string]$FocusDay = "",
  [string]$BaseUrl = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/",
  [string]$AirtableBaseId = "app6XS1RvsPNRT6os",
  [switch]$ForceSync,
  [switch]$RunMockLiveCheck
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$logDir = Join-Path $root "logs"
$statePath = Join-Path $logDir "wec-catalyst-workflow-state.json"
$logPath = Join-Path $logDir "wec-catalyst-workflow.log"
$jsonlLogPath = Join-Path $logDir "wec-logs.jsonl"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$mutexName = "Global\RingStatusWecCatalystWorkflow-$ShowNo"
$script:WecWorkflowMutex = New-Object System.Threading.Mutex($false, $mutexName)
$script:WecWorkflowMutexAcquired = $script:WecWorkflowMutex.WaitOne(0)
if (!$script:WecWorkflowMutexAcquired) {
  $stamp = (Get-Date).ToString("s")
  Add-Content -Path $logPath -Value "$stamp skipped overlapping WEC workflow show=$ShowNo"
  exit 0
}

function Write-WorkflowLog($message) {
  $stamp = (Get-Date).ToString("s")
  $line = "$stamp $message"
  for ($i = 0; $i -lt 5; $i++) {
    try {
      Add-Content -Path $logPath -Value $line
      return
    } catch {
      Start-Sleep -Milliseconds (200 * ($i + 1))
    }
  }
}

function ConvertTo-SafeJson($value) {
  $json = $value | ConvertTo-Json -Depth 12 -Compress
  if ($json.Length -gt 90000) {
    return $json.Substring(0, 90000)
  }
  return $json
}

function Get-PayloadValue($Payload, [string]$Name) {
  if ($null -eq $Payload) { return $null }
  if ($Payload -is [hashtable] -and $Payload.ContainsKey($Name)) { return $Payload[$Name] }
  $property = $Payload.PSObject.Properties[$Name]
  if ($property) { return $property.Value }
  return $null
}

function Resolve-WorkflowLane {
  param(
    [string]$LogType,
    [string]$CheckName
  )

  if ($LogType -eq "airtable_check") { return "Helpers" }
  if ($LogType -eq "live" -or $CheckName -in @("get_rings", "get_orders")) { return "Live" }
  if ($LogType -eq "alerts" -or $CheckName -in @("class_start_times", "entry_go_times")) { return "Alerts" }
  if ($CheckName -in @("core_update_schedule", "core_class_oog", "core_counts")) { return "Core" }
  if ($CheckName -in @("airtable_helpers_summary", "airtable_helper_backfill", "catalyst_heartbeat", "catalyst_sync_focus_day", "catalyst_sync_ring_days_counts")) { return "Audits" }
  return ""
}

function Test-LiveAlertWindow($FocusDayValue) {
  if (!$FocusDayValue) { return $false }
  try {
    $today = (Get-Date).ToString("yyyy-MM-dd")
    $hour = (Get-Date).Hour
    return ([string]$FocusDayValue -eq $today -and $hour -ge 6 -and $hour -lt 19)
  } catch {
    return $false
  }
}

function Write-WecAirtableLog {
  param(
    [string]$LogType,
    [string]$CheckName,
    [string]$Status = "ok",
    [int]$RecordsSeen = 0,
    [int]$RecordsChanged = 0,
    [string]$Summary = "",
    $Payload = @{}
  )

  $createdAt = (Get-Date).ToUniversalTime().ToString("o")
  $workflowLane = Resolve-WorkflowLane -LogType $LogType -CheckName $CheckName
  $fields = @{
    log_key_run = "$createdAt|$LogType|$CheckName"
    created_at = $createdAt
    log_type = $LogType
    check_name = $CheckName
    show_no = $ShowNo
    status = $Status
    records_seen = $RecordsSeen
    records_changed = $RecordsChanged
    summary = $Summary
    payload_json = ConvertTo-SafeJson $Payload
  }
  if ($workflowLane) {
    $fields.workflow_lanes = $workflowLane
  }
  if ($FocusDay) {
    $fields.focus_day = $FocusDay
  } elseif ($Payload -and $Payload.focus_day) {
    $fields.focus_day = [string]$Payload.focus_day
  }

  $fields | ConvertTo-Json -Depth 12 -Compress | Add-Content -Path $jsonlLogPath

  if (!$env:AIRTABLE_TOKEN) {
    Write-WorkflowLog "airtable-log skipped missing AIRTABLE_TOKEN check=$CheckName"
    return
  }

  try {
    $body = @{
      fields = $fields
      typecast = $true
    } | ConvertTo-Json -Depth 12
    $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-logs'))"
    Invoke-RestMethod -Method Post -Uri $uri -Headers @{
      Authorization = "Bearer $env:AIRTABLE_TOKEN"
      "Content-Type" = "application/json"
    } -Body $body -TimeoutSec 30 | Out-Null
  } catch {
    Write-WorkflowLog "airtable-log failed check=$CheckName error=$($_.Exception.Message)"
  }
}

function Test-WecAlertExists($alertKey) {
  if (!$env:AIRTABLE_TOKEN) { return $false }
  try {
    $formula = [uri]::EscapeDataString("{alert_key_run}='$alertKey'")
    $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-alerts'))?filterByFormula=$formula&pageSize=1"
    $result = Invoke-RestMethod -Method Get -Uri $uri -Headers @{
      Authorization = "Bearer $env:AIRTABLE_TOKEN"
    } -TimeoutSec 30
    return @($result.records).Count -gt 0
  } catch {
    Write-WorkflowLog "airtable-alert lookup failed key=$alertKey error=$($_.Exception.Message)"
    return $false
  }
}

function Write-WecAirtableAlert {
  param(
    [string]$AlertType,
    [string]$Severity = "warn",
    [string]$Message,
    [string]$DedupeKey = "",
    $Payload = @{}
  )

  $focus = if ($FocusDay) { $FocusDay } elseif ($Payload -and $Payload.focus_day) { [string]$Payload.focus_day } else { "" }
  $dedupe = if ($DedupeKey) { $DedupeKey } else { "$ShowNo|$focus|$AlertType" }
  $alertKey = "$AlertType|$dedupe"
  $createdAt = (Get-Date).ToUniversalTime().ToString("o")
  $fields = @{
    alert_key_run = $alertKey
    created_at = $createdAt
    severity = $Severity
    status = "open"
    alert_type = $AlertType
    show_no = [int]$ShowNo
    message = $Message
    payload_json = ConvertTo-SafeJson $Payload
  }
  if ($focus) {
    $fields.focus_day = $focus
  }
  foreach ($fieldName in @("alert_lane", "trigger_minutes", "time_till", "target_time", "alert_subject", "source_table")) {
    $value = Get-PayloadValue $Payload $fieldName
    if ($null -ne $value -and [string]$value -ne "") {
      $fields[$fieldName] = $value
    }
  }

  $fields | ConvertTo-Json -Depth 12 -Compress | Add-Content -Path (Join-Path $logDir "wec-alerts.jsonl")

  if (!$env:AIRTABLE_TOKEN) {
    Write-WorkflowLog "airtable-alert skipped missing AIRTABLE_TOKEN type=$AlertType"
    return
  }
  if (Test-WecAlertExists $alertKey) {
    Write-WorkflowLog "airtable-alert skipped existing type=$AlertType key=$alertKey"
    return
  }

  try {
    $body = @{ fields = $fields } | ConvertTo-Json -Depth 12
    $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-alerts'))"
    Invoke-RestMethod -Method Post -Uri $uri -Headers @{
      Authorization = "Bearer $env:AIRTABLE_TOKEN"
      "Content-Type" = "application/json"
    } -Body $body -TimeoutSec 30 | Out-Null
  } catch {
    $errorBody = if ($_.ErrorDetails -and $_.ErrorDetails.Message) { $_.ErrorDetails.Message } else { "" }
    Write-WorkflowLog "airtable-alert failed type=$AlertType error=$($_.Exception.Message) body=$errorBody"
  }
}

function Resolve-WecAirtableAlert {
  param(
    [string]$AlertType,
    [string]$DedupeKey = "",
    [string]$Message = "Resolved by current successful workflow run.",
    $Payload = @{}
  )

  if (!$env:AIRTABLE_TOKEN) { return }

  $focus = if ($FocusDay) { $FocusDay } elseif ($Payload -and $Payload.focus_day) { [string]$Payload.focus_day } else { "" }
  $dedupe = if ($DedupeKey) { $DedupeKey } else { "$ShowNo|$focus|$AlertType" }
  $alertKey = "$AlertType|$dedupe"

  try {
    $formula = [uri]::EscapeDataString("{alert_key_run}='$alertKey'")
    $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-alerts'))?filterByFormula=$formula&pageSize=100"
    $result = Invoke-RestMethod -Method Get -Uri $uri -Headers @{
      Authorization = "Bearer $env:AIRTABLE_TOKEN"
    } -TimeoutSec 30

    $records = @($result.records)
    if ($records.Count -eq 0) { return }

    $body = @{
      records = @($records | ForEach-Object {
        @{
          id = $_.id
          fields = @{
            status = "resolved"
            message = $Message
            payload_json = ConvertTo-SafeJson $Payload
          }
        }
      })
      typecast = $true
    } | ConvertTo-Json -Depth 12
    $patchUri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-alerts'))"
    Invoke-RestMethod -Method Patch -Uri $patchUri -Headers @{
      Authorization = "Bearer $env:AIRTABLE_TOKEN"
      "Content-Type" = "application/json"
    } -Body $body -TimeoutSec 30 | Out-Null
    Write-WorkflowLog "airtable-alert resolved type=$AlertType key=$alertKey records=$($records.Count)"
  } catch {
    Write-WorkflowLog "airtable-alert resolve failed type=$AlertType key=$alertKey error=$($_.Exception.Message)"
  }
}

function Int-OrZero($value) {
  if ($null -eq $value -or $value -eq "") { return 0 }
  try { return [int]$value } catch { return 0 }
}

function Get-ObjectMapValue($map, [string]$key) {
  if (!$map -or !$key) { return "" }
  if ($map -is [System.Collections.IDictionary] -and $map.Contains($key)) {
    return [string]$map[$key]
  }
  $property = $map.PSObject.Properties[$key]
  if ($property) { return [string]$property.Value }
  return ""
}

function Get-ScheduleHorseDisplay($classRow, [string]$entryOrder) {
  if (!$classRow -or !$entryOrder) { return "" }
  $suffix = "($entryOrder)"
  foreach ($trainerRollup in @($classRow.trainer_rollups)) {
    foreach ($horse in @($trainerRollup.horses)) {
      $text = [string]$horse
      if ($text.EndsWith($suffix)) {
        return $text.Substring(0, $text.Length - $suffix.Length).Trim()
      }
    }
  }
  return ""
}

function Get-WecAirtableRecordsByFormula {
  param(
    [string]$TableName,
    [string]$Formula,
    [int]$PageSize = 100
  )

  $records = @()
  if (!$env:AIRTABLE_TOKEN) { return $records }

  $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?filterByFormula=$([uri]::EscapeDataString($Formula))&pageSize=$PageSize"
  do {
    $result = Invoke-RestMethod -Method Get -Uri $uri -Headers @{
      Authorization = "Bearer $env:AIRTABLE_TOKEN"
    } -TimeoutSec 30
    $records += @($result.records)
    $uri = if ($result.offset) {
      "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?filterByFormula=$([uri]::EscapeDataString($Formula))&pageSize=$PageSize&offset=$($result.offset)"
    } else {
      $null
    }
  } while ($uri)

  return $records
}

function Read-State {
  if (!(Test-Path $statePath)) { return @{} }
  try {
    $raw = Get-Content -Path $statePath -Raw | ConvertFrom-Json
    $state = @{}
    foreach ($property in $raw.PSObject.Properties) {
      $state[$property.Name] = $property.Value
    }
    return $state
  } catch {
    return @{}
  }
}

function Save-State($state) {
  $state | ConvertTo-Json -Depth 4 | Set-Content -Path $statePath
}

function Invoke-CatalystAction($action) {
  $uri = "${BaseUrl}?action=$action&show_no=$ShowNo"
  if ($FocusDay) {
    $uri = "$uri&focus_day=$FocusDay"
  }
  $response = Invoke-WebRequest -UseBasicParsing -Uri $uri -TimeoutSec 45
  return $response.Content | ConvertFrom-Json
}

function Test-RetryableCatalystError($message) {
  return [string]$message -match "fetch failed|EXECUTION_TIME_EXCEEDED|Execution Time Exceeded|timed out|timeout"
}

function Invoke-CatalystActionWithRetry($action, $attempts = 3) {
  $last = $null
  for ($attempt = 1; $attempt -le $attempts; $attempt++) {
    try {
      return Invoke-CatalystAction $action
    } catch {
      $last = $_.Exception.Message
      if (!(Test-RetryableCatalystError $last)) { throw }
    }
    Start-Sleep -Seconds ([math]::Min(10, 2 * $attempt))
  }
  return [pscustomobject]@{
    ok = $false
    error = "retryable Catalyst action failed after $attempts attempts: $last"
  }
}

function Get-ClassTokenFromText($ClassText) {
  $text = [string]$ClassText
  if ($text -match "^\s*([^)]+)\)") {
    return $matches[1].Trim()
  }
  return ""
}

function Get-AirtableLiveExistingRecords {
  param(
    [string]$TableName,
    [string]$KeyField,
    [array]$Keys
  )

  $existing = @{}
  if (!$env:AIRTABLE_TOKEN -or @($Keys).Count -eq 0) { return $existing }

  $uniqueKeys = @($Keys | Where-Object { $_ } | Select-Object -Unique)
  for ($i = 0; $i -lt $uniqueKeys.Count; $i += 40) {
    $chunk = @($uniqueKeys[$i..([Math]::Min($i + 39, $uniqueKeys.Count - 1))])
    $parts = @($chunk | ForEach-Object {
      $safe = ([string]$_).Replace("'", "\\'")
      "{$KeyField}='$safe'"
    })
    $formula = if ($parts.Count -eq 1) { $parts[0] } else { "OR($($parts -join ','))" }
    $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?filterByFormula=$([uri]::EscapeDataString($formula))&pageSize=100"
    do {
      $result = Invoke-RestMethod -Method Get -Uri $uri -Headers @{
        Authorization = "Bearer $env:AIRTABLE_TOKEN"
      } -TimeoutSec 30
      foreach ($record in @($result.records)) {
        $key = [string]$record.fields.$KeyField
        if ($key) { $existing[$key] = $record.id }
      }
      $uri = if ($result.offset) {
        "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?filterByFormula=$([uri]::EscapeDataString($formula))&pageSize=100&offset=$($result.offset)"
      } else {
        $null
      }
    } while ($uri)
  }

  return $existing
}

function Submit-AirtableLiveBatch {
  param(
    [string]$TableName,
    [string]$Method,
    [array]$Records
  )

  if (@($Records).Count -eq 0) { return }
  $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))"
  for ($i = 0; $i -lt $Records.Count; $i += 10) {
    $chunk = @($Records[$i..([Math]::Min($i + 9, $Records.Count - 1))])
    $body = @{
      records = $chunk
      typecast = $true
    } | ConvertTo-Json -Depth 12
    Invoke-RestMethod -Method $Method -Uri $uri -Headers @{
      Authorization = "Bearer $env:AIRTABLE_TOKEN"
      "Content-Type" = "application/json"
    } -Body $body -TimeoutSec 30 | Out-Null
  }
}

function Convert-LiveRowToAirtableFields {
  param(
    [string]$Source,
    $Row,
    [string]$FocusDayValue
  )

  $classText = [string]$Row.class
  $classToken = Get-ClassTokenFromText $classText
  if ($Source -eq "rings") {
    $key = "$($Row.show_no)|$($Row.ring_day_no)|$($Row.class_no)"
    return @{
      get_rings_key_mirror = $key
      show_no = [string]$Row.show_no
      class_no = Int-OrZero $Row.class_no
      ring_day_no = Int-OrZero $Row.ring_day_no
      day_text = [string]$Row.day
      class_text = $classText
      entry_text = [string]$Row.entry
      total = Int-OrZero $Row.total
      n_to_go = Int-OrZero $Row.n_to_go
      n_gone = Int-OrZero $Row.n_gone
      time_text = [string]$Row.time
      timestamp = [string]$Row.timestamp
      focus_day = $FocusDayValue
      elapsed = [string]$Row.elapsed
      type = [string]$Row.type
    }
  }

  $key = "$($Row.show_no)|$($Row.ring_no)|$($Row.ring_day_no)|$classToken"
  return @{
    get_orders_key_mirror = $key
    show_no = Int-OrZero $Row.show_no
    ring_no = Int-OrZero $Row.ring_no
    ring_day_no = Int-OrZero $Row.ring_day_no
    ring_name = [string]$Row.ring
    day_text = [string]$Row.day
    class_text = $classText
    entry_text = [string]$Row.entry
    total = Int-OrZero $Row.total
    n_to_go = Int-OrZero $Row.n_to_go
    n_gone = Int-OrZero $Row.n_gone
    time_text = [string]$Row.time
    timestamp = Int-OrZero $Row.timestamp
    elapsed = Int-OrZero $Row.elapsed
    focus_day = $FocusDayValue
  }
}

function Get-HorseshowingDirectLiveRows {
  param(
    [string]$Source
  )

  $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
  $session.UserAgent = "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36"
  $base = "https://www.horseshowing.com"
  Invoke-WebRequest -UseBasicParsing -Uri "$base/show.php?show=$ShowNo" -WebSession $session -TimeoutSec 20 | Out-Null
  Invoke-WebRequest -UseBasicParsing -Uri "$base/schedule.php" -WebSession $session -TimeoutSec 20 | Out-Null
  $path = if ($Source -eq "orders") { "/get_orders.php" } else { "/get_rings.php" }
  $referer = if ($Source -eq "orders") { "$base/schedule.php" } else { "$base/rings.php?show=$ShowNo" }
  $rawResponse = Invoke-WebRequest -UseBasicParsing -Uri "$base$path" `
    -Method "POST" `
    -WebSession $session `
    -Headers @{
      "accept" = "application/json, text/javascript, */*; q=0.01"
      "origin" = $base
      "referer" = $referer
      "x-requested-with" = "XMLHttpRequest"
    } `
    -ContentType "application/x-www-form-urlencoded; charset=UTF-8" `
    -Body "show_no=$ShowNo" `
    -TimeoutSec 20
  $parsed = ([string]$rawResponse.Content) | ConvertFrom-Json
  foreach ($item in @($parsed)) {
    if ($item -is [System.Array]) {
      foreach ($inner in $item) { $inner }
    } else {
      $item
    }
  }
}

function Expand-LiveRowCollection($Value) {
  $expanded = @()
  foreach ($item in @($Value)) {
    if ($item -is [System.Array]) {
      foreach ($inner in $item) { $expanded += $inner }
    } else {
      $expanded += $item
    }
  }
  return $expanded
}

function Write-LiveRowsToAirtable {
  param(
    [string]$Source,
    $Payload,
    [string]$FocusDayValue
  )

  $rows = @()
  if ($Payload -and $Payload.PSObject.Properties.Name -contains "airtable_live_rows") {
    $rows = @(Expand-LiveRowCollection $Payload.airtable_live_rows)
  }
  if (@($rows).Count -eq 0 -and (Int-OrZero $Payload.parsed_rows) -gt 0) {
    $rows = @(Expand-LiveRowCollection (Get-HorseshowingDirectLiveRows -Source $Source))
  }
  if (!$env:AIRTABLE_TOKEN -or @($rows).Count -eq 0) {
    return [pscustomobject]@{
      seen = @($rows).Count
      changed = 0
      skipped = if (!$env:AIRTABLE_TOKEN) { "missing_airtable_token" } else { "no_live_rows" }
    }
  }

  $tableName = if ($Source -eq "orders") { "get_orders" } else { "get_rings" }
  $keyField = if ($Source -eq "orders") { "get_orders_key_mirror" } else { "get_rings_key_mirror" }
  $fieldRows = @($rows | ForEach-Object { Convert-LiveRowToAirtableFields -Source $Source -Row $_ -FocusDayValue $FocusDayValue })
  $fieldRows = @($fieldRows | Where-Object { $_.$keyField })
  $existing = Get-AirtableLiveExistingRecords -TableName $tableName -KeyField $keyField -Keys @($fieldRows | ForEach-Object { $_.$keyField })
  $creates = @()
  $updates = @()
  foreach ($fields in $fieldRows) {
    $key = [string]$fields.$keyField
    if ($existing.ContainsKey($key)) {
      $updates += @{ id = $existing[$key]; fields = $fields }
    } else {
      $creates += @{ fields = $fields }
    }
  }
  Submit-AirtableLiveBatch -TableName $tableName -Method "Patch" -Records $updates
  Submit-AirtableLiveBatch -TableName $tableName -Method "Post" -Records $creates

  return [pscustomobject]@{
    seen = $fieldRows.Count
    changed = $creates.Count + $updates.Count
    created = $creates.Count
    updated = $updates.Count
    table = $tableName
  }
}

function Invoke-AirtableLiveLinkSync {
  param(
    [string]$Source,
    [string]$FocusDayValue
  )

  if (!$env:AIRTABLE_TOKEN) {
    return [pscustomobject]@{
      source = $Source
      changed = 0
      skipped = "missing_airtable_token"
    }
  }

  $scriptPath = Join-Path $root "sync-airtable-live-links.js"
  if (!(Test-Path $scriptPath)) {
    return [pscustomobject]@{
      source = $Source
      changed = 0
      skipped = "missing_sync_script"
    }
  }

  $output = & node $scriptPath --source $Source --show-no $ShowNo --focus-day $FocusDayValue
  if ($LASTEXITCODE -ne 0) {
    throw "sync-airtable-live-links failed source=$Source output=$output"
  }
  return ($output | Select-Object -Last 1 | ConvertFrom-Json)
}

function Invoke-CatalystQuery($action, $params = @{}, $TimeoutSec = 45) {
  $uri = "${BaseUrl}?action=$action&show_no=$ShowNo"
  if ($FocusDay -and !$params.ContainsKey("focus_day")) {
    $uri = "$uri&focus_day=$FocusDay"
  }
  foreach ($key in $params.Keys) {
    $uri = "$uri&$key=$([uri]::EscapeDataString([string]$params[$key]))"
  }
  $response = Invoke-WebRequest -UseBasicParsing -Uri $uri -TimeoutSec $TimeoutSec
  return $response.Content | ConvertFrom-Json
}

function Invoke-CatalystQueryWithRetry($action, $params = @{}, $attempts = 3, $TimeoutSec = 45) {
  $last = $null
  for ($attempt = 1; $attempt -le $attempts; $attempt++) {
    try {
      $result = Invoke-CatalystQuery $action $params $TimeoutSec
      if ($result.ok -ne $false) { return $result }
      $last = $result.error
      if (!(Test-RetryableCatalystError $result.error)) { return $result }
    } catch {
      $last = $_.Exception.Message
      if (!(Test-RetryableCatalystError $last)) { throw }
    }
    Start-Sleep -Seconds ([math]::Min(10, 2 * $attempt))
  }
  return [pscustomobject]@{
    ok = $false
    error = "fetch failed after $attempts attempts: $last"
  }
}

function Invoke-HorseshowingDirectCurrent($source, $FocusDayValue) {
  $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
  $session.UserAgent = "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36"
  $base = "https://www.horseshowing.com"
  Invoke-WebRequest -UseBasicParsing -Uri "$base/show.php?show=$ShowNo" -WebSession $session -TimeoutSec 20 | Out-Null
  Invoke-WebRequest -UseBasicParsing -Uri "$base/schedule.php" -WebSession $session -TimeoutSec 20 | Out-Null
  $path = if ($source -eq "orders") { "/get_orders.php" } else { "/get_rings.php" }
  $referer = if ($source -eq "orders") { "$base/schedule.php" } else { "$base/rings.php?show=$ShowNo" }
  $rawResponse = Invoke-WebRequest -UseBasicParsing -Uri "$base$path" `
    -Method "POST" `
    -WebSession $session `
    -Headers @{
      "accept" = "application/json, text/javascript, */*; q=0.01"
      "origin" = $base
      "referer" = $referer
      "x-requested-with" = "XMLHttpRequest"
    } `
    -ContentType "application/x-www-form-urlencoded; charset=UTF-8" `
    -Body "show_no=$ShowNo" `
    -TimeoutSec 20
  $action = if ($source -eq "orders") { "sync-orders-payload" } else { "sync-rings-payload" }
  $payload = @{
    upstream_status = [int]$rawResponse.StatusCode
    raw = [string]$rawResponse.Content
  } | ConvertTo-Json -Depth 5 -Compress
  $payloadUri = "${BaseUrl}?action=$action&show_no=$ShowNo&focus_day=$([uri]::EscapeDataString([string]$FocusDayValue))"
  $result = Invoke-RestMethod -Method Post -Uri $payloadUri -ContentType "application/json" -Body $payload -TimeoutSec 45
  $result | Add-Member -NotePropertyName fallback_source -NotePropertyValue "direct_horseshowing_payload" -Force
  $result | Add-Member -NotePropertyName airtable_live_rows -NotePropertyValue @($parsed) -Force
  return $result
}

function Invoke-CatalystArray($action, $params = @{}) {
  $uri = "${BaseUrl}?action=$action&show_no=$ShowNo"
  if ($FocusDay -and !$params.ContainsKey("focus_day")) {
    $uri = "$uri&focus_day=$FocusDay"
  }
  foreach ($key in $params.Keys) {
    $uri = "$uri&$key=$([uri]::EscapeDataString([string]$params[$key]))"
  }
  $response = Invoke-WebRequest -UseBasicParsing -Uri $uri -TimeoutSec 45
  $parsed = $response.Content | ConvertFrom-Json
  if ($parsed -is [array]) { return $parsed }
  return @($parsed)
}

function Invoke-TimeWorkflowTableSync($FocusDayValue) {
  if (!$FocusDayValue) { return }
  $scriptPath = Join-Path $root "sync-airtable-time-workflows.js"
  $output = & node $scriptPath --show-no $ShowNo --focus-day $FocusDayValue 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "sync-airtable-time-workflows failed: $($output -join ' ')"
  }
  Write-WorkflowLog "sync-airtable-time-workflows $($output -join ' ')"
}

function Invoke-MockLiveCheck($FocusDayValue) {
  if (!$FocusDayValue) { return $null }
  $scriptPath = Join-Path $root "mock-live-enrichment-check.js"
  $output = & node $scriptPath --show-no $ShowNo --focus-day $FocusDayValue 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "mock-live-enrichment-check failed: $($output -join ' ')"
  }
  Write-WorkflowLog "mock-live-enrichment-check $($output -join ' ')"
  return ($output -join "`n") | ConvertFrom-Json
}

function Invoke-CoreWorkflowTableSync($FocusDayValue) {
  if (!$FocusDayValue) { return }
  $scriptPath = Join-Path $root "sync-airtable-core-workflows.js"
  $output = & node $scriptPath --show-no $ShowNo --focus-day $FocusDayValue 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "sync-airtable-core-workflows failed: $($output -join ' ')"
  }
  Write-WorkflowLog "sync-airtable-core-workflows $($output -join ' ')"
}

function Invoke-WecHeartbeatComposite {
  $scheduleRows = @(Invoke-CatalystArray "schedule-json")
  $resolvedFocusDay = ""
  if ($FocusDay) {
    $resolvedFocusDay = $FocusDay
  } elseif ($scheduleRows.Count -gt 0) {
    $resolvedFocusDay = [string]$scheduleRows[0].show_day_key
    if (!$resolvedFocusDay) {
      $resolvedFocusDay = [string]$scheduleRows[0].show_days_display_date
    }
  }
  if (!$resolvedFocusDay) {
    return [pscustomobject]@{
      ok = $false
      error = "heartbeat composite could not resolve focus_day from schedule-json"
    }
  }

  $live = $null
  $orders = $null
  $liveError = $null
  $ordersError = $null
  $params = @{ focus_day = $resolvedFocusDay }
  try {
    $live = Invoke-CatalystQueryWithRetry "sync-rings" $params 1 18
    if ($live.ok -eq $false) { $liveError = $live.error }
  } catch {
    $liveError = $_.Exception.Message
  }
  if ($liveError) {
    try {
      $live = Invoke-HorseshowingDirectCurrent "rings" $resolvedFocusDay
      if ($live.ok -ne $false) { $liveError = $null }
    } catch {
      $liveError = "$liveError; fallback failed: $($_.Exception.Message)"
    }
  }
  try {
    $orders = Invoke-CatalystQueryWithRetry "sync-orders" $params 1 18
    if ($orders.ok -eq $false) { $ordersError = $orders.error }
  } catch {
    $ordersError = $_.Exception.Message
  }
  if ($ordersError) {
    try {
      $orders = Invoke-HorseshowingDirectCurrent "orders" $resolvedFocusDay
      if ($orders.ok -ne $false) { $ordersError = $null }
    } catch {
      $ordersError = "$ordersError; fallback failed: $($_.Exception.Message)"
    }
  }

  return [pscustomobject]@{
    ok = $true
    action = "heartbeat-composite"
    show_no = $ShowNo
    focus_day = $resolvedFocusDay
    live = $live
    live_error = $liveError
    orders = $orders
    orders_error = $ordersError
    schedule_rows = $scheduleRows.Count
    created_triggers = 0
    trigger_error = $null
  }
}

function Get-MinutesUntil($focusDay, $timeText) {
  if (!$focusDay -or !$timeText) { return $null }
  try {
    $target = [datetime]::ParseExact("$focusDay $timeText", "yyyy-MM-dd HH:mm:ss", [Globalization.CultureInfo]::InvariantCulture)
    return ($target - (Get-Date)).TotalMinutes
  } catch {
    return $null
  }
}

function Add-MinutesToTime($focusDay, $timeText, $minutes) {
  if (!$focusDay -or !$timeText) { return $null }
  try {
    return ([datetime]::ParseExact("$focusDay $timeText", "yyyy-MM-dd HH:mm:ss", [Globalization.CultureInfo]::InvariantCulture)).AddMinutes($minutes)
  } catch {
    return $null
  }
}

function In-AlertWindow($minutesUntil, $threshold, $windowMinutes = 12) {
  if ($null -eq $minutesUntil) { return $false }
  return ($minutesUntil -le $threshold -and $minutesUntil -gt ($threshold - $windowMinutes))
}

function Resolve-StaleTimeAlerts {
  param(
    [string]$FocusDayValue,
    $ActiveAlertKeys = @{}
  )

  if (!$FocusDayValue) { return }
  Write-WorkflowLog "airtable-alert stale-time resolve disabled focus=$FocusDayValue active_keys=$(@($ActiveAlertKeys.Keys).Count)"
}

function Write-ClassStartTimesLog {
  param(
    [string]$FocusDayValue,
    [string]$Source = "update_schedule"
  )

  if (!$FocusDayValue) { return }
  $scheduleRows = @(Invoke-CatalystArray "schedule-json" @{ focus_day = $FocusDayValue })
  $classRows = @($scheduleRows | Where-Object { $_.class_no -and $_.class_start_time })
  $uniqueClasses = @($classRows | ForEach-Object { [string]$_.class_no } | Sort-Object -Unique)
  Write-WecAirtableLog -LogType "alerts" -CheckName "class_start_times" -RecordsSeen $classRows.Count -RecordsChanged 0 -Summary "class_start_times source=$Source rows=$($classRows.Count) unique_classes=$($uniqueClasses.Count) focus=$FocusDayValue" -Payload @{
    focus_day = $FocusDayValue
    source = $Source
    rows = $classRows.Count
    unique_classes = $uniqueClasses.Count
  }
}

function Write-TimeAlerts {
  param($FocusDayValue)

  $scheduleRows = @(Invoke-CatalystArray "schedule-json" @{ focus_day = $FocusDayValue })

  $classByNo = @{}
  $classRowsSeen = 0
  $classAlertWindows = 0
  $activeAlertKeys = @{}
  foreach ($row in $scheduleRows) {
    if (!$row.class_no -or !$row.class_start_time) { continue }
    $classRowsSeen += 1
    $classByNo[[string]$row.class_no] = $row
    foreach ($threshold in @(60, 30)) {
      $minutesUntil = Get-MinutesUntil $FocusDayValue $row.class_start_time
      if (In-AlertWindow $minutesUntil $threshold) {
        $classAlertWindows += 1
        $dedupeKey = "$ShowNo|$FocusDayValue|class_start|$($row.class_no)|$threshold"
        $activeAlertKeys["class_start_$threshold|$dedupeKey"] = $true
        $classSubject = if ($row.class_number) { "Class $($row.class_number)" } elseif ($row.class_no) { "Class $($row.class_no)" } else { "Class start alert" }
        Write-WecAirtableAlert -AlertType "class_start_$threshold" -Severity "info" -Message "$classSubject starts in about $threshold minutes at $($row.start_display)." -DedupeKey $dedupeKey -Payload @{
          focus_day = $FocusDayValue
          class_no = $row.class_no
          class_number = $row.class_number
          class_name = $row.class_name
          class_start_time = $row.class_start_time
          display_time = $row.start_display
          time_till = [math]::Round($minutesUntil, 1)
          alert_lane = "class_start"
          alert_type = "class_start_$threshold"
          trigger_minutes = $threshold
          target_time = $row.start_display
          alert_subject = $classSubject
          source_table = "class_start_times"
        }
      }
    }
  }

  $entryRowsSeen = 0
  $entryAlertWindows = 0
  $entryRows = @()
  if ($env:AIRTABLE_TOKEN) {
    $entryFormula = "AND({show_no}=$ShowNo, IS_SAME({focus_day}, DATETIME_PARSE('$FocusDayValue','YYYY-MM-DD'), 'day'), {status}='active')"
    $entryRows = @(Get-WecAirtableRecordsByFormula -TableName "entry_go_times" -Formula $entryFormula)
  } else {
    Write-WorkflowLog "entry_go_times alert source skipped missing AIRTABLE_TOKEN focus=$FocusDayValue"
  }
  foreach ($record in $entryRows) {
    $entry = $record.fields
    if (!$entry.class_no -or !$entry.entry_no) { continue }
    $entryRowsSeen += 1
    $classRow = $classByNo[[string]$entry.class_no]
    $entryOrder = Int-OrZero $entry.entry_order
    $horseDisplay = [string]$entry.horse_display
    if (!$horseDisplay) { $horseDisplay = [string]$entry.horse }
    $trainerDisplay = [string]$entry.trainer_display
    if (!$trainerDisplay) { $trainerDisplay = [string]$entry.trainer }
    $classStartTime = [string]$entry.class_start_time
    if (!$classStartTime -and $classRow) { $classStartTime = [string]$classRow.class_start_time }
    if (!$classStartTime) { continue }
    $paceSeconds = Int-OrZero $entry.pace_seconds
    if ($paceSeconds -lt 1) { $paceSeconds = 120 }
    $nGone = Int-OrZero $entry.n_gone
    if ($nGone -lt 1 -and $classRow) { $nGone = Int-OrZero $classRow.n_gone }
    $elapsedSeconds = Int-OrZero $entry.elapsed_seconds
    if ($elapsedSeconds -lt 1 -and $classRow) { $elapsedSeconds = Int-OrZero $classRow.elapsed_seconds }
    if ($nGone -gt 6 -and $elapsedSeconds -gt 0 -and !$entry.pace_seconds) {
      $paceSeconds = [math]::Max(30, [math]::Round($elapsedSeconds / $nGone, 0))
    }
    $estimatedGo = $null
    if ($entry.entry_go_time) {
      try {
        $estimatedGo = [datetime]::ParseExact("$FocusDayValue $($entry.entry_go_time)", "yyyy-MM-dd HH:mm:ss", [Globalization.CultureInfo]::InvariantCulture)
      } catch {
        $estimatedGo = $null
      }
    }
    if (!$estimatedGo) {
      if ($entryOrder -lt 1) { continue }
      $estimatedGo = Add-MinutesToTime $FocusDayValue $classStartTime ((($entryOrder - 1) * $paceSeconds) / 60)
    }
    if (!$estimatedGo) { continue }
    $minutesUntil = ($estimatedGo - (Get-Date)).TotalMinutes
    foreach ($threshold in @(40, 20)) {
      if (In-AlertWindow $minutesUntil $threshold) {
        $entryAlertWindows += 1
        $dedupeKey = "$ShowNo|$FocusDayValue|entry_go|$($entry.class_no)|$($entry.entry_no)|$threshold"
        $activeAlertKeys["entry_go_$threshold|$dedupeKey"] = $true
        Write-WecAirtableAlert -AlertType "entry_go_$threshold" -Severity "info" -Message "$horseDisplay entry $($entry.entry_no) estimated go in about $threshold minutes." -DedupeKey $dedupeKey -Payload @{
          focus_day = $FocusDayValue
          horse = $entry.horse
          horse_display = $horseDisplay
          rider = $entry.rider
          trainer = $entry.trainer
          trainer_display = $trainerDisplay
          class_no = $entry.class_no
          class_number = $entry.class_number
          entry_no = $entry.entry_no
          entry_order = $entry.entry_order
          class_start_time = $classStartTime
          entry_go_time = $estimatedGo.ToString("HH:mm:ss")
          time_till = [math]::Round($minutesUntil, 1)
          alert_lane = "entry_go"
          alert_type = "entry_go_$threshold"
          trigger_minutes = $threshold
          target_time = $estimatedGo.ToString("h:mm tt")
          alert_subject = "$horseDisplay ($($entry.entry_no))"
          source_table = "entry_go_times"
          estimate_note = "entry_go_time source is active Airtable entry_go_times; fallback estimate uses elapsed_seconds/n_gone when n_gone > 6, otherwise 120 seconds per entry"
          pace_seconds = $paceSeconds
          n_gone = $nGone
          elapsed_seconds = $elapsedSeconds
          source = "airtable.entry_go_times"
        }
      }
    }
  }
  Resolve-StaleTimeAlerts $FocusDayValue $activeAlertKeys
  Write-WecAirtableLog -LogType "alerts" -CheckName "entry_go_times" -RecordsSeen $entryRowsSeen -RecordsChanged $entryAlertWindows -Summary "entry_go_times checked=$entryRowsSeen alert_windows=$entryAlertWindows focus=$FocusDayValue" -Payload @{
    focus_day = $FocusDayValue
    source = "airtable.entry_go_times active"
    entries_checked = $entryRowsSeen
    alert_windows = $entryAlertWindows
    thresholds = "40|20"
    estimate_note = "entry_go_time source is active Airtable entry_go_times; fallback estimate uses elapsed_seconds/n_gone when n_gone > 6, otherwise 120 seconds per entry"
  }
}

function Invoke-FocusDayScheduleSync {
  $offset = 0
  $limit = 1
  $pages = 0
  $rows = 0
  $lastPage = $null
  do {
    $page = Invoke-CatalystQueryWithRetry "sync-focus-schedule-fast" @{
      days_offset = $offset
      days_limit = $limit
      use_stored_ring_days = 1
    } 1
    if ($page.ok -eq $false) {
      throw "sync-focus-day schedule failed: $($page.error)"
    }
    $pages += 1
    $rows += Int-OrZero $page.schedule_rows
    $lastPage = $page
    Write-WorkflowLog "sync-focus-day schedule page=$pages offset=$offset rows=$($page.schedule_rows) source=$($page.ring_day_source) complete=$($page.complete)"
    if (!$page.has_more) { break }
    $offset = [int]$page.next_offset
  } while ($pages -lt 20)
  return @{
    pages = $pages
    rows = $rows
    focus_day = $lastPage.focus_day
    complete = $lastPage.complete
    ring_day_source = $lastPage.ring_day_source
  }
}

function Invoke-FocusDayOogSync {
  $offset = 0
  $limit = 2
  $pages = 0
  $entriesTotal = 0
  $classesSyncedTotal = 0
  do {
    $page = Invoke-CatalystQueryWithRetry "sync-focus-day" @{
      skip_schedule = 1
      oog_offset = $offset
      oog_limit = $limit
    } 1
    if ($page.ok -eq $false) {
      throw "sync-focus-day oog failed: $($page.error)"
    }
    $pages += 1
    $entriesTotal += Int-OrZero $page.class_oog.entries
    $classesSyncedTotal += Int-OrZero $page.class_oog.classes_synced
    $missing = if ($page.audit) { @($page.audit.missing_active_entries).Count } else { "" }
    Write-WorkflowLog "sync-focus-day oog page=$pages offset=$offset classes=$($page.class_oog.classes_synced) entries=$($page.class_oog.entries) complete=$($page.complete) missing=$missing"
    if (!$page.class_oog.has_more) {
      $page.class_oog.entries = $entriesTotal
      $page.class_oog.classes_synced = $classesSyncedTotal
      return $page
    }
    $offset = [int]$page.class_oog.next_offset
  } while ($pages -lt 40)
  throw "sync-focus-day oog did not complete"
}

function Invoke-CountsSync {
  $offset = 0
  $limit = 100
  $pages = 0
  $rows = 0
  $lastPage = $null
  do {
    $page = Invoke-CatalystQueryWithRetry "sync-counts" @{
      counts_offset = $offset
      counts_limit = $limit
    }
    if ($page.ok -eq $false) {
      throw "sync-counts failed: $($page.error)"
    }
    $pages += 1
    $rows += Int-OrZero $page.parsed_rows
    $lastPage = $page
    Write-WorkflowLog "sync-counts page=$pages offset=$offset rows=$($page.parsed_rows) total=$($page.total_rows) complete=$(-not $page.has_more)"
    if (!$page.has_more) { break }
    $offset = [int]$page.next_offset
  } while ($pages -lt 20)

  if ($lastPage -and $lastPage.has_more) {
    throw "sync-counts did not complete"
  }

  return @{
    pages = $pages
    rows = $rows
    total_rows = Int-OrZero $lastPage.total_rows
    complete = if ($lastPage) { -not $lastPage.has_more } else { $false }
  }
}

function Due($state, $key, $minutes) {
  if ($ForceSync) { return $true }
  if (!$state.ContainsKey($key)) { return $true }
  $last = [datetime]$state[$key]
  return ((Get-Date) - $last).TotalMinutes -ge $minutes
}

$state = Read-State
$now = Get-Date

$heartbeat = Invoke-WecHeartbeatComposite
Write-WorkflowLog "heartbeat show=$ShowNo focus=$($heartbeat.focus_day) ok=$($heartbeat.ok) schedule_rows=$($heartbeat.schedule_rows) triggers=$($heartbeat.created_triggers)"
if ($heartbeat.ok -eq $false) {
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_heartbeat" -Status "error" -Summary "heartbeat failed show=$ShowNo error=$($heartbeat.error)" -Payload $heartbeat
  Write-WecAirtableAlert -AlertType "catalyst_heartbeat_failed" -Severity "error" -Message "Catalyst heartbeat failed for show=${ShowNo}: $($heartbeat.error)" -Payload $heartbeat
  throw "heartbeat failed: $($heartbeat.error)"
}
Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_heartbeat" -RecordsSeen (Int-OrZero $heartbeat.schedule_rows) -RecordsChanged (Int-OrZero $heartbeat.created_triggers) -Summary "heartbeat show=$ShowNo focus=$($heartbeat.focus_day) schedule_rows=$($heartbeat.schedule_rows) triggers=$($heartbeat.created_triggers)" -Payload $heartbeat
Resolve-WecAirtableAlert -AlertType "catalyst_heartbeat_failed" -DedupeKey "$ShowNo||catalyst_heartbeat_failed" -Message "Resolved: Catalyst heartbeat returned schedule rows." -Payload @{
  show_no = $ShowNo
  focus_day = $heartbeat.focus_day
  schedule_rows = $heartbeat.schedule_rows
}
$liveAlertWindow = Test-LiveAlertWindow $heartbeat.focus_day
if ($heartbeat.live_error) {
  $liveStatus = if ($liveAlertWindow) { "error" } else { "skipped" }
  $liveSummary = if ($liveAlertWindow) {
    "get_rings failed show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.live_error)"
  } else {
    "get_rings skipped outside live alert window focus=$($heartbeat.focus_day): $($heartbeat.live_error)"
  }
  Write-WecAirtableLog -LogType "live" -CheckName "get_rings" -Status $liveStatus -Summary $liveSummary -Payload @{
    focus_day = $heartbeat.focus_day
    error = $heartbeat.live_error
    live = $heartbeat.live
  }
} else {
  $liveMirror = Write-LiveRowsToAirtable -Source "rings" -Payload $heartbeat.live -FocusDayValue $heartbeat.focus_day
  $liveLinks = Invoke-AirtableLiveLinkSync -Source "rings" -FocusDayValue $heartbeat.focus_day
  Write-WecAirtableLog -LogType "live" -CheckName "get_rings" -RecordsSeen (Int-OrZero $heartbeat.live.parsed_rows) -RecordsChanged ((Int-OrZero $liveMirror.changed) + (Int-OrZero $liveLinks.changed)) -Summary "get_rings rows=$($heartbeat.live.parsed_rows) mirrored=$($liveMirror.changed) linked=$($liveLinks.changed) focus=$($heartbeat.focus_day)" -Payload @{
    live = $heartbeat.live
    mirror = $liveMirror
    links = $liveLinks
    focus_day = $heartbeat.focus_day
  }
  Resolve-WecAirtableAlert -AlertType "live_get_rings_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_rings" -Message "Resolved: get_rings returned without error." -Payload $heartbeat.live
}
if ($heartbeat.live_error -and $liveAlertWindow) {
  Write-WecAirtableAlert -AlertType "live_get_rings_failed" -Severity "warn" -Message "Live get_rings failed for show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.live_error)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_rings" -Payload $heartbeat
} elseif ($heartbeat.live_error) {
  Resolve-WecAirtableAlert -AlertType "live_get_rings_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_rings" -Message "Resolved: get_rings failure is outside the live alert window." -Payload @{
    show_no = $ShowNo
    focus_day = $heartbeat.focus_day
    error = $heartbeat.live_error
  }
}
$ordersAlertWindow = Test-LiveAlertWindow $heartbeat.focus_day
if ($heartbeat.orders_error) {
  $ordersStatus = if ($ordersAlertWindow) { "error" } else { "skipped" }
  $ordersSummary = if ($ordersAlertWindow) {
    "get_orders failed show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.orders_error)"
  } else {
    "get_orders skipped outside live alert window focus=$($heartbeat.focus_day): $($heartbeat.orders_error)"
  }
  Write-WecAirtableLog -LogType "live" -CheckName "get_orders" -Status $ordersStatus -Summary $ordersSummary -Payload @{
    focus_day = $heartbeat.focus_day
    error = $heartbeat.orders_error
    orders = $heartbeat.orders
  }
} else {
  $ordersMirror = Write-LiveRowsToAirtable -Source "orders" -Payload $heartbeat.orders -FocusDayValue $heartbeat.focus_day
  $ordersLinks = Invoke-AirtableLiveLinkSync -Source "orders" -FocusDayValue $heartbeat.focus_day
  Write-WecAirtableLog -LogType "live" -CheckName "get_orders" -RecordsSeen (Int-OrZero $heartbeat.orders.parsed_rows) -RecordsChanged ((Int-OrZero $ordersMirror.changed) + (Int-OrZero $ordersLinks.changed)) -Summary "get_orders rows=$($heartbeat.orders.parsed_rows) mirrored=$($ordersMirror.changed) linked=$($ordersLinks.changed) focus=$($heartbeat.focus_day)" -Payload @{
    orders = $heartbeat.orders
    mirror = $ordersMirror
    links = $ordersLinks
    focus_day = $heartbeat.focus_day
  }
  Resolve-WecAirtableAlert -AlertType "live_get_orders_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_orders" -Message "Resolved: get_orders returned without error." -Payload $heartbeat.orders
}
if ($heartbeat.orders_error -and $ordersAlertWindow) {
  Write-WecAirtableAlert -AlertType "live_get_orders_failed" -Severity "warn" -Message "Live get_orders failed for show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.orders_error)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_orders" -Payload $heartbeat
} elseif ($heartbeat.orders_error) {
  Resolve-WecAirtableAlert -AlertType "live_get_orders_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_orders" -Message "Resolved: get_orders failure is outside the live alert window." -Payload @{
    show_no = $ShowNo
    focus_day = $heartbeat.focus_day
    error = $heartbeat.orders_error
  }
}
if ($heartbeat.trigger_error) {
  Write-WecAirtableAlert -AlertType "time_trigger_failed" -Severity "error" -Message "Time trigger write failed for show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.trigger_error)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|time_triggers" -Payload $heartbeat
}
if ($heartbeat.focus_day) {
  $localCoreFocusChanged = (!$state.ContainsKey("local_core_focus_day")) -or ([string]$state["local_core_focus_day"] -ne [string]$heartbeat.focus_day)
  $localCoreDue = $localCoreFocusChanged -or (Due $state "local_core_workflow" 60)
  $catalystScheduleRows = Int-OrZero $heartbeat.schedule_rows
  try {
    if ($localCoreDue) {
      Invoke-CoreWorkflowTableSync $heartbeat.focus_day
      $state["local_core_workflow"] = (Get-Date).ToString("o")
      $state["local_core_focus_day"] = [string]$heartbeat.focus_day
      Resolve-WecAirtableAlert -AlertType "local_core_workflow_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|local_core_workflow" -Message "Resolved: local core workflow completed." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_update_schedule_failed" -Message "Resolved: local core workflow completed update_schedule." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_update_schedule_failed" -DedupeKey "$ShowNo||core_update_schedule_failed" -Message "Resolved: local core workflow completed update_schedule." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_class_oog_failed" -Message "Resolved: local core workflow completed class_oog." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_class_oog_failed" -DedupeKey "$ShowNo||core_class_oog_failed" -Message "Resolved: local core workflow completed class_oog." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_sync_ring_days_failed" -Message "Resolved: local core workflow completed ring_days." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_sync_ring_days_failed" -DedupeKey "$ShowNo||core_sync_ring_days_failed" -Message "Resolved: local core workflow completed ring_days." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_sync_counts_failed" -Message "Resolved: local core workflow completed counts." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_sync_counts_failed" -DedupeKey "$ShowNo||core_sync_counts_failed" -Message "Resolved: local core workflow completed counts." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
    } else {
      Write-WorkflowLog "local-core skipped focus=$($heartbeat.focus_day) next_due=60m"
    }
  } catch {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "local_core_workflow" -Status "error" -Summary "local core workflow failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "local_core_workflow_failed" -Severity "critical" -Message "Local core workflow failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|local_core_workflow" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
  }
  if ($localCoreFocusChanged -or $catalystScheduleRows -eq 0) {
    try {
      $schedule = Invoke-FocusDayScheduleSync
      Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -RecordsSeen (Int-OrZero $schedule.rows) -RecordsChanged (Int-OrZero $schedule.rows) -Summary "Catalyst schedule-json refreshed rows=$($schedule.rows) pages=$($schedule.pages) focus=$($schedule.focus_day)" -Payload @{
        show_no = $ShowNo
        focus_day = $schedule.focus_day
        rows = $schedule.rows
        pages = $schedule.pages
        ring_day_source = $schedule.ring_day_source
        complete = $schedule.complete
      }
      Resolve-WecAirtableAlert -AlertType "catalyst_schedule_json_empty" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|schedule-json" -Message "Resolved: Catalyst schedule-json was rebuilt for focus day." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
        rows = $schedule.rows
      }
    } catch {
      Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -Status "error" -Summary "Catalyst schedule-json refresh failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
        error = $_.Exception.Message
      }
      Write-WecAirtableAlert -AlertType "catalyst_schedule_json_empty" -Severity "critical" -Message "Catalyst schedule-json refresh failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|schedule-json" -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
        error = $_.Exception.Message
      }
    }
  }
  try {
    Invoke-TimeWorkflowTableSync $heartbeat.focus_day
    Resolve-WecAirtableAlert -AlertType "time_trigger_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|time_triggers" -Message "Resolved: time workflow tables updated successfully." -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
    }
  } catch {
    Write-WecAirtableLog -LogType "alerts" -CheckName "entry_go_times" -Status "error" -Summary "time workflow table sync failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "time_workflow_table_sync_failed" -Severity "critical" -Message "Time workflow table sync failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|time_workflow_table_sync" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
  }
  Write-TimeAlerts $heartbeat.focus_day
  if ($RunMockLiveCheck) {
    try {
      $mockLive = Invoke-MockLiveCheck $heartbeat.focus_day
      Write-WecAirtableLog -LogType "live" -CheckName "mock_live_enrichment" -RecordsSeen (Int-OrZero $mockLive.schedule_rows) -RecordsChanged 0 -Summary "mock live enrichment pass=$($mockLive.pass) rings=$($mockLive.live_source) orders=$($mockLive.order_live_source) pace=$($mockLive.pace_seconds)" -Payload $mockLive
    } catch {
      Write-WecAirtableLog -LogType "live" -CheckName "mock_live_enrichment" -Status "error" -Summary "mock live enrichment failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
        error = $_.Exception.Message
      }
      Write-WecAirtableAlert -AlertType "mock_live_enrichment_failed" -Severity "critical" -Message "Mock live enrichment failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|mock_live_enrichment" -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
        error = $_.Exception.Message
      }
    }
  }
}

if ($false -and (Due $state "sync_focus_day" 10)) {
  $schedule = $null
  try {
    $schedule = Invoke-FocusDayScheduleSync
  } catch {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -Status "error" -Summary "update_schedule failed show=$ShowNo error=$($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "core_update_schedule_failed" -Severity "critical" -Message "update_schedule failed for show=${ShowNo}: $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      error = $_.Exception.Message
    }
    $schedule = @{
      pages = 0
      rows = 0
      focus_day = $heartbeat.focus_day
      complete = $false
      ring_day_source = "failed"
    }
  }
  $schedulePages = Int-OrZero $schedule.pages
  $scheduleRows = Int-OrZero $schedule.rows
  if ($schedule.complete -ne $false -or $scheduleRows -gt 0) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -RecordsSeen $scheduleRows -RecordsChanged $scheduleRows -Summary "update_schedule rows=$scheduleRows pages=$schedulePages focus=$($schedule.focus_day)" -Payload @{
      show_no = $ShowNo
      focus_day = $schedule.focus_day
      rows = $scheduleRows
      pages = $schedulePages
      ring_day_source = $schedule.ring_day_source
      complete = $schedule.complete
    }
  }
  Write-ClassStartTimesLog -FocusDayValue $schedule.focus_day -Source "update_schedule"

  $focus = $null
  try {
    $focus = Invoke-FocusDayOogSync
  } catch {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog" -Status "error" -Summary "class_oog failed show=$ShowNo error=$($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "core_class_oog_failed" -Severity "critical" -Message "class_oog failed for show=${ShowNo}: $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      error = $_.Exception.Message
    }
    $focus = $null
  }
  if ($focus -and $focus.complete -ne $true) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog" -Status "error" -Summary "class_oog incomplete show=$ShowNo focus=$($focus.focus_day)" -Payload $focus
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_focus_day" -Status "error" -Summary "sync-focus-day incomplete show=$ShowNo" -Payload $focus
    Write-WecAirtableAlert -AlertType "core_sync_focus_day_incomplete" -Severity "critical" -Message "sync-focus-day incomplete for show=$ShowNo focus=$($focus.focus_day)" -Payload $focus
    throw "sync-focus-day incomplete"
  }
  if ($focus) {
    $state["sync_focus_day"] = $now.ToString("o")
    Write-WorkflowLog "sync-focus-day complete=$($focus.complete) schedule_pages=$schedulePages classes=$($focus.class_oog.classes_seen) active_expected=$($focus.audit.expected_active_entries) active_missing=$(@($focus.audit.missing_active_entries).Count)"
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog" -RecordsSeen (Int-OrZero $focus.class_oog.entries) -RecordsChanged (Int-OrZero $focus.class_oog.classes_seen) -Summary "class_oog classes=$($focus.class_oog.classes_seen) entries=$($focus.class_oog.entries) active_missing=$(@($focus.audit.missing_active_entries).Count)" -Payload @{
      complete = $focus.complete
      focus_day = $focus.focus_day
      classes = $focus.class_oog.classes_seen
      entries = $focus.class_oog.entries
      active_expected = $focus.audit.expected_active_entries
      active_missing = @($focus.audit.missing_active_entries).Count
    }
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_focus_day" -RecordsSeen (Int-OrZero $focus.class_oog.entries) -RecordsChanged (Int-OrZero $focus.class_oog.classes_seen) -Summary "sync-focus-day complete=$($focus.complete) schedule_pages=$schedulePages classes=$($focus.class_oog.classes_seen) active_expected=$($focus.audit.expected_active_entries) active_missing=$(@($focus.audit.missing_active_entries).Count)" -Payload @{
      complete = $focus.complete
      focus_day = $focus.focus_day
      schedule_pages = $schedulePages
      classes = $focus.class_oog.classes_seen
      entries = $focus.class_oog.entries
      active_expected = $focus.audit.expected_active_entries
      active_missing = @($focus.audit.missing_active_entries).Count
    }
    $activeMissing = @($focus.audit.missing_active_entries)
    if ($activeMissing.Count -gt 0) {
      Write-WecAirtableAlert -AlertType "missing_active_trainer_entries" -Severity "critical" -Message "Active trainer entries missing from schedule render: $($activeMissing.Count)" -DedupeKey "$ShowNo|$($focus.focus_day)|missing_active_entries" -Payload @{
        focus_day = $focus.focus_day
        active_expected = $focus.audit.expected_active_entries
        active_missing = $activeMissing.Count
        missing = $activeMissing
      }
    }
  }
}

if ($false -and (Due $state "sync_ring_days" 30)) {
  $ringDays = $null
  try {
    $ringDays = Invoke-CatalystActionWithRetry "sync-ring-days" 1
    if ($ringDays.ok -eq $false) {
      Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_ring_days_counts" -Status "error" -Summary "sync-ring-days failed show=$ShowNo error=$($ringDays.error)" -Payload $ringDays
      Write-WecAirtableAlert -AlertType "core_sync_ring_days_failed" -Severity "error" -Message "sync-ring-days failed for show=${ShowNo}: $($ringDays.error)" -Payload $ringDays
      $ringDays = $null
    }
  } catch {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_ring_days_counts" -Status "error" -Summary "sync-ring-days failed show=$ShowNo error=$($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "core_sync_ring_days_failed" -Severity "error" -Message "sync-ring-days failed for show=${ShowNo}: $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      error = $_.Exception.Message
    }
  }
  $counts = $null
  try {
    $counts = Invoke-CountsSync
  } catch {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_counts" -Status "error" -Summary "counts failed show=$ShowNo error=$($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      error = $_.Exception.Message
    }
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_ring_days_counts" -Status "error" -Summary "sync-counts failed show=$ShowNo error=$($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "core_sync_counts_failed" -Severity "error" -Message "sync-counts failed for show=${ShowNo}: $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      error = $_.Exception.Message
    }
    $counts = $null
  }
  if ($counts) {
    $state["sync_ring_days"] = $now.ToString("o")
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_counts" -RecordsSeen (Int-OrZero $counts.rows) -RecordsChanged (Int-OrZero $counts.rows) -Summary "counts rows=$($counts.rows) pages=$($counts.pages) total=$($counts.total_rows) focus=$($heartbeat.focus_day)" -Payload @{
      focus_day = $heartbeat.focus_day
      counts_rows = $counts.rows
      counts_pages = $counts.pages
      counts_total_rows = $counts.total_rows
      complete = $counts.complete
    }
  }
  if ($ringDays -or $counts) {
    Write-WorkflowLog "sync-ring-days rows=$($ringDays.parsed_rows) sync-counts rows=$($counts.rows) pages=$($counts.pages)"
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_ring_days_counts" -RecordsSeen ((Int-OrZero $ringDays.parsed_rows) + (Int-OrZero $counts.rows)) -Summary "sync-ring-days rows=$($ringDays.parsed_rows) sync-counts rows=$($counts.rows) pages=$($counts.pages)" -Payload @{
      focus_day = $heartbeat.focus_day
      ring_days_rows = $ringDays.parsed_rows
      counts_rows = $counts.rows
      counts_pages = $counts.pages
      counts_total_rows = $counts.total_rows
    }
  }
}

$state["last_run"] = $now.ToString("o")
Save-State $state

if ($script:WecWorkflowMutexAcquired) {
  $script:WecWorkflowMutex.ReleaseMutex()
  $script:WecWorkflowMutex.Dispose()
}
