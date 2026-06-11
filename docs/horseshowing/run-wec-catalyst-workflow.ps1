param(
  [string]$ShowNo = "14906",
  [string]$FocusDay = "",
  [string]$BaseUrl = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/",
  [string]$AirtableBaseId = "app6XS1RvsPNRT6os",
  [switch]$ForceSync
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$logDir = Join-Path $root "logs"
$statePath = Join-Path $logDir "wec-catalyst-workflow-state.json"
$logPath = Join-Path $logDir "wec-catalyst-workflow.log"
$jsonlLogPath = Join-Path $logDir "wec-logs.jsonl"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

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
  $fields = @{
    log_key = "$createdAt|$LogType|$CheckName"
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
    $body = @{ fields = $fields } | ConvertTo-Json -Depth 12
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
    $formula = [uri]::EscapeDataString("{alert_key}='$alertKey'")
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
    alert_key = $alertKey
    created_at = $createdAt
    severity = $Severity
    status = "open"
    alert_type = $AlertType
    show_no = $ShowNo
    message = $Message
    payload_json = ConvertTo-SafeJson $Payload
  }
  if ($focus) {
    $fields.focus_day = $focus
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
    Write-WorkflowLog "airtable-alert failed type=$AlertType error=$($_.Exception.Message)"
  }
}

function Int-OrZero($value) {
  if ($null -eq $value -or $value -eq "") { return 0 }
  try { return [int]$value } catch { return 0 }
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
  $response = Invoke-WebRequest -UseBasicParsing -Uri $uri -TimeoutSec 90
  return $response.Content | ConvertFrom-Json
}

function Invoke-CatalystQuery($action, $params = @{}) {
  $uri = "${BaseUrl}?action=$action&show_no=$ShowNo"
  if ($FocusDay) {
    $uri = "$uri&focus_day=$FocusDay"
  }
  foreach ($key in $params.Keys) {
    $uri = "$uri&$key=$([uri]::EscapeDataString([string]$params[$key]))"
  }
  $response = Invoke-WebRequest -UseBasicParsing -Uri $uri -TimeoutSec 180
  return $response.Content | ConvertFrom-Json
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

function Write-TimeAlerts {
  param($FocusDayValue)

  $scheduleRows = @(Invoke-CatalystQuery "schedule-json" @{ focus_day = $FocusDayValue })
  $snapshot = Invoke-CatalystQuery "focus-day-snapshot" @{ focus_day = $FocusDayValue }
  $debug = Invoke-CatalystQuery "debug-show-config" @{ focus_day = $FocusDayValue }
  $activeTrainers = @($debug.focus_source.active_trainers | ForEach-Object { [string]$_ })
  $activeTrainerSet = @{}
  foreach ($trainer in $activeTrainers) {
    if ($trainer) { $activeTrainerSet[$trainer] = $true }
  }

  $classByNo = @{}
  foreach ($row in $scheduleRows) {
    if (!$row.class_no -or !$row.class_start_time) { continue }
    $classByNo[[string]$row.class_no] = $row
    foreach ($threshold in @(60, 30)) {
      $minutesUntil = Get-MinutesUntil $FocusDayValue $row.class_start_time
      if (In-AlertWindow $minutesUntil $threshold) {
        Write-WecAirtableAlert -AlertType "class_start_$threshold" -Severity "info" -Message "Class $($row.class_number) starts in about $threshold minutes at $($row.start_display)." -DedupeKey "$ShowNo|$FocusDayValue|class_start|$($row.class_no)|$threshold" -Payload @{
          focus_day = $FocusDayValue
          class_no = $row.class_no
          class_number = $row.class_number
          class_name = $row.class_name
          class_start_time = $row.class_start_time
          display_time = $row.start_display
          time_till = [math]::Round($minutesUntil, 1)
          alert_lane = "class_start"
          alert_type = "class_start_$threshold"
        }
      }
    }
  }

  foreach ($entry in @($snapshot.class_oog)) {
    $trainer = [string]$entry.trainer
    if (!$trainer -or !$activeTrainerSet.ContainsKey($trainer)) { continue }
    $classRow = $classByNo[[string]$entry.class_no]
    if (!$classRow -or !$classRow.class_start_time) { continue }
    $entryOrder = Int-OrZero $entry.entry_order
    if ($entryOrder -lt 1) { continue }
    $estimatedGo = Add-MinutesToTime $FocusDayValue $classRow.class_start_time (($entryOrder - 1) * 2)
    if (!$estimatedGo) { continue }
    $minutesUntil = ($estimatedGo - (Get-Date)).TotalMinutes
    foreach ($threshold in @(40, 20)) {
      if (In-AlertWindow $minutesUntil $threshold) {
        Write-WecAirtableAlert -AlertType "entry_go_$threshold" -Severity "info" -Message "$($entry.horse) entry $($entry.entry_no) estimated go in about $threshold minutes." -DedupeKey "$ShowNo|$FocusDayValue|entry_go|$($entry.class_no)|$($entry.entry_no)|$threshold" -Payload @{
          focus_day = $FocusDayValue
          horse = $entry.horse
          rider = $entry.rider
          trainer = $entry.trainer
          class_no = $entry.class_no
          class_number = $classRow.class_number
          entry_no = $entry.entry_no
          entry_order = $entry.entry_order
          class_start_time = $classRow.class_start_time
          entry_go_time = $estimatedGo.ToString("HH:mm:ss")
          time_till = [math]::Round($minutesUntil, 1)
          alert_lane = "entry_go"
          alert_type = "entry_go_$threshold"
          estimate_note = "entry_go_time uses 2 minutes per entry until live pace calculation is available"
        }
      }
    }
  }
}

function Invoke-FocusDayScheduleSync {
  $offset = 0
  $limit = 2
  $pages = 0
  do {
    $page = Invoke-CatalystQuery "sync-focus-day" @{
      schedule_only = 1
      days_offset = $offset
      days_limit = $limit
    }
    if ($page.ok -eq $false) {
      throw "sync-focus-day schedule failed: $($page.error)"
    }
    $pages += 1
    Write-WorkflowLog "sync-focus-day schedule page=$pages offset=$offset rows=$($page.schedule_rows) source=$($page.ring_day_source) complete=$($page.complete)"
    if (!$page.has_more) { break }
    $offset = [int]$page.next_offset
  } while ($pages -lt 20)
  return $pages
}

function Invoke-FocusDayOogSync {
  $offset = 0
  $limit = 2
  $pages = 0
  do {
    $page = Invoke-CatalystQuery "sync-focus-day" @{
      skip_schedule = 1
      oog_offset = $offset
      oog_limit = $limit
    }
    if ($page.ok -eq $false) {
      throw "sync-focus-day oog failed: $($page.error)"
    }
    $pages += 1
    $missing = if ($page.audit) { @($page.audit.missing_active_entries).Count } else { "" }
    Write-WorkflowLog "sync-focus-day oog page=$pages offset=$offset classes=$($page.class_oog.classes_synced) entries=$($page.class_oog.entries) complete=$($page.complete) missing=$missing"
    if (!$page.class_oog.has_more) { return $page }
    $offset = [int]$page.class_oog.next_offset
  } while ($pages -lt 40)
  throw "sync-focus-day oog did not complete"
}

function Due($state, $key, $minutes) {
  if ($ForceSync) { return $true }
  if (!$state.ContainsKey($key)) { return $true }
  $last = [datetime]$state[$key]
  return ((Get-Date) - $last).TotalMinutes -ge $minutes
}

$state = Read-State
$now = Get-Date

$heartbeat = Invoke-CatalystAction "heartbeat"
Write-WorkflowLog "heartbeat show=$ShowNo focus=$($heartbeat.focus_day) ok=$($heartbeat.ok) schedule_rows=$($heartbeat.schedule_rows) triggers=$($heartbeat.created_triggers)"
if ($heartbeat.ok -eq $false) {
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_heartbeat" -Status "error" -Summary "heartbeat failed show=$ShowNo error=$($heartbeat.error)" -Payload $heartbeat
  Write-WecAirtableAlert -AlertType "catalyst_heartbeat_failed" -Severity "error" -Message "Catalyst heartbeat failed for show=${ShowNo}: $($heartbeat.error)" -Payload $heartbeat
  throw "heartbeat failed: $($heartbeat.error)"
}
Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_heartbeat" -RecordsSeen (Int-OrZero $heartbeat.schedule_rows) -RecordsChanged (Int-OrZero $heartbeat.created_triggers) -Summary "heartbeat show=$ShowNo focus=$($heartbeat.focus_day) schedule_rows=$($heartbeat.schedule_rows) triggers=$($heartbeat.created_triggers)" -Payload $heartbeat
if ($heartbeat.live_error) {
  Write-WecAirtableAlert -AlertType "live_get_rings_failed" -Severity "warn" -Message "Live get_rings failed for show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.live_error)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_rings" -Payload $heartbeat
}
if ($heartbeat.orders_error) {
  Write-WecAirtableAlert -AlertType "live_get_orders_failed" -Severity "warn" -Message "Live get_orders failed for show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.orders_error)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_orders" -Payload $heartbeat
}
if ($heartbeat.trigger_error) {
  Write-WecAirtableAlert -AlertType "time_trigger_failed" -Severity "error" -Message "Time trigger write failed for show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.trigger_error)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|time_triggers" -Payload $heartbeat
}
if ($heartbeat.focus_day) {
  Write-TimeAlerts $heartbeat.focus_day
}

if (Due $state "sync_focus_day" 10) {
  $schedulePages = Invoke-FocusDayScheduleSync
  $focus = Invoke-FocusDayOogSync
  if ($focus.complete -ne $true) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_focus_day" -Status "error" -Summary "sync-focus-day incomplete show=$ShowNo" -Payload $focus
    Write-WecAirtableAlert -AlertType "core_sync_focus_day_incomplete" -Severity "critical" -Message "sync-focus-day incomplete for show=$ShowNo focus=$($focus.focus_day)" -Payload $focus
    throw "sync-focus-day incomplete"
  }
  $state["sync_focus_day"] = $now.ToString("o")
  Write-WorkflowLog "sync-focus-day complete=$($focus.complete) schedule_pages=$schedulePages classes=$($focus.class_oog.classes_seen) active_expected=$($focus.audit.expected_active_entries) active_missing=$(@($focus.audit.missing_active_entries).Count)"
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

if (Due $state "sync_ring_days" 30) {
  $ringDays = Invoke-CatalystAction "sync-ring-days"
  if ($ringDays.ok -eq $false) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_ring_days_counts" -Status "error" -Summary "sync-ring-days failed show=$ShowNo error=$($ringDays.error)" -Payload $ringDays
    Write-WecAirtableAlert -AlertType "core_sync_ring_days_failed" -Severity "error" -Message "sync-ring-days failed for show=${ShowNo}: $($ringDays.error)" -Payload $ringDays
    throw "sync-ring-days failed: $($ringDays.error)"
  }
  $counts = Invoke-CatalystAction "sync-counts"
  if ($counts.ok -eq $false) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_ring_days_counts" -Status "error" -Summary "sync-counts failed show=$ShowNo error=$($counts.error)" -Payload $counts
    Write-WecAirtableAlert -AlertType "core_sync_counts_failed" -Severity "error" -Message "sync-counts failed for show=${ShowNo}: $($counts.error)" -Payload $counts
    throw "sync-counts failed: $($counts.error)"
  }
  $state["sync_ring_days"] = $now.ToString("o")
  Write-WorkflowLog "sync-ring-days rows=$($ringDays.parsed_rows) sync-counts rows=$($counts.parsed_rows)"
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_ring_days_counts" -RecordsSeen ((Int-OrZero $ringDays.parsed_rows) + (Int-OrZero $counts.parsed_rows)) -Summary "sync-ring-days rows=$($ringDays.parsed_rows) sync-counts rows=$($counts.parsed_rows)" -Payload @{
    focus_day = $heartbeat.focus_day
    ring_days_rows = $ringDays.parsed_rows
    counts_rows = $counts.parsed_rows
  }
}

$state["last_run"] = $now.ToString("o")
Save-State $state
