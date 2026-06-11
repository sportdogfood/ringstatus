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

function Invoke-CatalystArray($action, $params = @{}) {
  $uri = "${BaseUrl}?action=$action&show_no=$ShowNo"
  if ($FocusDay) {
    $uri = "$uri&focus_day=$FocusDay"
  }
  foreach ($key in $params.Keys) {
    $uri = "$uri&$key=$([uri]::EscapeDataString([string]$params[$key]))"
  }
  $response = Invoke-WebRequest -UseBasicParsing -Uri $uri -TimeoutSec 180
  $parsed = $response.Content | ConvertFrom-Json
  if ($parsed -is [array]) { return $parsed }
  return @($parsed)
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
  $snapshot = Invoke-CatalystQuery "focus-day-snapshot" @{ focus_day = $FocusDayValue }
  $debug = Invoke-CatalystQuery "debug-show-config" @{ focus_day = $FocusDayValue }
  $activeTrainers = @($debug.focus_source.active_trainers | ForEach-Object { [string]$_ })
  $activeTrainerSet = @{}
  foreach ($trainer in $activeTrainers) {
    if ($trainer) { $activeTrainerSet[$trainer] = $true }
  }

  $classByNo = @{}
  $classRowsSeen = 0
  $classAlertWindows = 0
  foreach ($row in $scheduleRows) {
    if (!$row.class_no -or !$row.class_start_time) { continue }
    $classRowsSeen += 1
    $classByNo[[string]$row.class_no] = $row
    foreach ($threshold in @(60, 30)) {
      $minutesUntil = Get-MinutesUntil $FocusDayValue $row.class_start_time
      if (In-AlertWindow $minutesUntil $threshold) {
        $classAlertWindows += 1
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

  $entryRowsSeen = 0
  $entryAlertWindows = 0
  foreach ($entry in @($snapshot.class_oog)) {
    $trainer = [string]$entry.trainer
    if (!$trainer -or !$activeTrainerSet.ContainsKey($trainer)) { continue }
    $classRow = $classByNo[[string]$entry.class_no]
    if (!$classRow -or !$classRow.class_start_time) { continue }
    $entryOrder = Int-OrZero $entry.entry_order
    if ($entryOrder -lt 1) { continue }
    $entryRowsSeen += 1
    $paceSeconds = 120
    $nGone = Int-OrZero $classRow.n_gone
    $elapsedSeconds = Int-OrZero $classRow.elapsed_seconds
    if ($nGone -gt 6 -and $elapsedSeconds -gt 0) {
      $paceSeconds = [math]::Max(30, [math]::Round($elapsedSeconds / $nGone, 0))
    }
    $estimatedGo = Add-MinutesToTime $FocusDayValue $classRow.class_start_time ((($entryOrder - 1) * $paceSeconds) / 60)
    if (!$estimatedGo) { continue }
    $minutesUntil = ($estimatedGo - (Get-Date)).TotalMinutes
    foreach ($threshold in @(40, 20)) {
      if (In-AlertWindow $minutesUntil $threshold) {
        $entryAlertWindows += 1
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
          estimate_note = "entry_go_time uses live elapsed_seconds/n_gone when n_gone > 6; fallback pace is 120 seconds per entry"
          pace_seconds = $paceSeconds
          n_gone = $classRow.n_gone
          elapsed_seconds = $classRow.elapsed_seconds
        }
      }
    }
  }
  Write-WecAirtableLog -LogType "alerts" -CheckName "entry_go_times" -RecordsSeen $entryRowsSeen -RecordsChanged $entryAlertWindows -Summary "entry_go_times checked=$entryRowsSeen alert_windows=$entryAlertWindows focus=$FocusDayValue" -Payload @{
    focus_day = $FocusDayValue
    entries_checked = $entryRowsSeen
    alert_windows = $entryAlertWindows
    thresholds = "40|20"
    estimate_note = "entry_go_time uses live elapsed_seconds/n_gone when n_gone > 6; fallback pace is 120 seconds per entry"
  }
}

function Invoke-FocusDayScheduleSync {
  $offset = 0
  $limit = 2
  $pages = 0
  $rows = 0
  $lastPage = $null
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
    $page = Invoke-CatalystQuery "sync-focus-day" @{
      skip_schedule = 1
      oog_offset = $offset
      oog_limit = $limit
    }
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
    $page = Invoke-CatalystQuery "sync-counts" @{
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

$heartbeat = Invoke-CatalystAction "heartbeat"
Write-WorkflowLog "heartbeat show=$ShowNo focus=$($heartbeat.focus_day) ok=$($heartbeat.ok) schedule_rows=$($heartbeat.schedule_rows) triggers=$($heartbeat.created_triggers)"
if ($heartbeat.ok -eq $false) {
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_heartbeat" -Status "error" -Summary "heartbeat failed show=$ShowNo error=$($heartbeat.error)" -Payload $heartbeat
  Write-WecAirtableAlert -AlertType "catalyst_heartbeat_failed" -Severity "error" -Message "Catalyst heartbeat failed for show=${ShowNo}: $($heartbeat.error)" -Payload $heartbeat
  throw "heartbeat failed: $($heartbeat.error)"
}
Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_heartbeat" -RecordsSeen (Int-OrZero $heartbeat.schedule_rows) -RecordsChanged (Int-OrZero $heartbeat.created_triggers) -Summary "heartbeat show=$ShowNo focus=$($heartbeat.focus_day) schedule_rows=$($heartbeat.schedule_rows) triggers=$($heartbeat.created_triggers)" -Payload $heartbeat
if ($heartbeat.live_error) {
  Write-WecAirtableLog -LogType "live" -CheckName "get_rings" -Status "error" -Summary "get_rings failed show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.live_error)" -Payload @{
    focus_day = $heartbeat.focus_day
    error = $heartbeat.live_error
    live = $heartbeat.live
  }
} else {
  Write-WecAirtableLog -LogType "live" -CheckName "get_rings" -RecordsSeen (Int-OrZero $heartbeat.live.parsed_rows) -Summary "get_rings rows=$($heartbeat.live.parsed_rows) focus=$($heartbeat.focus_day)" -Payload $heartbeat.live
}
if ($heartbeat.live_error) {
  Write-WecAirtableAlert -AlertType "live_get_rings_failed" -Severity "warn" -Message "Live get_rings failed for show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.live_error)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_rings" -Payload $heartbeat
}
if ($heartbeat.orders_error) {
  Write-WecAirtableLog -LogType "live" -CheckName "get_orders" -Status "error" -Summary "get_orders failed show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.orders_error)" -Payload @{
    focus_day = $heartbeat.focus_day
    error = $heartbeat.orders_error
    orders = $heartbeat.orders
  }
} else {
  Write-WecAirtableLog -LogType "live" -CheckName "get_orders" -RecordsSeen (Int-OrZero $heartbeat.orders.parsed_rows) -Summary "get_orders rows=$($heartbeat.orders.parsed_rows) focus=$($heartbeat.focus_day)" -Payload $heartbeat.orders
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
    throw
  }
  $schedulePages = Int-OrZero $schedule.pages
  $scheduleRows = Int-OrZero $schedule.rows
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -RecordsSeen $scheduleRows -RecordsChanged $scheduleRows -Summary "update_schedule rows=$scheduleRows pages=$schedulePages focus=$($schedule.focus_day)" -Payload @{
    show_no = $ShowNo
    focus_day = $schedule.focus_day
    rows = $scheduleRows
    pages = $schedulePages
    ring_day_source = $schedule.ring_day_source
    complete = $schedule.complete
  }
  Write-ClassStartTimesLog -FocusDayValue $schedule.focus_day -Source "update_schedule"

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
    throw
  }
  if ($focus.complete -ne $true) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog" -Status "error" -Summary "class_oog incomplete show=$ShowNo focus=$($focus.focus_day)" -Payload $focus
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_focus_day" -Status "error" -Summary "sync-focus-day incomplete show=$ShowNo" -Payload $focus
    Write-WecAirtableAlert -AlertType "core_sync_focus_day_incomplete" -Severity "critical" -Message "sync-focus-day incomplete for show=$ShowNo focus=$($focus.focus_day)" -Payload $focus
    throw "sync-focus-day incomplete"
  }
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

if (Due $state "sync_ring_days" 30) {
  $ringDays = Invoke-CatalystAction "sync-ring-days"
  if ($ringDays.ok -eq $false) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_ring_days_counts" -Status "error" -Summary "sync-ring-days failed show=$ShowNo error=$($ringDays.error)" -Payload $ringDays
    Write-WecAirtableAlert -AlertType "core_sync_ring_days_failed" -Severity "error" -Message "sync-ring-days failed for show=${ShowNo}: $($ringDays.error)" -Payload $ringDays
    throw "sync-ring-days failed: $($ringDays.error)"
  }
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
    throw
  }
  $state["sync_ring_days"] = $now.ToString("o")
  Write-WorkflowLog "sync-ring-days rows=$($ringDays.parsed_rows) sync-counts rows=$($counts.rows) pages=$($counts.pages)"
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_counts" -RecordsSeen (Int-OrZero $counts.rows) -RecordsChanged (Int-OrZero $counts.rows) -Summary "counts rows=$($counts.rows) pages=$($counts.pages) total=$($counts.total_rows) focus=$($heartbeat.focus_day)" -Payload @{
    focus_day = $heartbeat.focus_day
    counts_rows = $counts.rows
    counts_pages = $counts.pages
    counts_total_rows = $counts.total_rows
    complete = $counts.complete
  }
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "catalyst_sync_ring_days_counts" -RecordsSeen ((Int-OrZero $ringDays.parsed_rows) + (Int-OrZero $counts.rows)) -Summary "sync-ring-days rows=$($ringDays.parsed_rows) sync-counts rows=$($counts.rows) pages=$($counts.pages)" -Payload @{
    focus_day = $heartbeat.focus_day
    ring_days_rows = $ringDays.parsed_rows
    counts_rows = $counts.rows
    counts_pages = $counts.pages
    counts_total_rows = $counts.total_rows
  }
}

$state["last_run"] = $now.ToString("o")
Save-State $state
