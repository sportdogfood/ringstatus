param(
  [string]$ShowNo = "14906",
  [string]$FocusDay = "",
  [string]$BaseUrl = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$logDir = Join-Path $root "logs"
$statePath = Join-Path $logDir "wec-catalyst-workflow-state.json"
$logPath = Join-Path $logDir "wec-catalyst-workflow.log"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-WorkflowLog($message) {
  $stamp = (Get-Date).ToString("s")
  Add-Content -Path $logPath -Value "$stamp $message"
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
    $pages += 1
    $missing = if ($page.audit) { @($page.audit.missing_active_entries).Count } else { "" }
    Write-WorkflowLog "sync-focus-day oog page=$pages offset=$offset classes=$($page.class_oog.classes_synced) entries=$($page.class_oog.entries) complete=$($page.complete) missing=$missing"
    if (!$page.class_oog.has_more) { return $page }
    $offset = [int]$page.class_oog.next_offset
  } while ($pages -lt 40)
  throw "sync-focus-day oog did not complete"
}

function Due($state, $key, $minutes) {
  if (!$state.ContainsKey($key)) { return $true }
  $last = [datetime]$state[$key]
  return ((Get-Date) - $last).TotalMinutes -ge $minutes
}

$state = Read-State
$now = Get-Date

$heartbeat = Invoke-CatalystAction "heartbeat"
Write-WorkflowLog "heartbeat show=$ShowNo focus=$($heartbeat.focus_day) ok=$($heartbeat.ok) schedule_rows=$($heartbeat.schedule_rows) triggers=$($heartbeat.created_triggers)"

if (Due $state "sync_focus_day" 10) {
  $schedulePages = Invoke-FocusDayScheduleSync
  $focus = Invoke-FocusDayOogSync
  $state["sync_focus_day"] = $now.ToString("o")
  Write-WorkflowLog "sync-focus-day complete=$($focus.complete) schedule_pages=$schedulePages classes=$($focus.class_oog.classes_seen) active_expected=$($focus.audit.expected_active_entries) active_missing=$(@($focus.audit.missing_active_entries).Count)"
}

if (Due $state "sync_ring_days" 30) {
  $ringDays = Invoke-CatalystAction "sync-ring-days"
  $counts = Invoke-CatalystAction "sync-counts"
  $state["sync_ring_days"] = $now.ToString("o")
  Write-WorkflowLog "sync-ring-days rows=$($ringDays.parsed_rows) sync-counts rows=$($counts.parsed_rows)"
}

$state["last_run"] = $now.ToString("o")
Save-State $state
