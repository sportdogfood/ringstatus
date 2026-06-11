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
  $focus = Invoke-CatalystAction "sync-focus-day"
  $state["sync_focus_day"] = $now.ToString("o")
  Write-WorkflowLog "sync-focus-day rows=$($focus.parsed_rows) upstream=$($focus.upstream_requests)"
}

if (Due $state "sync_ring_days" 30) {
  $ringDays = Invoke-CatalystAction "sync-ring-days"
  $counts = Invoke-CatalystAction "sync-counts"
  $state["sync_ring_days"] = $now.ToString("o")
  Write-WorkflowLog "sync-ring-days rows=$($ringDays.parsed_rows) sync-counts rows=$($counts.parsed_rows)"
}

$state["last_run"] = $now.ToString("o")
Save-State $state
