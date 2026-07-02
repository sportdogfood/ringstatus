param(
  [string]$ShowNo = "",
  [string]$FocusDay = "",
  [string]$BaseUrl = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/",
  [string]$UpdateScheduleRunnerUrl = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_update_schedule_runner/",
  [string]$ClassOogRunnerUrl = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_class_oog_runner/",
  [string]$ClassStartTimesRunnerUrl = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_class_start_times_runner/",
  [string]$EntryGoTimesRunnerUrl = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_entry_go_times_runner/",
  [string]$ClassLaneRunnerUrl = "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_class_lane_runner/",
  [string]$AirtableBaseId = "app6XS1RvsPNRT6os",
  [int]$ClassOogChunksPerRun = 2,
  [switch]$ForceSync,
  [switch]$RunMockLiveCheck,
  [switch]$RunLiveEnrichment,
  [switch]$RunClassOogLocalProbeOnly,
  [switch]$RunClassOogUnlockedSafetyOnly,
  [switch]$RunClassStartTimesOnly,
  [switch]$WorkflowV4Stage1Only
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$logDir = Join-Path $root "logs"
$statePath = Join-Path $logDir "wec-catalyst-workflow-state.json"
$logPath = Join-Path $logDir "wec-catalyst-workflow.log"
$jsonlLogPath = Join-Path $logDir "wec-logs.jsonl"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$script:FocusPaused = $false
$script:FocusLocked = $false

if (!$ShowNo -and $env:WEC_SHOW_NO) {
  $ShowNo = [string]$env:WEC_SHOW_NO
}
if (!$ForceSync -and $env:WEC_FORCE_SYNC -eq "1") {
  $ForceSync = $true
}
if (!$RunLiveEnrichment -and $env:WEC_ENABLE_LIVE_ENRICHMENT -eq "1") {
  $RunLiveEnrichment = $true
}
if (!$RunClassStartTimesOnly -and $env:WEC_CLASS_START_TIMES_ONLY -eq "1") {
  $RunClassStartTimesOnly = $true
}
if (!$WorkflowV4Stage1Only -and $env:WEC_WORKFLOWV4_STAGE1_ONLY -eq "1") {
  $WorkflowV4Stage1Only = $true
}

$mutexName = "Global\RingStatusWecCatalystWorkflow"
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

function Add-ContentWithRetry {
  param(
    [string]$Path,
    [string]$Value
  )

  for ($i = 0; $i -lt 8; $i++) {
    try {
      Add-Content -Path $Path -Value $Value
      return
    } catch {
      Start-Sleep -Milliseconds (250 * ($i + 1))
    }
  }
  Write-WorkflowLog "local-log write failed path=$Path"
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
  if ($CheckName -in @("core_update_schedule", "core_class_oog", "core_class_oog_safety", "core_counts")) { return "Core" }
  if ($CheckName -in @("airtable_helpers_summary", "airtable_helper_backfill", "catalyst_heartbeat", "catalyst_sync_focus_day", "catalyst_sync_ring_days_counts", "cadence_gate", "cadence_pause")) { return "Audits" }
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

  Add-ContentWithRetry -Path $jsonlLogPath -Value ($fields | ConvertTo-Json -Depth 12 -Compress)

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

  Add-ContentWithRetry -Path (Join-Path $logDir "wec-alerts.jsonl") -Value ($fields | ConvertTo-Json -Depth 12 -Compress)

  if (!$env:AIRTABLE_TOKEN) {
    Write-WorkflowLog "airtable-alert skipped missing AIRTABLE_TOKEN type=$AlertType"
    return
  }
  try {
    $formula = [uri]::EscapeDataString("{alert_key_run}='$alertKey'")
    $lookupUri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-alerts'))?filterByFormula=$formula&pageSize=10"
    $existing = Invoke-RestMethod -Method Get -Uri $lookupUri -Headers @{
      Authorization = "Bearer $env:AIRTABLE_TOKEN"
    } -TimeoutSec 30
    $openRecord = @($existing.records | Where-Object { [string](Get-WecRecordField $_ "status") -ne "resolved" } | Select-Object -First 1)[0]
    $body = @{ fields = $fields } | ConvertTo-Json -Depth 12
    if ($openRecord) {
      $updateBody = @{ records = @(@{ id = $openRecord.id; fields = $fields }) } | ConvertTo-Json -Depth 12
      $updateUri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-alerts'))"
      Invoke-RestMethod -Method Patch -Uri $updateUri -Headers @{
        Authorization = "Bearer $env:AIRTABLE_TOKEN"
        "Content-Type" = "application/json"
      } -Body $updateBody -TimeoutSec 30 | Out-Null
      return
    }
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
  $token = Get-WecAirtableToken

  $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?filterByFormula=$([uri]::EscapeDataString($Formula))&pageSize=$PageSize"
  do {
    $result = Invoke-RestMethod -Method Get -Uri $uri -Headers @{
      Authorization = "Bearer $token"
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

function Get-WecAirtableRecordsByView {
  param(
    [string]$TableName,
    [string]$ViewName,
    [int]$PageSize = 100
  )

  $records = @()
  $token = Get-WecAirtableToken

  $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?view=$([uri]::EscapeDataString($ViewName))&pageSize=$PageSize"
  do {
    $result = Invoke-RestMethod -Method Get -Uri $uri -Headers @{
      Authorization = "Bearer $token"
    } -TimeoutSec 30
    $records += @($result.records)
    $uri = if ($result.offset) {
      "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?view=$([uri]::EscapeDataString($ViewName))&pageSize=$PageSize&offset=$($result.offset)"
    } else {
      $null
    }
  } while ($uri)

  return $records
}

function Get-WecAirtableTableRecords {
  param(
    [string]$TableName,
    [int]$PageSize = 100
  )

  $records = @()
  $token = Get-WecAirtableToken

  $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?pageSize=$PageSize"
  do {
    $result = Invoke-RestMethod -Method Get -Uri $uri -Headers @{
      Authorization = "Bearer $token"
    } -TimeoutSec 30
    $records += @($result.records)
    $uri = if ($result.offset) {
      "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?pageSize=$PageSize&offset=$($result.offset)"
    } else {
      $null
    }
  } while ($uri)

  return $records
}

function Get-WecRecordField($record, [string]$Name) {
  if (!$record -or !$record.fields) { return $null }
  $property = $record.fields.PSObject.Properties[$Name]
  if ($property) { return $property.Value }
  return $null
}

function Test-WecTruthy($value) {
  if ($null -eq $value) { return $false }
  if ($value -is [bool]) { return [bool]$value }
  $text = ([string]$value).Trim()
  return $text -in @("1", "true", "True", "TRUE", "yes", "Yes", "YES", "checked", "Checked")
}

function Convert-HtmlCellText([string]$Html) {
  $withoutTags = [regex]::Replace($Html, "(?is)<[^>]+>", " ")
  $decoded = [System.Net.WebUtility]::HtmlDecode($withoutTags)
  return ([regex]::Replace($decoded, "\s+", " ")).Trim()
}

function Get-WecActiveTrainerSet {
  $set = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
  $records = Get-WecAirtableRecordsByFormula -TableName "trainers" -Formula "{active}=1"
  foreach ($record in @($records)) {
    $trainer = [string](Get-WecRecordField $record "trainer")
    if ($trainer.Trim()) { [void]$set.Add($trainer.Trim()) }
  }
  return $set
}

function Get-WecActiveTrainerMap {
  $map = @{}
  $records = Get-WecAirtableRecordsByFormula -TableName "trainers" -Formula "{active}=1"
  foreach ($record in @($records)) {
    $trainer = [string](Get-WecRecordField $record "trainer")
    if ($trainer.Trim()) { $map[$trainer.Trim()] = $record.id }
  }
  return $map
}

function Get-WecLockedScheduleRows($FocusDayValue) {
  $records = Get-WecAirtableRecordsByView -TableName "update_schedule_staging" -ViewName "lock_schedule"
  $rows = @()
  foreach ($record in @($records)) {
    $showValue = [string](Get-WecRecordField $record "show_no")
    $isoDate = [string](Get-WecRecordField $record "iso_date")
    $classNo = Int-OrZero (Get-WecRecordField $record "class_no")
    if ($showValue -ne [string]$ShowNo) { continue }
    if ($isoDate -ne [string]$FocusDayValue) { continue }
    if ($classNo -le 0) { continue }
    $rows += [pscustomobject]@{
      record_id = $record.id
      show_no = [int]$ShowNo
      class_no = $classNo
      ring_day_no = Int-OrZero (Get-WecRecordField $record "ring_day_no")
      ring_no = Int-OrZero (Get-WecRecordField $record "ring_no")
      ring_name = [string](Get-WecRecordField $record "ring_name")
      event_name = [string](Get-WecRecordField $record "event_name")
      class_name = [string](Get-WecRecordField $record "class_name")
      time_text = [string](Get-WecRecordField $record "time_text")
      entry_count = Int-OrZero (Get-WecRecordField $record "entry_count")
    }
  }
  return @($rows | Sort-Object ring_no, time_text, class_no)
}

function Get-WecFocusScheduleRows($FocusDayValue) {
  $records = Get-WecAirtableTableRecords -TableName "update_schedule_staging"
  $rows = @()
  foreach ($record in @($records)) {
    $showValue = [string](Get-WecRecordField $record "show_no")
    $isoDate = [string](Get-WecRecordField $record "iso_date")
    $classNo = Int-OrZero (Get-WecRecordField $record "class_no")
    if ($showValue -ne [string]$ShowNo) { continue }
    if ($isoDate -ne [string]$FocusDayValue) { continue }
    $rows += [pscustomobject]@{
      record_id = $record.id
      show_no = [int]$ShowNo
      class_no = $classNo
      ring_day_no = Int-OrZero (Get-WecRecordField $record "ring_day_no")
      ring_no = Int-OrZero (Get-WecRecordField $record "ring_no")
      ring_name = [string](Get-WecRecordField $record "ring_name")
      event_name = [string](Get-WecRecordField $record "event_name")
      class_name = [string](Get-WecRecordField $record "class_name")
      time_text = [string](Get-WecRecordField $record "time_text")
      entry_count = Int-OrZero (Get-WecRecordField $record "entry_count")
      event_type = Int-OrZero (Get-WecRecordField $record "event_type")
      second_trip = Test-WecTruthy (Get-WecRecordField $record "2nd_trip")
    }
  }
  return @($rows | Sort-Object ring_no, time_text, class_no)
}

function Get-WecClassStartKeyFromStagingRecord($record) {
  $existingKey = [string](Get-WecRecordField $record "staging_key")
  if ($existingKey) { return $existingKey }
  $showNo = [string](Get-WecRecordField $record "show_no")
  $ringDayNo = [string](Get-WecRecordField $record "ring_day_no")
  $ringNo = [string](Get-WecRecordField $record "ring_no")
  $eventId = [string](Get-WecRecordField $record "event_id")
  $classNo = [string](Get-WecRecordField $record "class_no")
  return @($showNo, $ringDayNo, $ringNo, $eventId, $classNo) -join "|"
}

function Get-WecLinkedRecordId($value) {
  if ($null -eq $value) { return "" }
  if ($value -is [array]) {
    if ($value.Count -eq 0) { return "" }
    $first = $value[0]
    if ($first -is [string]) { return $first }
    if ($first.id) { return [string]$first.id }
    return [string]$first
  }
  if ($value.id) { return [string]$value.id }
  return [string]$value
}

function Repair-ClassStartTimesStagingLinks {
  param(
    [string]$FocusDayValue
  )

  $stagingRecords = @(Get-WecAirtableRecordsByView -TableName "update_schedule_staging" -ViewName "lock_schedule")
  $stagingByKey = @{}
  foreach ($record in @($stagingRecords)) {
    $showValue = [string](Get-WecRecordField $record "show_no")
    $isoDate = [string](Get-WecRecordField $record "iso_date")
    $classNo = Int-OrZero (Get-WecRecordField $record "class_no")
    if ($showValue -ne [string]$ShowNo) { continue }
    if ($isoDate -ne [string]$FocusDayValue) { continue }
    if ($classNo -le 0) { continue }
    $key = Get-WecClassStartKeyFromStagingRecord $record
    if ($key) { $stagingByKey[$key] = $record.id }
  }

  $formula = "AND({show_no}=$ShowNo,IS_SAME({focus_day},DATETIME_PARSE('$FocusDayValue'),'day'))"
  $classStartRecords = @(Get-WecAirtableRecordsByFormula -TableName "class_start_times" -Formula $formula)
  $updates = @()
  $missing = @()
  foreach ($record in @($classStartRecords)) {
    $key = [string](Get-WecRecordField $record "class_start_key")
    if (!$key -or !$stagingByKey.ContainsKey($key)) {
      $missing += $key
      continue
    }
    $expectedId = [string]$stagingByKey[$key]
    $actualId = Get-WecLinkedRecordId (Get-WecRecordField $record "update_schedule_staging")
    if ($actualId -ne $expectedId) {
      $updates += @{
        id = $record.id
        fields = @{
          update_schedule_staging = @($expectedId)
        }
      }
    }
  }

  Submit-AirtableLiveBatch -TableName "class_start_times" -Method "Patch" -Records $updates
  return [pscustomobject]@{
    source_locked_staging = $stagingByKey.Count
    class_start_times_seen = $classStartRecords.Count
    links_repaired = $updates.Count
    missing_staging_keys = @($missing | Where-Object { $_ } | Select-Object -Unique)
  }
}

function Get-LocalClassOogActiveRows {
  param(
    [object]$ClassRow,
    [System.Collections.Generic.HashSet[string]]$ActiveTrainers,
    [Microsoft.PowerShell.Commands.WebRequestSession]$Session
  )

  $base = "https://www.horseshowing.com"
  $response = Invoke-WebRequest -UseBasicParsing -Uri "$base/class_oog.php?class_no=$($ClassRow.class_no)" `
    -WebSession $Session `
    -Headers @{
      "accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
      "accept-encoding" = "identity"
      "referer" = "$base/schedule.php"
    } `
    -TimeoutSec 30

  if ([string]$response.Content -match "<title>Select Show</title>") {
    throw "class_oog.php returned Select Show page; session bootstrap failed class_no=$($ClassRow.class_no)"
  }

  $rows = @()
  foreach ($tr in [regex]::Matches([string]$response.Content, "(?is)<tr\b[^>]*>(.*?)</tr>")) {
    $cells = @()
    foreach ($td in [regex]::Matches($tr.Groups[1].Value, "(?is)<td\b[^>]*>(.*?)</td>")) {
      $cells += Convert-HtmlCellText $td.Groups[1].Value
    }
    if ($cells.Count -lt 5) { continue }
    $trainer = [string]$cells[4]
    if (!$ActiveTrainers.Contains($trainer)) { continue }
    $entryNo = Int-OrZero $cells[1]
    if ($entryNo -le 0) { continue }
    $rows += [pscustomobject]@{
      class_oog_key = "$ShowNo|$($ClassRow.ring_day_no)|$($ClassRow.ring_no)|$($ClassRow.class_no)|$entryNo"
      show_no = [int]$ShowNo
      ring_day_no = $ClassRow.ring_day_no
      ring_no = $ClassRow.ring_no
      ring = $ClassRow.ring_name
      class_no = $ClassRow.class_no
      class_label = if ($ClassRow.event_name) { $ClassRow.event_name } else { $ClassRow.class_name }
      entry_order = Int-OrZero $cells[0]
      entry_no = $entryNo
      horse = [string]$cells[2]
      rider = [string]$cells[3]
      trainer = $trainer
      staging_record_id = $ClassRow.record_id
    }
  }
  return @($rows)
}

function Get-HorseshowingSession {
  $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
  $session.UserAgent = "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36"
  $base = "https://www.horseshowing.com"
  Invoke-WebRequest -UseBasicParsing -Uri "$base/show.php?show=$ShowNo" -WebSession $session -TimeoutSec 20 | Out-Null
  Invoke-WebRequest -UseBasicParsing -Uri "$base/schedule.php" -WebSession $session -TimeoutSec 20 | Out-Null
  return $session
}

function Convert-LocalClassOogRowToAirtableFields {
  param(
    [object]$Row,
    [string]$FocusDayValue,
    [hashtable]$ActiveTrainerMap
  )

  $fields = @{
    mirror_class_oog_key = [string]$Row.class_oog_key
    show_no = Int-OrZero $Row.show_no
    focus_day = $FocusDayValue
    ring = [string]$Row.ring
    ring_no = Int-OrZero $Row.ring_no
    days = Int-OrZero $Row.ring_day_no
    class_no = Int-OrZero $Row.class_no
    class_label = [string]$Row.class_label
    entry_order = Int-OrZero $Row.entry_order
    entry_no = Int-OrZero $Row.entry_no
    horse = [string]$Row.horse
    rider = [string]$Row.rider
    trainer = [string]$Row.trainer
    source = "local_class_oog_probe"
  }
  if ($Row.staging_record_id) {
    $fields["update_schedule_staging"] = @([string]$Row.staging_record_id)
  }
  $trainerName = [string]$Row.trainer
  if ($trainerName -and $ActiveTrainerMap.ContainsKey($trainerName)) {
    $fields["trainers"] = @($ActiveTrainerMap[$trainerName])
  }
  return $fields
}

function Get-LocalClassOogScopeRecords {
  param(
    [string]$FocusDayValue
  )

  $formula = "AND({show_no}=$ShowNo,IS_SAME({focus_day},DATETIME_PARSE('$FocusDayValue'),'day'),{source}='local_class_oog_probe')"
  return @(Get-WecAirtableRecordsByFormula -TableName "class_oog" -Formula $formula)
}

function Sync-LocalClassOogRowsToAirtable {
  param(
    [array]$Rows,
    [string]$FocusDayValue,
    [hashtable]$ActiveTrainerMap
  )

  $fieldRows = @($Rows | ForEach-Object { Convert-LocalClassOogRowToAirtableFields -Row $_ -FocusDayValue $FocusDayValue -ActiveTrainerMap $ActiveTrainerMap })
  $fieldRows = @($fieldRows | Where-Object { $_.mirror_class_oog_key })
  $activeKeys = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
  foreach ($fields in @($fieldRows)) {
    if ($fields.mirror_class_oog_key) { [void]$activeKeys.Add([string]$fields.mirror_class_oog_key) }
  }
  $scopeRecords = @(Get-LocalClassOogScopeRecords -FocusDayValue $FocusDayValue)
  $staleIds = @()
  foreach ($record in @($scopeRecords)) {
    $key = [string](Get-WecRecordField $record "mirror_class_oog_key")
    if (!$key -or !$activeKeys.Contains($key)) { $staleIds += $record.id }
  }
  $deleted = Remove-AirtableLiveRecords -TableName "class_oog" -RecordIds $staleIds

  $existing = Get-AirtableLiveExistingRecords -TableName "class_oog" -KeyField "mirror_class_oog_key" -Keys @($fieldRows | ForEach-Object { $_.mirror_class_oog_key })
  $creates = @()
  $updates = @()
  foreach ($fields in $fieldRows) {
    $key = [string]$fields.mirror_class_oog_key
    if ($existing.ContainsKey($key)) {
      $updates += @{ id = $existing[$key]; fields = $fields }
    } else {
      $creates += @{ fields = $fields }
    }
  }

  Submit-AirtableLiveBatch -TableName "class_oog" -Method "Patch" -Records $updates
  Submit-AirtableLiveBatch -TableName "class_oog" -Method "Post" -Records $creates

  return [pscustomobject]@{
    records_seen = $fieldRows.Count
    changed = $creates.Count + $updates.Count + $deleted
    created = $creates.Count
    updated = $updates.Count
    deleted = $deleted
  }
}

function Resolve-WecCadenceMinutes {
  param(
    [array]$CadenceRows,
    [string]$Mode = "day"
  )

  $preferredWorkflows = @(
    "wec_heartbeat",
    "wec_alerts_time_check",
    "entry_go_times",
    "live_get_orders",
    "live_get_rings"
  )
  $row = $null
  foreach ($workflow in $preferredWorkflows) {
    $row = @($CadenceRows | Where-Object { [string](Get-WecRecordField $_ "workflow") -eq $workflow } | Select-Object -First 1)[0]
    if ($row) { break }
  }
  if (!$row -and @($CadenceRows).Count -gt 0) {
    $row = @($CadenceRows | Select-Object -First 1)[0]
  }

  $modeValue = ([string]$Mode).Trim().ToLowerInvariant()
  $minutes = $null
  if ($modeValue -eq "evening") {
    $minutes = Get-WecRecordField $row "night_cadence_minutes"
  }
  if ($null -eq $minutes -or [string]$minutes -eq "") {
    $minutes = Get-WecRecordField $row "cadence_minutes"
  }
  if ($null -eq $minutes -or [string]$minutes -eq "") {
    return 12
  }
  return [math]::Max(1, [int]$minutes)
}

function Test-WecCadenceGate {
  param(
    [hashtable]$State
  )

  try {
    $null = Get-WecAirtableToken
  } catch {
    Write-WorkflowLog "cadence-gate bypass missing AIRTABLE_TOKEN fallback"
    return @{
      should_run = $true
      reason = "missing_airtable_token_fallback"
      cadence_minutes = 0
    }
  }

  try {
    $activeShows = @(Get-WecAirtableRecordsByView -TableName "shows" -ViewName "active" -PageSize 10)
    if ($activeShows.Count -eq 0) {
      Write-WecAirtableLog -LogType "heartbeat" -CheckName "cadence_gate" -Status "skipped" -Summary "cadence stop: no active show in shows.active" -Payload @{
        reason = "no_active_show"
        table = "shows"
        view = "active"
      }
      Write-WorkflowLog "cadence-gate stop no_active_show"
      return @{
        should_run = $false
        reason = "no_active_show"
        cadence_minutes = 0
      }
    }
    if ($activeShows.Count -gt 1) {
      Write-WecAirtableLog -LogType "heartbeat" -CheckName "cadence_gate" -Status "error" -Summary "cadence stop: multiple active records in shows.active" -Payload @{
        reason = "multiple_active_shows"
        table = "shows"
        view = "active"
        active_shows = $activeShows.Count
        show_nos = @($activeShows | ForEach-Object { Get-WecRecordField $_ "show_no" })
      }
      Write-WorkflowLog "cadence-gate stop multiple_active_shows count=$($activeShows.Count)"
      return @{
        should_run = $false
        reason = "multiple_active_shows"
        cadence_minutes = 0
      }
    }

    $activeFocusShows = @(Get-WecAirtableRecordsByView -TableName "focus_show" -ViewName "active" -PageSize 10)
    if ($activeFocusShows.Count -eq 0) {
      Write-WecAirtableLog -LogType "heartbeat" -CheckName "cadence_gate" -Status "skipped" -Summary "cadence stop: no active record in focus_show.active" -Payload @{
        reason = "no_active_focus_show"
        table = "focus_show"
        view = "active"
        active_shows = $activeShows.Count
      }
      Write-WorkflowLog "cadence-gate stop no_active_focus_show"
      return @{
        should_run = $false
        reason = "no_active_focus_show"
        cadence_minutes = 0
      }
    }
    $activeShowRecord = @($activeShows | Select-Object -First 1)[0]
    $activeShowNo = [string](Get-WecRecordField $activeShowRecord "show_no")
    $matchingFocusShows = @($activeFocusShows | Where-Object { [string](Get-WecRecordField $_ "show_no") -eq $activeShowNo })
    if ($matchingFocusShows.Count -ne 1) {
      Write-WecAirtableLog -LogType "heartbeat" -CheckName "cadence_gate" -Status "error" -Summary "cadence stop: focus_show.active does not have exactly one record matching shows.active show_no=$activeShowNo" -Payload @{
        reason = "active_show_focus_mismatch"
        active_show_no = $activeShowNo
        active_shows = $activeShows.Count
        active_focus_shows = $activeFocusShows.Count
        matching_focus_shows = $matchingFocusShows.Count
        focus_show_nos = @($activeFocusShows | ForEach-Object { Get-WecRecordField $_ "show_no" })
      }
      Write-WorkflowLog "cadence-gate stop active_show_focus_mismatch show=$activeShowNo matching=$($matchingFocusShows.Count)"
      return @{
        should_run = $false
        reason = "active_show_focus_mismatch"
        cadence_minutes = 0
      }
    }

    $focusRecord = @($matchingFocusShows | Select-Object -First 1)[0]
    $focusShowNo = Get-WecRecordField $focusRecord "show_no"
    $focusDayValue = Get-WecRecordField $focusRecord "focus_day"
    $modeValue = Get-WecRecordField $focusRecord "mode"
    if (!$modeValue) { $modeValue = "day" }
    $isPause = Test-WecTruthy (Get-WecRecordField $focusRecord "is_pause")
    $isLock = Test-WecTruthy (Get-WecRecordField $focusRecord "is_lock")
    $script:FocusPaused = $isPause
    $script:FocusLocked = $isLock
    if ($focusShowNo) { $script:ShowNo = [string]$focusShowNo }
    if ($focusDayValue) { $script:FocusDay = [string]$focusDayValue }

    $cadenceRows = @(Get-WecAirtableTableRecords -TableName "cadence" -PageSize 100)
    $cadenceMinutes = Resolve-WecCadenceMinutes -CadenceRows $cadenceRows -Mode $modeValue
    $focusKey = "$ShowNo|$FocusDay|$modeValue"
    if ($ForceSync) {
      Write-WecAirtableLog -LogType "heartbeat" -CheckName "cadence_gate" -Summary "cadence pass: force sync focus=$FocusDay mode=$modeValue cadence=$cadenceMinutes" -Payload @{
        reason = "force_sync"
        focus_key = $focusKey
        mode = $modeValue
        cadence_minutes = $cadenceMinutes
        active_shows = $activeShows.Count
        active_focus_shows = $activeFocusShows.Count
        is_pause = $isPause
        is_lock = $isLock
      }
      Write-WorkflowLog "cadence-gate bypass force-sync focus=$focusKey cadence=$cadenceMinutes"
      return @{
        should_run = $true
        reason = "force_sync"
        cadence_minutes = $cadenceMinutes
      }
    }
    $lastKey = "cadence_gate_last_run"
    $lastFocusKey = "cadence_gate_last_focus_key"
    $isFocusChanged = (!$State.ContainsKey($lastFocusKey)) -or ([string]$State[$lastFocusKey] -ne $focusKey)
    $isDue = $true
    $elapsedMinutes = $null
    if (!$isFocusChanged -and $State.ContainsKey($lastKey)) {
      try {
        $lastRun = [datetime]$State[$lastKey]
        $elapsedMinutes = ((Get-Date) - $lastRun).TotalMinutes
        $isDue = $elapsedMinutes -ge $cadenceMinutes
      } catch {
        $isDue = $true
      }
    }

    if (!$isDue) {
      $summary = "cadence skip: focus=$FocusDay mode=$modeValue cadence=$cadenceMinutes elapsed=$([math]::Round($elapsedMinutes, 2))"
      Write-WecAirtableLog -LogType "heartbeat" -CheckName "cadence_gate" -Status "skipped" -Summary $summary -Payload @{
        reason = "not_due"
        focus_key = $focusKey
        mode = $modeValue
        cadence_minutes = $cadenceMinutes
        elapsed_minutes = $elapsedMinutes
        is_pause = $isPause
        is_lock = $isLock
      }
      Write-WorkflowLog "cadence-gate skip not_due focus=$focusKey cadence=$cadenceMinutes elapsed=$([math]::Round($elapsedMinutes, 2))"
      return @{
        should_run = $false
        reason = "not_due"
        cadence_minutes = $cadenceMinutes
      }
    }

    $State[$lastKey] = (Get-Date).ToString("o")
    $State[$lastFocusKey] = $focusKey
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "cadence_gate" -Summary "cadence pass: focus=$FocusDay mode=$modeValue cadence=$cadenceMinutes" -Payload @{
      reason = if ($isFocusChanged) { "focus_changed" } else { "due" }
      focus_key = $focusKey
      mode = $modeValue
      cadence_minutes = $cadenceMinutes
      active_shows = $activeShows.Count
      active_focus_shows = $activeFocusShows.Count
      is_pause = $isPause
      is_lock = $isLock
    }
    Write-WorkflowLog "cadence-gate pass focus=$focusKey cadence=$cadenceMinutes"
    return @{
      should_run = $true
      reason = if ($isFocusChanged) { "focus_changed" } else { "due" }
      cadence_minutes = $cadenceMinutes
      is_pause = $isPause
      is_lock = $isLock
    }
  } catch {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "cadence_gate" -Status "error" -Summary "cadence gate failed: $($_.Exception.Message)" -Payload @{
      reason = "gate_error"
      error = $_.Exception.Message
    }
    Write-WorkflowLog "cadence-gate error $($_.Exception.Message)"
    return @{
      should_run = $false
      reason = "gate_error"
      cadence_minutes = 0
    }
  }
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
  return [string]$message -match "fetch failed|EXECUTION_TIME_EXCEEDED|Execution Time Exceeded|timed out|timeout|Internal Server Error|\(500\)|remote server returned an error:\s*\(500\)"
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
  if (@($Keys).Count -eq 0) { return $existing }
  $token = Get-WecAirtableToken

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
        Authorization = "Bearer $token"
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
  $token = Get-WecAirtableToken
  $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))"
  for ($i = 0; $i -lt $Records.Count; $i += 10) {
    $chunk = @($Records[$i..([Math]::Min($i + 9, $Records.Count - 1))])
    $body = @{
      records = $chunk
      typecast = $true
    } | ConvertTo-Json -Depth 12
    Invoke-RestMethod -Method $Method -Uri $uri -Headers @{
      Authorization = "Bearer $token"
      "Content-Type" = "application/json"
    } -Body $body -TimeoutSec 30 | Out-Null
  }
}

function Remove-AirtableLiveRecords {
  param(
    [string]$TableName,
    [array]$RecordIds
  )

  $ids = @($RecordIds | Where-Object { $_ } | Select-Object -Unique)
  if ($ids.Count -eq 0) { return 0 }
  $token = Get-WecAirtableToken
  $deleted = 0
  for ($i = 0; $i -lt $ids.Count; $i += 10) {
    $chunk = @($ids[$i..([Math]::Min($i + 9, $ids.Count - 1))])
    $query = @($chunk | ForEach-Object { "records[]=$([uri]::EscapeDataString([string]$_))" }) -join "&"
    $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString($TableName))?$query"
    $result = Invoke-RestMethod -Method Delete -Uri $uri -Headers @{
      Authorization = "Bearer $token"
    } -TimeoutSec 30
    $deleted += @($result.records).Count
  }
  return $deleted
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
    if ($null -eq $item) { continue }
    if ($item -is [System.Array]) {
      foreach ($inner in $item) {
        if ($null -ne $inner) { $expanded += $inner }
      }
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
  $parsed = ([string]$rawResponse.Content) | ConvertFrom-Json
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

function Get-WecAirtableToken {
  if ($env:AIRTABLE_TOKEN) { return $env:AIRTABLE_TOKEN }
  throw "AIRTABLE_TOKEN fallback is required for Catalyst WEC runners"
}

function Resolve-WecFocusDayValue {
  if ($FocusDay) { return [string]$FocusDay }
  $focusRows = @(Get-WecAirtableRecordsByView -TableName "focus_show" -ViewName "active" -PageSize 10)
  $matching = @($focusRows | Where-Object { [string](Get-WecRecordField $_ "show_no") -eq [string]$ShowNo })
  if ($matching.Count -ne 1) {
    throw "focus_show.active must have exactly one record for show=$ShowNo; found $($matching.Count)"
  }
  $value = [string](Get-WecRecordField $matching[0] "focus_day")
  if (!$value) {
    throw "focus_show.active record for show=$ShowNo has blank focus_day"
  }
  return $value
}

function Invoke-WecRunnerPost {
  param(
    [string]$Url,
    [hashtable]$Payload,
    [int]$TimeoutSec = 180
  )

  $body = $Payload | ConvertTo-Json -Depth 8 -Compress
  return Invoke-RestMethod -Method Post -Uri $Url -ContentType "application/json" -Body $body -TimeoutSec $TimeoutSec
}

function Invoke-ClassOogQueueSync($FocusDayValue) {
  if (!$FocusDayValue) { return }

  $lockedClasses = @(Get-WecLockedScheduleRows $FocusDayValue)
  $activeTrainerMap = Get-WecActiveTrainerMap
  $activeTrainers = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
  foreach ($trainerName in $activeTrainerMap.Keys) {
    [void]$activeTrainers.Add([string]$trainerName)
  }
  $session = Get-HorseshowingSession
  $activeRows = @()
  $matchedClassNos = @()
  $probeErrors = @()
  foreach ($classRow in $lockedClasses) {
    try {
      $rows = @(Get-LocalClassOogActiveRows -ClassRow $classRow -ActiveTrainers $activeTrainers -Session $session)
      if ($rows.Count -gt 0) {
        $activeRows += $rows
        $matchedClassNos += [int]$classRow.class_no
      }
    } catch {
      $probeErrors += [pscustomobject]@{
        class_no = [int]$classRow.class_no
        ring_day_no = [int]$classRow.ring_day_no
        ring_no = [int]$classRow.ring_no
        error = $_.Exception.Message
      }
      Write-WorkflowLog "class_oog_local_probe_error class_no=$($classRow.class_no) error=$($_.Exception.Message)"
    }
  }
  $matchedClassNos = @($matchedClassNos | Sort-Object -Unique)
  if ($probeErrors.Count -gt 0) {
    $sample = @($probeErrors | Select-Object -First 5 | ForEach-Object { "$($_.class_no): $($_.error)" }) -join "; "
    throw "class_oog local probe failed for $($probeErrors.Count) of $($lockedClasses.Count) locked classes: $sample"
  }
  $airtableWrite = Sync-LocalClassOogRowsToAirtable -Rows $activeRows -FocusDayValue $FocusDayValue -ActiveTrainerMap $activeTrainerMap

  $summary = @{
    ok = $true
    show_no = $ShowNo
    focus_day = $FocusDayValue
    chunks = 1
    total_chunks = 1
    chunk_offset = 0
    next_offset = 0
    target_classes_total = $lockedClasses.Count
    probed_classes = $lockedClasses.Count
    matched_classes = $matchedClassNos.Count
    matched_class_nos = $matchedClassNos
    active_trainer_rows = $activeRows.Count
    probe_errors = $probeErrors
    class_results = $matchedClassNos.Count
    rows_written = $activeRows.Count
    airtable_records = (Int-OrZero $airtableWrite.changed)
    source = "local_class_oog_probe"
    airtable_created = (Int-OrZero $airtableWrite.created)
    airtable_updated = (Int-OrZero $airtableWrite.updated)
  }
  Write-WorkflowLog "class_oog_local_probe probed=$($summary.probed_classes) matched=$($summary.matched_classes) active_rows=$($summary.active_trainer_rows) errors=$($probeErrors.Count) rows=$($summary.rows_written) airtable=$($summary.airtable_records) focus=$FocusDayValue"
  return $summary
}

function Invoke-ClassOogUnlockedSafetyAudit($FocusDayValue) {
  if (!$FocusDayValue) { return }

  $lockedClasses = @(Get-WecLockedScheduleRows $FocusDayValue)
  $lockedRecordIds = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
  foreach ($classRow in $lockedClasses) {
    if ($classRow.record_id) { [void]$lockedRecordIds.Add([string]$classRow.record_id) }
  }

  $targets = @(Get-WecFocusScheduleRows $FocusDayValue | Where-Object {
    $_.class_no -gt 0 -and
    !$lockedRecordIds.Contains([string]$_.record_id)
  })

  $activeTrainerMap = Get-WecActiveTrainerMap
  $activeTrainers = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
  foreach ($trainerName in $activeTrainerMap.Keys) {
    [void]$activeTrainers.Add([string]$trainerName)
  }
  $session = Get-HorseshowingSession
  $activeRows = @()
  $probeErrors = @()
  foreach ($classRow in $targets) {
    try {
      $activeRows += @(Get-LocalClassOogActiveRows -ClassRow $classRow -ActiveTrainers $activeTrainers -Session $session)
    } catch {
      $probeErrors += [pscustomobject]@{
        class_no = $classRow.class_no
        ring_day_no = $classRow.ring_day_no
        ring_no = $classRow.ring_no
        error = $_.Exception.Message
      }
    }
  }
  if (@($probeErrors).Count -gt 0) {
    $sample = @($probeErrors | Select-Object -First 3) | ConvertTo-Json -Depth 5 -Compress
    throw "class_oog unlocked safety failed for $(@($probeErrors).Count) of $($targets.Count) unlocked classes: $sample"
  }

  $matchedClassNos = @($activeRows | ForEach-Object { [int]$_.class_no } | Sort-Object -Unique)
  $targetByClass = @{}
  foreach ($target in $targets) {
    $targetByClass[[string]$target.class_no] = $target
  }
  $blockingActiveRows = @()
  $approvedSecondTripRows = @()
  foreach ($row in @($activeRows)) {
    $target = $targetByClass[[string]$row.class_no]
    if ($target -and $target.second_trip) {
      $approvedSecondTripRows += $row
    } else {
      $blockingActiveRows += $row
    }
  }
  $timedUnlockedRows = @($targets | Where-Object { $_.time_text -and $_.event_type -ne 5 })
  $blockingTimedUnlockedRows = @($timedUnlockedRows | Where-Object { !$_.second_trip })
  $summary = @{
    ok = $true
    show_no = $ShowNo
    focus_day = $FocusDayValue
    locked_classes = $lockedClasses.Count
    unlocked_classes_probed = $targets.Count
    timed_unlocked_classes = $timedUnlockedRows.Count
    timed_unlocked_classes_blocking = $blockingTimedUnlockedRows.Count
    active_trainer_rows_in_unlocked_classes = $activeRows.Count
    active_trainer_rows_blocking = $blockingActiveRows.Count
    active_trainer_rows_approved_2nd_trip = $approvedSecondTripRows.Count
    matched_class_nos = $matchedClassNos
    timed_unlocked_class_nos = @($timedUnlockedRows | ForEach-Object { [int]$_.class_no } | Sort-Object -Unique)
    timed_unlocked_class_nos_blocking = @($blockingTimedUnlockedRows | ForEach-Object { [int]$_.class_no } | Sort-Object -Unique)
    active_rows = @($activeRows | Select-Object class_oog_key, ring_day_no, ring_no, class_no, class_label, entry_order, entry_no, horse, rider, trainer)
    blocking_active_rows = @($blockingActiveRows | Select-Object class_oog_key, ring_day_no, ring_no, class_no, class_label, entry_order, entry_no, horse, rider, trainer)
    approved_2nd_trip_rows = @($approvedSecondTripRows | Select-Object class_oog_key, ring_day_no, ring_no, class_no, class_label, entry_order, entry_no, horse, rider, trainer)
    source = "local_class_oog_probe_unlocked_safety"
  }

  Write-WorkflowLog "class_oog_unlocked_safety probed=$($summary.unlocked_classes_probed) active_rows=$($summary.active_trainer_rows_in_unlocked_classes) blocking=$($summary.active_trainer_rows_blocking) approved_2nd_trip=$($summary.active_trainer_rows_approved_2nd_trip) focus=$FocusDayValue"
  if ((Int-OrZero $summary.timed_unlocked_classes_blocking) -gt 0) {
    Write-WecAirtableAlert -AlertType "timed_unlocked_schedule_classes" -Severity "warn" -DedupeKey "$ShowNo|$FocusDayValue|timed_unlocked_schedule_classes" -Message "Timed update_schedule_staging classes are not locked or marked 2nd_trip: $($summary.timed_unlocked_classes_blocking)" -Payload $summary
  } else {
    Resolve-WecAirtableAlert -AlertType "timed_unlocked_schedule_classes" -DedupeKey "$ShowNo|$FocusDayValue|timed_unlocked_schedule_classes" -Message "Resolved: no timed unlocked update_schedule_staging classes without 2nd_trip approval." -Payload $summary
  }
  if ($blockingActiveRows.Count -gt 0) {
    Write-WecAirtableAlert -AlertType "class_oog_unlocked_active_entries" -Severity "critical" -DedupeKey "$ShowNo|$FocusDayValue|class_oog_unlocked_active_entries" -Message "Active trainer entries found in unlocked update_schedule_staging classes without 2nd_trip approval: $($blockingActiveRows.Count)" -Payload $summary
  } else {
    Resolve-WecAirtableAlert -AlertType "class_oog_unlocked_active_entries" -DedupeKey "$ShowNo|$FocusDayValue|class_oog_unlocked_active_entries" -Message "Resolved: no unapproved active trainer entries found in unlocked update_schedule_staging classes." -Payload $summary
  }
  return $summary
}

function Invoke-TimeWorkflowTableSync($FocusDayValue) {
  if (!$FocusDayValue) { return }
  $token = Get-WecAirtableToken

  $classStart = Invoke-WecRunnerPost -Url $ClassStartTimesRunnerUrl -TimeoutSec 180 -Payload @{
    show_no = $ShowNo
    focus_day = $FocusDayValue
    airtable_token = $token
  }
  if (!$classStart.ok) {
    throw "class_start_times runner failed phase=$($classStart.phase) error=$($classStart.error)"
  }

  $entryGo = Invoke-WecRunnerPost -Url $EntryGoTimesRunnerUrl -TimeoutSec 180 -Payload @{
    show_no = $ShowNo
    focus_day = $FocusDayValue
    airtable_token = $token
  }
  if (!$entryGo.ok) {
    throw "entry_go_times runner failed phase=$($entryGo.phase) error=$($entryGo.error) verify_airtable=$($entryGo.verify_airtable | ConvertTo-Json -Depth 6 -Compress)"
  }

  Write-WorkflowLog "time-workflow class_start_active=$($classStart.verify_airtable.active_rows) entry_go_active=$($entryGo.verify_airtable.active_rows) focus=$FocusDayValue"
  return @{
    class_start_times = $classStart
    entry_go_times = $entryGo
  }
}

function Invoke-EntryGoTimesSync($FocusDayValue) {
  if (!$FocusDayValue) { return }
  $token = Get-WecAirtableToken
  $entryGo = Invoke-WecRunnerPost -Url $EntryGoTimesRunnerUrl -TimeoutSec 180 -Payload @{
    show_no = $ShowNo
    focus_day = $FocusDayValue
    airtable_token = $token
  }
  if (!$entryGo.ok) {
    throw "entry_go_times runner failed phase=$($entryGo.phase) error=$($entryGo.error) skipped=$($entryGo.skipped | ConvertTo-Json -Depth 6 -Compress) verify_airtable=$($entryGo.verify_airtable | ConvertTo-Json -Depth 6 -Compress)"
  }
  Write-WorkflowLog "entry-go-times source=$($entryGo.source_rows) active=$($entryGo.verify_airtable.active_rows) focus=$FocusDayValue"
  return $entryGo
}

function Invoke-ClassLaneAction {
  param(
    [string]$Action,
    [string]$FocusDayValue,
    [int]$TimeoutSec = 120
  )
  if (!$FocusDayValue) { return $null }
  $token = Get-WecAirtableToken
  $result = Invoke-WecRunnerPost -Url $ClassLaneRunnerUrl -TimeoutSec $TimeoutSec -Payload @{
    action = $Action
    show_no = $ShowNo
    focus_day = $FocusDayValue
    airtable_token = $token
    base_id = $AirtableBaseId
  }
  if (!$result.ok) {
    throw "class_lane $Action failed: $($result.error)"
  }
  Write-WorkflowLog "class-lane $Action focus=$FocusDayValue result=$($result.result | ConvertTo-Json -Depth 6 -Compress)"
  return $result.result
}

function Invoke-ClassLaneGetOrdersSync($FocusDayValue) {
  return Invoke-ClassLaneAction -Action "sync-get-orders" -FocusDayValue $FocusDayValue -TimeoutSec 120
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

function Invoke-HorseshowingUpdateScheduleRaw {
  param(
    [string]$RingDayNo
  )

  if (!$RingDayNo) { throw "ring_day_no is required for update_schedule raw fetch" }
  $session = New-Object Microsoft.PowerShell.Commands.WebRequestSession
  $session.UserAgent = "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36"
  $base = "https://www.horseshowing.com"
  Invoke-WebRequest -UseBasicParsing -Uri "$base/show.php?show=$ShowNo" -WebSession $session -TimeoutSec 20 | Out-Null
  Invoke-WebRequest -UseBasicParsing -Uri "$base/schedule.php" -WebSession $session -TimeoutSec 20 | Out-Null
  $response = Invoke-WebRequest -UseBasicParsing -Uri "$base/update_schedule.php" `
    -Method "POST" `
    -WebSession $session `
    -Headers @{
      "accept" = "*/*"
      "origin" = $base
      "referer" = "$base/schedule.php"
      "x-requested-with" = "XMLHttpRequest"
    } `
    -ContentType "application/x-www-form-urlencoded; charset=UTF-8" `
    -Body "show_no=$ShowNo&ring_day_no=$RingDayNo" `
    -TimeoutSec 30
  if ([int]$response.StatusCode -lt 200 -or [int]$response.StatusCode -ge 300) {
    throw "update_schedule.php HTTP $([int]$response.StatusCode) ring_day_no=$RingDayNo"
  }
  $raw = [string]$response.Content
  if (!$raw) { throw "update_schedule.php returned empty raw ring_day_no=$RingDayNo" }
  return $raw
}

function Invoke-UpdateScheduleMirrorWorkflow($FocusDayValue, [string]$TriggerReason) {
  if (!$FocusDayValue) { return $null }
  if ($TriggerReason -notin @("focus_day_change", "cadence")) {
    throw "unsupported update_schedule mirror trigger_reason=$TriggerReason"
  }
  $token = Get-WecAirtableToken
  $body = @{
    action = "sync-update-schedule-mirror"
    show_no = $ShowNo
    focus_day = $FocusDayValue
    trigger_reason = $TriggerReason
    base_id = $AirtableBaseId
    airtable_token = $token
  }
  $result = Invoke-RestMethod -Method Post -Uri $UpdateScheduleRunnerUrl -ContentType "application/x-www-form-urlencoded" -Body $body -TimeoutSec 120
  if ($result.ok -eq $false) {
    $mirrorError = if ($result.blocker) { $result.blocker } else { $result.error }
    throw "horseshowing_update_schedule_runner mirror failed: $mirrorError"
  }
  Write-WorkflowLog "update-schedule-mirror trigger=$TriggerReason catalyst=$($result.catalyst_count) airtable=$($result.airtable_count) confirm_delete_after=$($result.confirm_delete_rows_after) stale_deleted=$($result.stale_update_schedule_deleted) focus=$FocusDayValue"
  return $result
}

function Invoke-UpdateScheduleCatalystMaterialization($FocusDayValue) {
  if (!$FocusDayValue) { return $null }
  $safeFocusDay = ([string]$FocusDayValue).Replace("'", "\'")
  $formula = "AND({show_no}=$ShowNo,IS_SAME({ISO},DATETIME_PARSE('$safeFocusDay'),'day'))"
  $ringDayRows = @(Get-WecAirtableRecordsByFormula -TableName "get_ring_days" -Formula $formula -PageSize 100)
  if ($ringDayRows.Count -le 0) {
    throw "update_schedule materialization failed: no get_ring_days rows for show=$ShowNo focus=$FocusDayValue"
  }

  $results = @()
  $totalParsed = 0
  $totalMaterialized = 0
  $sortedRows = @($ringDayRows | Sort-Object {
    Int-OrZero (Get-WecRecordField $_ "ring_day_no")
  })

  foreach ($record in $sortedRows) {
    $ringDayNo = [string](Get-WecRecordField $record "ring_day_no")
    $ringNo = [string](Get-WecRecordField $record "ring_no")
    $ringName = [string](Get-WecRecordField $record "ring_name")
    if (!$ringName) { $ringName = [string](Get-WecRecordField $record "ring_name_normalized") }
    $dayLabel = [string](Get-WecRecordField $record "ISO")
    if (!$dayLabel) { $dayLabel = [string]$FocusDayValue }

    if (!$ringDayNo -or !$ringNo) {
      throw "update_schedule materialization failed: get_ring_days row missing ring_day_no or ring_no record=$($record.id)"
    }

    $body = @{
      action = "sync-update-schedule-only"
      show_no = $ShowNo
      ring_day_no = $ringDayNo
      ring_no = $ringNo
      ring_name = $ringName
      day_label = $dayLabel
    }
    $response = Invoke-RestMethod -Method Post -Uri $BaseUrl -ContentType "application/x-www-form-urlencoded" -Body $body -TimeoutSec 120
    if ($response.ok -eq $false) {
      $materializeError = if ($response.error) { $response.error } else { $response.blocker }
      throw "horseshowing_sync sync-update-schedule-only failed ring_day_no=${ringDayNo}: $materializeError"
    }

    $parsedRows = Int-OrZero $response.parsed_rows
    $materializedRows = Int-OrZero $response.counters.rows
    $totalParsed += $parsedRows
    $totalMaterialized += $materializedRows
    $results += [pscustomobject]@{
      ring_day_no = $ringDayNo
      ring_no = $ringNo
      ring_name = $ringName
      upstream_status = $response.upstream_status
      parsed_rows = $parsedRows
      materialized_rows = $materializedRows
      inserted = Int-OrZero $response.counters.inserted
      updated = Int-OrZero $response.counters.updated
      skipped = Int-OrZero $response.counters.skipped
    }
    Write-WorkflowLog "update-schedule-materialize ring_day_no=$ringDayNo ring_no=$ringNo upstream_status=$($response.upstream_status) parsed=$parsedRows materialized=$materializedRows inserted=$($response.counters.inserted) updated=$($response.counters.updated) skipped=$($response.counters.skipped)"
  }

  if ($totalParsed -le 0) {
    throw "source_empty_for_all_ring_days: update_schedule.php parsed 0 rows for show=$ShowNo focus=$FocusDayValue"
  }
  if ($totalMaterialized -le 0) {
    throw "source_nonempty_write_failed: update_schedule.php parsed $totalParsed rows but hs_update_schedule materialized 0 rows"
  }

  return [pscustomobject]@{
    ok = $true
    show_no = $ShowNo
    focus_day = $FocusDayValue
    eligible_ring_days = $ringDayRows.Count
    parsed_rows = $totalParsed
    materialized_rows = $totalMaterialized
    results = $results
  }
}

function Invoke-UpdateScheduleStagingWorkflow($FocusDayValue, [bool]$ResetFocus = $false, [string]$TriggerReason = "") {
  if (!$FocusDayValue) { return $null }
  $token = Get-WecAirtableToken
  $baseBody = @{
    show_no = $ShowNo
    focus_day = $FocusDayValue
    batch_size = "1"
    window_minutes = "60"
    mark_focus_state = "1"
    base_id = $AirtableBaseId
    airtable_token = $token
  }
  if ($TriggerReason) {
    if ($TriggerReason -notin @("focus_day_change", "cadence")) {
      throw "unsupported update_schedule trigger_reason=$TriggerReason"
    }
    $baseBody.trigger_reason = $TriggerReason
  }
  if ($ResetFocus) {
    Write-WorkflowLog "update-schedule-staging reset skipped; mirror-based Stage2C preserves staging records focus=$FocusDayValue"
  }
  $body = $baseBody.Clone()
  $body.action = "sync-update-schedule-staging-from-mirror"
  $response = Invoke-RestMethod -Method Post -Uri $BaseUrl -ContentType "application/x-www-form-urlencoded" -Body $body -TimeoutSec 120
  if ($response.ok -eq $false) {
    throw "horseshowing_sync sync-update-schedule-staging-from-mirror failed: $($response.error)"
  }
  $stagingRows = Int-OrZero $response.update_schedule_staging_rows
  $updateRows = Int-OrZero $response.update_schedule_rows
  $stagingUpserts = Int-OrZero $response.update_schedule_staging_upserts
  $staleStagingDeleted = Int-OrZero $response.update_schedule_staging_stale_deleted
  $sourceLinkRows = Int-OrZero $response.update_schedule_staging_source_link_rows
  $sourceLinkMissing = Int-OrZero $response.update_schedule_staging_source_link_missing
  $outOfFocusMarkedInactive = Int-OrZero $response.update_schedule_staging_out_of_focus_marked_inactive
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -RecordsSeen $stagingRows -RecordsChanged $stagingUpserts -Summary "update_schedule_staging refreshed from Airtable update_schedule staging_rows=$stagingRows update_rows=$updateRows source_links=$sourceLinkRows missing_source_links=$sourceLinkMissing out_of_focus_marked_inactive=$outOfFocusMarkedInactive stale_deleted=$staleStagingDeleted focus=$($response.focus_day)" -Payload @{
    show_no = $ShowNo
    focus_day = $response.focus_day
    source = "airtable.update_schedule"
    update_schedule_rows = $updateRows
    update_schedule_staging_rows = $stagingRows
    update_schedule_staging_upserts = $stagingUpserts
    update_schedule_staging_stale_deleted = $staleStagingDeleted
    update_schedule_staging_source_link_rows = $sourceLinkRows
    update_schedule_staging_source_link_missing = $sourceLinkMissing
    update_schedule_staging_out_of_focus_marked_inactive = $outOfFocusMarkedInactive
    focus_show_control = $response.focus_show_control
    raw_payload_used = $false
    legacy_update_schedule_runner_used = $false
  }
  Write-WorkflowLog "update-schedule-staging source=airtable.update_schedule staging_rows=$stagingRows update_rows=$updateRows source_links=$sourceLinkRows missing_source_links=$sourceLinkMissing out_of_focus_marked_inactive=$outOfFocusMarkedInactive stale_deleted=$staleStagingDeleted focus=$($response.focus_day)"
  return $response
}

function Invoke-WecLaneAudit($FocusDayValue) {
  if (!$FocusDayValue) { return }
  $scriptPath = Join-Path $root "audit-wec-lanes.js"
  $output = & node $scriptPath --show-no $ShowNo --focus-day $FocusDayValue 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw "audit-wec-lanes failed: $($output -join ' ')"
  }
  Write-WorkflowLog "audit-wec-lanes $($output -join ' ')"
}

function Invoke-WecHeartbeatComposite {
  $scheduleRows = @()
  if (!$WorkflowV4Stage1Only) {
    $scheduleRows = @(Invoke-CatalystArray "schedule-json")
  }
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
  $ringDays = $null
  $liveError = $null
  $ordersError = $null
  $ringDaysError = $null
  $liveSkipped = $false
  $ordersSkipped = $false
  $ringDaysSkipped = $false
  $liveSkipReason = ""
  $ordersSkipReason = ""
  $ringDaysSkipReason = ""
  $params = @{ focus_day = $resolvedFocusDay }
  if ($script:FocusPaused) {
    $liveSkipped = $true
    $ordersSkipped = $true
    $ringDaysSkipped = $true
    $liveSkipReason = "focus_show.is_pause"
    $ordersSkipReason = "focus_show.is_pause"
    $ringDaysSkipReason = "focus_show.is_pause"
  } elseif (!$RunLiveEnrichment) {
    try {
      $ringDays = Invoke-CatalystActionWithRetry "sync-ring-days" 2
      if ($ringDays.ok -eq $false) { $ringDaysError = $ringDays.error }
    } catch {
      $ringDaysError = $_.Exception.Message
    }
    $liveSkipped = $true
    $ordersSkipped = $true
    $liveSkipReason = "live_enrichment_disabled"
    $ordersSkipReason = "live_enrichment_disabled"
  } else {
    try {
      $ringDays = Invoke-CatalystActionWithRetry "sync-ring-days" 2
      if ($ringDays.ok -eq $false) { $ringDaysError = $ringDays.error }
    } catch {
      $ringDaysError = $_.Exception.Message
    }
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
  }

  return [pscustomobject]@{
    ok = $true
    action = "heartbeat-composite"
    show_no = $ShowNo
    focus_day = $resolvedFocusDay
    live = $live
    live_error = $liveError
    live_skipped = $liveSkipped
    live_skip_reason = $liveSkipReason
    live_enrichment_enabled = [bool]$RunLiveEnrichment
    ring_days = $ringDays
    ring_days_error = $ringDaysError
    ring_days_skipped = $ringDaysSkipped
    ring_days_skip_reason = $ringDaysSkipReason
    hs_ring_days_count = Int-OrZero $ringDays.materialized_ring_day_rows
    get_ring_days_source_count = Int-OrZero $ringDays.parsed_rows
    get_ring_days_source_sequence = $ringDays.source_request_sequence
    orders = $orders
    orders_error = $ordersError
    orders_skipped = $ordersSkipped
    orders_skip_reason = $ordersSkipReason
    schedule_rows = $scheduleRows.Count
    created_triggers = 0
    trigger_error = $null
  }
}

function Get-Stage1ProofCounts($Heartbeat, $UpdateScheduleMirrorResult, $UpdateScheduleResult, $UpdateScheduleMaterializationResult, $UpdateScheduleError) {
  $updateScheduleRows = Int-OrZero $UpdateScheduleResult.update_schedule_rows
  if ($updateScheduleRows -eq 0) {
    $updateScheduleRows = Int-OrZero $UpdateScheduleMaterializationResult.materialized_rows
  }
  return [pscustomobject]@{
    get_ring_days_source_count = Int-OrZero $Heartbeat.get_ring_days_source_count
    hs_ring_days_count = Int-OrZero $Heartbeat.hs_ring_days_count
    get_ring_days_error = [string]$Heartbeat.ring_days_error
    get_ring_days_source_sequence = $Heartbeat.get_ring_days_source_sequence
    update_schedule_count = $updateScheduleRows
    update_schedule_staging_count = Int-OrZero $UpdateScheduleResult.update_schedule_staging_rows
    update_schedule_staging_error = [string]$UpdateScheduleError
  }
}

function Get-Stage1CountGateBlocker($Counts) {
  if ((Int-OrZero $Counts.hs_ring_days_count) -eq 0) {
    if ($Counts.get_ring_days_error) { return "get_ring_days_failed: $($Counts.get_ring_days_error)" }
    if ((Int-OrZero $Counts.get_ring_days_source_count) -eq 0) { return "source_empty: get_ring_days returned 0 rows" }
    return "source_nonempty_write_failed: get_ring_days returned rows but hs_ring_days count is 0"
  }
  if ((Int-OrZero $Counts.update_schedule_count) -eq 0) { return "source_empty: update_schedule count is 0" }
  if ($Counts.update_schedule_staging_error) {
    if ([string]$Counts.update_schedule_staging_error -match "408|timeout|timed out") {
      return "staging_timeout: $($Counts.update_schedule_staging_error)"
    }
    return "staging_failed: $($Counts.update_schedule_staging_error)"
  }
  if ((Int-OrZero $Counts.update_schedule_staging_count) -eq 0) { return "write_or_parse_failed: update_schedule_staging count is 0" }
  return $null
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
  if (!$env:AIRTABLE_TOKEN) { return }

  try {
    $formulaRaw = "AND({show_no}=$ShowNo, IS_SAME({focus_day}, DATETIME_PARSE('$FocusDayValue','YYYY-MM-DD'), 'day'), {status}='open', OR({alert_lane}='class_start',{alert_lane}='entry_go'))"
    $formula = [uri]::EscapeDataString($formulaRaw)
    $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-alerts'))?filterByFormula=$formula&pageSize=100"
    $records = @()
    do {
      $result = Invoke-RestMethod -Method Get -Uri $uri -Headers @{
        Authorization = "Bearer $env:AIRTABLE_TOKEN"
      } -TimeoutSec 30
      $records += @($result.records)
      if ($result.offset) {
        $uri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-alerts'))?filterByFormula=$formula&pageSize=100&offset=$([uri]::EscapeDataString($result.offset))"
      } else {
        $uri = $null
      }
    } while ($uri)

    $updates = @()
    foreach ($record in $records) {
      $alertKey = [string]$record.fields.alert_key_run
      if ($alertKey -and $ActiveAlertKeys.ContainsKey($alertKey)) { continue }
      $updates += @{
        id = $record.id
        fields = @{
          status = "resolved"
          message = "Resolved: alert window is no longer active."
          payload_json = ConvertTo-SafeJson @{
            focus_day = $FocusDayValue
            resolved_reason = "alert_window_inactive"
            resolved_at = (Get-Date).ToUniversalTime().ToString("o")
          }
        }
      }
    }

    for ($index = 0; $index -lt $updates.Count; $index += 10) {
      $batch = @($updates[$index..([math]::Min($index + 9, $updates.Count - 1))])
      if ($batch.Count -eq 0) { continue }
      $body = @{
        records = $batch
        typecast = $true
      } | ConvertTo-Json -Depth 12
      $patchUri = "https://api.airtable.com/v0/$AirtableBaseId/$([uri]::EscapeDataString('wec-alerts'))"
      Invoke-RestMethod -Method Patch -Uri $patchUri -Headers @{
        Authorization = "Bearer $env:AIRTABLE_TOKEN"
        "Content-Type" = "application/json"
      } -Body $body -TimeoutSec 30 | Out-Null
    }
    Write-WorkflowLog "airtable-alert stale-time resolved focus=$FocusDayValue open_seen=$($records.Count) resolved=$($updates.Count) active_keys=$(@($ActiveAlertKeys.Keys).Count)"
  } catch {
    Write-WorkflowLog "airtable-alert stale-time resolve failed focus=$FocusDayValue error=$($_.Exception.Message)"
  }
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
  $ringDays = Invoke-CatalystActionWithRetry "sync-ring-days" 2
  if ($ringDays.ok -eq $false) {
    throw "sync-ring-days prerequisite failed: $($ringDays.error)"
  }
  Write-WorkflowLog "sync-focus-day prerequisite sync-ring-days rows=$($ringDays.parsed_rows)"

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

function Get-WecClassOogRowsForFocus($FocusDayValue) {
  $safeFocusDay = ([string]$FocusDayValue).Replace("'", "\'")
  $formula = "AND({show_no}=$ShowNo,IS_SAME({focus_day},DATETIME_PARSE('$safeFocusDay'),'day'))"
  return @(Get-WecAirtableRecordsByFormula -TableName "class_oog" -Formula $formula -PageSize 100)
}

function Invoke-ClassStartTimesRetryStage($FocusDayValue) {
  if (!$FocusDayValue) {
    throw "class_start_times hard gate failed: active focus_day missing"
  }
  if ($script:FocusPaused) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_start_times_gate" -Status "skipped" -Summary "class_start_times skipped: focus_show.is_pause" -Payload @{
      show_no = $ShowNo
      focus_day = $FocusDayValue
      gate = "focus_show.is_pause"
    }
    throw "class_start_times hard gate failed: focus_show.is_pause"
  }

  $lockedRows = @(Get-WecLockedScheduleRows $FocusDayValue)
  if ($lockedRows.Count -le 0) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_start_times_gate" -Status "skipped" -RecordsSeen 0 -Summary "class_start_times skipped: no locked update_schedule_staging rows" -Payload @{
      show_no = $ShowNo
      focus_day = $FocusDayValue
      gate = "locked_update_schedule_staging"
    }
    throw "class_start_times hard gate failed: no locked update_schedule_staging rows"
  }

  $classOogRows = @(Get-WecClassOogRowsForFocus $FocusDayValue)
  if ($classOogRows.Count -le 0) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_start_times_gate" -Status "skipped" -RecordsSeen $lockedRows.Count -Summary "class_start_times skipped: no class_oog rows" -Payload @{
      show_no = $ShowNo
      focus_day = $FocusDayValue
      gate = "class_oog"
      locked_staging_rows = $lockedRows.Count
    }
    throw "class_start_times hard gate failed: no class_oog rows"
  }

  try {
    $classStartLane = Invoke-ClassLaneAction -Action "sync-class-start-times" -FocusDayValue $FocusDayValue -TimeoutSec 180
    $classStartChanged = (Int-OrZero $classStartLane.class_start_times_airtable_upserted) + (Int-OrZero $classStartLane.class_start_times_airtable_deleted) + (Int-OrZero $classStartLane.class_start_times_catalyst.inserted) + (Int-OrZero $classStartLane.class_start_times_catalyst.updated) + (Int-OrZero $classStartLane.class_start_times_catalyst.deleted)
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_start_times" -RecordsSeen (Int-OrZero $classStartLane.source_locked_staging) -RecordsChanged $classStartChanged -Summary "class_start_times source=$($classStartLane.source_locked_staging) focus=$FocusDayValue" -Payload $classStartLane
    Resolve-WecAirtableAlert -AlertType "core_class_start_times_failed" -DedupeKey "$ShowNo|$FocusDayValue|class_start_times" -Message "Resolved: class_start_times completed." -Payload $classStartLane
    $classStartLinkRepair = Repair-ClassStartTimesStagingLinks $FocusDayValue
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_start_times_links" -RecordsSeen (Int-OrZero $classStartLinkRepair.class_start_times_seen) -RecordsChanged (Int-OrZero $classStartLinkRepair.links_repaired) -Summary "class_start_times staging links repaired=$($classStartLinkRepair.links_repaired) focus=$FocusDayValue" -Payload $classStartLinkRepair
    if (@($classStartLinkRepair.missing_staging_keys).Count -gt 0) {
      throw "class_start_times staging link repair missing $(@($classStartLinkRepair.missing_staging_keys).Count) staging keys"
    }
    return [pscustomobject]@{
      ok = $true
      focus_day = $FocusDayValue
      focus_show_is_pause = $false
      locked_staging_rows = $lockedRows.Count
      class_oog_rows = $classOogRows.Count
      result = $classStartLane
      link_repair = $classStartLinkRepair
    }
  } catch {
    $retryable = Test-RetryableCatalystError $_.Exception.Message
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_start_times" -Status "error" -Summary "class_start_times failed show=$ShowNo focus=${FocusDayValue}: $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $FocusDayValue
      error = $_.Exception.Message
      retryable = $retryable
      locked_staging_rows = $lockedRows.Count
      class_oog_rows = $classOogRows.Count
    }
    Write-WecAirtableAlert -AlertType "core_class_start_times_failed" -Severity "critical" -Message "class_start_times failed for show=$ShowNo focus=${FocusDayValue}: $($_.Exception.Message)" -DedupeKey "$ShowNo|$FocusDayValue|class_start_times" -Payload @{
      show_no = $ShowNo
      focus_day = $FocusDayValue
      error = $_.Exception.Message
      retryable = $retryable
    }
    if ($retryable) {
      Write-WorkflowLog "class_start_times retryable failure show=$ShowNo focus=$FocusDayValue error=$($_.Exception.Message)"
    }
    throw
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

if ($RunClassOogLocalProbeOnly) {
  $focusDayValue = Resolve-WecFocusDayValue
  $result = Invoke-ClassOogQueueSync $focusDayValue
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog" -RecordsSeen (Int-OrZero $result.rows_written) -RecordsChanged (Int-OrZero $result.airtable_records) -Summary "class_oog catalyst runner probed=$($result.probed_classes) matched=$($result.matched_classes) rows=$($result.rows_written) focus=$focusDayValue" -Payload $result
  $state["class_oog_catalyst_runner_last_run"] = (Get-Date).ToString("o")
  Save-State $state
  if ($script:WecWorkflowMutexAcquired) {
    $script:WecWorkflowMutex.ReleaseMutex()
    $script:WecWorkflowMutex.Dispose()
  }
  $result | ConvertTo-Json -Depth 8
  exit 0
}

if ($RunClassOogUnlockedSafetyOnly) {
  $focusDayValue = Resolve-WecFocusDayValue
  $result = Invoke-ClassOogUnlockedSafetyAudit $focusDayValue
  $status = if ((Int-OrZero $result.active_trainer_rows_blocking) -gt 0) { "error" } else { "ok" }
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog_safety" -Status $status -RecordsSeen (Int-OrZero $result.unlocked_classes_probed) -RecordsChanged (Int-OrZero $result.active_trainer_rows_blocking) -Summary "class_oog unlocked safety probed=$($result.unlocked_classes_probed) blocking_active_rows=$($result.active_trainer_rows_blocking) approved_2nd_trip=$($result.active_trainer_rows_approved_2nd_trip) timed_unlocked_blocking=$($result.timed_unlocked_classes_blocking) focus=$focusDayValue" -Payload $result
  if ($script:WecWorkflowMutexAcquired) {
    $script:WecWorkflowMutex.ReleaseMutex()
    $script:WecWorkflowMutex.Dispose()
  }
  $result | ConvertTo-Json -Depth 8
  exit 0
}

$cadenceGate = Test-WecCadenceGate -State $state
if (!$cadenceGate.should_run) {
  $state["cadence_gate_last_check"] = (Get-Date).ToString("o")
  $state["cadence_gate_last_reason"] = [string]$cadenceGate.reason
  Save-State $state
  if ($script:WecWorkflowMutexAcquired) {
    $script:WecWorkflowMutex.ReleaseMutex()
    $script:WecWorkflowMutex.Dispose()
  }
  exit 0
}

if ($RunClassStartTimesOnly) {
  $focusDayValue = if ($FocusDay) { $FocusDay } else { $script:FocusDay }
  try {
    $result = Invoke-ClassStartTimesRetryStage $focusDayValue
    $state["class_start_times_retry_last_run"] = (Get-Date).ToString("o")
    $state["class_start_times_retry_last_result"] = "ok"
    Save-State $state
    if ($script:WecWorkflowMutexAcquired) {
      $script:WecWorkflowMutex.ReleaseMutex()
      $script:WecWorkflowMutex.Dispose()
    }
    $result | ConvertTo-Json -Depth 10
    exit 0
  } catch {
    $state["class_start_times_retry_last_run"] = (Get-Date).ToString("o")
    $state["class_start_times_retry_last_result"] = "error"
    $state["class_start_times_retry_last_error"] = [string]$_.Exception.Message
    Save-State $state
    if ($script:WecWorkflowMutexAcquired) {
      $script:WecWorkflowMutex.ReleaseMutex()
      $script:WecWorkflowMutex.Dispose()
    }
    throw
  }
}

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
if ($heartbeat.live_skipped) {
  Write-WecAirtableLog -LogType "live" -CheckName "get_rings" -Status "skipped" -Summary "get_rings paused by focus_show.is_pause" -Payload @{
    focus_day = $heartbeat.focus_day
    reason = "focus_show.is_pause"
  }
} elseif ($heartbeat.live_error) {
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
if ($heartbeat.live_skipped) {
  Resolve-WecAirtableAlert -AlertType "live_get_rings_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_rings" -Message "Resolved: get_rings is paused by focus_show.is_pause." -Payload @{
    show_no = $ShowNo
    focus_day = $heartbeat.focus_day
    reason = "focus_show.is_pause"
  }
} elseif ($heartbeat.live_error -and $liveAlertWindow) {
  Write-WecAirtableAlert -AlertType "live_get_rings_failed" -Severity "warn" -Message "Live get_rings failed for show=$ShowNo focus=$($heartbeat.focus_day): $($heartbeat.live_error)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_rings" -Payload $heartbeat
} elseif ($heartbeat.live_error) {
  Resolve-WecAirtableAlert -AlertType "live_get_rings_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_rings" -Message "Resolved: get_rings failure is outside the live alert window." -Payload @{
    show_no = $ShowNo
    focus_day = $heartbeat.focus_day
    error = $heartbeat.live_error
  }
}
$ordersAlertWindow = Test-LiveAlertWindow $heartbeat.focus_day
if ($heartbeat.orders_skipped) {
  Write-WecAirtableLog -LogType "live" -CheckName "get_orders" -Status "skipped" -Summary "get_orders paused by focus_show.is_pause" -Payload @{
    focus_day = $heartbeat.focus_day
    reason = "focus_show.is_pause"
  }
} elseif ($heartbeat.orders_error) {
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
  $ordersRecordsChanged = (Int-OrZero $ordersMirror.changed) + (Int-OrZero $ordersLinks.changed)
  Write-WecAirtableLog -LogType "live" -CheckName "get_orders" -RecordsSeen (Int-OrZero $heartbeat.orders.parsed_rows) -RecordsChanged $ordersRecordsChanged -Summary "get_orders rows=$($heartbeat.orders.parsed_rows) mirrored=$($ordersMirror.changed) linked=$($ordersLinks.changed) focus=$($heartbeat.focus_day)" -Payload @{
    orders = $heartbeat.orders
    mirror = $ordersMirror
    links = $ordersLinks
    focus_day = $heartbeat.focus_day
  }
  Resolve-WecAirtableAlert -AlertType "live_get_orders_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_orders" -Message "Resolved: get_orders returned without error." -Payload $heartbeat.orders
}
if ($heartbeat.orders_skipped) {
  Resolve-WecAirtableAlert -AlertType "live_get_orders_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_orders" -Message "Resolved: get_orders is paused by focus_show.is_pause." -Payload @{
    show_no = $ShowNo
    focus_day = $heartbeat.focus_day
    reason = "focus_show.is_pause"
  }
} elseif ($heartbeat.orders_error -and $ordersAlertWindow) {
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
  $updateScheduleFocusChanged = (!$state.ContainsKey("update_schedule_staging_focus_day")) -or ([string]$state["update_schedule_staging_focus_day"] -ne [string]$heartbeat.focus_day)
  $updateScheduleTriggerReason = if ($cadenceGate.reason -eq "focus_changed") { "focus_day_change" } else { "cadence" }
  $updateScheduleMaterializationResult = Invoke-UpdateScheduleCatalystMaterialization $heartbeat.focus_day
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule_materialization" -Status "ok" -RecordsSeen (Int-OrZero $updateScheduleMaterializationResult.parsed_rows) -RecordsChanged (Int-OrZero $updateScheduleMaterializationResult.materialized_rows) -Summary "update_schedule materialized from get_ring_days parsed=$($updateScheduleMaterializationResult.parsed_rows) hs_update_schedule=$($updateScheduleMaterializationResult.materialized_rows) focus=$($heartbeat.focus_day)" -Payload $updateScheduleMaterializationResult
  $updateScheduleMirrorResult = Invoke-UpdateScheduleMirrorWorkflow $heartbeat.focus_day -TriggerReason $updateScheduleTriggerReason
  Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule_mirror" -Status "ok" -RecordsSeen (Int-OrZero $updateScheduleMirrorResult.catalyst_count) -RecordsChanged ((Int-OrZero $updateScheduleMirrorResult.update_schedule_upserts) + (Int-OrZero $updateScheduleMirrorResult.stale_update_schedule_deleted)) -Summary "update_schedule mirror trigger=$updateScheduleTriggerReason catalyst=$($updateScheduleMirrorResult.catalyst_count) airtable=$($updateScheduleMirrorResult.airtable_count) confirm_delete_after=$($updateScheduleMirrorResult.confirm_delete_rows_after)" -Payload $updateScheduleMirrorResult
  $catalystScheduleRows = Int-OrZero $heartbeat.schedule_rows
  $updateScheduleMissingWorkingRows = ($catalystScheduleRows -eq 0)
  $updateScheduleRefreshDue = (Due $state "update_schedule_staging_workflow" 60)
  $updateScheduleDue = $WorkflowV4Stage1Only -or $ForceSync -or $updateScheduleFocusChanged -or $updateScheduleMissingWorkingRows
  $updateScheduleError = ""
  try {
    if ($updateScheduleDue) {
      $updateScheduleResult = Invoke-UpdateScheduleStagingWorkflow $heartbeat.focus_day -ResetFocus:$updateScheduleFocusChanged -TriggerReason $updateScheduleTriggerReason
      $state["update_schedule_staging_workflow"] = (Get-Date).ToString("o")
      $state["update_schedule_staging_focus_day"] = [string]$heartbeat.focus_day
      Resolve-WecAirtableAlert -AlertType "local_core_workflow_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|local_core_workflow" -Message "Resolved: update_schedule staging workflow completed." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_update_schedule_failed" -Message "Resolved: update_schedule staging workflow completed." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
      Resolve-WecAirtableAlert -AlertType "core_update_schedule_failed" -DedupeKey "$ShowNo||core_update_schedule_failed" -Message "Resolved: update_schedule staging workflow completed." -Payload @{
        show_no = $ShowNo
        focus_day = $heartbeat.focus_day
      }
    } else {
      if ($updateScheduleRefreshDue) {
        $state["update_schedule_staging_refresh_deferred"] = (Get-Date).ToString("o")
        Save-State $state
        Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -Status "skipped" -Summary "update_schedule_staging refresh deferred; class lane already has working rows" -Payload @{
          show_no = $ShowNo
          focus_day = $heartbeat.focus_day
          reason = "defer_nonblocking_refresh"
          schedule_rows = $catalystScheduleRows
        }
      }
      Write-WorkflowLog "update-schedule-staging skipped focus=$($heartbeat.focus_day) working_rows=$catalystScheduleRows"
    }
  } catch {
    $updateScheduleError = $_.Exception.Message
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -Status "error" -Summary "update_schedule staging workflow failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "local_core_workflow_failed" -Severity "critical" -Message "update_schedule staging workflow failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|local_core_workflow" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
  }
  if ($script:FocusPaused) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -Status "skipped" -Summary "Catalyst schedule-json refresh paused by focus_show.is_pause" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      reason = "focus_show.is_pause"
    }
  } elseif ($updateScheduleDue -or $updateScheduleRefreshDue) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_update_schedule" -Status "skipped" -Summary "Catalyst schedule-json refresh skipped; heartbeat uses update_schedule_staging -> class_start_times" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      reason = "schedule_json_not_blocking_class_lane"
      update_schedule_due = $updateScheduleDue
      update_schedule_refresh_due = $updateScheduleRefreshDue
      focus_changed = $updateScheduleFocusChanged
      catalyst_schedule_rows = $catalystScheduleRows
    }
  }
  $stage1ProofCounts = Get-Stage1ProofCounts $heartbeat $updateScheduleMirrorResult $updateScheduleResult $updateScheduleMaterializationResult $updateScheduleError
  $stage1CountBlocker = Get-Stage1CountGateBlocker $stage1ProofCounts
  Write-WorkflowLog "workflowv4-stage1-count-gate focus=$($heartbeat.focus_day) hs_ring_days=$($stage1ProofCounts.hs_ring_days_count) update_schedule=$($stage1ProofCounts.update_schedule_count) update_schedule_staging=$($stage1ProofCounts.update_schedule_staging_count) blocker=$stage1CountBlocker"
  if ($stage1CountBlocker) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "workflowv4_stage1_count_gate" -Status "error" -RecordsSeen (Int-OrZero $stage1ProofCounts.get_ring_days_source_count) -RecordsChanged 0 -Summary "WorkflowV4 Stage 1 count gate failed show=$ShowNo focus=$($heartbeat.focus_day): $stage1CountBlocker" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      blocker = $stage1CountBlocker
      counts = $stage1ProofCounts
    }
    Write-WorkflowLog "workflowv4-stage1-count-gate fail focus=$($heartbeat.focus_day) blocker=$stage1CountBlocker"
    $state["workflowv4_stage1_only_last_run"] = (Get-Date).ToString("o")
    $state["workflowv4_stage1_only_last_result"] = "error"
    $state["workflowv4_stage1_only_last_blocker"] = $stage1CountBlocker
    Save-State $state
    if ($script:WecWorkflowMutexAcquired) {
      $script:WecWorkflowMutex.ReleaseMutex()
      $script:WecWorkflowMutex.Dispose()
    }
    [pscustomobject]@{
      ok = $false
      action = "workflowv4-stage1-only"
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      blocker = $stage1CountBlocker
      get_ring_days_ran = (-not [bool]$heartbeat.ring_days_skipped)
      update_schedule_ran = $null -ne $updateScheduleMirrorResult
      update_schedule_staging_ran = $null -ne $updateScheduleResult
      get_ring_days_source_count = $stage1ProofCounts.get_ring_days_source_count
      hs_ring_days_count = $stage1ProofCounts.hs_ring_days_count
      update_schedule_count = $stage1ProofCounts.update_schedule_count
      update_schedule_staging_count = $stage1ProofCounts.update_schedule_staging_count
      class_start_times_run = $false
      entry_go_times_run = $false
      mobile_run = $false
      print_run = $false
      alerts_run = $false
      results_run = $false
    } | ConvertTo-Json -Depth 8
    exit 1
  }
  if ($WorkflowV4Stage1Only) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "workflowv4_legacy_stage1_stop" -Status "ok" -Summary "WorkflowV4 cadence stopped legacy runner before downstream" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      counts = $stage1ProofCounts
      skipped_stages = @("class_start_times", "entry_go_times", "mobile", "print", "alerts", "results")
    }
    Write-WorkflowLog "workflowv4-stage1-only stop focus=$($heartbeat.focus_day) hs_ring_days=$($stage1ProofCounts.hs_ring_days_count) update_schedule=$($stage1ProofCounts.update_schedule_count) update_schedule_staging=$($stage1ProofCounts.update_schedule_staging_count)"
    $state["workflowv4_stage1_only_last_run"] = (Get-Date).ToString("o")
    $state["workflowv4_stage1_only_last_result"] = "ok"
    Save-State $state
    if ($script:WecWorkflowMutexAcquired) {
      $script:WecWorkflowMutex.ReleaseMutex()
      $script:WecWorkflowMutex.Dispose()
    }
    [pscustomobject]@{
      ok = $true
      action = "workflowv4-stage1-only"
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      get_ring_days_ran = $true
      update_schedule_ran = $true
      update_schedule_staging_ran = $true
      get_ring_days_source_count = $stage1ProofCounts.get_ring_days_source_count
      hs_ring_days_count = $stage1ProofCounts.hs_ring_days_count
      update_schedule_count = $stage1ProofCounts.update_schedule_count
      update_schedule_staging_count = $stage1ProofCounts.update_schedule_staging_count
      class_start_times_run = $false
      entry_go_times_run = $false
      mobile_run = $false
      print_run = $false
      alerts_run = $false
      results_run = $false
    } | ConvertTo-Json -Depth 8
    exit 0
  }
  if ($script:FocusPaused) {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "cadence_pause" -Status "skipped" -Summary "downstream stages paused by focus_show.is_pause" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      reason = "focus_show.is_pause"
      allowed_stages = @("get_ring_days", "update_schedule", "update_schedule_staging")
      skipped_stages = @("class_start_times", "class_oog", "entry_go_times", "get_orders", "get_rings", "class_alerts", "results", "lane_audit")
    }
    Write-WorkflowLog "cadence-pause downstream skipped focus=$($heartbeat.focus_day)"
  } else {
  Invoke-ClassStartTimesRetryStage $heartbeat.focus_day | Out-Null
  try {
    $classOogQueue = Invoke-ClassOogQueueSync $heartbeat.focus_day
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog" -RecordsSeen (Int-OrZero $classOogQueue.rows_written) -RecordsChanged (Int-OrZero $classOogQueue.airtable_records) -Summary "class_oog local probe classes=$($classOogQueue.probed_classes) matched=$($classOogQueue.matched_classes) rows=$($classOogQueue.rows_written) focus=$($heartbeat.focus_day)" -Payload $classOogQueue
    Resolve-WecAirtableAlert -AlertType "core_class_oog_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|class_oog_local_probe" -Message "Resolved: class_oog local probe completed." -Payload $classOogQueue
  } catch {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog" -Status "error" -Summary "class_oog local probe failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "core_class_oog_failed" -Severity "critical" -Message "class_oog local probe failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|class_oog_local_probe" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    throw
  }
  try {
    $classOogRollups = Invoke-ClassLaneAction -Action "sync-class-oog-rollups" -FocusDayValue $heartbeat.focus_day -TimeoutSec 180
    $classOogRollupChanged = (Int-OrZero $classOogRollups.airtable_updated) + (Int-OrZero $classOogRollups.catalyst_updated)
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog_rollups" -RecordsSeen (Int-OrZero $classOogRollups.class_start_times) -RecordsChanged $classOogRollupChanged -Summary "class_oog rollups class_start_times=$($classOogRollups.class_start_times) groups=$($classOogRollups.class_oog_groups) focus=$($heartbeat.focus_day)" -Payload $classOogRollups
    Resolve-WecAirtableAlert -AlertType "core_class_oog_rollups_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|class_oog_rollups" -Message "Resolved: class_oog rollups completed." -Payload $classOogRollups
  } catch {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "core_class_oog_rollups" -Status "error" -Summary "class_oog rollups failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "core_class_oog_rollups_failed" -Severity "critical" -Message "class_oog rollups failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|class_oog_rollups" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    throw
  }
  try {
    $ordersClassStart = Invoke-ClassLaneGetOrdersSync $heartbeat.focus_day
    $ordersClassStartChanged = (Int-OrZero $ordersClassStart.airtable_updated) + (Int-OrZero $ordersClassStart.catalyst_updated)
    Write-WecAirtableLog -LogType "live" -CheckName "get_orders_class_start" -RecordsSeen (Int-OrZero $ordersClassStart.orders) -RecordsChanged $ordersClassStartChanged -Summary "get_orders class_start_matches=$($ordersClassStart.matches) focus=$($heartbeat.focus_day)" -Payload $ordersClassStart
    Resolve-WecAirtableAlert -AlertType "get_orders_class_start_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_orders_class_start" -Message "Resolved: get_orders class_start enrichment completed." -Payload $ordersClassStart
  } catch {
    Write-WecAirtableLog -LogType "live" -CheckName "get_orders_class_start" -Status "error" -Summary "get_orders class_start enrichment failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "get_orders_class_start_failed" -Severity "critical" -Message "get_orders class_start enrichment failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|get_orders_class_start" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    throw
  }
  try {
    $entryGoTimes = Invoke-EntryGoTimesSync $heartbeat.focus_day
    $entryGoChanged = (Int-OrZero $entryGoTimes.airtable.upserted) + (Int-OrZero $entryGoTimes.airtable.inactivated) + (Int-OrZero $entryGoTimes.catalyst.inserted) + (Int-OrZero $entryGoTimes.catalyst.updated) + (Int-OrZero $entryGoTimes.catalyst.deleted)
    Write-WecAirtableLog -LogType "alerts" -CheckName "entry_go_times" -RecordsSeen (Int-OrZero $entryGoTimes.source_rows) -RecordsChanged $entryGoChanged -Summary "entry_go_times active=$($entryGoTimes.verify_airtable.active_rows) source=$($entryGoTimes.source_rows) focus=$($heartbeat.focus_day)" -Payload $entryGoTimes
    Resolve-WecAirtableAlert -AlertType "entry_go_times_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|entry_go_times" -Message "Resolved: entry_go_times completed." -Payload $entryGoTimes
  } catch {
    Write-WecAirtableLog -LogType "alerts" -CheckName "entry_go_times" -Status "error" -Summary "entry_go_times failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "entry_go_times_failed" -Severity "critical" -Message "entry_go_times failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|entry_go_times" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    throw
  }
  try {
    $classAlerts = Invoke-ClassLaneAction -Action "sync-class-alerts" -FocusDayValue $heartbeat.focus_day -TimeoutSec 120
    Write-WecAirtableLog -LogType "alerts" -CheckName "class_alerts" -RecordsSeen ((Int-OrZero $classAlerts.class_start_times) + (Int-OrZero $classAlerts.entry_go_times)) -RecordsChanged ((Int-OrZero $classAlerts.alerts_upserted) + (Int-OrZero $classAlerts.alerts_resolved)) -Summary "class_alerts class_start_times=$($classAlerts.class_start_times) entry_go_times=$($classAlerts.entry_go_times) class_alerts=$($classAlerts.class_alerts) entry_alerts=$($classAlerts.entry_alerts) alerts=$($classAlerts.alerts_upserted) resolved=$($classAlerts.alerts_resolved) focus=$($heartbeat.focus_day)" -Payload $classAlerts
    Resolve-WecAirtableAlert -AlertType "class_alerts_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|class_alerts" -Message "Resolved: class_alerts completed." -Payload $classAlerts
  } catch {
    Write-WecAirtableLog -LogType "alerts" -CheckName "class_alerts" -Status "error" -Summary "class_alerts failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "class_alerts_failed" -Severity "critical" -Message "class_alerts failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|class_alerts" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    throw
  }
  try {
    Invoke-WecLaneAudit $heartbeat.focus_day
    Resolve-WecAirtableAlert -AlertType "wec_lane_audit_failed" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|wec_lane_audit" -Message "Resolved: WEC lane audit passed." -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
    }
  } catch {
    Write-WecAirtableLog -LogType "heartbeat" -CheckName "wec_lane_audit" -Status "error" -Summary "lane audit failed show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    Write-WecAirtableAlert -AlertType "wec_lane_audit_failed" -Severity "critical" -Message "WEC lane audit failed for show=$ShowNo focus=$($heartbeat.focus_day): $($_.Exception.Message)" -DedupeKey "$ShowNo|$($heartbeat.focus_day)|wec_lane_audit" -Payload @{
      show_no = $ShowNo
      focus_day = $heartbeat.focus_day
      error = $_.Exception.Message
    }
    throw
  }
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
