const { spawnSync } = require("child_process");
const {
  normalizeHeartbeatMode,
  resolveHeartbeatCadenceSeconds,
} = require("./lib/heartbeat_mode");

const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";
const HEARTBEAT_CREATED_FIELD = process.env.HEARTBEAT_CREATED_FIELD || "created_time";
const HEARTBEAT_MODE_FIELD = process.env.HEARTBEAT_MODE_FIELD || process.env.FIELD_MODE || "mode";
const HEARTBEAT_CADENCE_FIELD = process.env.HEARTBEAT_CADENCE_FIELD || process.env.FIELD_CADENCE || "cadence";
const HEARTBEAT_SET_INTERVALS_FIELD = process.env.HEARTBEAT_SET_INTERVALS_FIELD || process.env.FIELD_SET_INTERVALS || "set_intervals";
const HEARTBEAT_INTERVAL_FIELD = process.env.HEARTBEAT_INTERVAL_FIELD || process.env.FIELD_INTERVAL || "interval";

const TASK_NAME = process.env.HEARTBEAT_TASK_NAME || "ringstatus-heartbeat";
const MIN_SECONDS = Math.max(60, Number(process.env.HEARTBEAT_TASK_MIN_SECONDS || "60") || 60);
const MAX_SECONDS = Math.max(MIN_SECONDS, Number(process.env.HEARTBEAT_TASK_MAX_SECONDS || "86400") || 86400);
const SYNC_ENABLED = String(process.env.HEARTBEAT_TASK_CADENCE_SYNC || "1") !== "0";
const DRY_RUN = String(process.env.DRY_RUN || "0") === "1";
const DISABLE_ON_OFF = String(process.env.HEARTBEAT_TASK_DISABLE_ON_OFF || "0") === "1";

if (!AIRTABLE_TOKEN) throw new Error("Missing AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing AIRTABLE_BASE_ID");

function clampSeconds(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value)) return MIN_SECONDS;
  return Math.max(MIN_SECONDS, Math.min(MAX_SECONDS, Math.floor(value)));
}

function airtableUrl(tableName, params = {}) {
  const url = new URL(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`);
  for (const [key, value] of Object.entries(params)) {
    if (Array.isArray(value)) {
      for (const item of value) url.searchParams.append(key, item);
    } else if (value !== undefined && value !== null) {
      url.searchParams.set(key, String(value));
    }
  }
  return url;
}

async function latestHeartbeat() {
  const url = airtableUrl(TABLE_HEARTBEAT, {
    maxRecords: 1,
    "sort[0][field]": HEARTBEAT_CREATED_FIELD,
    "sort[0][direction]": "desc",
    "fields[]": [
      HEARTBEAT_CREATED_FIELD,
      HEARTBEAT_MODE_FIELD,
      HEARTBEAT_CADENCE_FIELD,
      HEARTBEAT_SET_INTERVALS_FIELD,
      HEARTBEAT_INTERVAL_FIELD,
    ],
  });

  const response = await fetch(url, {
    headers: { Authorization: `Bearer ${AIRTABLE_TOKEN}` },
  });
  const body = await response.text();
  if (!response.ok) {
    throw new Error(`Airtable latest heartbeat failed (${response.status}): ${body.slice(0, 500)}`);
  }

  const json = JSON.parse(body);
  return json.records?.[0] || null;
}

function powershellLiteral(value) {
  return String(value ?? "").replace(/'/g, "''");
}

function syncTaskInterval({ taskName, cadenceSeconds, mode }) {
  const taskNameLiteral = powershellLiteral(taskName);
  const seconds = clampSeconds(cadenceSeconds);
  const disable = mode === "OFF" && DISABLE_ON_OFF;
  const command = `
$ErrorActionPreference = 'Stop'
$taskName = '${taskNameLiteral}'
$seconds = ${seconds}
if (${disable ? "$true" : "$false"}) {
  Disable-ScheduledTask -TaskName $taskName | Out-Null
  [pscustomobject]@{ ok = $true; event = 'heartbeat_task_disabled'; task_name = $taskName; cadence_seconds = $seconds } | ConvertTo-Json -Compress
  exit 0
}
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Seconds $seconds) -RepetitionDuration (New-TimeSpan -Days 3650)
Set-ScheduledTask -TaskName $taskName -Trigger $trigger | Out-Null
Enable-ScheduledTask -TaskName $taskName | Out-Null
$task = Get-ScheduledTask -TaskName $taskName
[pscustomobject]@{
  ok = $true
  event = 'heartbeat_task_cadence_synced'
  task_name = $taskName
  cadence_seconds = $seconds
  repetition_interval = $task.Triggers[0].Repetition.Interval
} | ConvertTo-Json -Compress
`;

  return spawnSync("powershell.exe", [
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-Command",
    command,
  ], {
    encoding: "utf8",
    windowsHide: true,
  });
}

(async () => {
  const heartbeat = await latestHeartbeat();
  const fields = heartbeat?.fields || {};
  const mode = normalizeHeartbeatMode(fields[HEARTBEAT_MODE_FIELD]);
  const cadenceSeconds = clampSeconds(resolveHeartbeatCadenceSeconds({
    mode,
    cadence: fields[HEARTBEAT_CADENCE_FIELD],
    set_intervals: fields[HEARTBEAT_SET_INTERVALS_FIELD],
    interval: fields[HEARTBEAT_INTERVAL_FIELD],
  }));

  const basePayload = {
    ok: true,
    event: "heartbeat_task_cadence_resolved",
    heartbeat_id: heartbeat?.id || null,
    mode,
    cadence_seconds: cadenceSeconds,
    task_name: TASK_NAME,
    sync_enabled: SYNC_ENABLED,
    dry_run: DRY_RUN,
  };

  if (!SYNC_ENABLED || DRY_RUN) {
    console.log(JSON.stringify(basePayload));
    return;
  }

  const result = syncTaskInterval({ taskName: TASK_NAME, cadenceSeconds, mode });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);

  if (result.status !== 0) {
    throw new Error(`Task cadence sync failed (${result.status}): ${String(result.stderr || result.stdout || "").slice(0, 500)}`);
  }

  console.log(JSON.stringify({
    ...basePayload,
    event: "heartbeat_task_cadence_sync_completed",
  }));
})().catch((error) => {
  console.error(JSON.stringify({
    ok: false,
    event: "heartbeat_task_cadence_failed",
    error: String(error?.message || error).slice(0, 500),
  }));
  process.exit(1);
});
