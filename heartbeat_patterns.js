// heartbeat_patterns.js
// heartbeat table only:
// - newest heartbeat = target
// - previous older heartbeat = source
// - source task boxes -> false
// - target task boxes -> true
// - toggle isA / isB on target based on source

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

if (!AIRTABLE_TOKEN) throw new Error("Missing AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing AIRTABLE_BASE_ID");

const TABLE_HEARTBEAT = process.env.TABLE_HEARTBEAT || "heartbeat";

const HEARTBEAT_CREATED_FIELD    = process.env.HEARTBEAT_CREATED_FIELD || "created_time";
const HEARTBEAT_SCHEDULERS_FIELD = process.env.HEARTBEAT_SCHEDULERS_FIELD || "schedulers";
const HEARTBEAT_PUBLISHERS_FIELD = process.env.HEARTBEAT_PUBLISHERS_FIELD || "publishers";
const HEARTBEAT_SCHEDULES_FIELD  = process.env.HEARTBEAT_SCHEDULES_FIELD || "schedules";
const HEARTBEAT_TRIPS_FIELD      = process.env.HEARTBEAT_TRIPS_FIELD || "trips";
const HEARTBEAT_TENANTS_FIELD    = process.env.HEARTBEAT_TENANTS_FIELD || "tenants";
const HEARTBEAT_ALERTS_FIELD     = process.env.HEARTBEAT_ALERTS_FIELD || "alerts";
const HEARTBEAT_ISA_FIELD        = process.env.HEARTBEAT_ISA_FIELD || "isA";
const HEARTBEAT_ISB_FIELD        = process.env.HEARTBEAT_ISB_FIELD || "isB";

const HTTP_TIMEOUT_MS = Number(process.env.HTTP_TIMEOUT_MS || "20000");

function airtableUrl(tableName) {
  return `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${encodeURIComponent(tableName)}`;
}

function headers() {
  return {
    Authorization: `Bearer ${AIRTABLE_TOKEN}`,
    "Content-Type": "application/json",
  };
}

async function fetchWithTimeout(url, opts = {}, timeoutMs = HTTP_TIMEOUT_MS) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    return await fetch(url, { ...opts, signal: ac.signal });
  } finally {
    clearTimeout(t);
  }
}

async function listAll({ table, fields }) {
  const out = [];
  let offset = null;

  while (true) {
    const url = new URL(airtableUrl(table));
    if (offset) url.searchParams.set("offset", offset);
    for (const f of fields || []) url.searchParams.append("fields[]", f);

    const res = await fetchWithTimeout(url.toString(), {
      method: "GET",
      headers: headers(),
    });

    const txt = await res.text();
    let json = {};
    try { json = JSON.parse(txt); } catch {}

    if (!res.ok) {
      throw new Error(`Airtable list failed (${table}) ${res.status} ${txt.slice(0, 300)}`);
    }

    out.push(...(json.records || []));
    offset = json.offset || null;
    if (!offset) break;
  }

  return out;
}

async function updateRecord(table, recordId, fields) {
  const res = await fetchWithTimeout(`${airtableUrl(table)}/${recordId}`, {
    method: "PATCH",
    headers: headers(),
    body: JSON.stringify({ fields }),
  });

  const txt = await res.text();
  if (!res.ok) {
    throw new Error(`Airtable update failed (${table}/${recordId}) ${res.status} ${txt.slice(0, 300)}`);
  }
}

function toMs(v) {
  const d = new Date(v);
  const ms = d.getTime();
  return Number.isFinite(ms) ? ms : null;
}

(async () => {
  const fieldsToRead = [
    HEARTBEAT_CREATED_FIELD,
    HEARTBEAT_SCHEDULERS_FIELD,
    HEARTBEAT_PUBLISHERS_FIELD,
    HEARTBEAT_SCHEDULES_FIELD,
    HEARTBEAT_TRIPS_FIELD,
    HEARTBEAT_TENANTS_FIELD,
    HEARTBEAT_ALERTS_FIELD,
    HEARTBEAT_ISA_FIELD,
    HEARTBEAT_ISB_FIELD
  ];

  const hbRecords = await listAll({
    table: TABLE_HEARTBEAT,
    fields: fieldsToRead
  });

  if (!hbRecords.length) {
    throw new Error("No heartbeat records found.");
  }

  // newest heartbeat = target
  let target = null;
  let targetMs = null;

  for (const rec of hbRecords) {
    const ms = toMs(rec.fields?.[HEARTBEAT_CREATED_FIELD]);
    if (ms === null) continue;

    if (targetMs === null || ms > targetMs) {
      target = rec;
      targetMs = ms;
    }
  }

  if (!target) {
    throw new Error("No heartbeat target record found.");
  }

  // most recent older heartbeat = source
  let source = null;
  let sourceMs = null;

  for (const rec of hbRecords) {
    if (rec.id === target.id) continue;

    const ms = toMs(rec.fields?.[HEARTBEAT_CREATED_FIELD]);
    if (ms === null) continue;
    if (ms >= targetMs) continue;

    if (sourceMs === null || ms > sourceMs) {
      source = rec;
      sourceMs = ms;
    }
  }

  // source boxes off, if source exists
  if (source) {
    await updateRecord(TABLE_HEARTBEAT, source.id, {
      [HEARTBEAT_SCHEDULERS_FIELD]: false,
      [HEARTBEAT_PUBLISHERS_FIELD]: false,
      [HEARTBEAT_SCHEDULES_FIELD]: false,
      [HEARTBEAT_TRIPS_FIELD]: false,
      [HEARTBEAT_TENANTS_FIELD]: false,
      [HEARTBEAT_ALERTS_FIELD]: false
    });
  }

  // toggle target isA/isB from source, otherwise default A=true B=false
  let targetIsA = true;
  let targetIsB = false;

  if (source) {
    const sourceIsA = !!source.fields?.[HEARTBEAT_ISA_FIELD];
    const sourceIsB = !!source.fields?.[HEARTBEAT_ISB_FIELD];

    if (sourceIsA && !sourceIsB) {
      targetIsA = false;
      targetIsB = true;
    } else if (sourceIsB && !sourceIsA) {
      targetIsA = true;
      targetIsB = false;
    } else {
      targetIsA = true;
      targetIsB = false;
    }
  }

  // target boxes on
  await updateRecord(TABLE_HEARTBEAT, target.id, {
    [HEARTBEAT_SCHEDULERS_FIELD]: true,
    [HEARTBEAT_PUBLISHERS_FIELD]: true,
    [HEARTBEAT_SCHEDULES_FIELD]: true,
    [HEARTBEAT_TRIPS_FIELD]: true,
    [HEARTBEAT_TENANTS_FIELD]: true,
    [HEARTBEAT_ALERTS_FIELD]: true,
    [HEARTBEAT_ISA_FIELD]: targetIsA,
    [HEARTBEAT_ISB_FIELD]: targetIsB
  });

  console.log(JSON.stringify({
    ok: true,
    target_heartbeat_id: target.id,
    source_heartbeat_id: source ? source.id : null,
    target_isA: targetIsA,
    target_isB: targetIsB
  }, null, 2));
})().catch(err => {
  console.error(String(err?.message || err));
  process.exit(1);
});