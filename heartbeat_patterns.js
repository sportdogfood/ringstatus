// heartbeat_patterns.js
// Reassign linked child records from previous heartbeat -> newest heartbeat
// Child tables handled:
// - scheduler
// - active_tenants
// - publish_queue

const AIRTABLE_TOKEN   = process.env.AIRTABLE_TOKEN || "";
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || "";

if (!AIRTABLE_TOKEN) throw new Error("Missing AIRTABLE_TOKEN");
if (!AIRTABLE_BASE_ID) throw new Error("Missing AIRTABLE_BASE_ID");

const TABLE_HEARTBEAT      = process.env.TABLE_HEARTBEAT || "heartbeat";
const TABLE_SCHEDULER      = process.env.TABLE_SCHEDULER || "scheduler";
const TABLE_ACTIVE_TENANTS = process.env.TABLE_ACTIVE_TENANTS || "active_tenants";
const TABLE_PUBLISH_QUEUE  = process.env.TABLE_PUBLISH_QUEUE || "publish_queue";

const VIEW_EPOCH_SCHEDULER      = process.env.VIEW_EPOCH_SCHEDULER || "epoch";
const VIEW_EPOCH_ACTIVE_TENANTS = process.env.VIEW_EPOCH_ACTIVE_TENANTS || "epoch";
const VIEW_EPOCH_PUBLISH_QUEUE  = process.env.VIEW_EPOCH_PUBLISH_QUEUE || "epoch";

const HEARTBEAT_CREATED_FIELD            = process.env.HEARTBEAT_CREATED_FIELD || "created_time";
const HEARTBEAT_SCHEDULER_LINK_FIELD     = process.env.HEARTBEAT_SCHEDULER_LINK_FIELD || "scheduler";
const HEARTBEAT_ACTIVE_TENANTS_LINK_FIELD= process.env.HEARTBEAT_ACTIVE_TENANTS_LINK_FIELD || "active_tenants";
const HEARTBEAT_PUBLISH_QUEUE_LINK_FIELD = process.env.HEARTBEAT_PUBLISH_QUEUE_LINK_FIELD || "publish_queue";

const HEARTBEAT_CLEARED_FIELD    = process.env.HEARTBEAT_CLEARED_FIELD || "cleared";
const HEARTBEAT_SCHEDULERS_FIELD = process.env.HEARTBEAT_SCHEDULERS_FIELD || "schedulers";
const HEARTBEAT_PUBLISHERS_FIELD = process.env.HEARTBEAT_PUBLISHERS_FIELD || "publishers";
const HEARTBEAT_SCHEDULES_FIELD  = process.env.HEARTBEAT_SCHEDULES_FIELD || "schedules";
const HEARTBEAT_TRIPS_FIELD      = process.env.HEARTBEAT_TRIPS_FIELD || "trips";
const HEARTBEAT_TENANTS_FIELD    = process.env.HEARTBEAT_TENANTS_FIELD || "tenants";
const HEARTBEAT_ALERTS_FIELD     = process.env.HEARTBEAT_ALERTS_FIELD || "alerts";
const HEARTBEAT_ISA_FIELD        = process.env.HEARTBEAT_ISA_FIELD || "isA";
const HEARTBEAT_ISB_FIELD        = process.env.HEARTBEAT_ISB_FIELD || "isB";

const CHILD_HEARTBEAT_LINK_FIELD = process.env.CHILD_HEARTBEAT_LINK_FIELD || "heartbeat";

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

async function listAll({ table, view, fields }) {
  const out = [];
  let offset = null;

  while (true) {
    const url = new URL(airtableUrl(table));
    if (view) url.searchParams.set("view", view);
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
      throw new Error(`Airtable list failed (${table}/${view || "-"}) ${res.status} ${txt.slice(0, 300)}`);
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

async function batchUpdate(table, updates) {
  if (!updates.length) return;
  for (let i = 0; i < updates.length; i += 10) {
    const chunk = updates.slice(i, i + 10);
    const res = await fetchWithTimeout(airtableUrl(table), {
      method: "PATCH",
      headers: headers(),
      body: JSON.stringify({ records: chunk }),
    });

    const txt = await res.text();
    if (!res.ok) {
      throw new Error(`Airtable batch update failed (${table}) ${res.status} ${txt.slice(0, 300)}`);
    }
  }
}

function toMs(v) {
  const d = new Date(v);
  const ms = d.getTime();
  return Number.isFinite(ms) ? ms : null;
}

function idsFromLinkArray(v) {
  if (!Array.isArray(v)) return [];
  return v
    .map(x => typeof x === "string" ? x : x?.id)
    .filter(Boolean);
}

function hasAnyLinkedChildren(fields) {
  return (
    idsFromLinkArray(fields[HEARTBEAT_SCHEDULER_LINK_FIELD]).length > 0 ||
    idsFromLinkArray(fields[HEARTBEAT_ACTIVE_TENANTS_LINK_FIELD]).length > 0 ||
    idsFromLinkArray(fields[HEARTBEAT_PUBLISH_QUEUE_LINK_FIELD]).length > 0
  );
}

function childUpdatesForSourceLinks({ source, targetId, childRecords, sourceLinkField }) {
  const epochIds = new Set(childRecords.map(r => r.id));
  const sourceLinkedIds = idsFromLinkArray(source.fields[sourceLinkField]).filter(id => epochIds.has(id));

  const updates = [];

  for (const childId of sourceLinkedIds) {
    const rec = childRecords.find(r => r.id === childId);
    if (!rec) continue;

    const current = idsFromLinkArray(rec.fields[CHILD_HEARTBEAT_LINK_FIELD]);
    const alreadyCorrect = current.length === 1 && current[0] === targetId;
    if (alreadyCorrect) continue;

    updates.push({
      id: childId,
      fields: {
        [CHILD_HEARTBEAT_LINK_FIELD]: [targetId]
      }
    });
  }

  return {
    found: sourceLinkedIds.length,
    updates
  };
}

(async () => {
  const heartbeatFields = [
    HEARTBEAT_CREATED_FIELD,
    HEARTBEAT_SCHEDULER_LINK_FIELD,
    HEARTBEAT_ACTIVE_TENANTS_LINK_FIELD,
    HEARTBEAT_PUBLISH_QUEUE_LINK_FIELD,
    HEARTBEAT_CLEARED_FIELD,
    HEARTBEAT_ISA_FIELD,
    HEARTBEAT_ISB_FIELD
  ];

  const [hbRecords, schedulerEpoch, activeTenantsEpoch, publishQueueEpoch] = await Promise.all([
    listAll({ table: TABLE_HEARTBEAT, view: null, fields: heartbeatFields }),
    listAll({ table: TABLE_SCHEDULER, view: VIEW_EPOCH_SCHEDULER, fields: [CHILD_HEARTBEAT_LINK_FIELD] }),
    listAll({ table: TABLE_ACTIVE_TENANTS, view: VIEW_EPOCH_ACTIVE_TENANTS, fields: [CHILD_HEARTBEAT_LINK_FIELD] }),
    listAll({ table: TABLE_PUBLISH_QUEUE, view: VIEW_EPOCH_PUBLISH_QUEUE, fields: [CHILD_HEARTBEAT_LINK_FIELD] }),
  ]);

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

  // most recent older heartbeat with any child links = source
  let source = null;
  let sourceMs = null;

  for (const rec of hbRecords) {
    if (rec.id === target.id) continue;

    const ms = toMs(rec.fields?.[HEARTBEAT_CREATED_FIELD]);
    if (ms === null) continue;
    if (ms >= targetMs) continue;
    if (!hasAnyLinkedChildren(rec.fields || {})) continue;

    if (sourceMs === null || ms > sourceMs) {
      source = rec;
      sourceMs = ms;
    }
  }

  if (!source) {
    console.log(JSON.stringify({
      ok: false,
      reason: "No previous heartbeat record with linked child records found.",
      target_heartbeat_id: target.id
    }, null, 2));
    process.exit(0);
  }

  const schedulerMove = childUpdatesForSourceLinks({
    source,
    targetId: target.id,
    childRecords: schedulerEpoch,
    sourceLinkField: HEARTBEAT_SCHEDULER_LINK_FIELD
  });

  const activeTenantsMove = childUpdatesForSourceLinks({
    source,
    targetId: target.id,
    childRecords: activeTenantsEpoch,
    sourceLinkField: HEARTBEAT_ACTIVE_TENANTS_LINK_FIELD
  });

  const publishQueueMove = childUpdatesForSourceLinks({
    source,
    targetId: target.id,
    childRecords: publishQueueEpoch,
    sourceLinkField: HEARTBEAT_PUBLISH_QUEUE_LINK_FIELD
  });

  if (schedulerMove.updates.length) {
    await batchUpdate(TABLE_SCHEDULER, schedulerMove.updates);
  }

  if (activeTenantsMove.updates.length) {
    await batchUpdate(TABLE_ACTIVE_TENANTS, activeTenantsMove.updates);
  }

  if (publishQueueMove.updates.length) {
    await batchUpdate(TABLE_PUBLISH_QUEUE, publishQueueMove.updates);
  }

  // preserve original cleared write as separate update
  await updateRecord(TABLE_HEARTBEAT, source.id, {
    [HEARTBEAT_CLEARED_FIELD]: true
  });

  // source task checkboxes off
  await updateRecord(TABLE_HEARTBEAT, source.id, {
    [HEARTBEAT_SCHEDULERS_FIELD]: false,
    [HEARTBEAT_PUBLISHERS_FIELD]: false,
    [HEARTBEAT_SCHEDULES_FIELD]: false,
    [HEARTBEAT_TRIPS_FIELD]: false,
    [HEARTBEAT_TENANTS_FIELD]: false,
    [HEARTBEAT_ALERTS_FIELD]: false
  });

  const sourceIsA = !!source.fields?.[HEARTBEAT_ISA_FIELD];
  const sourceIsB = !!source.fields?.[HEARTBEAT_ISB_FIELD];

  const targetHeartbeatFields = {
    [HEARTBEAT_SCHEDULERS_FIELD]: true,
    [HEARTBEAT_PUBLISHERS_FIELD]: true,
    [HEARTBEAT_SCHEDULES_FIELD]: true,
    [HEARTBEAT_TRIPS_FIELD]: true,
    [HEARTBEAT_TENANTS_FIELD]: true,
    [HEARTBEAT_ALERTS_FIELD]: true,
    [HEARTBEAT_ISA_FIELD]: true,
    [HEARTBEAT_ISB_FIELD]: false
  };

  if (sourceIsA && !sourceIsB) {
    targetHeartbeatFields[HEARTBEAT_ISA_FIELD] = false;
    targetHeartbeatFields[HEARTBEAT_ISB_FIELD] = true;
  } else if (sourceIsB && !sourceIsA) {
    targetHeartbeatFields[HEARTBEAT_ISA_FIELD] = true;
    targetHeartbeatFields[HEARTBEAT_ISB_FIELD] = false;
  }

  await updateRecord(TABLE_HEARTBEAT, target.id, targetHeartbeatFields);

  console.log(JSON.stringify({
    ok: true,
    target_heartbeat_id: target.id,
    source_heartbeat_id: source.id,
    scheduler_records_found_from_source: schedulerMove.found,
    scheduler_records_moved: schedulerMove.updates.length,
    active_tenants_records_found_from_source: activeTenantsMove.found,
    active_tenants_records_moved: activeTenantsMove.updates.length,
    publish_queue_records_found_from_source: publishQueueMove.found,
    publish_queue_records_moved: publishQueueMove.updates.length,
    source_cleared: true
  }, null, 2));
})().catch(err => {
  console.error(String(err?.message || err));
  process.exit(1);
});
