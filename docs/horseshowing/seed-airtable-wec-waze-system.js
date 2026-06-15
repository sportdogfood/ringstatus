const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;

const SEED = {
  show_no: 14906,
  focus_day: "2026-06-14",
  ring_no: 670,
  class_no: 29100,
  entry_no: 1856,
  session_id: "seed_session_wec_waze_14906_20260614",
  device_id: "seed_device_wec_waze",
  user_name: "Seed Waze User",
  checkin_id: "seed_checkin_wec_waze_14906_20260614",
  comment_id: "seed_comment_wec_waze_14906_20260614",
  observation_key: "seed_observation_wec_waze_14906_20260614",
  preset_key: "seed_preset_wec_waze_ring_14906_20260614",
  question_key: "seed_question_wec_waze_entry_14906_20260614"
};

const SYSTEM_TABLES = [
  "waze_users",
  "wec_sessions",
  "wec_ring_checkins",
  "wec_comments",
  "wec_ring_comments",
  "wec_class_comments",
  "wec_entry_comments",
  "wec_observations",
  "wec_comment_presets",
  "wec_question_templates"
];

function requireToken() {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
}

async function airtableFetch(url, options = {}) {
  requireToken();
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Airtable failed ${response.status}: ${text}`);
  return text ? JSON.parse(text) : {};
}

async function getMeta() {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`);
}

function tableByName(meta, name) {
  const table = (meta.tables || []).find((item) => item.name === name);
  if (!table) throw new Error(`Missing Airtable table: ${name}`);
  return table;
}

function fieldNames(table) {
  return new Set((table.fields || []).map((field) => field.name));
}

async function listRecords(tableId, params = {}) {
  const records = [];
  let offset = "";
  do {
    const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${tableId}`);
    url.searchParams.set("pageSize", "100");
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, value);
    }
    if (offset) url.searchParams.set("offset", offset);
    const data = await airtableFetch(url.toString());
    records.push(...(data.records || []));
    offset = data.offset || "";
  } while (offset);
  return records;
}

async function createRecord(tableId, fields) {
  return airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}`, {
    method: "POST",
    body: JSON.stringify({ fields })
  });
}

async function updateRecord(tableId, recordId, fields) {
  return airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${tableId}/${recordId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
}

async function firstByFormula(tableId, formula) {
  const records = await listRecords(tableId, { filterByFormula: formula, maxRecords: "1" });
  return records[0] || null;
}

function keepAllowed(table, fields) {
  const allowed = fieldNames(table);
  return Object.fromEntries(Object.entries(fields).filter(([key]) => allowed.has(key)));
}

async function upsert(table, keyField, keyValue, fields) {
  const formula = `{${keyField}} = "${String(keyValue).replace(/"/g, '\\"')}"`;
  const existing = await firstByFormula(table.id, formula);
  const allowedFields = keepAllowed(table, fields);
  return existing
    ? { action: "updated", record: await updateRecord(table.id, existing.id, allowedFields) }
    : { action: "created", record: await createRecord(table.id, allowedFields) };
}

async function linkRecord(table, keyField, value) {
  const formula = `VALUE({${keyField}} & "") = ${Number(value)}`;
  const record = await firstByFormula(table.id, formula);
  if (!record) throw new Error(`Missing ${table.name} record where ${keyField}=${value}`);
  return record.id;
}

async function linkFocusShow(table) {
  const formula = `AND(VALUE({show_no} & "") = ${SEED.show_no}, IS_SAME({focus_day}, DATETIME_PARSE("${SEED.focus_day}"), "day"))`;
  const record = await firstByFormula(table.id, formula);
  if (!record) throw new Error(`Missing focus_show record for ${SEED.show_no} ${SEED.focus_day}`);
  return record.id;
}

function linked(ids) {
  return ids.filter(Boolean);
}

async function main() {
  const meta = await getMeta();
  const tables = Object.fromEntries([
    ...SYSTEM_TABLES,
    "shows",
    "focus_show",
    "rings",
    "classes",
    "entries"
  ].map((name) => [name, tableByName(meta, name)]));

  const links = {
    shows: [await linkRecord(tables.shows, "show_no", SEED.show_no)],
    focus_show: [await linkFocusShow(tables.focus_show)],
    rings: [await linkRecord(tables.rings, "ring_no", SEED.ring_no)],
    classes: [await linkRecord(tables.classes, "class_no", SEED.class_no)],
    entries: [await linkRecord(tables.entries, "entry_no", SEED.entry_no)]
  };

  const now = new Date().toISOString();
  const results = [];

  const user = await upsert(tables.waze_users, "waze_name", SEED.user_name, {
    waze_name: SEED.user_name,
    ...links
  });
  const wazeUsers = [user.record.id];
  results.push({ table: "waze_users", action: user.action, id: user.record.id });

  const session = await upsert(tables.wec_sessions, "session_id", SEED.session_id, {
    session_id: SEED.session_id,
    device_id: SEED.device_id,
    user_name: SEED.user_name,
    show_no: SEED.show_no,
    focus_day: SEED.focus_day,
    started_at: now,
    last_seen_at: now,
    status: "active",
    page: "seed",
    source: "seed-airtable-wec-waze-system",
    ...links,
    waze_users: wazeUsers
  });
  results.push({ table: "wec_sessions", action: session.action, id: session.record.id });

  const checkin = await upsert(tables.wec_ring_checkins, "checkin_id", SEED.checkin_id, {
    checkin_id: SEED.checkin_id,
    session_id: SEED.session_id,
    user_name: SEED.user_name,
    show_no: SEED.show_no,
    focus_day: SEED.focus_day,
    ring_no: SEED.ring_no,
    checked_in_at: now,
    last_seen_at: now,
    status: "active",
    source_confidence: "first_hand",
    source: "seed-airtable-wec-waze-system",
    ...links,
    waze_users: wazeUsers
  });
  results.push({ table: "wec_ring_checkins", action: checkin.action, id: checkin.record.id });

  const sharedComment = {
    comment_id: SEED.comment_id,
    session_id: SEED.session_id,
    device_id: SEED.device_id,
    user_name: SEED.user_name,
    show_no: SEED.show_no,
    focus_day: SEED.focus_day,
    ring_no: SEED.ring_no,
    class_no: SEED.class_no,
    entry_no: SEED.entry_no,
    comment_text: "Seed comment verifies WEC Waze table links.",
    created_at: now,
    status: "open",
    source: "seed-airtable-wec-waze-system",
    ring_checkin_id: SEED.checkin_id,
    source_confidence: "first_hand",
    ...links,
    waze_users: wazeUsers
  };

  for (const [tableName, scope] of [
    ["wec_comments", "entry"],
    ["wec_ring_comments", "ring"],
    ["wec_class_comments", "class"],
    ["wec_entry_comments", "entry"]
  ]) {
    const comment = await upsert(tables[tableName], "comment_id", `${SEED.comment_id}_${scope}`, {
      ...sharedComment,
      comment_id: `${SEED.comment_id}_${scope}`,
      comment_scope: scope
    });
    results.push({ table: tableName, action: comment.action, id: comment.record.id });
  }

  const observation = await upsert(tables.wec_observations, "observation_key", SEED.observation_key, {
    observation_key: SEED.observation_key,
    session_id: SEED.session_id,
    user_name: SEED.user_name,
    show_no: SEED.show_no,
    focus_day: SEED.focus_day,
    scope: "entry",
    ring_no: SEED.ring_no,
    class_no: SEED.class_no,
    entry_no: SEED.entry_no,
    entry_order: 1,
    prompt_key: SEED.question_key,
    prompt_label: "Seed entry prompt",
    answer: "yes",
    observed_at: now,
    source: "seed-airtable-wec-waze-system",
    ring_checkin_id: SEED.checkin_id,
    source_confidence: "first_hand",
    ...links,
    waze_users: wazeUsers
  });
  results.push({ table: "wec_observations", action: observation.action, id: observation.record.id });

  const preset = await upsert(tables.wec_comment_presets, "preset_key", SEED.preset_key, {
    preset_key: SEED.preset_key,
    scope: "ring",
    label: "Ring running on time",
    comment_text: "Ring appears to be running on time.",
    show_no: SEED.show_no,
    focus_day: SEED.focus_day,
    ring_no: SEED.ring_no,
    class_no: SEED.class_no,
    entry_no: SEED.entry_no,
    status: "active",
    sort_order: 10,
    source: "seed-airtable-wec-waze-system",
    notes: "Seed record proving preset links resolve.",
    ...links,
    waze_users: wazeUsers
  });
  results.push({ table: "wec_comment_presets", action: preset.action, id: preset.record.id });

  const question = await upsert(tables.wec_question_templates, "question_key", SEED.question_key, {
    question_key: SEED.question_key,
    scope: "entry",
    prompt_label: "Is this entry in the ring?",
    prompt_text: "Is the selected entry currently in the ring?",
    answer_type: "yes_no_unsure",
    choices: "yes\nno\nunsure",
    show_no: SEED.show_no,
    focus_day: SEED.focus_day,
    ring_no: SEED.ring_no,
    class_no: SEED.class_no,
    entry_no: SEED.entry_no,
    status: "active",
    sort_order: 10,
    trigger_context: "entry_open",
    source: "seed-airtable-wec-waze-system",
    notes: "Seed record proving question template links resolve.",
    ...links,
    waze_users: wazeUsers
  });
  results.push({ table: "wec_question_templates", action: question.action, id: question.record.id });

  console.log(JSON.stringify({ base: BASE_ID, seed: SEED, links, results }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
