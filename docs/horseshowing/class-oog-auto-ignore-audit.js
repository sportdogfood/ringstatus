const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const TABLE_NAME = "class_oog";
const VIEW_NAME = "CWF_TODAY";

const AUTO_FIELDS = {
  candidate: "auto_ignore_candidate",
  reason: "auto_ignore_reason",
  groupKey: "auto_ignore_group_key",
  rank: "auto_ignore_rank",
  matchesManual: "auto_ignore_matches_manual"
};

function argFlag(name) {
  return process.argv.includes(name);
}

function clean(value) {
  return String(value ?? "").trim();
}

function first(value) {
  return Array.isArray(value) ? clean(value[0]) : clean(value);
}

function bool(value) {
  return value === true;
}

async function airtableFetch(url, options = {}) {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Airtable ${response.status}: ${text}`);
  return text ? JSON.parse(text) : {};
}

async function getTableMeta() {
  const meta = await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`);
  const table = meta.tables.find((item) => item.name === TABLE_NAME);
  if (!table) throw new Error(`Missing Airtable table ${TABLE_NAME}`);
  return table;
}

async function ensureField(table, fieldName, type) {
  const existing = table.fields.find((field) => field.name === fieldName);
  if (existing) return false;
  const body = { name: fieldName, type };
  if (type === "number") body.options = { precision: 0 };
  if (type === "checkbox") body.options = { icon: "check", color: "greenBright" };
  await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${table.id}/fields`, {
    method: "POST",
    body: JSON.stringify(body)
  });
  return true;
}

async function ensureAutoFields() {
  const table = await getTableMeta();
  const created = [];
  const specs = [
    [AUTO_FIELDS.candidate, "checkbox"],
    [AUTO_FIELDS.reason, "singleLineText"],
    [AUTO_FIELDS.groupKey, "singleLineText"],
    [AUTO_FIELDS.rank, "number"],
    [AUTO_FIELDS.matchesManual, "checkbox"]
  ];
  for (const [name, type] of specs) {
    if (await ensureField(table, name, type)) created.push(name);
  }
  return created;
}

async function listViewRecords() {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", view: VIEW_NAME });
    if (offset) params.set("offset", offset);
    const result = await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE_NAME)}?${params.toString()}`);
    records.push(...(result.records || []));
    offset = result.offset || "";
  } while (offset);
  return records;
}

function rowFromRecord(record) {
  const fields = record.fields || {};
  const classStartTime = first(fields["class_start_time (from class_start_times)"]);
  return {
    id: record.id,
    fields,
    show_no: clean(fields.show_no),
    focus_day: clean(fields.focus_day).slice(0, 10),
    ring_no: clean(fields.ring_no),
    entry_no: clean(fields.entry_no),
    class_no: clean(fields.class_no),
    class_order: Number(fields.class_order || 0),
    class_start_time: classStartTime,
    left_15: clean(fields.left_15),
    manual_ignore: bool(fields.ignore),
    class_name: clean(fields.class_name),
    horse: clean(fields.horse)
  };
}

function groupKey(row) {
  return [
    row.show_no,
    row.focus_day,
    row.ring_no,
    row.entry_no,
    row.class_start_time
  ].join("|");
}

function classifyRows(rows) {
  const byGroup = new Map();
  for (const row of rows) {
    if (!row.class_start_time) continue;
    const key = groupKey(row);
    if (!byGroup.has(key)) byGroup.set(key, []);
    byGroup.get(key).push(row);
  }

  const resultById = new Map(rows.map((row) => [row.id, {
    row,
    candidate: false,
    reason: "",
    group_key: row.class_start_time ? groupKey(row) : "",
    rank: null
  }]));

  for (const [key, groupRows] of byGroup.entries()) {
    if (groupRows.length < 2) continue;
    const ordered = [...groupRows].sort((a, b) => {
      if (a.class_order !== b.class_order) return a.class_order - b.class_order;
      return Number(a.class_no || 0) - Number(b.class_no || 0);
    });
    ordered.forEach((row, index) => {
      const rank = index + 1;
      const result = resultById.get(row.id);
      result.group_key = key;
      result.rank = rank;
      if (index > 0) {
        result.candidate = true;
        result.reason = `same ring+entry+start; keep class_order ${ordered[0].class_order}, ignore later class_order ${row.class_order}; left_15=${row.left_15}`;
      }
    });
  }

  return [...resultById.values()];
}

async function updateRecords(results) {
  let changed = 0;
  const updates = results.map((result) => ({
    id: result.row.id,
    fields: {
      [AUTO_FIELDS.candidate]: result.candidate,
      [AUTO_FIELDS.reason]: result.reason,
      [AUTO_FIELDS.groupKey]: result.group_key,
      [AUTO_FIELDS.rank]: result.rank,
      [AUTO_FIELDS.matchesManual]: result.candidate === result.row.manual_ignore
    }
  }));

  for (let index = 0; index < updates.length; index += 10) {
    const batch = updates.slice(index, index + 10);
    await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(TABLE_NAME)}`, {
      method: "PATCH",
      body: JSON.stringify({ records: batch, typecast: true })
    });
    changed += batch.length;
  }
  return changed;
}

async function main() {
  const write = argFlag("--write");
  const created = write ? await ensureAutoFields() : [];
  const records = await listViewRecords();
  const rows = records.map(rowFromRecord);
  const results = classifyRows(rows);
  const candidates = results.filter((result) => result.candidate);
  const manual = results.filter((result) => result.row.manual_ignore);
  const falsePositive = results.filter((result) => result.candidate && !result.row.manual_ignore);
  const falseNegative = results.filter((result) => !result.candidate && result.row.manual_ignore);
  const changed = write ? await updateRecords(results) : 0;

  console.log(JSON.stringify({
    ok: true,
    table: TABLE_NAME,
    view: VIEW_NAME,
    rows: rows.length,
    candidates: candidates.length,
    manual_ignore: manual.length,
    false_positive: falsePositive.length,
    false_negative: falseNegative.length,
    fields_created: created,
    records_written: changed,
    candidate_rows: candidates.map((result) => ({
      ring_no: result.row.ring_no,
      entry_no: result.row.entry_no,
      class_start_time: result.row.class_start_time,
      class_order: result.row.class_order,
      class_no: result.row.class_no,
      left_15: result.row.left_15,
      horse: result.row.horse,
      class_name: result.row.class_name,
      reason: result.reason
    }))
  }, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exit(1);
  });
}

module.exports = {
  classifyRows,
  groupKey,
  rowFromRecord
};
