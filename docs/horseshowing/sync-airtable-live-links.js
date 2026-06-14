const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;

function argValue(name, fallback = "") {
  const index = process.argv.indexOf(name);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function clean(value) {
  return String(value ?? "").trim();
}

function firstLinkedId(value) {
  return Array.isArray(value) && value.length ? value[0] : "";
}

function intOrNull(value) {
  const parsed = Number.parseInt(clean(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function linked(id) {
  return id ? [id] : undefined;
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

async function listAll(tableName, params = {}) {
  const records = [];
  let offset = "";
  do {
    const query = new URLSearchParams({ pageSize: "100", ...params });
    if (offset) query.set("offset", offset);
    const payload = await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}?${query.toString()}`);
    records.push(...(payload.records || []));
    offset = payload.offset || "";
  } while (offset);
  return records;
}

function mapByField(records, fieldName) {
  const map = new Map();
  for (const record of records) {
    const value = clean(record.fields?.[fieldName]);
    if (value) map.set(value, record);
  }
  return map;
}

function getLinkedIds(fields, fieldName) {
  return Array.isArray(fields[fieldName]) ? fields[fieldName] : [];
}

function sameLinkedIds(current, next) {
  if (!next) return true;
  const left = getLinkedIds(current, "").slice().sort().join("|");
  const right = next.slice().sort().join("|");
  return left === right;
}

function setLinkedIfChanged(fields, out, fieldName, next) {
  if (!next) return;
  const current = getLinkedIds(fields, fieldName).slice().sort().join("|");
  const target = next.slice().sort().join("|");
  if (current !== target) out[fieldName] = next;
}

function setValueIfChanged(fields, out, fieldName, next) {
  if (next === undefined || next === null || next === "") return;
  if (clean(fields[fieldName]) !== clean(next)) out[fieldName] = next;
}

async function syncGetRings({ showNo, focusDay }) {
  const formulaParts = [];
  if (showNo) formulaParts.push(`{show_no}='${String(showNo).replace(/'/g, "\\'")}'`);
  if (focusDay) formulaParts.push(`IS_SAME({focus_day},'${String(focusDay).replace(/'/g, "\\'")}','day')`);
  const formula = formulaParts.length > 1 ? `AND(${formulaParts.join(",")})` : formulaParts[0] || "";

  const [
    getRings,
    getRingDays,
    shows,
    classes,
    entries,
    rings
  ] = await Promise.all([
    listAll("get_rings", formula ? { filterByFormula: formula } : {}),
    listAll("get_ring_days"),
    listAll("shows"),
    listAll("classes"),
    listAll("entries"),
    listAll("rings")
  ]);

  const getRingDayByRingDayNo = mapByField(getRingDays, "ring_day_no");
  const showByShowNo = mapByField(shows, "show_no");
  const classByClassNo = mapByField(classes, "class_no");
  const entryByEntryNo = mapByField(entries, "entry_no");
  const ringById = new Map(rings.map((record) => [record.id, record]));

  const updates = [];
  const missing = {
    get_ring_days: 0,
    shows: 0,
    classes: 0,
    entries: 0,
    rings: 0
  };

  for (const record of getRings) {
    const fields = record.fields || {};
    const ringDayNo = clean(fields.ring_day_no);
    const getRingDay = getRingDayByRingDayNo.get(ringDayNo);
    const show = showByShowNo.get(clean(fields.show_no));
    const classRecord = classByClassNo.get(clean(fields.class_no));
    const entryRecord = entryByEntryNo.get(clean(fields.entry_number));
    const ringId = firstLinkedId(getRingDay?.fields?.rings);
    const ring = ringId ? ringById.get(ringId) : null;

    const next = {};
    if (show) setLinkedIfChanged(fields, next, "shows", linked(show.id)); else missing.shows += 1;
    if (classRecord) setLinkedIfChanged(fields, next, "classes", linked(classRecord.id)); else missing.classes += 1;
    if (entryRecord) setLinkedIfChanged(fields, next, "entries", linked(entryRecord.id)); else missing.entries += 1;
    if (getRingDay) {
      setLinkedIfChanged(fields, next, "get_ring_days", linked(getRingDay.id));
      setLinkedIfChanged(fields, next, "ring_days", linked(firstLinkedId(getRingDay.fields?.ring_days)));
      setLinkedIfChanged(fields, next, "rings", linked(ringId));
      setValueIfChanged(fields, next, "ring_no", intOrNull(ring?.fields?.ring_no));
      if (!ringId) missing.rings += 1;
    } else {
      missing.get_ring_days += 1;
      missing.rings += 1;
    }

    if (Object.keys(next).length) {
      updates.push({ id: record.id, fields: next });
    }
  }

  for (let index = 0; index < updates.length; index += 10) {
    const batch = updates.slice(index, index + 10);
    await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent("get_rings")}`, {
      method: "PATCH",
      body: JSON.stringify({ records: batch, typecast: true })
    });
  }

  return {
    table: "get_rings",
    seen: getRings.length,
    changed: updates.length,
    missing
  };
}

async function main() {
  const source = argValue("--source", "rings");
  const showNo = argValue("--show-no", "");
  const focusDay = argValue("--focus-day", "");
  if (source !== "rings") {
    console.log(JSON.stringify({ table: source, seen: 0, changed: 0, skipped: "source_not_supported" }));
    return;
  }
  const result = await syncGetRings({ showNo, focusDay });
  console.log(JSON.stringify(result));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
