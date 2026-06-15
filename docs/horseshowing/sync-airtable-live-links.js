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

function formulaForShowFocus(showNo, focusDay) {
  const parts = [];
  if (showNo) {
    const showValue = /^\d+$/.test(String(showNo)) ? String(Number(showNo)) : `'${String(showNo).replace(/'/g, "\\'")}'`;
    parts.push(`{show_no}=${showValue}`);
  }
  if (focusDay) parts.push(`IS_SAME({focus_day},'${String(focusDay).replace(/'/g, "\\'")}','day')`);
  return parts.length > 1 ? `AND(${parts.join(",")})` : parts[0] || "";
}

function formulaForActiveShow(showNo) {
  const value = clean(showNo);
  if (!value) return "{active}=1";
  const showFormula = /^\d+$/.test(value) ? `{show_no}=${Number(value)}` : `{show_no}='${value.replace(/'/g, "\\'")}'`;
  return `AND(${showFormula},{active}=1)`;
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

async function getBaseTables() {
  return airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables`);
}

async function ensureNumberField(tableName, fieldName) {
  const meta = await getBaseTables();
  const table = meta.tables.find((item) => item.name === tableName);
  if (!table) throw new Error(`Missing Airtable table ${tableName}`);
  if (table.fields.some((field) => field.name === fieldName)) return false;
  await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${table.id}/fields`, {
    method: "POST",
    body: JSON.stringify({ name: fieldName, type: "number", options: { precision: 0 } })
  });
  return true;
}

async function ensureLinkFields(tableName, fieldMap) {
  const meta = await getBaseTables();
  const table = meta.tables.find((item) => item.name === tableName);
  if (!table) throw new Error(`Missing Airtable table ${tableName}`);
  const tablesByName = new Map(meta.tables.map((item) => [item.name, item]));
  const existing = new Set(table.fields.map((field) => field.name));
  let created = 0;
  for (const [fieldName, linkedTableName] of Object.entries(fieldMap)) {
    if (existing.has(fieldName)) continue;
    const linkedTable = tablesByName.get(linkedTableName);
    if (!linkedTable) throw new Error(`Missing linked Airtable table ${linkedTableName}`);
    await airtableFetch(`https://api.airtable.com/v0/meta/bases/${BASE_ID}/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify({
        name: fieldName,
        type: "multipleRecordLinks",
        options: { linkedTableId: linkedTable.id }
      })
    });
    created += 1;
  }
  return created;
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

function mapByComposite(records, keyFn) {
  const map = new Map();
  for (const record of records) {
    const key = keyFn(record.fields || {});
    if (key) map.set(key, record);
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

function classNumberFromText(value) {
  const match = clean(value).match(/^(\d+)\)/);
  return match ? clean(match[1]) : "";
}

function classNameFromText(value) {
  return clean(value).replace(/^\d+[A-Za-z]?\)\s*/, "");
}

function isoFromDateText(value) {
  const parsed = new Date(`${clean(value)} 00:00:00 GMT-0400`);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toISOString().slice(0, 10);
}

function updateScheduleKey(fields) {
  const ringNo = clean(fields.ring_no);
  const ringDayNo = clean(fields.days || fields.ring_day_no);
  const classNumber = clean(fields.class_number) || classNumberFromText(fields.event_name || fields.class_text);
  return [clean(fields.show_no), ringNo, ringDayNo, classNumber].join("|");
}

function liveClassKey(fields) {
  const classNumber = clean(fields.class_number) || classNumberFromText(fields.class_text);
  return [clean(fields.show_no), clean(fields.ring_no), clean(fields.ring_day_no), classNumber].join("|");
}

async function patchRecords(tableName, updates) {
  for (let index = 0; index < updates.length; index += 10) {
    const batch = updates.slice(index, index + 10);
    await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
      method: "PATCH",
      body: JSON.stringify({ records: batch, typecast: true })
    });
  }
}

async function createRecords(tableName, rows) {
  const created = [];
  for (let index = 0; index < rows.length; index += 10) {
    const batch = rows.slice(index, index + 10);
    const payload = await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
      method: "POST",
      body: JSON.stringify({ records: batch.map((fields) => ({ fields })), typecast: true })
    });
    created.push(...(payload.records || []));
  }
  return created;
}

async function syncGetRings({ showNo, focusDay }) {
  const formula = formulaForShowFocus(showNo, focusDay);
  await ensureLinkFields("get_rings", {
    shows: "shows",
    focus_show: "focus_show",
    classes: "classes",
    get_ring_days: "get_ring_days",
    ring_days: "ring_days",
    rings: "rings",
    ring_names: "ring_names",
    entries: "entries"
  });
  await ensureLinkFields("get_ring_days", {
    focus_show: "focus_show"
  });

  const [
    getRings,
    getRingDays,
    shows,
    focusShow,
    classes,
    entries,
    rings,
    ringNames
  ] = await Promise.all([
    listAll("get_rings", formula ? { filterByFormula: formula } : {}),
    listAll("get_ring_days"),
    listAll("shows", { filterByFormula: formulaForActiveShow(showNo) }),
    listAll("focus_show", formula ? { filterByFormula: formula } : {}),
    listAll("classes"),
    listAll("entries"),
    listAll("rings"),
    listAll("ring_names")
  ]);

  const getRingDayByRingDayNo = mapByField(getRingDays, "ring_day_no");
  const showByShowNo = mapByField(shows, "show_no");
  const focusShowByShowNo = mapByField(focusShow, "show_no");
  const classByClassNo = mapByField(classes, "class_no");
  const classByClassNumber = mapByField(classes, "class_number");
  const entryByEntryNo = mapByField(entries, "entry_no");
  const ringById = new Map(rings.map((record) => [record.id, record]));
  const ringNameByName = mapByField(ringNames, "ring_name");

  const updates = [];
  const getRingDayUpdates = [];
  const classHelpersToCreate = new Map();
  const missing = {
    get_ring_days: 0,
    shows: 0,
    classes: 0,
    entries: 0,
    rings: 0,
    ring_names: 0
  };

  for (const record of getRings) {
    const fields = record.fields || {};
    const ringDayNo = clean(fields.ring_day_no);
    const getRingDay = getRingDayByRingDayNo.get(ringDayNo);
    const show = showByShowNo.get(clean(fields.show_no));
    const focusShowRecord = focusShowByShowNo.get(clean(fields.show_no));
    const classRecord = classByClassNo.get(clean(fields.class_no));
    const entryRecord = entryByEntryNo.get(clean(fields.entry_number));
    const ringId = firstLinkedId(getRingDay?.fields?.rings);
    const ring = ringId ? ringById.get(ringId) : null;
    const ringName = ringNameByName.get(clean(ring?.fields?.ring_name || fields.ring_name));

    const next = {};
    if (show) setLinkedIfChanged(fields, next, "shows", linked(show.id)); else missing.shows += 1;
    if (focusShowRecord) setLinkedIfChanged(fields, next, "focus_show", linked(focusShowRecord.id));
    if (classRecord) setLinkedIfChanged(fields, next, "classes", linked(classRecord.id)); else missing.classes += 1;
    if (entryRecord) setLinkedIfChanged(fields, next, "entries", linked(entryRecord.id)); else missing.entries += 1;
    if (getRingDay) {
      setLinkedIfChanged(fields, next, "get_ring_days", linked(getRingDay.id));
      setLinkedIfChanged(fields, next, "ring_days", linked(firstLinkedId(getRingDay.fields?.ring_days)));
      setLinkedIfChanged(fields, next, "rings", linked(ringId));
      if (ringName) setLinkedIfChanged(fields, next, "ring_names", linked(ringName.id)); else missing.ring_names += 1;
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

  const focusShowRecord = focusShowByShowNo.get(clean(showNo));
  for (const record of getRingDays) {
    const fields = record.fields || {};
    if (!focusShowRecord || isoFromDateText(fields.date_text) !== clean(focusDay)) continue;
    const next = {};
    setLinkedIfChanged(fields, next, "focus_show", linked(focusShowRecord.id));
    if (Object.keys(next).length) getRingDayUpdates.push({ id: record.id, fields: next });
  }

  await patchRecords("get_ring_days", getRingDayUpdates);
  await patchRecords("get_rings", updates);

  return {
    table: "get_rings",
    seen: getRings.length,
    changed: updates.length,
    get_ring_days_changed: getRingDayUpdates.length,
    missing
  };
}

async function syncGetOrders({ showNo, focusDay }) {
  const formula = formulaForShowFocus(showNo, focusDay);
  await ensureNumberField("get_orders", "class_no");
  await ensureLinkFields("get_orders", {
    shows: "shows",
    focus_show: "focus_show",
    classes: "classes",
    ring_days: "ring_days",
    rings: "rings",
    ring_names: "ring_names",
    entries: "entries"
  });

  const [
    getOrders,
    updateSchedule,
    getRings,
    shows,
    focusShow,
    classes,
    entries,
    rings,
    ringDays,
    ringNames
  ] = await Promise.all([
    listAll("get_orders", formula ? { filterByFormula: formula } : {}),
    listAll("update_schedule", formula ? { filterByFormula: formula } : {}),
    listAll("get_rings", formula ? { filterByFormula: formula } : {}),
    listAll("shows", { filterByFormula: formulaForActiveShow(showNo) }),
    listAll("focus_show", formula ? { filterByFormula: formula } : {}),
    listAll("classes"),
    listAll("entries"),
    listAll("rings"),
    listAll("ring_days"),
    listAll("ring_names")
  ]);

  const updateByKey = mapByComposite(updateSchedule, updateScheduleKey);
  const getRingsByKey = mapByComposite(getRings, liveClassKey);
  const showByShowNo = mapByField(shows, "show_no");
  const focusShowByShowNo = mapByField(focusShow, "show_no");
  const classByClassNo = mapByField(classes, "class_no");
  const classByClassNumber = mapByField(classes, "class_number");
  const entryByEntryNo = mapByField(entries, "entry_no");
  const ringByRingNo = mapByField(rings, "ring_no");
  const ringDayByRingDayNo = mapByField(ringDays, "ring_day_no");
  const ringNameByName = mapByField(ringNames, "ring_name");

  const updates = [];
  const classHelpersToCreate = new Map();
  const missing = {
    update_schedule: 0,
    shows: 0,
    classes: 0,
    entries: 0,
    rings: 0,
    ring_days: 0,
    ring_names: 0
  };

  for (const record of getOrders) {
    const fields = record.fields || {};
    const update = updateByKey.get(liveClassKey(fields));
    const getRing = getRingsByKey.get(liveClassKey(fields));
    const resolvedClassNo = clean(fields.class_no) || clean(update?.fields?.class_no) || clean(getRing?.fields?.class_no);
    const classNumber = clean(fields.class_number) || classNumberFromText(fields.class_text);
    if (!resolvedClassNo && classNumber && !classByClassNumber.has(classNumber) && !classHelpersToCreate.has(classNumber)) {
      classHelpersToCreate.set(classNumber, {
        class_number: intOrNull(classNumber),
        class_label: clean(fields.class_text),
        class_name: classNameFromText(fields.class_text),
        source: "get_orders.php"
      });
    }
  }

  const createdClassHelpers = await createRecords("classes", [...classHelpersToCreate.values()]);
  for (const record of createdClassHelpers) {
    const classNumber = clean(record.fields?.class_number);
    if (classNumber) classByClassNumber.set(classNumber, record);
  }

  for (const record of getOrders) {
    const fields = record.fields || {};
    const update = updateByKey.get(liveClassKey(fields));
    const getRing = getRingsByKey.get(liveClassKey(fields));
    const resolvedClassNo = clean(fields.class_no) || clean(update?.fields?.class_no) || clean(getRing?.fields?.class_no);
    const classNumber = clean(fields.class_number) || classNumberFromText(fields.class_text);
    const show = showByShowNo.get(clean(fields.show_no));
    const focusShowRecord = focusShowByShowNo.get(clean(fields.show_no));
    const classRecord = classByClassNo.get(resolvedClassNo) || classByClassNumber.get(classNumber);
    const entryRecord = entryByEntryNo.get(clean(fields.entry_no || fields.entry_number));
    const ring = ringByRingNo.get(clean(fields.ring_no));
    const ringDay = ringDayByRingDayNo.get(clean(fields.ring_day_no));
    const ringName = ringNameByName.get(clean(fields.ring_name || ring?.fields?.ring_name));

    const next = {};
    if (resolvedClassNo) setValueIfChanged(fields, next, "class_no", intOrNull(resolvedClassNo)); else if (!classRecord) missing.update_schedule += 1;
    if (show) setLinkedIfChanged(fields, next, "shows", linked(show.id)); else missing.shows += 1;
    if (focusShowRecord) setLinkedIfChanged(fields, next, "focus_show", linked(focusShowRecord.id));
    if (classRecord) setLinkedIfChanged(fields, next, "classes", linked(classRecord.id)); else missing.classes += 1;
    if (entryRecord) setLinkedIfChanged(fields, next, "entries", linked(entryRecord.id)); else missing.entries += 1;
    if (ring) setLinkedIfChanged(fields, next, "rings", linked(ring.id)); else missing.rings += 1;
    if (ringDay) setLinkedIfChanged(fields, next, "ring_days", linked(ringDay.id)); else missing.ring_days += 1;
    if (ringName) setLinkedIfChanged(fields, next, "ring_names", linked(ringName.id)); else missing.ring_names += 1;

    if (Object.keys(next).length) updates.push({ id: record.id, fields: next });
  }

  await patchRecords("get_orders", updates);
  return {
    table: "get_orders",
    seen: getOrders.length,
    changed: updates.length,
    missing
  };
}

async function main() {
  const source = argValue("--source", "rings");
  const showNo = argValue("--show-no", "");
  const focusDay = argValue("--focus-day", "");
  const result = source === "orders"
    ? await syncGetOrders({ showNo, focusDay })
    : await syncGetRings({ showNo, focusDay });
  console.log(JSON.stringify(result));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
