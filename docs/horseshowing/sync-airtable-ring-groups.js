function argValue(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

const BASE_ID = argValue("base-id", "app6XS1RvsPNRT6os");
const TOKEN = process.env.AIRTABLE_TOKEN;

if (!TOKEN) {
  console.error("AIRTABLE_TOKEN is required");
  process.exit(1);
}

const API = `https://api.airtable.com/v0/${BASE_ID}`;
const META = `https://api.airtable.com/v0/meta/bases/${BASE_ID}`;
const headers = {
  Authorization: `Bearer ${TOKEN}`,
  "Content-Type": "application/json",
};

function encodeName(name) {
  return encodeURIComponent(name);
}

function text(value) {
  if (value == null) return "";
  if (Array.isArray(value)) {
    return value
      .map((item) => {
        if (item == null) return "";
        if (typeof item === "object") return item.name || item.id || "";
        return String(item);
      })
      .filter(Boolean)
      .join(", ");
  }
  return String(value);
}

function number(value) {
  if (value == null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalized(value) {
  return text(value).toLowerCase().replace(/\s+/g, " ").trim();
}

function linkedIds(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => {
      if (typeof item === "string") return item;
      if (item && typeof item === "object") return item.id;
      return "";
    })
    .filter(Boolean);
}

async function request(url, options = {}) {
  const response = await fetch(url, { ...options, headers: { ...headers, ...(options.headers || {}) } });
  const body = await response.json().catch(async () => ({ text: await response.text() }));
  if (!response.ok) {
    throw new Error(`${response.status} ${url}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function listRecords(table, params = {}) {
  const records = [];
  let offset;
  do {
    const qs = new URLSearchParams(params);
    qs.set("pageSize", "100");
    if (offset) qs.set("offset", offset);
    const body = await request(`${API}/${encodeName(table)}?${qs.toString()}`);
    records.push(...body.records);
    offset = body.offset;
  } while (offset);
  return records;
}

async function airtableMeta() {
  return request(`${META}/tables`);
}

async function ensureTable(meta, name, linkedTableIds) {
  let table = meta.tables.find((candidate) => candidate.name === name);
  if (table) return table;

  await request(`${META}/tables`, {
    method: "POST",
    body: JSON.stringify({
      name,
      description: "Computed WEC print ring-group planning table.",
      fields: [
        { name: "ring_group_key", type: "singleLineText" },
        { name: "shows", type: "multipleRecordLinks", options: { linkedTableId: linkedTableIds.shows } },
        { name: "show_no", type: "number", options: { precision: 0 } },
        { name: "focus_day", type: "date", options: { dateFormat: { name: "iso" } } },
        { name: "ring_days", type: "multipleRecordLinks", options: { linkedTableId: linkedTableIds.ring_days } },
        { name: "ring_day_no", type: "number", options: { precision: 0 } },
        { name: "ring_no", type: "number", options: { precision: 0 } },
        { name: "rings", type: "multipleRecordLinks", options: { linkedTableId: linkedTableIds.rings } },
        { name: "ring_name", type: "singleLineText" },
        { name: "ring_names", type: "multipleRecordLinks", options: { linkedTableId: linkedTableIds.ring_names } },
        { name: "source_rows", type: "number", options: { precision: 0 } },
        { name: "hidden_rows", type: "number", options: { precision: 0 } },
        { name: "visible_classes", type: "number", options: { precision: 0 } },
        { name: "visible_rollups", type: "number", options: { precision: 0 } },
        { name: "print_rows", type: "number", options: { precision: 0 } },
        { name: "portrait_col", type: "number", options: { precision: 0 } },
        { name: "landscape_col", type: "number", options: { precision: 0 } },
        { name: "source", type: "singleLineText" },
      ],
    }),
  });

  return (await airtableMeta()).tables.find((candidate) => candidate.name === name);
}

async function ensurePrintMetaTable(meta, linkedTableIds) {
  let table = meta.tables.find((candidate) => candidate.name === "wec_print_meta");
  if (table) return table;

  await request(`${META}/tables`, {
    method: "POST",
    body: JSON.stringify({
      name: "wec_print_meta",
      description: "Computed WEC print-day layout rollup for portrait and landscape output.",
      fields: [
        { name: "print_meta_key", type: "singleLineText" },
        { name: "shows", type: "multipleRecordLinks", options: { linkedTableId: linkedTableIds.shows } },
        { name: "show_no", type: "number", options: { precision: 0 } },
        { name: "focus_day", type: "date", options: { dateFormat: { name: "iso" } } },
        { name: "ring_day_no", type: "singleLineText" },
        { name: "ring_groups", type: "multipleRecordLinks", options: { linkedTableId: linkedTableIds.ring_groups } },
        { name: "templates", type: "multipleRecordLinks", options: { linkedTableId: linkedTableIds.templates } },
        { name: "ring_group_count", type: "number", options: { precision: 0 } },
        { name: "visible_classes", type: "number", options: { precision: 0 } },
        { name: "visible_rollups", type: "number", options: { precision: 0 } },
        { name: "total_print_rows", type: "number", options: { precision: 0 } },
        { name: "portrait_summary", type: "multilineText" },
        { name: "portrait_col_1", type: "singleLineText" },
        { name: "portrait_col_2", type: "singleLineText" },
        { name: "landscape_summary", type: "multilineText" },
        { name: "landscape_col_1", type: "singleLineText" },
        { name: "landscape_col_2", type: "singleLineText" },
        { name: "landscape_col_3", type: "singleLineText" },
        { name: "source", type: "singleLineText" },
      ],
    }),
  });

  return (await airtableMeta()).tables.find((candidate) => candidate.name === "wec_print_meta");
}

async function ensurePrintMetaFields(table, linkedTableIds) {
  const needed = [
    ["shows", "multipleRecordLinks", { linkedTableId: linkedTableIds.shows }],
    ["show_no", "number", { precision: 0 }],
    ["focus_day", "date", { dateFormat: { name: "iso" } }],
    ["ring_day_no", "singleLineText"],
    ["ring_groups", "multipleRecordLinks", { linkedTableId: linkedTableIds.ring_groups }],
    ["templates", "multipleRecordLinks", { linkedTableId: linkedTableIds.templates }],
    ["ring_group_count", "number", { precision: 0 }],
    ["visible_classes", "number", { precision: 0 }],
    ["visible_rollups", "number", { precision: 0 }],
    ["total_print_rows", "number", { precision: 0 }],
    ["portrait_summary", "multilineText"],
    ["portrait_col_1", "singleLineText"],
    ["portrait_col_2", "singleLineText"],
    ["landscape_summary", "multilineText"],
    ["landscape_col_1", "singleLineText"],
    ["landscape_col_2", "singleLineText"],
    ["landscape_col_3", "singleLineText"],
    ["source", "singleLineText"],
  ];

  const existing = new Set(table.fields.map((field) => field.name));
  for (const [name, type, options] of needed) {
    if (existing.has(name)) continue;
    await request(`${META}/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify({ name, type, ...(options ? { options } : {}) }),
    });
  }
}

async function ensureFields(table, linkedTableIds) {
  const needed = [
    ["shows", "multipleRecordLinks", { linkedTableId: linkedTableIds.shows }],
    ["show_no", "number", { precision: 0 }],
    ["focus_day", "date", { dateFormat: { name: "iso" } }],
    ["ring_days", "multipleRecordLinks", { linkedTableId: linkedTableIds.ring_days }],
    ["ring_day_no", "number", { precision: 0 }],
    ["ring_no", "number", { precision: 0 }],
    ["rings", "multipleRecordLinks", { linkedTableId: linkedTableIds.rings }],
    ["ring_name", "singleLineText"],
    ["ring_names", "multipleRecordLinks", { linkedTableId: linkedTableIds.ring_names }],
    ["source_rows", "number", { precision: 0 }],
    ["hidden_rows", "number", { precision: 0 }],
    ["visible_classes", "number", { precision: 0 }],
    ["visible_rollups", "number", { precision: 0 }],
    ["print_rows", "number", { precision: 0 }],
    ["portrait_col", "number", { precision: 0 }],
    ["landscape_col", "number", { precision: 0 }],
    ["wec_print_meta", "multipleRecordLinks", { linkedTableId: linkedTableIds.wec_print_meta }],
    ["source", "singleLineText"],
  ];

  const existing = new Set(table.fields.map((field) => field.name));
  for (const [name, type, options] of needed) {
    if (existing.has(name)) continue;
    await request(`${META}/tables/${table.id}/fields`, {
      method: "POST",
      body: JSON.stringify({ name, type, ...(options ? { options } : {}) }),
    });
  }
}

function parseTimeMinutes(value) {
  const raw = text(value).trim().toLowerCase();
  if (!raw || raw === "check time") return 999999;
  const match = raw.match(/^(\d{1,2})(?::(\d{2}))?\s*([ap])?m?$/i);
  if (!match) return 999999;
  let hour = Number(match[1]);
  const minute = Number(match[2] || 0);
  const ampm = match[3];
  if (ampm === "p" && hour < 12) hour += 12;
  if (ampm === "a" && hour === 12) hour = 0;
  return hour * 60 + minute;
}

function assignColumns(rings, columnCount) {
  const buckets = Array.from({ length: columnCount }, (_, i) => ({ column: i + 1, rows: 0, rings: [] }));
  for (const ring of rings) {
    buckets.sort((a, b) => a.rows - b.rows);
    buckets[0].rings.push(ring);
    buckets[0].rows += ring.print_rows;
  }
  const byKey = new Map();
  for (const bucket of buckets) {
    for (const ring of bucket.rings) byKey.set(ring.ring_group_key, bucket.column);
  }
  return byKey;
}

function summarizeColumns(rings, columnCount) {
  const buckets = Array.from({ length: columnCount }, (_, i) => ({ column: i + 1, rows: 0, rings: [] }));
  for (const ring of rings) {
    buckets.sort((a, b) => a.rows - b.rows);
    buckets[0].rings.push(ring);
    buckets[0].rows += ring.print_rows;
  }
  return buckets
    .sort((a, b) => a.column - b.column)
    .map((bucket) => ({
      column: bucket.column,
      rows: bucket.rows,
      rings: bucket.rings.map((ring) => `${ring.ring_name}(${ring.print_rows})`),
    }));
}

async function main() {
  const showNo = Number(argValue("show-no", "14906"));
  const focusDay = argValue("focus-day", null);

  let activeFocus = null;
  if (!focusDay) {
    const focusRecords = await listRecords("focus_show", { filterByFormula: "{active}=1" });
    activeFocus = focusRecords[0] || null;
  }
  const effectiveFocusDay = focusDay || text(activeFocus?.fields?.focus_day).slice(0, 10);
  if (!effectiveFocusDay) throw new Error("focus_day is required or focus_show.active must exist");

  let meta = await airtableMeta();
  const showsTable = meta.tables.find((table) => table.name === "shows");
  const ringDaysTable = meta.tables.find((table) => table.name === "ring_days");
  const ringsTable = meta.tables.find((table) => table.name === "rings");
  const ringNamesTable = meta.tables.find((table) => table.name === "ring_names");
  const templatesTable = meta.tables.find((table) => table.name === "templates");
  if (!showsTable || !ringDaysTable || !ringsTable || !ringNamesTable || !templatesTable) {
    throw new Error("shows, ring_days, rings, ring_names, and templates tables are required");
  }

  let ringGroupsTable = await ensureTable(meta, "ring_groups", {
    shows: showsTable.id,
    ring_days: ringDaysTable.id,
    rings: ringsTable.id,
    ring_names: ringNamesTable.id,
  });
  meta = await airtableMeta();
  ringGroupsTable = meta.tables.find((table) => table.name === "ring_groups");
  let printMetaTable = await ensurePrintMetaTable(meta, {
    shows: showsTable.id,
    ring_groups: ringGroupsTable.id,
    templates: templatesTable.id,
  });
  meta = await airtableMeta();
  ringGroupsTable = meta.tables.find((table) => table.name === "ring_groups");
  printMetaTable = meta.tables.find((table) => table.name === "wec_print_meta");
  await ensurePrintMetaFields(printMetaTable, {
    shows: showsTable.id,
    ring_groups: ringGroupsTable.id,
    templates: templatesTable.id,
  });
  meta = await airtableMeta();
  ringGroupsTable = meta.tables.find((table) => table.name === "ring_groups");
  printMetaTable = meta.tables.find((table) => table.name === "wec_print_meta");
  await ensureFields(ringGroupsTable, {
    shows: showsTable.id,
    ring_days: ringDaysTable.id,
    rings: ringsTable.id,
    ring_names: ringNamesTable.id,
    wec_print_meta: printMetaTable.id,
  });

  const formula = `AND({show_no}=${showNo},IS_SAME({focus_day},DATETIME_PARSE('${effectiveFocusDay}'),'day'))`;
  const updateSchedule = await listRecords("update_schedule", { filterByFormula: formula });
  const classStartTimes = await listRecords("class_start_times", { filterByFormula: formula });
  const classHide = await listRecords("class_hide", { filterByFormula: "{active}=1" });
  const shows = await listRecords("shows", { filterByFormula: `{show_no}=${showNo}` });
  const showLink = shows[0] ? [shows[0].id] : [];

  const allRingDays = await listRecords("ring_days");
  const ringDayById = new Map(allRingDays.map((record) => [record.id, number(record.fields.ring_day_no)]));
  const allRings = await listRecords("rings");
  const ringByNo = new Map(
    allRings
      .map((record) => [number(record.fields.ring_no), record.id])
      .filter(([ringNo]) => ringNo != null)
  );
  const allRingNames = await listRecords("ring_names");
  const ringNameByName = new Map(
    allRingNames
      .map((record) => [normalized(record.fields.ring_name), record.id])
      .filter(([ringName]) => ringName)
  );
  const templates = await listRecords("templates", { filterByFormula: "{templates_id}='wec-print'" });
  const templateLink = templates[0] ? [templates[0].id] : [];

  const hideTexts = classHide.map((record) => normalized(record.fields.hide_text)).filter(Boolean);
  const hideClassNos = new Set(
    classHide
      .map((record) => number(record.fields.class_no))
      .filter((value) => value != null)
      .map(String)
  );

  const rollupByClass = new Map();
  for (const record of classStartTimes) {
    const classNo = number(record.fields.class_no);
    const rollup = text(record.fields["rollup_label Rollup (from entry_go_times)"]).trim();
    if (classNo != null && rollup) rollupByClass.set(String(classNo), rollup);
  }

  const rings = new Map();
  for (const record of updateSchedule) {
    const fields = record.fields;
    const classNo = number(fields.class_no);
    const classText = normalized(fields.class_name || fields.event_name);
    let hidden = false;
    if (fields.ignore === true) hidden = true;
    if (classNo == null || classNo === 0) hidden = true;
    if (classNo != null && hideClassNos.has(String(classNo))) hidden = true;
    if (hideTexts.some((hideText) => hideText && classText.includes(hideText))) hidden = true;

    const ringNo = number(fields.ring_no);
    const ringKey = ringNo == null ? "NO_RING" : String(ringNo);
    const ringDayIds = linkedIds(fields.ring_days);
    const ringDayNo = ringDayIds.map((id) => ringDayById.get(id)).find((value) => value != null) || null;

    const ringName = text(fields.ring_name || fields.ring_names || ringKey);

    if (!rings.has(ringKey)) {
      rings.set(ringKey, {
        ring_group_key: `${showNo}|${effectiveFocusDay}|${ringKey}`,
        shows: showLink,
        show_no: showNo,
        focus_day: effectiveFocusDay,
        ring_days: ringDayIds.slice(0, 1),
        ring_day_no: ringDayNo,
        ring_no: ringNo,
        rings: ringByNo.has(ringNo) ? [ringByNo.get(ringNo)] : [],
        ring_name: ringName,
        ring_names: ringNameByName.has(normalized(ringName)) ? [ringNameByName.get(normalized(ringName))] : [],
        source_rows: 0,
        hidden_rows: 0,
        visible_classes: 0,
        visible_rollups: 0,
        print_rows: 1,
        first_minutes: 999999,
      });
    }

    const ring = rings.get(ringKey);
    ring.source_rows += 1;
    if (hidden) {
      ring.hidden_rows += 1;
      continue;
    }

    ring.visible_classes += 1;
    ring.first_minutes = Math.min(
      ring.first_minutes,
      parseTimeMinutes(fields.display_time || fields.display_time2 || fields.time_text)
    );
    const hasRollup = classNo != null && rollupByClass.has(String(classNo));
    if (hasRollup) ring.visible_rollups += 1;
    ring.print_rows += hasRollup ? 2 : 1;
  }

  const ringRows = [...rings.values()]
    .filter((ring) => ring.visible_classes > 0)
    .sort((a, b) => a.first_minutes - b.first_minutes || a.ring_name.localeCompare(b.ring_name, undefined, { numeric: true }));

  const portraitCols = assignColumns(ringRows, 2);
  const landscapeCols = assignColumns(ringRows, 3);
  const portraitSummary = summarizeColumns(ringRows, 2);
  const landscapeSummary = summarizeColumns(ringRows, 3);

  const existing = await listRecords("ring_groups", {
    filterByFormula: `AND({show_no}=${showNo},IS_SAME({focus_day},DATETIME_PARSE('${effectiveFocusDay}'),'day'))`,
  });
  const existingByKey = new Map(existing.map((record) => [text(record.fields.ring_group_key), record.id]));

  const payload = ringRows.map((ring) => ({
    fields: {
      ring_group_key: ring.ring_group_key,
      shows: ring.shows,
      show_no: ring.show_no,
      focus_day: ring.focus_day,
      ring_days: ring.ring_days,
      ring_day_no: ring.ring_day_no,
      ring_no: ring.ring_no,
      rings: ring.rings,
      ring_name: ring.ring_name,
      ring_names: ring.ring_names,
      source_rows: ring.source_rows,
      hidden_rows: ring.hidden_rows,
      visible_classes: ring.visible_classes,
      visible_rollups: ring.visible_rollups,
      print_rows: ring.print_rows,
      portrait_col: portraitCols.get(ring.ring_group_key),
      landscape_col: landscapeCols.get(ring.ring_group_key),
      source: "update_schedule+class_start_times+class_hide",
    },
  }));

  let created = 0;
  let updated = 0;
  const writtenRingIds = [];
  for (let i = 0; i < payload.length; i += 10) {
    const chunk = payload.slice(i, i + 10);
    const creates = [];
    const updates = [];
    for (const row of chunk) {
      const id = existingByKey.get(row.fields.ring_group_key);
      if (id) updates.push({ id, fields: row.fields });
      else creates.push(row);
    }
    if (creates.length) {
      const body = await request(`${API}/${encodeName("ring_groups")}`, {
        method: "POST",
        body: JSON.stringify({ records: creates }),
      });
      writtenRingIds.push(...body.records.map((record) => record.id));
      created += creates.length;
    }
    if (updates.length) {
      const body = await request(`${API}/${encodeName("ring_groups")}`, {
        method: "PATCH",
        body: JSON.stringify({ records: updates }),
      });
      writtenRingIds.push(...body.records.map((record) => record.id));
      updated += updates.length;
    }
  }

  const ringIds = writtenRingIds.length
    ? writtenRingIds
    : (await listRecords("ring_groups", {
        filterByFormula: `AND({show_no}=${showNo},IS_SAME({focus_day},DATETIME_PARSE('${effectiveFocusDay}'),'day'))`,
      })).map((record) => record.id);

  const printMetaKey = `${showNo}|${effectiveFocusDay}`;
  const existingPrintMeta = await listRecords("wec_print_meta", {
    filterByFormula: `{print_meta_key}='${printMetaKey}'`,
  });
  const ringDayNos = [...new Set(ringRows.map((ring) => ring.ring_day_no).filter((value) => value != null))]
    .sort((a, b) => a - b)
    .join(", ");
  const printMetaFields = {
    print_meta_key: printMetaKey,
    shows: showLink,
    show_no: showNo,
    focus_day: effectiveFocusDay,
    ring_day_no: ringDayNos,
    ring_groups: ringIds,
    templates: templateLink,
    ring_group_count: ringRows.length,
    visible_classes: ringRows.reduce((sum, ring) => sum + ring.visible_classes, 0),
    visible_rollups: ringRows.reduce((sum, ring) => sum + ring.visible_rollups, 0),
    total_print_rows: ringRows.reduce((sum, ring) => sum + ring.print_rows, 0),
    portrait_summary: portraitSummary.map((col) => `col${col.column} rows=${col.rows}: ${col.rings.join(" | ")}`).join("\n"),
    portrait_col_1: portraitSummary[0]?.rings.join(" | ") || "",
    portrait_col_2: portraitSummary[1]?.rings.join(" | ") || "",
    landscape_summary: landscapeSummary.map((col) => `col${col.column} rows=${col.rows}: ${col.rings.join(" | ")}`).join("\n"),
    landscape_col_1: landscapeSummary[0]?.rings.join(" | ") || "",
    landscape_col_2: landscapeSummary[1]?.rings.join(" | ") || "",
    landscape_col_3: landscapeSummary[2]?.rings.join(" | ") || "",
    source: "ring_groups",
  };

  let printMetaId;
  if (existingPrintMeta[0]) {
    const body = await request(`${API}/${encodeName("wec_print_meta")}`, {
      method: "PATCH",
      body: JSON.stringify({ records: [{ id: existingPrintMeta[0].id, fields: printMetaFields }] }),
    });
    printMetaId = body.records[0].id;
  } else {
    const body = await request(`${API}/${encodeName("wec_print_meta")}`, {
      method: "POST",
      body: JSON.stringify({ records: [{ fields: printMetaFields }] }),
    });
    printMetaId = body.records[0].id;
  }

  for (let i = 0; i < ringIds.length; i += 10) {
    await request(`${API}/${encodeName("ring_groups")}`, {
      method: "PATCH",
      body: JSON.stringify({
        records: ringIds.slice(i, i + 10).map((id) => ({
          id,
          fields: { wec_print_meta: [printMetaId] },
        })),
      }),
    });
  }

  console.log(JSON.stringify({
    show_no: showNo,
    focus_day: effectiveFocusDay,
    ring_groups: payload.length,
    created,
    updated,
    wec_print_meta: printMetaKey,
    portrait_summary: printMetaFields.portrait_summary,
    landscape_summary: printMetaFields.landscape_summary,
    rows: payload.map((row) => row.fields),
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
