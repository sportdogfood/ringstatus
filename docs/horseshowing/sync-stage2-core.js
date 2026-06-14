const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const CATALYST_ENDPOINT = process.env.HORSESHOWING_CATALYST_ENDPOINT ||
  "https://horseshowing-700800454.development.catalystserverless.com/server/horseshowing_sync/";

function requireToken() {
  if (!AIRTABLE_TOKEN) throw new Error("AIRTABLE_TOKEN is required");
}

function clean(value) {
  return String(value ?? "").trim();
}

function safeJson(value) {
  return JSON.stringify(value, null, 2).slice(0, 90000);
}

async function airtableFetch(table, options = {}) {
  requireToken();
  const url = new URL(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(table)}`);
  if (options.params) {
    for (const [key, value] of Object.entries(options.params)) {
      if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, value);
    }
  }
  const response = await fetch(url, {
    method: options.method || "GET",
    headers: {
      Authorization: `Bearer ${AIRTABLE_TOKEN}`,
      "Content-Type": "application/json"
    },
    body: options.body ? JSON.stringify(options.body) : undefined
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Airtable ${table} failed ${response.status}: ${text.slice(0, 1000)}`);
  return text ? JSON.parse(text) : {};
}

async function getFocusShow() {
  const payload = await airtableFetch("focus_show", {
    params: { pageSize: "10" }
  });
  const record = (payload.records || []).find((item) => item.fields?.active) || (payload.records || [])[0];
  if (!record) throw new Error("No focus_show record found");
  const fields = record.fields || {};
  const showNo = clean(fields.show_no);
  const focusDay = clean(fields.focus_day).slice(0, 10);
  if (!showNo || !focusDay) throw new Error("focus_show missing show_no or focus_day");
  return { showNo, focusDay };
}

async function writeLog({ checkName, showNo, focusDay, status = "ok", recordsSeen = 0, recordsChanged = 0, summary, payload = {} }) {
  const createdAt = new Date().toISOString();
  await airtableFetch("wec-logs", {
    method: "POST",
    body: {
      fields: {
        log_key_run: `${createdAt}|catalyst_core|${checkName}`,
        created_at: createdAt,
        log_type: "heartbeat",
        workflow_lanes: "Core",
        check_name: checkName,
        show_no: String(showNo),
        focus_day: focusDay,
        status,
        records_seen: recordsSeen,
        records_changed: recordsChanged,
        summary,
        payload_json: safeJson(payload)
      },
      typecast: true
    }
  });
}

async function upsertAirtableRows(table, mergeFields, rows) {
  const deduped = new Map();
  for (const row of rows) {
    const key = mergeFields.map((field) => clean(row[field])).join("|");
    if (!key.replace(/\|/g, "")) continue;
    deduped.set(key, row);
  }
  const cleanRows = [...deduped.values()]
    .map((row) => {
      const fields = {};
      for (const [key, value] of Object.entries(row)) {
        if (value !== undefined && value !== null && value !== "") fields[key] = value;
      }
      return fields;
    })
    .filter((fields) => Object.keys(fields).length);
  let changed = 0;
  for (let index = 0; index < cleanRows.length; index += 10) {
    const batch = cleanRows.slice(index, index + 10);
    await airtableFetch(table, {
      method: "PATCH",
      body: {
        performUpsert: { fieldsToMergeOn: mergeFields },
        records: batch.map((fields) => ({ fields })),
        typecast: true
      }
    });
    changed += batch.length;
  }
  return { seen: cleanRows.length, changed };
}

async function listAllAirtableRecords(table, params = {}) {
  const records = [];
  let offset = "";
  do {
    const payload = await airtableFetch(table, {
      params: {
        pageSize: "100",
        ...params,
        ...(offset ? { offset } : {})
      }
    });
    records.push(...(payload.records || []));
    offset = payload.offset || "";
  } while (offset);
  return records;
}

async function recordIdMap(table, fieldName, params = {}) {
  const records = await listAllAirtableRecords(table, params);
  const map = new Map();
  for (const record of records) {
    const value = clean(record.fields?.[fieldName]);
    if (value) map.set(value, record.id);
  }
  return map;
}

function linkedRecord(map, value) {
  const id = map.get(clean(value));
  return id ? [id] : undefined;
}

async function catalystGet(params, timeoutMs = 120000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const url = `${CATALYST_ENDPOINT}?${params.toString()}`;
    const response = await fetch(url, { signal: controller.signal });
    const text = await response.text();
    if (!response.ok) throw new Error(`Catalyst failed ${response.status}: ${text.slice(0, 1000)}`);
    return text ? JSON.parse(text) : {};
  } finally {
    clearTimeout(timer);
  }
}

function numberOrNull(value) {
  const parsed = Number.parseInt(clean(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

async function buildClassOogLinkMaps(showNo, focusDay, rows) {
  const support = {
    shows: [{ show_no: numberOrNull(showNo) }],
    classes: [],
    rings: [],
    entries: []
  };

  const classes = new Map();
  const rings = new Map();
  const entries = new Map();

  for (const row of [...rows.updateSchedule, ...rows.classOog]) {
    if (row.class_no && !classes.has(clean(row.class_no))) {
      classes.set(clean(row.class_no), {
        class_no: numberOrNull(row.class_no),
        class_label: clean(row.class_label),
        class_name: clean(row.class_name),
        class_payout: clean(row.class_payout),
        source: "class_oog"
      });
    }
    if (row.ring_no && !rings.has(clean(row.ring_no))) {
      rings.set(clean(row.ring_no), {
        ring_no: numberOrNull(row.ring_no),
        ring_name: clean(row.ring),
        source: "class_oog"
      });
    }
    if (row.entry_no && !entries.has(clean(row.entry_no))) {
      entries.set(clean(row.entry_no), {
        entry_no: numberOrNull(row.entry_no),
        horse: clean(row.horse),
        rider: clean(row.rider),
        trainer: clean(row.trainer),
        source: "class_oog"
      });
    }
  }

  support.classes = [...classes.values()].filter((row) => row.class_no);
  support.rings = [...rings.values()].filter((row) => row.ring_no);
  support.entries = [...entries.values()].filter((row) => row.entry_no);

  await upsertAirtableRows("shows", ["show_no"], support.shows);
  await upsertAirtableRows("classes", ["class_no"], support.classes);
  await upsertAirtableRows("rings", ["ring_no"], support.rings);
  await upsertAirtableRows("entries", ["entry_no"], support.entries);

  return {
    shows: await recordIdMap("shows", "show_no"),
    focusShow: await recordIdMap("focus_show", "show_no", {
      filterByFormula: `AND({show_no}=${numberOrNull(showNo)},IS_SAME({focus_day},'${focusDay}','day'))`
    }),
    classes: await recordIdMap("classes", "class_no"),
    rings: await recordIdMap("rings", "ring_no"),
    ringDays: await recordIdMap("ring_days", "ring_day_no"),
    entries: await recordIdMap("entries", "entry_no")
  };
}

function mirrorRowsFromSnapshot(snapshot, showNo, focusDay) {
  const updateSchedule = Array.isArray(snapshot.update_schedule) ? snapshot.update_schedule : [];
  const counts = Array.isArray(snapshot.counts) ? snapshot.counts : [];
  const classOog = Array.isArray(snapshot.class_oog) ? snapshot.class_oog : [];
  const ringDaysByNo = new Map();
  for (const row of updateSchedule) {
    const ringDayNo = clean(row.ring_day_no);
    if (!ringDayNo) continue;
    ringDaysByNo.set(ringDayNo, {
      ring_day_no: ringDayNo,
      date_text: clean(row.date_text || focusDay)
    });
  }
  return {
    ringDays: [...ringDaysByNo.values()],
    updateSchedule: updateSchedule
      .filter((row) => numberOrNull(row.class_no))
      .map((row) => {
        const showNoValue = numberOrNull(row.show_no) || numberOrNull(showNo);
        const daysValue = numberOrNull(row.ring_day_no);
        const classNoValue = numberOrNull(row.class_no);
        return {
          mirror_update_schedule_key: [showNoValue, daysValue, classNoValue].join("|"),
          show_no: showNoValue,
          focus_day: clean(row.focus_day || focusDay).slice(0, 10),
          days: daysValue,
          ring_no: numberOrNull(row.ring_no),
          ring_name: clean(row.ring_name),
          date_text: clean(row.date_text || focusDay),
          class_no: classNoValue,
          event_name: clean(row.event_name),
          time_text: clean(row.time_text),
          time: clean(row.time),
          entry_count: numberOrNull(row.entry_count),
          source: "update_schedule"
        };
      }),
    counts: counts
      .filter((row) => numberOrNull(row.class_no))
      .map((row) => {
        const showNoValue = numberOrNull(row.show_no) || numberOrNull(showNo);
        const classNoValue = numberOrNull(row.class_no);
        return {
          mirror_class_key: [showNoValue, classNoValue].join("|"),
          show_no: showNoValue,
          class_no: classNoValue,
          class_number: numberOrNull(row.class_number),
          class_name: clean(row.class_name),
          entry_count: numberOrNull(row.entry_count)
        };
      }),
    classOog: classOog
      .filter((row) => numberOrNull(row.class_no) && numberOrNull(row.entry_no))
      .map((row) => {
        const classNoValue = numberOrNull(row.class_no);
        const entryNoValue = numberOrNull(row.entry_no);
        return {
          mirror_class_oog_key: [classNoValue, entryNoValue].join("|"),
          ring: clean(row.ring || row.ring_name),
          ring_no: numberOrNull(row.ring_no),
          days: numberOrNull(row.ring_day_no || row.days),
          class_order: numberOrNull(row.class_order),
          class_no: classNoValue,
          class_label: clean(row.class_label),
          class_payout: clean(row.class_payout),
          class_name: clean(row.class_name),
          entry_order: numberOrNull(row.entry_order),
          entry_no: entryNoValue,
          horse: clean(row.horse),
          rider: clean(row.rider),
          trainer: clean(row.trainer),
          source: clean(row.source || "class_oog")
        };
      })
  };
}

async function mirrorSnapshotToAirtable(snapshot, showNo, focusDay) {
  const rows = mirrorRowsFromSnapshot(snapshot, showNo, focusDay);
  const ringDays = await upsertAirtableRows("ring_days", ["ring_day_no"], rows.ringDays);
  const links = await buildClassOogLinkMaps(showNo, focusDay, rows);
  const updateSchedule = await upsertAirtableRows("update_schedule", ["show_no", "days", "class_no"], rows.updateSchedule);
  const counts = await upsertAirtableRows("counts", ["show_no", "class_no"], rows.counts);
  const classOogRows = rows.classOog.map((row) => ({
    ...row,
    show_no: numberOrNull(showNo),
    focus_day: focusDay,
    shows: linkedRecord(links.shows, showNo),
    focus_show: linkedRecord(links.focusShow, showNo),
    classes: linkedRecord(links.classes, row.class_no),
    rings: linkedRecord(links.rings, row.ring_no),
    ring_days: linkedRecord(links.ringDays, row.days),
    entries: linkedRecord(links.entries, row.entry_no)
  }));
  const classOog = await upsertAirtableRows("class_oog", ["class_no", "entry_no"], classOogRows);
  return {
    ring_days: ringDays,
    update_schedule: updateSchedule,
    counts,
    class_oog: classOog
  };
}

async function syncCounts(showNo) {
  const pages = [];
  let offset = 0;
  for (;;) {
    const result = await catalystGet(new URLSearchParams({
      action: "sync-counts",
      show_no: showNo,
      counts_offset: String(offset),
      counts_limit: "100"
    }));
    pages.push(result);
    if (!result.has_more || result.next_offset === null || result.next_offset === undefined) break;
    offset = Number(result.next_offset);
  }
  return {
    pages: pages.length,
    records_seen: pages.reduce((sum, page) => sum + Number(page.parsed_rows || 0), 0),
    total_rows: Math.max(...pages.map((page) => Number(page.total_rows || 0))),
    pages_payload: pages.map((page) => ({
      offset: page.offset,
      parsed_rows: page.parsed_rows,
      has_more: page.has_more,
      counters: page.counters
    }))
  };
}

async function syncFocusDaySchedule(showNo, focusDay) {
  const pages = [];
  let offset = 0;
  const limit = 4;
  for (;;) {
    const result = await catalystGet(new URLSearchParams({
      action: "sync-focus-day",
      show_no: showNo,
      focus_day: focusDay,
      schedule_only: "1",
      days_offset: String(offset),
      days_limit: String(limit),
      use_stored_ring_days: "1"
    }));
    pages.push(result);
    if (!result.has_more || result.next_offset === null || result.next_offset === undefined) break;
    offset = Number(result.next_offset);
  }
  return {
    pages: pages.length,
    selected_ring_days: pages.reduce((sum, page) => sum + Number(page.selected_ring_days || 0), 0),
    selected_ring_days_total: Math.max(...pages.map((page) => Number(page.selected_ring_days_total || 0))),
    schedule_rows: pages.reduce((sum, page) => sum + Number(page.schedule_rows || 0), 0),
    inserted: pages.reduce((sum, page) => sum + Number(page.inserted || 0), 0),
    updated: pages.reduce((sum, page) => sum + Number(page.updated || 0), 0),
    pages_payload: pages.map((page) => ({
      offset: page.offset,
      selected_ring_days: page.selected_ring_days,
      schedule_rows: page.schedule_rows,
      inserted: page.inserted,
      updated: page.updated,
      has_more: page.has_more,
      next_offset: page.next_offset
    }))
  };
}

async function refreshFocusClassOog(showNo, focusDay, classNos) {
  const results = [];
  for (const classNo of classNos) {
    const result = await catalystGet(new URLSearchParams({
      action: "sync-class-oog",
      show_no: showNo,
      class_no: String(classNo)
    }));
    results.push({
      class_no: classNo,
      parsed_rows: result.parsed_rows,
      counters: result.counters,
      upstream_status: result.upstream_status
    });
  }
  return {
    classes_seen: classNos.length,
    records_seen: results.reduce((sum, item) => sum + Number(item.parsed_rows || 0), 0),
    results
  };
}

async function main() {
  const focus = await getFocusShow();
  const showNo = process.argv[2] || process.env.WEC_SHOW_NO || focus.showNo;
  const focusDay = process.argv[3] || process.env.WEC_FOCUS_DAY || focus.focusDay;

  const ringDays = await catalystGet(new URLSearchParams({
    action: "sync-ring-days",
    show_no: showNo
  }));
  await writeLog({
    checkName: "sync-ring-days",
    showNo,
    focusDay,
    recordsSeen: Number(ringDays.parsed_rows || 0),
    recordsChanged: Number(ringDays.counters?.rings || 0),
    summary: `Catalyst get_ring_days rows=${ringDays.parsed_rows}`,
    payload: ringDays
  });

  const schedule = await syncFocusDaySchedule(showNo, focusDay);
  await writeLog({
    checkName: "core_update_schedule",
    showNo,
    focusDay,
    recordsSeen: Number(schedule.schedule_rows || 0),
    recordsChanged: Number(schedule.schedule_rows || 0),
    summary: `Catalyst update_schedule rows=${schedule.schedule_rows} ring_days=${schedule.selected_ring_days} pages=${schedule.pages}`,
    payload: schedule
  });

  const counts = await syncCounts(showNo);
  await writeLog({
    checkName: "core_counts",
    showNo,
    focusDay,
    recordsSeen: counts.records_seen,
    recordsChanged: counts.records_seen,
    summary: `Catalyst counts rows=${counts.records_seen} pages=${counts.pages}`,
    payload: counts
  });

  const snapshotBeforeOog = await catalystGet(new URLSearchParams({
    action: "focus-day-snapshot",
    show_no: showNo,
    focus_day: focusDay
  }));
  const classNos = [...new Set((snapshotBeforeOog.update_schedule || [])
    .map((row) => clean(row.class_no))
    .filter((value) => value && value !== "0"))];
  const oog = await refreshFocusClassOog(showNo, focusDay, classNos);
  await writeLog({
    checkName: "core_class_oog",
    showNo,
    focusDay,
    recordsSeen: oog.records_seen,
    recordsChanged: oog.records_seen,
    summary: `Catalyst class_oog classes=${oog.classes_seen} entries=${oog.records_seen}`,
    payload: {
      classes_seen: oog.classes_seen,
      records_seen: oog.records_seen,
      sample: oog.results.slice(0, 10)
    }
  });

  const snapshot = await catalystGet(new URLSearchParams({
    action: "focus-day-snapshot",
    show_no: showNo,
    focus_day: focusDay
  }));

  const mirror = await mirrorSnapshotToAirtable(snapshot, showNo, focusDay);
  await writeLog({
    checkName: "core_airtable_mirror",
    showNo,
    focusDay,
    recordsSeen: mirror.ring_days.seen + mirror.update_schedule.seen + mirror.counts.seen + mirror.class_oog.seen,
    recordsChanged: mirror.ring_days.changed + mirror.update_schedule.changed + mirror.counts.changed + mirror.class_oog.changed,
    summary: `Airtable mirror ring_days=${mirror.ring_days.seen}; update_schedule=${mirror.update_schedule.seen}; counts=${mirror.counts.seen}; class_oog=${mirror.class_oog.seen}`,
    payload: mirror
  });

  console.log(JSON.stringify({
    ok: true,
    show_no: showNo,
    focus_day: focusDay,
    catalyst_primary: true,
    ring_days: Number(ringDays.parsed_rows || 0),
    update_schedule: Number(snapshot.update_schedule?.length || 0),
    counts: Number(snapshot.counts?.length || 0),
    class_oog: Number(snapshot.class_oog?.length || 0),
    airtable_mirror: mirror
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
