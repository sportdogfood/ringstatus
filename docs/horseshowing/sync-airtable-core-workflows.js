const BASE_ID = process.env.WEC_AIRTABLE_BASE_ID || "app6XS1RvsPNRT6os";
const AIRTABLE_TOKEN = process.env.AIRTABLE_TOKEN;
const HORSESHOWING_BASE = "https://www.horseshowing.com";
let cookieJar = "";

function argValue(name, fallback = "") {
  const index = process.argv.indexOf(name);
  return index >= 0 && process.argv[index + 1] ? process.argv[index + 1] : fallback;
}

function clean(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

function intOrNull(value) {
  const parsed = Number.parseInt(clean(value), 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function htmlDecode(value) {
  return clean(value)
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#039;|&apos;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function attrs(tag) {
  const out = {};
  for (const match of tag.matchAll(/([a-zA-Z0-9_-]+)\s*=\s*"([^"]*)"/g)) {
    out[match[1]] = htmlDecode(match[2]);
  }
  return out;
}

function classParts(label) {
  const raw = htmlDecode(label);
  const match = raw.match(/^(\d+)\)\s*(.*)$/);
  return {
    class_number: match ? intOrNull(match[1]) : null,
    class_label: raw,
    class_name: match ? clean(match[2]) : raw
  };
}

function isoFromDateText(value) {
  const parsed = new Date(clean(value));
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toISOString().slice(0, 10);
}

function dowNumber(dateText) {
  const parsed = new Date(clean(dateText));
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.getDay() + 1;
}

function normalizeTime(value) {
  const raw = clean(value).toLowerCase();
  const match = raw.match(/^(\d{1,2})(?::(\d{2}))?\s*([ap])m?$/i);
  if (!match) return "";
  let hour = Number(match[1]);
  const minute = Number(match[2] || "0");
  const meridiem = match[3].toLowerCase();
  if (meridiem === "p" && hour !== 12) hour += 12;
  if (meridiem === "a" && hour === 12) hour = 0;
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00`;
}

function displayTime(value) {
  const raw = clean(value).toLowerCase();
  const match = raw.match(/^(\d{1,2})(?::(\d{2}))?\s*([ap])m?$/i);
  if (!match) return raw ? "check time" : "check time";
  return `${Number(match[1])}:${match[2] || "00"} ${match[3].toUpperCase()}M`;
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

async function upsert(tableName, mergeFields, rows) {
  if (!rows.length) return { seen: 0, changed: 0 };
  let changed = 0;
  for (let index = 0; index < rows.length; index += 10) {
    const batch = rows.slice(index, index + 10);
    await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
      method: "PATCH",
      body: JSON.stringify({
        performUpsert: { fieldsToMergeOn: mergeFields },
        records: batch.map((fields) => ({ fields })),
        typecast: true
      })
    });
    changed += batch.length;
  }
  return { seen: rows.length, changed };
}

async function createMissing(tableName, keyField, rows) {
  if (!rows.length) return { seen: 0, changed: 0 };
  const existing = await recordIdMap(tableName, keyField);
  const missing = rows.filter((row) => {
    const key = clean(row[keyField]);
    return key && !existing.has(key);
  });
  let changed = 0;
  for (let index = 0; index < missing.length; index += 10) {
    const batch = missing.slice(index, index + 10);
    await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent(tableName)}`, {
      method: "POST",
      body: JSON.stringify({
        records: batch.map((fields) => ({ fields })),
        typecast: true
      })
    });
    changed += batch.length;
  }
  return { seen: rows.length, changed };
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

async function recordIdMap(tableName, fieldName, params = {}) {
  const records = await listAll(tableName, params);
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

function dowName(value) {
  return clean(value).slice(0, 3).toUpperCase();
}

async function buildCoreLinkMaps(showNo, focusDay, updateRows, classOogRows = []) {
  const classes = new Map();
  const rings = new Map();
  const ringNames = new Map();
  const dows = new Map();
  const entries = new Map();

  for (const row of [...updateRows, ...classOogRows]) {
    if (row.class_no && !classes.has(clean(row.class_no))) {
      classes.set(clean(row.class_no), {
        class_no: intOrNull(row.class_no),
        class_label: clean(row.event_name || row.class_label),
        class_name: clean(row.class_name),
        class_payout: clean(row.class_payout),
        source: "update_schedule"
      });
    }
    if (row.ring_no && !rings.has(clean(row.ring_no))) {
      rings.set(clean(row.ring_no), {
        ring_no: intOrNull(row.ring_no),
        ring_name: clean(row.ring_name || row.ring),
        source: "update_schedule"
      });
    }
    const ringName = clean(row.ring_name || row.ring);
    if (ringName && !ringNames.has(ringName)) {
      ringNames.set(ringName, { ring_name: ringName });
    }
    const dow = dowName(row.date_text);
    if (dow && !dows.has(dow)) {
      dows.set(dow, { Name: dow });
    }
    if (row.entry_no && !entries.has(clean(row.entry_no))) {
      entries.set(clean(row.entry_no), {
        entry_no: intOrNull(row.entry_no),
        horse: clean(row.horse),
        rider: clean(row.rider),
        trainer: clean(row.trainer),
        source: "class_oog"
      });
    }
  }

  await upsert("shows", ["show_no"], [{ show_no: intOrNull(showNo) }]);
  await upsert("classes", ["class_no"], [...classes.values()].filter((row) => row.class_no));
  await upsert("rings", ["ring_no"], [...rings.values()].filter((row) => row.ring_no));
  await upsert("ring_names", ["ring_name"], [...ringNames.values()].filter((row) => row.ring_name));
  await upsert("dows", ["Name"], [...dows.values()].filter((row) => row.Name));
  await createMissing("entries", "entry_no", [...entries.values()].filter((row) => row.entry_no));

  return {
    shows: await recordIdMap("shows", "show_no"),
    focusShow: await recordIdMap("focus_show", "show_no", {
      filterByFormula: `AND({show_no}=${intOrNull(showNo)},IS_SAME({focus_day},'${focusDay}','day'))`
    }),
    classes: await recordIdMap("classes", "class_no"),
    rings: await recordIdMap("rings", "ring_no"),
    ringNames: await recordIdMap("ring_names", "ring_name"),
    ringDays: await recordIdMap("ring_days", "ring_day_no"),
    dows: await recordIdMap("dows", "Name"),
    entries: await recordIdMap("entries", "entry_no")
  };
}

async function writeLog({ checkName, showNo, focusDay, status = "ok", recordsSeen = 0, recordsChanged = 0, summary, payload = {} }) {
  const createdAt = new Date().toISOString();
  await airtableFetch(`https://api.airtable.com/v0/${BASE_ID}/${encodeURIComponent("wec-logs")}`, {
    method: "POST",
    body: JSON.stringify({
      fields: {
        log_key_run: `${createdAt}|local_core|${checkName}`,
        created_at: createdAt,
        log_type: "heartbeat",
        workflow_lanes: "Core",
        check_name: checkName,
        show_no: showNo,
        focus_day: focusDay,
        status,
        records_seen: recordsSeen,
        records_changed: recordsChanged,
        summary,
        payload_json: JSON.stringify(payload).slice(0, 90000)
      },
      typecast: true
    })
  });
}

async function runRingGroups({ showNo, focusDay }) {
  const { spawnSync } = require("node:child_process");
  const result = spawnSync(process.execPath, [
    "docs/horseshowing/sync-airtable-ring-groups.js",
    "--show-no",
    String(showNo),
    "--focus-day",
    String(focusDay),
  ], {
    cwd: process.cwd(),
    env: process.env,
    encoding: "utf8",
  });

  if (result.status !== 0) {
    throw new Error(`sync-airtable-ring-groups failed: ${result.stderr || result.stdout}`);
  }

  return JSON.parse(result.stdout);
}

async function hsFetch(path, { showNo, method = "GET", body = "" } = {}) {
  const response = await fetch(`${HORSESHOWING_BASE}${path}`, {
    method,
    headers: {
      accept: path.endsWith(".php") ? "*/*" : "application/json, text/javascript, */*; q=0.01",
      "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
      cookie: cookieJar || `HscomShowNo=${showNo}`,
      origin: HORSESHOWING_BASE,
      referer: `${HORSESHOWING_BASE}/schedule.php`,
      "x-requested-with": "XMLHttpRequest",
      "user-agent": "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Mobile Safari/537.36"
    },
    body: method === "POST" ? body : undefined
  });
  const text = await response.text();
  const setCookie = response.headers.get("set-cookie");
  if (setCookie) {
    const cookies = setCookie
      .split(/,(?=[^;,]+=)/)
      .map((item) => item.split(";")[0])
      .filter(Boolean);
    const merged = new Map((cookieJar || "").split(";").map((item) => item.trim().split("=")).filter((item) => item.length === 2));
    for (const cookie of cookies) {
      const [key, ...rest] = cookie.split("=");
      merged.set(key.trim(), rest.join("=").trim());
    }
    cookieJar = [...merged.entries()].map(([key, value]) => `${key}=${value}`).join("; ");
  }
  if (!response.ok) throw new Error(`${path} HTTP ${response.status}: ${text.slice(0, 300)}`);
  return text;
}

async function bootstrapHorseShowing(showNo) {
  await hsFetch(`/show.php?show=${encodeURIComponent(showNo)}`, { showNo });
  await hsFetch("/schedule.php", { showNo });
}

function parseRingDays(raw, showNo) {
  const payload = JSON.parse(raw || "[]");
  const rows = [];
  for (const ring of Array.isArray(payload) ? payload : []) {
    for (const day of ring.ring_days || []) {
      rows.push({
        show_no_text: String(showNo),
        ring_no_value: intOrNull(ring.ring_no),
        ring_day_no: clean(day.ring_day_no),
        ring_name: clean(ring.name),
        date_text: clean(day.date),
        iso_date: isoFromDateText(day.date),
        days: dowNumber(day.date)
      });
    }
  }
  return rows;
}

function parseUpdateSchedule(raw, showNo, ringDay) {
  const rows = [];
  const tags = raw.match(/<h3\b[^>]*>/gi) || [];
  tags.forEach((tag, index) => {
    const a = attrs(tag);
    const parts = classParts(a["data-name"]);
    rows.push({
      show_no: intOrNull(a["data-show"]) || intOrNull(showNo),
      focus_day: ringDay.iso_date,
      days: intOrNull(ringDay.ring_day_no),
      ring_no: ringDay.ring_no_value,
      ring_name: ringDay.ring_name,
      date_text: ringDay.date_text,
      iso_date: ringDay.iso_date,
      class_no: intOrNull(a["data-class"]),
      event_id: intOrNull(a.id),
      event_name: parts.class_label,
      class_payout: (parts.class_name.match(/\$[\d,]+/) || [""])[0],
      class_name: parts.class_name.replace(/^\$[\d,]+\s*/, ""),
      time_text: clean(a["data-time"]),
      time: normalizeTime(a["data-time"]),
      entry_count: intOrNull(a["data-n_entries"]),
      event_type: intOrNull(a["data-re_type"]),
      oc_id: intOrNull(a["data-oc_id"]),
      live_flag: intOrNull(a["data-live"]),
      mirror_update_schedule_key: `${showNo}|${ringDay.ring_day_no}|${intOrNull(a["data-class"])}`,
      source: "update_schedule",
      class_order_local: index + 1
    });
  });
  return rows.filter((row) => row.class_no);
}

function parseCounts(raw, showNo, focusDay) {
  const rows = [];
  for (const match of raw.matchAll(/<tr[\s\S]*?<\/tr>/gi)) {
    const tr = match[0];
    const link = tr.match(/<[^>]*class="[^"]*\blink\b[^"]*"[^>]*>/i);
    if (!link) continue;
    const a = attrs(link[0]);
    const entryText = clean((tr.match(/entries_cell[\s\S]*?<[^>]+>([^<]+)/i) || [])[1]);
    const parts = classParts(`${a["data-num"] || ""}) ${a["data-name"] || ""}`);
    rows.push({
      show_no: intOrNull(showNo),
      focus_day: focusDay,
      class_no: intOrNull(a["data-class"]),
      class_number: parts.class_number,
      class_name: clean(a["data-name"]),
      entry_count: intOrNull(entryText),
      mirror_class_key: `${showNo}|${intOrNull(a["data-class"])}`
    });
  }
  return rows.filter((row) => row.class_no);
}

function parseClassOog(raw, classNo, context) {
  const orderStatus = clean((raw.match(/<div id="order_option"><b>(.*?)<\/b><\/div>/i) || [])[1]);
  const table = (raw.match(/<div class="lg">[\s\S]*?<table[\s\S]*?<\/table>/i) || raw.match(/<table[\s\S]*?<\/table>/i) || [""])[0];
  const rows = [];
  let rowIndex = 0;
  for (const match of table.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi)) {
    const cells = [...match[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)].map((cell) => htmlDecode(cell[1].replace(/<[^>]+>/g, " ")));
    if (cells.length < 4) continue;
    rowIndex += 1;
    rows.push({
      show_no: intOrNull(context.show_no),
      ring: context.ring_name,
      ring_no: context.ring_no,
      focus_day: context.focus_day,
      days: context.days,
      class_order: context.class_order_local || null,
      class_no: intOrNull(classNo),
      class_label: context.event_name,
      class_payout: context.class_payout,
      class_name: context.class_name,
      entry_order: intOrNull(cells[0]) || rowIndex,
      entry_no: intOrNull(cells[1]),
      horse: clean(cells[2]),
      rider: clean(cells[3]),
      trainer: clean(cells[4]),
      mirror_class_oog_key: `${intOrNull(context.show_no)}|${context.focus_day}|${context.days}|${intOrNull(classNo)}|${intOrNull(cells[1])}`,
      source: orderStatus || "class_oog"
    });
  }
  return rows.filter((row) => row.class_no && row.entry_no);
}

function helperRows(classOogRows) {
  const horses = new Map();
  const riders = new Map();
  const trainers = new Map();
  for (const row of classOogRows) {
    if (row.horse) horses.set(row.horse, { horse: row.horse, rider: row.rider, trainer: row.trainer, source: "class_oog" });
    if (row.rider) riders.set(row.rider, { rider: row.rider, horse: row.horse, trainer: row.trainer, source: "class_oog" });
    if (row.trainer) trainers.set(row.trainer, { trainer: row.trainer, source: "class_oog" });
  }
  return { horses: [...horses.values()], riders: [...riders.values()], trainers: [...trainers.values()] };
}

async function main() {
  const showNo = argValue("--show-no", process.env.WEC_SHOW_NO || "14906");
  const focusDay = argValue("--focus-day", process.env.WEC_FOCUS_DAY || "");
  if (!showNo || !focusDay) throw new Error("--show-no and --focus-day are required");

  await bootstrapHorseShowing(showNo);
  const ringDays = parseRingDays(await hsFetch("/get_ring_days.php", { showNo }), showNo)
    .filter((row) => row.iso_date === focusDay);
  const ringDayResult = await upsert("ring_days", ["ring_day_no"], ringDays.map((row) => ({
    ring_day_no: row.ring_day_no,
    date_text: row.date_text
  })));
  await writeLog({
    checkName: "sync-ring-days",
    showNo,
    focusDay,
    recordsSeen: ringDays.length,
    recordsChanged: ringDayResult.changed,
    summary: `local get_ring_days rows=${ringDays.length}`,
    payload: { ring_days: ringDays }
  });

  const updateRows = [];
  const classOogRows = [];
  for (const ringDay of ringDays) {
    const raw = await hsFetch("/update_schedule.php", {
      showNo,
      method: "POST",
      body: new URLSearchParams({ show_no: showNo, ring_day_no: ringDay.ring_day_no }).toString()
    });
    const parsedUpdateRows = parseUpdateSchedule(raw, showNo, ringDay);
    updateRows.push(...parsedUpdateRows);
    for (const row of parsedUpdateRows) {
      const rawOog = await hsFetch(`/class_oog.php?class_no=${encodeURIComponent(row.class_no)}`, { showNo });
      classOogRows.push(...parseClassOog(rawOog, row.class_no, row));
    }
  }
  const updateLinks = await buildCoreLinkMaps(showNo, focusDay, updateRows);
  const updateResult = await upsert("update_schedule", ["show_no", "days", "class_no"], updateRows.map((row) => {
    const { class_order_local, ...fields } = row;
    return {
      ...fields,
      shows: linkedRecord(updateLinks.shows, row.show_no || showNo),
      focus_show: linkedRecord(updateLinks.focusShow, row.show_no || showNo),
      ring_days: linkedRecord(updateLinks.ringDays, row.days),
      rings: linkedRecord(updateLinks.rings, row.ring_no),
      ring_names: linkedRecord(updateLinks.ringNames, row.ring_name),
      classes: linkedRecord(updateLinks.classes, row.class_no),
      dows: linkedRecord(updateLinks.dows, dowName(row.date_text))
    };
  }));
  await writeLog({
    checkName: "core_update_schedule",
    showNo,
    focusDay,
    recordsSeen: updateRows.length,
    recordsChanged: updateResult.changed,
    summary: `local update_schedule rows=${updateRows.length} ring_days=${ringDays.length}`,
    payload: { ring_days: ringDays.length, rows: updateRows.length }
  });

  const ringGroupsResult = await runRingGroups({ showNo, focusDay });
  await writeLog({
    checkName: "wec_print_meta",
    showNo,
    focusDay,
    recordsSeen: ringGroupsResult.ring_groups,
    recordsChanged: ringGroupsResult.created + ringGroupsResult.updated,
    summary: `ring_groups=${ringGroupsResult.ring_groups}; wec_print_meta=${ringGroupsResult.wec_print_meta}`,
    payload: {
      ring_groups: ringGroupsResult.ring_groups,
      created: ringGroupsResult.created,
      updated: ringGroupsResult.updated,
      portrait_summary: ringGroupsResult.portrait_summary,
      landscape_summary: ringGroupsResult.landscape_summary
    }
  });

  const countsRows = parseCounts(await hsFetch("/counts.php", { showNo }), showNo, focusDay);
  const countsResult = await upsert("counts", ["show_no", "class_no"], countsRows);
  await writeLog({
    checkName: "core_counts",
    showNo,
    focusDay,
    recordsSeen: countsRows.length,
    recordsChanged: countsResult.changed,
    summary: `local counts rows=${countsRows.length}`,
    payload: { rows: countsRows.length }
  });

  const classOogLinks = await buildCoreLinkMaps(showNo, focusDay, updateRows, classOogRows);
  const classOogResult = await upsert("class_oog", ["mirror_class_oog_key"], classOogRows.map((row) => ({
    ...row,
    shows: linkedRecord(classOogLinks.shows, row.show_no || showNo),
    focus_show: linkedRecord(classOogLinks.focusShow, row.show_no || showNo),
    classes: linkedRecord(classOogLinks.classes, row.class_no),
    rings: linkedRecord(classOogLinks.rings, row.ring_no),
    ring_names: linkedRecord(classOogLinks.ringNames, row.ring),
    ring_days: linkedRecord(classOogLinks.ringDays, row.days),
    entries: linkedRecord(classOogLinks.entries, row.entry_no)
  })));
  await writeLog({
    checkName: "core_class_oog",
    showNo,
    focusDay,
    recordsSeen: classOogRows.length,
    recordsChanged: classOogResult.changed,
    summary: `local class_oog classes=${updateRows.length} entries=${classOogRows.length}`,
    payload: { classes: updateRows.length, entries: classOogRows.length }
  });

  const helpers = helperRows(classOogRows);
  await upsert("horses", ["horse"], helpers.horses);
  await upsert("riders", ["rider"], helpers.riders);
  await upsert("trainers", ["trainer"], helpers.trainers);

  console.log(JSON.stringify({
    ok: true,
    show_no: showNo,
    focus_day: focusDay,
    ring_days: ringDays.length,
    update_schedule: updateRows.length,
    counts: countsRows.length,
    class_oog: classOogRows.length,
    helpers: {
      horses: helpers.horses.length,
      riders: helpers.riders.length,
      trainers: helpers.trainers.length
    }
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
