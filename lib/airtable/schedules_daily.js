/**
 * Airtable Automation Script (FULL DROP v5.2)
 * schedule ingest → UPSERT + AUTO GOTCHA MARK + GOTCHA DELETE (SAFE)
 *
 * Change (v5.2):
 *  - Preserves all v5.1 behavior.
 *  - Adds optional input:
 *      ping_view (string, OPTIONAL)
 *  - ping_view is READ-ONLY in this script:
 *      - does NOT affect target_view logic
 *      - does NOT affect orphan/gotcha logic
 *      - does NOT affect create/update scope
 *      - is only surfaced as reporting so downstream automations can use a separate Airtable view for polling
 *
 * Existing behavior preserved:
 *  - NEVER hard-fail the automation run.
 *  - Always sets output:
 *      ok   = true|false
 *      done = true
 *    (optional) error = short message
 *
 * Existing v5 behavior preserved:
 *   - is_gotcha checkbox workflow
 *   - Marks + deletes:
 *       A) Duplicate keys (keeps 1 winner)
 *       B) Blank key rows
 *       C) Orphans in target_view that are NOT in today’s payload
 *   - Deletes only records visible in gotcha_view (default "gotcha")
 *
 * Inputs:
 *   - target_table      (string, REQUIRED)
 *   - target_view       (string, REQUIRED)  (scope for existing lookup + orphan detection)
 *   - schedule_endpoint (string, REQUIRED)
 *   - gotcha_view       (string, OPTIONAL) default "gotcha"
 *   - ping_view         (string, OPTIONAL) reporting only; no effect on gotcha/upsert scope
 *
 * Key:
 *   - class_groupxclasses_id
 *
 * Writes when fields exist + writable:
 *   - last_updated_at (dateTime) = nowIso
 *   - record_state (text)        = "new" | "existing"
 *   - run_tag (text)             = sqlDate (YYYY-MM-DD)
 *   - is_gotcha (checkbox)       = false on good rows, true on gotcha rows
 *   - gotcha_reason (text)       = "dup" | "blank_key" | "orphan_not_in_payload"
 */

// SAFE OUTPUT (works even if main() throws early)
function SAFE_OUT(k, v) {
  try {
    if (typeof output === "undefined" || !output || typeof output.set !== "function") return;
    output.set(k, v);
  } catch {}
}

async function main() {
  //////////////////////
  // 0) Inputs
  //////////////////////
  let cfg = {};
  if (typeof input !== "undefined" && input && typeof input.config === "function") cfg = input.config();

  const targetTableName = cfg.target_table;
  const targetViewName = cfg.target_view;
  const schedule_endpoint = cfg.schedule_endpoint;
  const gotchaViewName = (cfg.gotcha_view && String(cfg.gotcha_view).trim()) || "gotcha";
  const pingViewName = cfg.ping_view && String(cfg.ping_view).trim() ? String(cfg.ping_view).trim() : "";

  if (!targetTableName || typeof targetTableName !== "string") throw new Error("Missing required input: target_table");
  if (!targetViewName || typeof targetViewName !== "string") throw new Error("Missing required input: target_view");
  if (!schedule_endpoint || typeof schedule_endpoint !== "string") throw new Error("Missing required input: schedule_endpoint");

  //////////////////////
  // OUTPUT HELPERS
  //////////////////////
  function outSet(k, v) {
    SAFE_OUT(k, v);
  }

  outSet("target_table", targetTableName);
  outSet("target_view", targetViewName);
  outSet("gotcha_view", gotchaViewName);
  outSet("ping_view", pingViewName || "");
  outSet("schedule_endpoint", schedule_endpoint);

  //////////////////////
  // CONFIG
  //////////////////////
  const BATCH_SIZE = 50;

  const UNIQUE_KEY_FIELD = "class_groupxclasses_id";
  const LAST_UPDATED_FIELD = "last_updated_at";
  const RECORD_STATE_FIELD = "record_state";
  const RUN_TAG_FIELD = "run_tag";

  const IS_GOTCHA_FIELD = "is_gotcha";         // checkbox (user added)
  const GOTCHA_REASON_FIELD = "gotcha_reason"; // single line text (optional but recommended)

  // HARD SAFETY CAPS
  const MAX_GOTCHA_MARK = 8000;
  const MAX_GOTCHA_DELETE = 8000;

  //////////////////////
  // HELPERS
  //////////////////////
  function isObj(v) {
    return v && typeof v === "object" && !Array.isArray(v);
  }
  function normalizeKey(v) {
    if (v === null || v === undefined) return "";
    return String(v).trim();
  }
  function pickFirst(...vals) {
    for (const v of vals) if (v !== undefined && v !== null && v !== "") return v;
    return undefined;
  }
  function toISODateOnly(v) {
    if (v === null || v === undefined) return undefined;
    if (v instanceof Date) return v.toISOString().slice(0, 10);
    if (typeof v !== "string") return undefined;
    return v.includes("T") ? v.split("T")[0] : v;
  }
  function parseNum(v) {
    if (v === null || v === undefined || v === "") return undefined;
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
  }
  function chunk(arr, size) {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
  }
  function dateToMs(v) {
    if (!v) return 0;
    if (v instanceof Date) return v.getTime();
    if (typeof v === "string") {
      const ms = Date.parse(v);
      return Number.isFinite(ms) ? ms : 0;
    }
    return 0;
  }

  function compareSqlDate(left, right) {
    const a = toISODateOnly(left);
    const b = toISODateOnly(right);
    if (!a || !b) return 0;
    if (a < b) return -1;
    if (a > b) return 1;
    return 0;
  }

  function extractScheduleDefaultInfo(payload) {
    const show = isObj(payload?.show) ? payload.show : {};
    const defaultAppSqlDateIs = toISODateOnly(pickFirst(payload?.show_date, payload?.showDate));
    const showAppSqlStartDate = toISODateOnly(pickFirst(show.start_date, payload?.start_date));
    const showAppSqlEndDate = toISODateOnly(pickFirst(show.end_date, payload?.end_date));
    const validDates = Array.isArray(payload?.show_days_list)
      ? payload.show_days_list.map((item) => toISODateOnly(item?.date)).filter(Boolean)
      : [];

    return {
      defaultAppSqlDateIs,
      showAppSqlStartDate,
      showAppSqlEndDate,
      validDates,
    };
  }

  function isValidScheduleDate(candidateDate, scheduleInfo) {
    const date = toISODateOnly(candidateDate);
    if (!date) return false;

    if (scheduleInfo.validDates.length) {
      return scheduleInfo.validDates.includes(date);
    }

    if (scheduleInfo.showAppSqlStartDate && compareSqlDate(date, scheduleInfo.showAppSqlStartDate) < 0) {
      return false;
    }

    if (scheduleInfo.showAppSqlEndDate && compareSqlDate(date, scheduleInfo.showAppSqlEndDate) > 0) {
      return false;
    }

    return true;
  }

  function buildDerivedScheduleEndpoint(urlText, showId, dateText) {
    const u = new URL(urlText);
    u.searchParams.set("show_id", String(showId));
    u.searchParams.set("date", String(dateText));
    return u.toString();
  }

  function getTableMeta(tbl) {
    const fieldsByName = new Map(tbl.fields.map((f) => [f.name, f]));
    const writableFields = tbl.fields.filter((f) => !f.isComputed);
    const writableByName = new Map(writableFields.map((f) => [f.name, f]));
    const writableNames = new Set(writableFields.map((f) => f.name));
    return { fieldsByName, writableByName, writableNames };
  }

  function coerceForField(field, v) {
    if (!field) return undefined;
    if (v === undefined || v === null) return undefined;

    const isEmptyString = typeof v === "string" && v === "";

    switch (field.type) {
      case "singleLineText":
      case "multilineText":
      case "richText":
      case "url":
      case "email":
      case "phoneNumber":
        return String(v);

      case "number":
      case "currency":
      case "percent":
      case "rating":
      case "duration": {
        if (isEmptyString) return undefined;
        const n = parseNum(v);
        return n === undefined ? undefined : n;
      }

      case "checkbox":
        if (isEmptyString) return undefined;
        return Boolean(v);

      case "date": {
        if (isEmptyString) return undefined;
        const d = toISODateOnly(v);
        return d ? d : undefined;
      }

      case "dateTime": {
        if (isEmptyString) return undefined;
        if (v instanceof Date) return v.toISOString();
        if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}T/.test(v)) return v;
        return undefined;
      }

      case "singleSelect":
        if (isEmptyString) return undefined;
        return { name: String(v) };

      case "multipleSelects":
        if (isEmptyString) return undefined;
        if (Array.isArray(v)) {
          const arr = v.filter((x) => x !== null && x !== undefined && x !== "");
          return arr.length ? arr.map((x) => ({ name: String(x) })) : undefined;
        }
        return [{ name: String(v) }];

      default:
        return v;
    }
  }

  function buildWritableFields(flat, meta, tbl) {
    const out = {};
    for (const [k, raw] of Object.entries(flat)) {
      if (!meta.writableNames.has(k)) continue;
      const field = meta.writableByName.get(k);
      const coerced = coerceForField(field, raw);
      if (coerced === undefined) continue;
      out[k] = coerced;
    }

    // best-effort: ensure primary field is populated if writable
    try {
      const pf = tbl.primaryField;
      if (pf && !pf.isComputed && meta.writableNames.has(pf.name) && out[pf.name] === undefined) {
        // no-op here; caller may set primary explicitly
      }
    } catch {}

    return out;
  }

  function ensureWritablePrimary(tbl, meta, fields, fallbackVal) {
    try {
      const pf = tbl.primaryField;
      if (!pf) return;
      if (pf.isComputed) return;
      if (!meta.writableNames.has(pf.name)) return;
      if (fields[pf.name] !== undefined) return;
      if (fallbackVal === undefined || fallbackVal === null || fallbackVal === "") return;
      fields[pf.name] = String(fallbackVal);
    } catch {}
  }

  function deriveShowDayKey(showId, showDateIso) {
    const d = toISODateOnly(showDateIso);
    if (!d || showId === undefined || showId === null || showId === "") return undefined;
    return `${showId}-${d.replaceAll("-", "")}`;
  }
  function deriveShowRingKey(showId, ringNumberRaw) {
    const rn = parseNum(ringNumberRaw);
    if (showId === undefined || showId === null || showId === "" || rn === undefined) return undefined;
    return `${showId}-${rn}`;
  }

  function mergePreserveJoinKeys(prev, next) {
    if (!prev) return next;
    const merged = { ...prev, ...next };

    if (prev.show_ring_key && !next.show_ring_key) merged.show_ring_key = prev.show_ring_key;
    if (prev.show_day_key && !next.show_day_key) merged.show_day_key = prev.show_day_key;

    if (prev.ring_number !== undefined && next.ring_number === undefined) merged.ring_number = prev.ring_number;
    if (prev.grouped_class !== undefined && next.grouped_class === undefined) merged.grouped_class = prev.grouped_class;

    return merged;
  }

  function mergeGroupOntoClass(classRow, groupRow) {
    if (!groupRow) return classRow;
    const merged = { ...groupRow, ...classRow };

    if (groupRow.group_name && !merged.group_name) merged.group_name = groupRow.group_name;
    if (groupRow.class_group_sequence !== undefined && merged.class_group_sequence === undefined)
      merged.class_group_sequence = groupRow.class_group_sequence;

    if (groupRow.group_has_warmup !== undefined && merged.group_has_warmup === undefined)
      merged.group_has_warmup = groupRow.group_has_warmup;

    if (groupRow.is_open_card_warmup !== undefined && merged.is_open_card_warmup === undefined)
      merged.is_open_card_warmup = groupRow.is_open_card_warmup;

    if (groupRow.grouped_class !== undefined && merged.grouped_class === undefined)
      merged.grouped_class = groupRow.grouped_class;

    if (groupRow.show_day_key && !merged.show_day_key) merged.show_day_key = groupRow.show_day_key;
    if (groupRow.show_ring_key && !merged.show_ring_key) merged.show_ring_key = groupRow.show_ring_key;
    if (groupRow.ring_number !== undefined && merged.ring_number === undefined) merged.ring_number = groupRow.ring_number;

    if (groupRow.show_id !== undefined && merged.show_id === undefined) merged.show_id = groupRow.show_id;
    if (groupRow.show_date && !merged.show_date) merged.show_date = groupRow.show_date;

    return merged;
  }

  async function fetchJson(url) {
    const res = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`Fetch failed (${res.status}): ${txt.slice(0, 1200)}`);
    }
    const txt = await res.text();
    try {
      return JSON.parse(txt);
    } catch {
      throw new Error(`Response was not valid JSON. First 1200 chars:\n${txt.slice(0, 1200)}`);
    }
  }

  async function resolveScheduleEndpointWithDefault(urlText) {
    const result = {
      requestedEndpoint: urlText,
      effectiveEndpoint: urlText,
      requestedDate: "",
      effectiveDate: "",
      requestedShowId: "",
      usedDefaultDate: false,
      defaultAppSqlDateIs: "",
      emptyScheduleEndpoint: "",
      appSqlDateSource: "",
    };

    let urlObj = null;
    try {
      urlObj = new URL(urlText);
    } catch {
      return result;
    }

    const requestedShowId = normalizeKey(urlObj.searchParams.get("show_id"));
    const requestedDate = toISODateOnly(urlObj.searchParams.get("date"));

    result.requestedShowId = requestedShowId;
    result.requestedDate = requestedDate || "";

    if (!requestedShowId) return result;

    const emptyEndpoint = buildDerivedScheduleEndpoint(urlText, requestedShowId, "00/00/00");
    result.emptyScheduleEndpoint = emptyEndpoint;

    const emptyPayload = await fetchJson(emptyEndpoint);
    const scheduleInfo = extractScheduleDefaultInfo(emptyPayload);
    result.defaultAppSqlDateIs = scheduleInfo.defaultAppSqlDateIs || "";

    const candidateDate = requestedDate || scheduleInfo.defaultAppSqlDateIs || "";
    const validCandidate = isValidScheduleDate(candidateDate, scheduleInfo);
    const effectiveDate = validCandidate
      ? candidateDate
      : (scheduleInfo.defaultAppSqlDateIs || candidateDate);

    result.effectiveDate = effectiveDate || "";
    result.usedDefaultDate = Boolean(effectiveDate && effectiveDate !== requestedDate);
    result.appSqlDateSource = result.usedDefaultDate ? "default_day" : (effectiveDate ? "schedule_endpoint" : "");

    if (effectiveDate) {
      result.effectiveEndpoint = buildDerivedScheduleEndpoint(urlText, requestedShowId, effectiveDate);
    }

    return result;
  }

  //////////////////////
  // 1) Resolve target table/views + meta
  //////////////////////
  const targetTable = base.getTable(targetTableName);
  if (!targetTable) throw new Error(`Table not found: ${targetTableName}`);

  const targetView = targetTable.getView(targetViewName);
  if (!targetView) throw new Error(`View not found: ${targetViewName} (table: ${targetTableName})`);

  const gotchaView = targetTable.getView(gotchaViewName);
  if (!gotchaView) throw new Error(`GOTCHA view not found: ${gotchaViewName} (table: ${targetTableName})`);

  let pingView = null;
  if (pingViewName) {
    pingView = targetTable.getView(pingViewName);
    if (!pingView) throw new Error(`PING view not found: ${pingViewName} (table: ${targetTableName})`);
  }

  if (normalizeKey(gotchaViewName) === normalizeKey(targetViewName)) {
    throw new Error(`Safety abort: gotcha_view must NOT equal target_view (${gotchaViewName})`);
  }

  const meta = getTableMeta(targetTable);

  if (!meta.fieldsByName.has(UNIQUE_KEY_FIELD)) {
    throw new Error(`Missing required field in table: ${UNIQUE_KEY_FIELD}`);
  }
  if (!meta.fieldsByName.has(IS_GOTCHA_FIELD)) {
    throw new Error(`Missing required field in table: ${IS_GOTCHA_FIELD} (checkbox)`);
  }

  //////////////////////
  // 1A) Optional ping view pre-run reporting only
  //////////////////////
  if (pingView) {
    const pingPre = await pingView.selectRecordsAsync({ fields: [UNIQUE_KEY_FIELD] });
    outSet("ping_view_count_before", pingPre.records.length);

    const pingKeysBefore = new Set();
    for (const r of pingPre.records) {
      const k = normalizeKey(r.getCellValue(UNIQUE_KEY_FIELD));
      if (k) pingKeysBefore.add(k);
    }
    outSet("ping_key_count_before", pingKeysBefore.size);
  } else {
    outSet("ping_view_count_before", 0);
    outSet("ping_key_count_before", 0);
  }

  //////////////////////
  // 2) FETCH schedule payload (validate first)
  //////////////////////
  let resolvedSchedule = {
    requestedEndpoint: schedule_endpoint,
    effectiveEndpoint: schedule_endpoint,
    requestedDate: "",
    effectiveDate: "",
    requestedShowId: "",
    usedDefaultDate: false,
    defaultAppSqlDateIs: "",
    emptyScheduleEndpoint: "",
  };

  try {
    resolvedSchedule = await resolveScheduleEndpointWithDefault(schedule_endpoint);
  } catch (err) {
    outSet("schedule_default_resolution_error", String(err?.message || err).slice(0, 500));
  }

  const effectiveScheduleEndpoint = resolvedSchedule.effectiveEndpoint || schedule_endpoint;
  outSet("effective_schedule_endpoint", effectiveScheduleEndpoint);
  outSet("requested_schedule_date", resolvedSchedule.requestedDate || "");
  outSet("effective_schedule_date", resolvedSchedule.effectiveDate || "");
  outSet("default_app_sql_date_is", resolvedSchedule.defaultAppSqlDateIs || "");
  outSet("set_to_default_app_sql_date", resolvedSchedule.usedDefaultDate);
  outSet("app_sql_date_source", resolvedSchedule.appSqlDateSource || "");
  outSet("empty_schedule_endpoint", resolvedSchedule.emptyScheduleEndpoint || "");

  const payload = await fetchJson(effectiveScheduleEndpoint);
  if (!payload || (!isObj(payload) && !Array.isArray(payload))) {
    throw new Error("Unexpected payload shape: expected object/array JSON.");
  }

  //////////////////////
  // 3) Derive sqlDate (YYYY-MM-DD) from endpoint ?date= OR payload fallback
  //////////////////////
  let urlShowId = "";
  let urlDate = "";
  try {
    const u = new URL(effectiveScheduleEndpoint);
    urlShowId = u.searchParams.get("show_id") || "";
    urlDate = u.searchParams.get("date") || "";
  } catch {}

  const payloadShowId = parseNum(pickFirst(payload?.show?.show_id, payload?.show_id, payload?.showId, urlShowId));
  const payloadShowDate = toISODateOnly(pickFirst(payload?.show_date, payload?.showDate, payload?.date, urlDate));
  const sqlDate = payloadShowDate || "";

  outSet("payload_show_id", String(payloadShowId ?? ""));
  outSet("sqlDate", sqlDate);

  //////////////////////
  // 4) WALK payload → build payloadKeys + groups/classes maps (today’s truth)
  //////////////////////
  const groupsById = new Map();  // key: class_group_id string
  const classesById = new Map(); // key: class_groupxclasses_id string

  function liftContext(node, ctx) {
    const next = { ...ctx };

    if (isObj(node.show)) next.show_id = pickFirst(next.show_id, node.show.show_id, node.show.showId, node.show.id);
    next.show_id = pickFirst(next.show_id, node.show_id, node.showId, payloadShowId);

    const d = pickFirst(node.show_date, node.showDate, node.date, node.show_day_date, payloadShowDate);
    const iso = toISODateOnly(d);
    if (iso) next.show_date = iso;

    // ring_number (FIX: never from node.ring string)
    if (isObj(node.ring)) {
      next.ring_number = pickFirst(next.ring_number, node.ring.ring_number, node.ring.ringNumber, node.ring.number);
    }
    next.ring_number = pickFirst(next.ring_number, node.ring_number, node.ringNumber, node.ring_no, node.ringNo);

    return next;
  }

  function isClassNode(node) {
    if (!isObj(node)) return false;
    const hasKey = node.class_groupxclasses_id !== undefined || node.classGroupXClassesId !== undefined;
    const hasClassId = node.class_id !== undefined || node.classId !== undefined || node.id !== undefined;
    return Boolean(hasKey && hasClassId);
  }

  function walk(node, ctx) {
    if (Array.isArray(node)) {
      for (const it of node) walk(it, ctx);
      return;
    }
    if (!isObj(node)) return;

    const next = liftContext(node, ctx);

    const show_id = parseNum(pickFirst(next.show_id, payloadShowId));
    const show_date = toISODateOnly(pickFirst(next.show_date, payloadShowDate));
    const ring_number = parseNum(next.ring_number);

    const show_day_key = deriveShowDayKey(show_id, show_date);
    const show_ring_key = deriveShowRingKey(show_id, ring_number);

    const crdObj = isObj(node.class_related_data)
      ? node.class_related_data
      : isObj(node.classRelatedData)
      ? node.classRelatedData
      : undefined;

    // GROUP ROW
    const class_group_id = parseNum(pickFirst(node.class_group_id, node.classGroupId));
    if (class_group_id !== undefined) {
      const groupRow = {
        class_group_id,
        group_name: pickFirst(node.group_name, node.groupName, node.name),
        class_group_sequence: parseNum(pickFirst(node.class_group_sequence, node.group_sequence, node.groupSequence)),
        show_id,
        show_date,
        show_day_key,
        show_ring_key,
        ring_number,
        group_has_warmup: pickFirst(node.group_has_warmup, node.groupHasWarmup),
        is_open_card_warmup: pickFirst(node.is_open_card_warmup, node.isOpenCardWarmup),
        grouped_class: pickFirst(crdObj?.grouped_class, crdObj?.groupedClass, node.grouped_class, node.groupedClass),
      };

      const gk = normalizeKey(class_group_id);
      groupsById.set(gk, mergePreserveJoinKeys(groupsById.get(gk), groupRow));
    }

    // CLASS ROW
    if (isClassNode(node)) {
      const class_groupxclasses_id = parseNum(pickFirst(node.class_groupxclasses_id, node.classGroupXClassesId));
      if (class_groupxclasses_id !== undefined) {
        const classObj = isObj(node.class) ? node.class : undefined;

        const schedule_ring_id_raw = pickFirst(
          node.schedule_ring_id,
          node.scheduleRingId,
          classObj?.schedule_ring_id,
          classObj?.scheduleRingId
        );
        const unscratched_count_raw = pickFirst(
          node.unscratched_count,
          node.unscratchedCount,
          crdObj?.unscratched_count,
          crdObj?.unscratchedCount
        );

        const ring_raw = pickFirst(
          crdObj?.ring,
          node.ring_text,
          node.ringText,
          typeof node.ring === "string" ? node.ring : undefined,
          typeof node.ring_name === "string" ? node.ring_name : undefined,
          typeof node.ringName === "string" ? node.ringName : undefined
        );

        const total_trips_raw = pickFirst(crdObj?.total_trips, crdObj?.totalTrips, node.total_trips, node.totalTrips);

        const classRow = {
          class_groupxclasses_id,
          class_group_id:
            class_group_id !== undefined ? class_group_id : parseNum(pickFirst(node.class_group_id, node.classGroupId)),
          class_id: parseNum(pickFirst(node.class_id, node.classId, node.id, classObj?.class_id, classObj?.classId)),

          class_number: parseNum(pickFirst(node.class_number, node.classNumber, node.number, classObj?.number)),
          class_name: pickFirst(node.class_name, node.className, node.name, classObj?.name),
          class_list: pickFirst(node.class_list, node.classList),
          class_type: pickFirst(node.class_type, node.classType, classObj?.class_type, classObj?.classType),
          jumper_table: pickFirst(node.jumper_table, node.jumperTable, classObj?.jumper_table, classObj?.jumperTable),
          sponsor: pickFirst(node.sponsor, classObj?.sponsor),
          schedule_sequencetype: pickFirst(
            node.schedule_sequencetype,
            node.scheduleSequenceType,
            node.sequencetype,
            node.sequence_type,
            classObj?.schedule_sequencetype,
            classObj?.scheduleSequenceType
          ),

          group_has_warmup: pickFirst(node.group_has_warmup, node.groupHasWarmup),
          is_open_card_warmup: pickFirst(node.is_open_card_warmup, node.isOpenCardWarmup),

          show_id,
          show_date,
          show_day_key,
          show_ring_key,
          ring_number,

          estimated_start_time: pickFirst(node.estimated_start_time, node.estimatedStartTime),
          start_time_default: pickFirst(node.start_time_default, node.startTimeDefault),
          estimated_end_time: pickFirst(node.estimated_end_time, node.estimatedEndTime),

          schedule_ring_id: parseNum(schedule_ring_id_raw),
          unscratched_count: parseNum(unscratched_count_raw),
          ring: ring_raw,
          total_trips: total_trips_raw,

          grouped_class: pickFirst(crdObj?.grouped_class, crdObj?.groupedClass, node.grouped_class, node.groupedClass),
        };

        const ck = normalizeKey(class_groupxclasses_id);
        classesById.set(ck, mergePreserveJoinKeys(classesById.get(ck), classRow));
      }
    }

    for (const [, v] of Object.entries(node)) {
      if (typeof v === "string") continue;
      walk(v, next);
    }
  }

  walk(payload, { show_id: payloadShowId, show_date: payloadShowDate });

  outSet("groups_unique", groupsById.size);
  outSet("classes_unique", classesById.size);

  const payloadKeySet = new Set([...classesById.keys()].filter(Boolean));
  outSet("payload_key_count", payloadKeySet.size);

  //////////////////////
  // 5) Build target_view sets (for orphan detection + preferred winners)
  //////////////////////
  const targetViewQuery = await targetView.selectRecordsAsync({
    fields: [UNIQUE_KEY_FIELD, ...(meta.fieldsByName.has(LAST_UPDATED_FIELD) ? [LAST_UPDATED_FIELD] : [])],
  });

  const targetViewIdSet = new Set(targetViewQuery.records.map((r) => r.id));

  // existingByKey winner selection inside target_view (if duplicates exist in view)
  const existingByKey = new Map(); // key -> recordId (winner)
  let existingKeyDupsInView = 0;

  for (const r of targetViewQuery.records) {
    const key = normalizeKey(r.getCellValue(UNIQUE_KEY_FIELD));
    if (!key) continue;

    if (!existingByKey.has(key)) {
      existingByKey.set(key, r.id);
      continue;
    }

    // if dup in view: keep the one with newest last_updated_at (if field exists)
    existingKeyDupsInView += 1;

    const currentWinnerId = existingByKey.get(key);
    const currentWinner = targetViewQuery.records.find((x) => x.id === currentWinnerId);

    const msA = meta.fieldsByName.has(LAST_UPDATED_FIELD) ? dateToMs(currentWinner?.getCellValue(LAST_UPDATED_FIELD)) : 0;
    const msB = meta.fieldsByName.has(LAST_UPDATED_FIELD) ? dateToMs(r.getCellValue(LAST_UPDATED_FIELD)) : 0;

    if (msB > msA) existingByKey.set(key, r.id);
  }

  outSet("existing_view_count", targetViewQuery.records.length);
  outSet("existing_key_count", existingByKey.size);
  outSet("existing_key_dups_in_view", existingKeyDupsInView);

  //////////////////////
  // 6) AUTO MARK GOTCHA (dups, blank keys, orphans in target_view not in payload)
  //////////////////////
  outSet("gotcha_mark_cap", MAX_GOTCHA_MARK);
  outSet("gotcha_delete_cap", MAX_GOTCHA_DELETE);

  // select whole table for dup scan (minimal fields)
  const scanFields = [UNIQUE_KEY_FIELD];
  if (meta.fieldsByName.has(LAST_UPDATED_FIELD)) scanFields.push(LAST_UPDATED_FIELD);
  if (meta.fieldsByName.has(RUN_TAG_FIELD)) scanFields.push(RUN_TAG_FIELD);
  if (meta.fieldsByName.has(IS_GOTCHA_FIELD)) scanFields.push(IS_GOTCHA_FIELD);
  if (meta.fieldsByName.has(GOTCHA_REASON_FIELD)) scanFields.push(GOTCHA_REASON_FIELD);

  const allQuery = await targetTable.selectRecordsAsync({ fields: scanFields });

  // map key -> record list
  const recsByKey = new Map();
  const blankKeyIds = [];

  for (const r of allQuery.records) {
    const key = normalizeKey(r.getCellValue(UNIQUE_KEY_FIELD));
    if (!key) {
      blankKeyIds.push(r.id);
      continue;
    }
    if (!recsByKey.has(key)) recsByKey.set(key, []);
    recsByKey.get(key).push(r);
  }

  // choose winner for dup sets:
  // prefer a record in target_view if present; else pick newest last_updated_at
  function chooseWinnerRecord(list) {
    if (!list || !list.length) return null;

    const preferred = list.filter((r) => targetViewIdSet.has(r.id));
    const pool = preferred.length ? preferred : list;

    let best = pool[0];
    let bestMs = meta.fieldsByName.has(LAST_UPDATED_FIELD) ? dateToMs(best.getCellValue(LAST_UPDATED_FIELD)) : 0;

    for (const r of pool) {
      const ms = meta.fieldsByName.has(LAST_UPDATED_FIELD) ? dateToMs(r.getCellValue(LAST_UPDATED_FIELD)) : 0;
      if (ms > bestMs) {
        best = r;
        bestMs = ms;
      }
    }
    return best;
  }

  const gotchaUpdates = [];
  let gotchaDupLosers = 0;
  let gotchaBlankKeys = 0;
  let gotchaOrphans = 0;

  // A) mark blank keys
  if (blankKeyIds.length) {
    gotchaBlankKeys = blankKeyIds.length;
    for (const id of blankKeyIds) {
      const fields = {};
      if (meta.writableNames.has(IS_GOTCHA_FIELD)) fields[IS_GOTCHA_FIELD] = true;
      if (meta.writableNames.has(GOTCHA_REASON_FIELD)) fields[GOTCHA_REASON_FIELD] = "blank_key";
      gotchaUpdates.push({ id, fields });
    }
  }

  // B) mark duplicates (losers only)
  for (const [key, list] of recsByKey.entries()) {
    if (!list || list.length <= 1) continue;

    const winner = chooseWinnerRecord(list);
    if (!winner) continue;

    for (const r of list) {
      if (r.id === winner.id) continue;

      const fields = {};
      if (meta.writableNames.has(IS_GOTCHA_FIELD)) fields[IS_GOTCHA_FIELD] = true;
      if (meta.writableNames.has(GOTCHA_REASON_FIELD)) fields[GOTCHA_REASON_FIELD] = "dup";

      gotchaUpdates.push({ id: r.id, fields });
      gotchaDupLosers += 1;
    }
  }

  // C) mark orphans IN target_view that are NOT in today's payload
  //    (this is how you get back to the expected payload count)
  for (const r of targetViewQuery.records) {
    const key = normalizeKey(r.getCellValue(UNIQUE_KEY_FIELD));
    if (!key) continue;
    if (payloadKeySet.has(key)) continue;

    const fields = {};
    if (meta.writableNames.has(IS_GOTCHA_FIELD)) fields[IS_GOTCHA_FIELD] = true;
    if (meta.writableNames.has(GOTCHA_REASON_FIELD)) fields[GOTCHA_REASON_FIELD] = "orphan_not_in_payload";

    gotchaUpdates.push({ id: r.id, fields });
    gotchaOrphans += 1;
  }

  outSet("gotcha_mark_blank_key_count", gotchaBlankKeys);
  outSet("gotcha_mark_dup_loser_count", gotchaDupLosers);
  outSet("gotcha_mark_orphan_count", gotchaOrphans);
  outSet("gotcha_mark_total", gotchaUpdates.length);

  if (gotchaUpdates.length > 0) {
    if (gotchaUpdates.length > MAX_GOTCHA_MARK) {
      outSet(
        "abort_reason",
        `Safety abort: gotcha_mark_total (${gotchaUpdates.length}) exceeds MAX_GOTCHA_MARK (${MAX_GOTCHA_MARK})`
      );
      throw new Error("Abort: GOTCHA mark safety cap exceeded.");
    }

    // apply mark updates
    for (const batch of chunk(gotchaUpdates, BATCH_SIZE)) {
      await targetTable.updateRecordsAsync(batch);
    }
  }

  //////////////////////
  // 7) DELETE GOTCHA VIEW RECORDS (after marking)
  //////////////////////
  const gotchaQuery = await gotchaView.selectRecordsAsync();
  const gotchaIds = gotchaQuery.records.map((r) => r.id);

  outSet("gotcha_view_count_after_mark", gotchaIds.length);

  if (gotchaIds.length > 0) {
    if (gotchaIds.length > MAX_GOTCHA_DELETE) {
      outSet(
        "abort_reason",
        `Safety abort: gotcha_view_count (${gotchaIds.length}) exceeds MAX_GOTCHA_DELETE (${MAX_GOTCHA_DELETE})`
      );
      throw new Error("Abort: GOTCHA delete safety cap exceeded.");
    }

    let deleted = 0;
    for (const batch of chunk(gotchaIds, BATCH_SIZE)) {
      await targetTable.deleteRecordsAsync(batch);
      deleted += batch.length;
    }
    outSet("gotcha_deleted_count", deleted);
  } else {
    outSet("gotcha_deleted_count", 0);
  }

  //////////////////////
  // 8) Refresh existingByKey AFTER gotcha deletes (clean view state)
  //////////////////////
  const existingQuery2 = await targetView.selectRecordsAsync({ fields: [UNIQUE_KEY_FIELD] });
  const existingByKey2 = new Map();
  for (const r of existingQuery2.records) {
    const key = normalizeKey(r.getCellValue(UNIQUE_KEY_FIELD));
    if (!key) continue;
    if (existingByKey2.has(key)) continue;
    existingByKey2.set(key, r.id);
  }

  outSet("existing_key_count_after_gotcha", existingByKey2.size);

  //////////////////////
  // 9) Build UPSERT payloads (create vs update)
  //////////////////////
  const nowIso = new Date().toISOString();
  const toCreate = [];
  const toUpdate = [];

  let skippedNoKey = 0;
  let skippedNoFields = 0;

  for (const [, classRow0] of classesById.entries()) {
    const gid = parseNum(classRow0.class_group_id);
    const groupRow = gid !== undefined ? groupsById.get(normalizeKey(gid)) : undefined;

    const mergedRow = mergeGroupOntoClass(classRow0, groupRow);

    const key = normalizeKey(mergedRow.class_groupxclasses_id);
    if (!key) {
      skippedNoKey += 1;
      continue;
    }

    const fields = buildWritableFields(mergedRow, meta, targetTable);

    // last_updated_at
    if (meta.writableNames.has(LAST_UPDATED_FIELD)) fields[LAST_UPDATED_FIELD] = nowIso;

    // record_state
    const isExisting = existingByKey2.has(key);
    if (meta.writableNames.has(RECORD_STATE_FIELD)) fields[RECORD_STATE_FIELD] = isExisting ? "existing" : "new";

    // run_tag = sqlDate
    if (sqlDate && meta.writableNames.has(RUN_TAG_FIELD)) fields[RUN_TAG_FIELD] = sqlDate;

    // clear gotcha flags on good rows
    if (meta.writableNames.has(IS_GOTCHA_FIELD)) fields[IS_GOTCHA_FIELD] = false;
    if (meta.writableNames.has(GOTCHA_REASON_FIELD)) fields[GOTCHA_REASON_FIELD] = "";

    // ensure primary (best effort)
    ensureWritablePrimary(targetTable, meta, fields, mergedRow.class_groupxclasses_id);

    if (!fields || Object.keys(fields).length === 0) {
      skippedNoFields += 1;
      continue;
    }

    if (isExisting) {
      toUpdate.push({ id: existingByKey2.get(key), fields });
    } else {
      toCreate.push({ fields });
    }
  }

  outSet("create_candidate_count", toCreate.length);
  outSet("update_candidate_count", toUpdate.length);
  outSet("skipped_no_key", skippedNoKey);
  outSet("skipped_no_fields", skippedNoFields);

  // NOOP SAFE EXIT
  if (!toCreate.length && !toUpdate.length) {
    outSet("noop", true);

    if (pingView) {
      const pingPostNoop = await pingView.selectRecordsAsync({ fields: [UNIQUE_KEY_FIELD] });
      outSet("ping_view_count_after", pingPostNoop.records.length);

      const pingKeysAfterNoop = new Set();
      for (const r of pingPostNoop.records) {
        const k = normalizeKey(r.getCellValue(UNIQUE_KEY_FIELD));
        if (k) pingKeysAfterNoop.add(k);
      }
      outSet("ping_key_count_after", pingKeysAfterNoop.size);
    } else {
      outSet("ping_view_count_after", 0);
      outSet("ping_key_count_after", 0);
    }

    outSet("done", true);
    return;
  }

  //////////////////////
  // 10) CREATE missing records
  //////////////////////
  let created = 0;
  for (const batch of chunk(toCreate, BATCH_SIZE)) {
    const ids = await targetTable.createRecordsAsync(batch);
    created += Array.isArray(ids) ? ids.length : batch.length;
  }
  outSet("created_count", created);

  //////////////////////
  // 11) UPDATE existing records
  //////////////////////
  let updated = 0;
  for (const batch of chunk(toUpdate, BATCH_SIZE)) {
    await targetTable.updateRecordsAsync(batch);
    updated += batch.length;
  }
  outSet("updated_count", updated);

  //////////////////////
  // 11A) Optional ping view post-run reporting only
  //////////////////////
  if (pingView) {
    const pingPost = await pingView.selectRecordsAsync({ fields: [UNIQUE_KEY_FIELD] });
    outSet("ping_view_count_after", pingPost.records.length);

    const pingKeysAfter = new Set();
    for (const r of pingPost.records) {
      const k = normalizeKey(r.getCellValue(UNIQUE_KEY_FIELD));
      if (k) pingKeysAfter.add(k);
    }
    outSet("ping_key_count_after", pingKeysAfter.size);
  } else {
    outSet("ping_view_count_after", 0);
    outSet("ping_key_count_after", 0);
  }

  //////////////////////
  // 12) Summary
  //////////////////////
  outSet("seen_count", classesById.size);
  outSet("last_updated_at_value", nowIso);
  outSet("done", true);
}

// RUNNER: never hard-fail; always set ok + done
try {
  await main();
  SAFE_OUT("ok", true);
  SAFE_OUT("done", true);
} catch (err) {
  SAFE_OUT("ok", false);
  SAFE_OUT("done", true);
  const msg = String(err && err.message ? err.message : err || "");
  SAFE_OUT("error", msg.slice(0, 500));
}
