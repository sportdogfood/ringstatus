const assert = require("assert");
const path = require("path");

const root = path.resolve(__dirname, "..");
const liveGroupsPath = path.join(root, "live_groups_daily.js");

process.env.AIRTABLE_TOKEN = "test_token";
process.env.AIRTABLE_BASE_ID = "app_test";
process.env.SGL_FETCH_TRANSPORT = "node";
process.env.SGL_LIVE_STATUS_BASE_URL = "https://sglapi.test";
process.env.SGL_LIVE_BASE_URL = "https://sgl.test";
process.env.VIEW_SHOW_HEARTBEAT = "heartbeat";
process.env.HEARTBEAT_SORT_FIELD = "hb_at";
process.env.DRY_RUN = "0";

const metaTables = [
  table("show", [
    field("show_id", "number"),
    field("customer_id", "number"),
    field("focus_day", "date"),
    field("heartbeat", "checkbox"),
  ]),
  table("heartbeat", [
    field("mode", "singleLineText"),
    field("show_id", "number"),
    field("app_show_id", "number"),
    field("app_sql_date", "singleLineText"),
    field("sql_date", "singleLineText"),
    field("focus_day", "date"),
    field("show", "multipleRecordLinks"),
    field("hb_at", "dateTime"),
  ]),
  table("live_groups", [
    field("live_groups_key", "singleLineText"),
    field("class_group_id", "number"),
    field("show_id", "number"),
    field("show", "multipleRecordLinks"),
    field("day", "singleLineText"),
    field("live_focus_day", "date"),
    field("ring_number", "number"),
    field("ring_id", "number"),
    field("group_name", "multilineText"),
    field("classes", "multilineText"),
    field("class_ids", "multilineText"),
    field("classNumbers", "singleLineText"),
    field("class_numbers", "singleLineText"),
    field("classNames", "multilineText"),
    field("class_names", "multilineText"),
    field("estimated_start_time", "singleLineText"),
    field("gone", "number"),
    field("total", "number"),
    field("curr_updated_at", "singleLineText"),
    field("ingested_at", "singleLineText"),
    field("run_tag", "singleLineText"),
    field("is_live", "checkbox"),
    field("has_JSON", "checkbox"),
    field("is_cuurent_scope", "checkbox"),
    field("dropped_at", "singleLineText"),
    field("status", "singleLineText"),
    field("customer_id", "number"),
    field("watch_schedule", "multipleRecordLinks"),
    field("watch_trips", "multipleRecordLinks"),
  ]),
  table("live_group_changes", [
    field("change_key", "singleLineText"),
    field("live_groups", "multipleRecordLinks"),
    field("show", "multipleRecordLinks"),
    field("show_id", "number"),
    field("focus_day", "date"),
    field("is_cuurent_scope", "checkbox"),
    field("class_group_id", "number"),
    field("group_name", "singleLineText"),
    field("ring_number", "number"),
    field("field_changed", "singleLineText"),
    field("old_value", "singleLineText"),
    field("new_value", "singleLineText"),
    field("changed_at", "dateTime"),
    field("run_tag", "singleLineText"),
  ]),
  table("watch_schedule", [
    field("show_id", "number"),
    field("app_show_idv2", "number"),
    field("app_show_id", "number"),
    field("scheduled_date", "singleLineText"),
    field("app_sql_datev2", "singleLineText"),
    field("schedule_show_datev2", "singleLineText"),
    field("show_date", "singleLineText"),
    field("focus_day", "date"),
    field("ring_number", "number"),
    field("class_group_id", "number"),
    field("class_number", "number"),
    field("class_id", "number"),
    field("estimated_start_time", "singleLineText"),
    field("status", "singleLineText"),
    field("completed_trips", "number"),
    field("total_trips", "number"),
    field("group_name", "singleLineText"),
    field("manual_time_override", "checkbox"),
    field("archive", "checkbox"),
    field("inactive", "checkbox"),
    field("dropped_at", "date"),
  ]),
  table("watch_trips", [
    field("show_id", "number"),
    field("app_show_idv2", "number"),
    field("app_show_id", "number"),
    field("scheduled_date", "singleLineText"),
    field("app_sql_datev2", "singleLineText"),
    field("app_sql_date", "singleLineText"),
    field("show_date", "date"),
    field("schedule_show_datev2", "date"),
    field("focus_day", "date"),
    field("ring_number", "number"),
    field("class_group_id", "number"),
    field("class_id", "number"),
    field("class_number", "number"),
    field("entry_number", "number"),
    field("rider_name", "singleLineText"),
    field("horse", "singleLineText"),
    field("estimated_start_time", "multilineText"),
    field("status", "singleLineText"),
    field("completed_trips", "number"),
    field("total_trips", "number"),
    field("rs_completed_trips", "number"),
    field("group_name", "singleLineText"),
    field("manual_time_override", "checkbox"),
    field("archive", "checkbox"),
    field("inactive", "checkbox"),
    field("dropped_at", "date"),
  ]),
  table("automation_errs", [
    field("automation_key", "singleLineText"),
    field("automation_name", "singleLineText"),
    field("error_type", "singleLineText"),
    field("app_sql_date", "singleLineText"),
    field("run_id", "number"),
    field("last_run", "singleLineText"),
    field("message", "multilineText"),
    field("app_show_id", "number"),
  ]),
];

const airtableWrites = [];

global.fetch = async (url, init = {}) => {
  const href = String(url?.url || url);
  const parsed = new URL(href);

  if (href === "https://sglapi.test/homepage/getLiveClassStatus?customer_id=15") {
    return jsonResponse(true);
  }
  if (href === "https://sgl.test/iphonev2/index.php/esp/liveclassv2/ListAjax?from_wp_api=true") {
    return jsonResponse([{
      show_id: "200000063",
      json_data: [{
        class_group_id: "200024520",
        group_name: "WIHS Hunter Phase",
        day: "2026-05-30",
        estimated_start_time: "09:10:02",
        ring_number: "3",
        ring_id: "44",
        classes: ["200025701"],
        classNumbers: ["580"],
        classNames: ["WIHS Hunter Phase"],
        status: "Underway",
        gone: "4",
        total: "12",
        curr_updated_at: "1780050000",
        is_live: true,
        has_JSON: true,
      }],
    }]);
  }

  if (parsed.pathname === "/v0/meta/bases/app_test/tables") {
    return jsonResponse({ tables: metaTables });
  }

  const tableName = decodeURIComponent(parsed.pathname.split("/").pop());
  if (init.method === "PATCH" || init.method === "POST") {
    const body = JSON.parse(init.body || "{}");
    airtableWrites.push({ method: init.method, tableName, records: body.records || [] });
    return jsonResponse({ records: body.records || [] });
  }

  if (tableName === "show") {
    return jsonResponse({ records: [record("show_rec", {
      show_id: 200000063,
      customer_id: 15,
      focus_day: "2026-05-30",
      heartbeat: true,
    })] });
  }
  if (tableName === "heartbeat") {
    return jsonResponse({ records: [record("hb_rec", {
      mode: "DAY",
      show_id: 200000063,
      app_show_id: 200000063,
      app_sql_date: "2026-05-30",
      focus_day: "2026-05-30",
      show: ["show_rec"],
      hb_at: "2026-05-30T14:00:00.000Z",
    })] });
  }
  if (tableName === "live_groups") {
    return jsonResponse({ records: [record("lg_rec", {
      live_groups_key: "200000063|2026-05-30|15|3|200024520",
      estimated_start_time: "09:05:00",
      gone: 3,
      is_cuurent_scope: true,
      dropped_at: "",
    })] });
  }
  if (tableName === "live_group_changes") {
    return jsonResponse({ records: [
      record("lgc_current", {
        show_id: 200000063,
        focus_day: "2026-05-30",
        is_cuurent_scope: false,
      }),
      record("lgc_old", {
        show_id: 200000063,
        focus_day: "2026-05-29",
        is_cuurent_scope: true,
      }),
    ] });
  }
  if (tableName === "watch_schedule") {
    return jsonResponse({ records: [
      record("ws_rec", {
        show_id: 200000063,
        show_date: "2026-05-30",
        ring_number: 3,
        class_group_id: 200024520,
        class_number: 580,
        estimated_start_time: "09:00:00",
        status: "Upcoming",
        completed_trips: 3,
        total_trips: 12,
        group_name: "WIHS Hunter Phase",
        manual_time_override: false,
      }),
      record("ws_manual", {
        show_id: 200000063,
        show_date: "2026-05-30",
        ring_number: 3,
        class_group_id: 200024520,
        class_number: 580,
        estimated_start_time: "08:55:00",
        status: "Upcoming",
        completed_trips: 3,
        total_trips: 12,
        manual_time_override: true,
      }),
    ] });
  }
  if (tableName === "watch_trips") {
    return jsonResponse({ records: [record("wt_rec", {
      show_id: 200000063,
      show_date: "2026-05-30",
      ring_number: 3,
      class_group_id: 200024520,
      class_id: null,
      class_number: 580,
      entry_number: 214,
      estimated_start_time: "09:00:00",
      status: "Upcoming",
      completed_trips: 3,
      total_trips: 12,
      rs_completed_trips: 3,
      manual_time_override: false,
    })] });
  }

  throw new Error(`Unexpected fetch ${href}`);
};

delete require.cache[liveGroupsPath];
const mod = require(liveGroupsPath);

assert.equal(typeof mod.main, "function", "live_groups_daily must export main for fixture testing");

(async () => {
  const result = await mod.main();
  assert.equal(result.watch_schedule_updates, 2);
  assert.equal(result.watch_trips_updates, 1);

  const schedulePatch = airtableWrites.find((write) => write.method === "PATCH" && write.tableName === "watch_schedule");
  assert.ok(schedulePatch, "watch_schedule must be patched from live_groups_daily");

  const scheduleById = Object.fromEntries(schedulePatch.records.map((item) => [item.id, item.fields]));
  assert.deepEqual(scheduleById.ws_rec, {
    estimated_start_time: "09:10:02",
    status: "Underway",
    completed_trips: 4,
    class_id: 200025701,
  });
  assert.deepEqual(scheduleById.ws_manual, {
    status: "Underway",
    completed_trips: 4,
    group_name: "WIHS Hunter Phase",
    class_id: 200025701,
  });

  const tripPatch = airtableWrites.find((write) => write.method === "PATCH" && write.tableName === "watch_trips");
  assert.ok(tripPatch, "watch_trips must be patched from live_groups_daily");
  assert.deepEqual(tripPatch.records[0].fields, {
    estimated_start_time: "09:10:02",
    status: "Underway",
    completed_trips: 4,
    rs_completed_trips: 4,
    group_name: "WIHS Hunter Phase",
    class_id: 200025701,
  });

  const livePatch = airtableWrites.find((write) => write.method === "PATCH" && write.tableName === "live_groups");
  assert.ok(livePatch, "live_groups must still be refreshed");
  assert.deepEqual(livePatch.records[0].fields.watch_schedule, ["ws_rec", "ws_manual"]);
  assert.deepEqual(livePatch.records[0].fields.watch_trips, ["wt_rec"]);

  const changeWrite = airtableWrites.find((write) => write.method === "POST" && write.tableName === "live_group_changes");
  assert.ok(changeWrite, "live_group_changes must receive group change logs");
  assert.ok(
    changeWrite.records.every((item) => item.fields.focus_day === "2026-05-30" && item.fields.is_cuurent_scope === true),
    "new live_group_changes rows must carry current focus_day and is_cuurent_scope"
  );

  const staleChangePatch = airtableWrites.find((write) => write.method === "PATCH" && write.tableName === "live_group_changes");
  assert.ok(staleChangePatch, "older live_group_changes rows for the same show must be marked not current");
  assert.deepEqual(staleChangePatch.records, [
    { id: "lgc_current", fields: { is_cuurent_scope: true } },
    { id: "lgc_old", fields: { is_cuurent_scope: false } },
  ]);

  const writable = new Set([
    "estimated_start_time",
    "status",
    "completed_trips",
    "total_trips",
    "ring_number",
    "group_name",
    "class_id",
  ]);

  assert.deepEqual(
    mod.buildWatchPropagationFields(
      {
        class_numbers: "580",
        class_ids: "200025701",
        estimated_start_time: null,
        status: null,
        gone: null,
        total: null,
        ring_number: 3,
        group_name: null,
      },
      record("watch_existing", {
        class_number: 580,
        class_id: 200025701,
        estimated_start_time: "09:00:00",
        status: "Upcoming",
        completed_trips: 3,
        total_trips: 12,
        ring_number: 3,
      }),
      writable
    ),
    {},
    "blank live group values must not clear existing watch fields"
  );

  assert.deepEqual(
    mod.buildWatchPropagationFields(
      {
        class_numbers: "580,581",
        class_ids: "200025701,200025702",
        estimated_start_time: "09:10:02",
        status: "Underway",
        gone: 4,
        total: 12,
        ring_number: 3,
        group_name: "Combined Group",
      },
      record("watch_multiclass", {
        class_number: 581,
        estimated_start_time: "09:00:00",
        status: "Upcoming",
        completed_trips: 3,
        total_trips: 12,
      }),
      writable
    ),
    {
      estimated_start_time: "09:10:02",
      status: "Underway",
      completed_trips: 4,
      ring_number: 3,
      group_name: "Combined Group",
      class_id: 200025702,
    },
    "multi-class group class_id must pair by classNumbers/classes index"
  );

  console.log("live_groups_daily watch propagation fixture tests passed");
})().catch((error) => {
  console.error(error);
  process.exit(1);
});

function field(name, type) {
  return { id: `fld_${name}`, name, type };
}

function table(name, fields) {
  return { id: `tbl_${name}`, name, fields };
}

function record(id, fields) {
  return { id, fields };
}

function jsonResponse(value) {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}
