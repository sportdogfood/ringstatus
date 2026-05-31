const assert = require("assert");
const path = require("path");

const root = path.resolve(__dirname, "..");
const detailPath = path.join(root, "live_class_detail.js");

process.env.AIRTABLE_TOKEN = "test_token";
process.env.AIRTABLE_BASE_ID = "app_test";
process.env.SGL_FETCH_TRANSPORT = "node";
process.env.SGL_LIVE_BASE_URL = "https://sgl.test";
process.env.DRY_RUN = "0";
process.env.ORCH_CURRENT_MODE = "DAY";
process.env.ORCH_CURRENT_SLOT = "B";
process.env.LIVE_CLASS_DETAIL_HAS_JSON_SLOTS = "A,C";
process.env.LIVE_CLASS_DETAIL_IS_LIVE_SLOTS = "B";

const writes = [];

const metaTables = [
  table("heartbeat", [
    field("mode", "singleLineText"),
    field("isA", "checkbox"),
    field("isB", "checkbox"),
    field("isC", "checkbox"),
    field("isD", "checkbox"),
    field("hb_at", "dateTime"),
  ]),
  table("live_groups", [
    field("live_groups_key", "singleLineText"),
    field("show_id", "number"),
    field("live_focus_day", "date"),
    field("class_group_id", "number"),
    field("class_ids", "multilineText"),
    field("class_numbers", "singleLineText"),
    field("watch_trips", "multipleRecordLinks"),
    field("has_JSON", "checkbox"),
    field("is_live", "checkbox"),
    field("status", "singleLineText"),
    field("is_cuurent_scope", "checkbox"),
    field("dropped_at", "singleLineText"),
  ]),
  table("watch_trips", [
    field("class_id", "number"),
    field("entry_number", "number"),
    field("rider_name", "singleLineText"),
    field("horse", "singleLineText"),
    field("scratch_trip", "number"),
    field("lastPosition", "number"),
    field("rider_running_oog", "number"),
    field("actual_order", "number"),
    field("gone_in", "number"),
    field("rs_gone_in", "number"),
    field("archive", "checkbox"),
    field("inactive", "checkbox"),
    field("dropped_at", "date"),
  ]),
  table("live_classes", [
    field("log_key", "singleLineText"),
    field("source_view", "singleLineText"),
    field("live_groups", "multipleRecordLinks"),
    field("watch_trips", "multipleRecordLinks"),
    field("show_id", "number"),
    field("focus_day", "date"),
    field("is_cuurent_scope", "checkbox"),
    field("class_group_id", "number"),
    field("class_id", "number"),
    field("entry_number", "number"),
    field("rider_name", "singleLineText"),
    field("horse", "singleLineText"),
    field("payload_row_id", "singleLineText"),
    field("gone", "number"),
    field("scr", "number"),
    field("pos", "number"),
    field("field_changed", "singleLineText"),
    field("old_value", "singleLineText"),
    field("new_value", "singleLineText"),
    field("detail_fetched_at", "dateTime"),
    field("run_tag", "singleLineText"),
    field("class_detail_endpoint", "singleLineText"),
  ]),
];

global.fetch = async (url, init = {}) => {
  const href = String(url?.url || url);
  const parsed = new URL(href);

  if (href.startsWith("https://sgl.test/iphonev2/index.php/esp/liveclassv2/getLiveClassData")) {
    return jsonResponse({
      ID: "200025595",
      rows: [
        {
          id: "payload_hit",
          ENo: "214",
          Hor: "SHERLOCK 46",
          Rid: "HAILEY ROYCE",
          Scr: "1",
          Pos: "2",
          OOG: "5",
          Actual_OOG: "6",
          Gone: "1",
        },
        {
          id: "payload_other",
          ENo: "999",
          Hor: "OTHER HORSE",
          Rid: "OTHER RIDER",
          Scr: "1",
          Pos: "1",
          Gone: "1",
        },
      ],
    });
  }

  if (parsed.pathname === "/v0/meta/bases/app_test/tables") {
    return jsonResponse({ tables: metaTables });
  }

  const parts = parsed.pathname.split("/");
  const tableName = decodeURIComponent(parts[3] || "");
  const maybeRecordId = parts.length > 4 ? decodeURIComponent(parts[4]) : null;

  if (init.method === "PATCH" || init.method === "POST") {
    const body = JSON.parse(init.body || "{}");
    writes.push({ method: init.method, tableName, records: body.records || [] });
    return jsonResponse({ records: body.records || [] });
  }

  if (tableName === "heartbeat") {
    return jsonResponse({ records: [record("hb_no_slot", {
      mode: "DAY",
      isA: false,
      isB: false,
      isC: false,
      isD: false,
      hb_at: "2026-05-30T14:00:00.000Z",
    })] });
  }
  if (tableName === "live_groups") {
    return jsonResponse({ records: [record("lg_live", {
      live_groups_key: "lg",
      show_id: 200000063,
      live_focus_day: "2026-05-30",
      class_group_id: 200024520,
      class_ids: "200025595",
      class_numbers: "580",
      watch_trips: ["wt_hit", "wt_gone", "wt_scratched"],
      has_JSON: true,
      is_live: true,
      status: "Underway",
      is_cuurent_scope: true,
      dropped_at: "",
    })] });
  }
  if (tableName === "live_classes") {
    return jsonResponse({ records: [
      record("lc_current", {
        show_id: 200000063,
        focus_day: "2026-05-30",
        is_cuurent_scope: false,
      }),
      record("lc_old", {
        show_id: 200000063,
        focus_day: "2026-05-29",
        is_cuurent_scope: true,
      }),
    ] });
  }
  if (tableName === "watch_trips" && maybeRecordId) {
    if (maybeRecordId === "wt_hit") {
      return jsonResponse(record("wt_hit", {
        class_id: 200025595,
        entry_number: 214,
        rider_name: "HAILEY ROYCE",
        horse: "SHERLOCK 46",
        scratch_trip: 0,
        lastPosition: 0,
        rider_running_oog: 0,
        actual_order: 0,
        gone_in: 0,
      }));
    }
    if (maybeRecordId === "wt_gone") {
      return jsonResponse(record("wt_gone", {
        class_id: 200025595,
        entry_number: 214,
        rider_name: "HAILEY ROYCE",
        horse: "SHERLOCK 46",
        gone_in: 1,
      }));
    }
    if (maybeRecordId === "wt_scratched") {
      return jsonResponse(record("wt_scratched", {
        class_id: 200025595,
        entry_number: 214,
        rider_name: "HAILEY ROYCE",
        horse: "SHERLOCK 46",
        scratch_trip: 1,
      }));
    }
  }

  throw new Error(`Unexpected fetch ${href}`);
};

delete require.cache[detailPath];
const mod = require(detailPath);

assert.equal(typeof mod.main, "function", "live_class_detail must export main for fixture testing");

(async () => {
  const summary = await mod.main();
  assert.equal(summary.slot, "B", "orchestrator slot override must prevent false slot:null skips");
  assert.equal(summary.results.length, 1);
  assert.equal(summary.results[0].source, "is_live");
  assert.equal(summary.results[0].rows, 1);
  assert.equal(summary.results[0].pings, 1);
  assert.equal(summary.results[0].matched, 1);
  assert.equal(summary.results[0].trip_updates, 1);
  assert.equal(summary.results[0].logs, 3);
  assert.equal(summary.results[0].skipped_no_linked_trips, 0);
  assert.equal(summary.results[0].skipped_no_actionable_trips, 0);
  assert.equal(summary.results[0].skipped_missing_mapping, 0);

  const tripPatch = writes.find((write) => write.method === "PATCH" && write.tableName === "watch_trips");
  assert.ok(tripPatch, "watch_trips must be patched before log writes");
  assert.deepEqual(tripPatch.records, [{
    id: "wt_hit",
    fields: {
      scratch_trip: 1,
      lastPosition: 2,
      gone_in: 1,
    },
  }]);

  const logWrite = writes.find((write) => write.method === "POST" && write.tableName === "live_classes");
  assert.ok(logWrite, "live_classes must receive change logs");
  assert.equal(logWrite.records.length, 3);
  assert.ok(
    logWrite.records.every((item) => item.fields.focus_day === "2026-05-30" && item.fields.is_cuurent_scope === true),
    "new live_classes rows must carry current focus_day and is_cuurent_scope"
  );
  assert.deepEqual(
    logWrite.records.map((item) => item.fields.field_changed).sort(),
    ["gone", "pos", "scr"]
  );
  const staleClassPatch = writes.find((write) => write.method === "PATCH" && write.tableName === "live_classes");
  assert.ok(staleClassPatch, "older live_classes rows for the same show must be marked not current");
  assert.deepEqual(staleClassPatch.records, [
    { id: "lc_current", fields: { is_cuurent_scope: true } },
    { id: "lc_old", fields: { is_cuurent_scope: false } },
  ]);

  writes.length = 0;
  process.env.ORCH_CURRENT_SLOT = "A";
  const hasJsonSummary = await mod.main();
  assert.equal(hasJsonSummary.slot, "A");
  assert.equal(hasJsonSummary.results.length, 1);
  assert.equal(hasJsonSummary.results[0].source, "has_json");
  assert.equal(hasJsonSummary.results[0].rows, 1);
  assert.equal(hasJsonSummary.results[0].pings, 1);
  assert.equal(hasJsonSummary.results[0].matched, 1);
  assert.equal(hasJsonSummary.results[0].trip_updates, 1);
  assert.equal(hasJsonSummary.results[0].logs, 3);
  assert.equal(hasJsonSummary.results[0].skipped_no_linked_trips, 0);
  assert.equal(hasJsonSummary.results[0].skipped_no_actionable_trips, 0);
  assert.equal(hasJsonSummary.results[0].skipped_missing_mapping, 0);

  const hasJsonTripPatch = writes.find((write) => write.method === "PATCH" && write.tableName === "watch_trips");
  assert.ok(hasJsonTripPatch, "has_json pull must patch watch_trips");
  assert.deepEqual(hasJsonTripPatch.records, [{
    id: "wt_hit",
    fields: {
      rider_running_oog: 5,
      actual_order: 6,
      gone_in: 1,
    },
  }]);

  const hasJsonLogWrite = writes.find((write) => write.method === "POST" && write.tableName === "live_classes");
  assert.ok(hasJsonLogWrite, "has_json pull must log live_classes changes");
  assert.deepEqual(
    hasJsonLogWrite.records.map((item) => item.fields.field_changed).sort(),
    ["actual_oog", "gone", "oog"]
  );

  const noActionable = await mod.processLiveGroup(
    {
      id: "lg_done",
      fields: {
        show_id: 200000063,
        class_ids: "200025595",
        watch_trips: ["wt_gone", "wt_scratched"],
      },
    },
    "is_live",
    new Set(["scratch_trip", "lastPosition", "gone_in"]),
    new Set()
  );
  assert.equal(noActionable.pings, 0);
  assert.equal(noActionable.skipped_no_actionable_trips, 1);

  console.log("live_class_detail watch propagation fixture tests passed");
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
