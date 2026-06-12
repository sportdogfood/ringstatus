const assert = require("assert");

const {
  applyLiveTimingToClassRows,
  buildEntryGoRows,
  inactiveRecordUpdates
} = require("../docs/horseshowing/sync-airtable-time-workflows.js");

const classRows = [
  {
    show_no: "14906",
    focus_day: "2026-06-12",
    ring_day_no: "3862",
    ring_no: 670,
    class_no: 29455,
    class_number: "804",
    class_name: "Welcome Prix",
    class_start_time: "08:00:00",
    display_time: "8:00 AM",
    entry_count: 33,
    source: "update_schedule.php"
  }
];

const liveRows = [
  {
    show_no: "14906",
    focus_day: "2026-06-12",
    class_no: 29455,
    n_gone: 12,
    n_to_go: 18,
    elapsed_seconds: 900,
    current_entry_no: "9999",
    current_horse: "Test Horse",
    live_source: "get_orders.php"
  }
];

const enriched = applyLiveTimingToClassRows(classRows, liveRows);
assert.strictEqual(enriched[0].n_gone, 12);
assert.strictEqual(enriched[0].n_to_go, 18);
assert.strictEqual(enriched[0].elapsed_seconds, 900);
assert.strictEqual(enriched[0].pace_seconds, 75);
assert.strictEqual(enriched[0].current_entry_no, "9999");
assert.strictEqual(enriched[0].current_horse, "Test Horse");
assert.strictEqual(enriched[0].source, "update_schedule.php|get_orders.php");

const entryRows = buildEntryGoRows({
  showNo: "14906",
  focusDay: "2026-06-12",
  scheduleRows: [{
    show_id: "14906",
    show_day_key: "2026-06-12",
    class_no: "29455",
    class_start_time: "08:00:00",
    start_display: "8:00 AM",
    class_number: "804",
    class_name: "Welcome Prix",
    entry_count: 33,
    n_gone: 12,
    elapsed_seconds: 900
  }],
  classOogRows: [{
    class_no: "29455",
    entry_no: "1234",
    entry_order: "10",
    horse: "Actual Horse",
    rider: "Actual Rider",
    trainer: "Alan Korotkin"
  }],
  activeTrainers: ["Alan Korotkin"],
  horseDisplays: { "Actual Horse": "Barn Horse" },
  trainerDisplays: { "Alan Korotkin": "CWF" },
  nowIso: "2026-06-12T12:00:00.000Z"
});

assert.strictEqual(entryRows.length, 1);
assert.strictEqual(entryRows[0].entry_go_key_mirror, "14906|2026-06-12|29455|1234");
assert.strictEqual(entryRows[0].pace_seconds, 75);
assert.strictEqual(entryRows[0].entry_go_time, "08:11:15");
assert.strictEqual(entryRows[0].horse_display, "Barn Horse");
assert.strictEqual(entryRows[0].trainer_display, "CWF");

const inactiveUpdates = inactiveRecordUpdates({
  existingRows: [
    { id: "recActive", fields: { entry_go_key_mirror: "14906|2026-06-12|29455|1234" } },
    { id: "recScratch", fields: { entry_go_key_mirror: "14906|2026-06-12|29455|9999" } }
  ],
  keyField: "entry_go_key_mirror",
  activeKeys: new Set(["14906|2026-06-12|29455|1234"]),
  reason: "missing_from_class_oog",
  nowIso: "2026-06-12T18:30:00.000Z"
});

assert.deepStrictEqual(inactiveUpdates, [
  {
    id: "recScratch",
    fields: {
      status: "inactive",
      inactive_reason: "missing_from_class_oog",
      inactive_at: "2026-06-12T18:30:00.000Z"
    }
  }
]);

console.log("wec_live_timing_enrichment tests passed");
