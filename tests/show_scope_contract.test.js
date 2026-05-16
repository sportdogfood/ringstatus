const assert = require("assert");
const fs = require("fs");
const path = require("path");

const {
  buildShowScopeKey,
} = require("../lib/show_scope");

assert.strictEqual(
  buildShowScopeKey({
    customerId: 10002,
    showId: 200000006,
    focusDay: "2026-05-16",
  }),
  "10002|200000006|2026-05-16",
  "show scope key must be customer_id|show_id|focus_day"
);

assert.strictEqual(
  buildShowScopeKey({
    customer_id: "10002",
    show_id: "200000006",
    focus_day: "2026-05-16T00:00:00.000Z",
  }),
  "10002|200000006|2026-05-16",
  "show scope key must accept Airtable-style field names and date values"
);

assert.strictEqual(
  buildShowScopeKey({
    customerId: 10002,
    showId: 200000006,
  }),
  "",
  "show scope key must not be built from incomplete scope values"
);

const tagger = fs.readFileSync(path.resolve(__dirname, "..", "tagger.js"), "utf8");
assert.ok(
  tagger.includes("HEARTBEAT_TARGET_SHOW_RECORD_ID") &&
    tagger.includes("FIELD_SHOW_SCOPE_KEY") &&
    tagger.includes("FIELD_RING_COLLECTION") &&
    tagger.includes("FIELD_LINK_SHOW"),
  "tagger must support record-targeted focused show heartbeat snapshots"
);

assert.ok(
  tagger.includes("modeControlFromTargetDecision") &&
    tagger.includes("heartbeatTargetDecision?.mode_control"),
  "tagger mode control must use the focused show target before legacy shows rows"
);

const focusedShowsRunner = fs.readFileSync(path.resolve(__dirname, "..", "run_tagger_task_focused_shows.ps1"), "utf8");
assert.ok(
  focusedShowsRunner.includes("HEARTBEAT_TARGET_SHOW_RECORD_ID") &&
    focusedShowsRunner.includes("run_tagger_task.ps1"),
  "focused show runner must invoke the existing pipeline once per focused show record"
);

console.log("show_scope_contract tests passed");
