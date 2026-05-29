const assert = require("assert");
const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const detailPath = path.join(root, "live_class_detail.js");
const orchestratorPath = path.join(root, "heartbeat_slot_orchestrator.js");

assert.ok(fs.existsSync(detailPath), "live_class_detail.js must exist");

const detail = fs.readFileSync(detailPath, "utf8");
const orchestrator = fs.readFileSync(orchestratorPath, "utf8");

assert.ok(
  detail.includes('TABLE_LIVE_CLASSES = process.env.TABLE_LIVE_CLASSES || "live_classes"'),
  "detail runner must write logs to live_classes"
);

assert.ok(
  detail.includes('VIEW_HAS_JSON = process.env.VIEW_LIVE_GROUPS_HAS_JSON || "has_json"') &&
    detail.includes('VIEW_IS_LIVE = process.env.VIEW_LIVE_GROUPS_IS_LIVE || "is_live"'),
  "detail runner must use separate has_json and is_live views"
);

assert.ok(
  detail.includes('DEFAULT_HAS_JSON_SLOTS = "A,C"') &&
    detail.includes('DEFAULT_IS_LIVE_SLOTS = "A,B,C,D"'),
  "detail runner must keep separate has_json and is_live cadences"
);

assert.ok(
  detail.includes("LIVE_CLASS_DETAIL_DISABLED") &&
    orchestrator.includes("ORCH_DISABLE_LIVE_CLASS_DETAIL"),
  "detail lane must have a disable switch"
);

assert.ok(
  detail.includes("watch_trips") &&
    detail.includes("only keeps linked watch_trips") &&
    detail.includes("updateWatchTripsFirst"),
  "detail runner must only keep linked trips and update watch_trips before logs"
);

assert.ok(
  detail.includes("getLiveClassData") &&
    detail.includes("rows"),
  "detail runner must call getLiveClassData and process payload rows"
);

assert.ok(
  orchestrator.includes('live_class_detail.js') &&
    orchestrator.includes("live_class_detail: liveClassDetailDue"),
  "orchestrator must expose the detail lane separately"
);

console.log("live_class_detail_contract tests passed");
