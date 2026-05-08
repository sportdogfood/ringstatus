const assert = require("assert");
const fs = require("fs");
const path = require("path");

const orchestratorPath = path.resolve(__dirname, "..", "heartbeat_slot_orchestrator.js");
const heartbeatLanePath = path.resolve(__dirname, "..", "run_tagger_heartbeat_lane.ps1");
const monitorPath = path.resolve(__dirname, "..", "monitor_watch_trips_health.js");
const modeModulePath = path.resolve(__dirname, "..", "lib", "heartbeat_mode.js");

assert.ok(fs.existsSync(orchestratorPath), "heartbeat_slot_orchestrator.js must exist");
assert.ok(fs.existsSync(monitorPath), "monitor_watch_trips_health.js must exist");
assert.ok(fs.existsSync(modeModulePath), "lib/heartbeat_mode.js must exist");

const orchestrator = fs.readFileSync(orchestratorPath, "utf8");
const heartbeatLane = fs.readFileSync(heartbeatLanePath, "utf8");
const monitor = fs.readFileSync(monitorPath, "utf8");

assert.ok(
  orchestrator.includes('DEFAULT_TRIPS_DAILY_SLOTS = "A,C"'),
  "trips_dailyv2 must default to heartbeat slots A/C"
);

assert.ok(
  orchestrator.includes('DEFAULT_TRIPS_TAGGER_SLOTS = "C"'),
  "trips_tagger must default to a slower class-detail/classsignup slot"
);

assert.ok(
  orchestrator.includes('DEFAULT_SCHEDULES_DAILY_SLOTS = "B,D"'),
  "schedules_dailyv2 must default to slots B/D in non-NIGHT modes"
);

assert.ok(
  orchestrator.includes('DEFAULT_SCHEDULES_DAILY_NIGHT_SLOTS = "A,C"'),
  "NIGHT schedules_dailyv2 must run on A/C before trips so next-day schedule exists"
);

assert.ok(
  orchestrator.includes('DEFAULT_SCHEDULES_CALCULATOR_SLOTS = "A,B,C,D"'),
  "schedules_calculatorv2 must run after any due schedules_dailyv2 slot so groups_live overlays are promoted"
);

assert.ok(
  orchestrator.includes('DEFAULT_PUBLISHER_SLOTS = "A,B,C,D"'),
  "publisher must be due on every heartbeat slot so dirty queue records are not stranded"
);

assert.ok(
  /runNodeScript\("trips_dailyv2\.js"\)[\s\S]+if\s*\(!tripsDailyResult\.ok\)/.test(orchestrator),
  "trips downstream work must be blocked when trips_dailyv2 fails"
);

assert.ok(
  /runNodeScript\("schedules_dailyv2\.js"\)[\s\S]+if\s*\(!schedulesDailyResult\.ok\)/.test(orchestrator),
  "schedule downstream work must be blocked when schedules_dailyv2 fails"
);

assert.ok(
  orchestrator.includes('reason: "schedules_dailyv2_failed"') &&
    /if\s*\(scheduleDueFailed\s*&&\s*\(tripsDailyDue\s*\|\|\s*tripsTaggerDue\s*\|\|\s*tripsCalcDue\)\)/.test(orchestrator),
  "trip lanes must be blocked when a due schedule refresh fails"
);

assert.ok(
  /if\s*\(publisherDue\s*&&\s*upstreamOk\)\s*\{[\s\S]+runNodeScript\("publisher\.js"\)/.test(orchestrator),
  "publisher must be blocked when an upstream due lane fails"
);

assert.ok(
  orchestrator.includes('event: "publisher_blocked"'),
  "publisher block events must be logged when upstream lanes fail"
);

assert.ok(
  orchestrator.includes("SCRIPT_LOG_FILES") &&
    orchestrator.includes("appendScriptLog"),
  "detached orchestrator must preserve script stdout/stderr in per-script logs"
);

assert.ok(
  orchestrator.includes("modeAllowsHeavy") &&
    orchestrator.includes("orchestrator_mode_noop"),
  "slot orchestrator must skip heavy lanes when heartbeat mode is IDLE or OFF"
);

assert.ok(
  orchestrator.includes("HEARTBEAT_MODE_FIELD") &&
    orchestrator.includes("HEARTBEAT_CADENCE_FIELD") &&
    orchestrator.includes("HEARTBEAT_SET_INTERVALS_FIELD"),
  "slot orchestrator must read Airtable mode and cadence fields from the latest heartbeat"
);

assert.ok(
  heartbeatLane.includes("SLOT_ORCHESTRATOR") &&
    heartbeatLane.includes("heartbeat_slot_orchestrator.js"),
  "heartbeat lane must invoke the slot orchestrator after heartbeat_patterns"
);

assert.ok(
  !/method\s*:\s*["'](?:POST|PATCH|PUT|DELETE)["']/i.test(monitor),
  "monitor must be read-only and must not write to Airtable"
);

console.log("heartbeat_slot_orchestrator tests passed");
