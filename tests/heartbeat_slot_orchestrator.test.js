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
  orchestrator.includes('DEFAULT_TRIPS_DAILY_SLOTS = "A,B,C,D"'),
  "trips_dailyv2 must default to every heartbeat slot"
);

assert.ok(
  orchestrator.includes('DEFAULT_TRIPS_TAGGER_SLOTS = "A,C"'),
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
  !orchestrator.includes("DEFAULT_TRIPS_DAILY_NIGHT_SHIFTED_SLOTS") &&
    !orchestrator.includes("ORCH_TRIPS_DAILY_NIGHT_SHIFTED_SLOTS") &&
    orchestrator.includes("const tripsDailyDefaultSlots = DEFAULT_TRIPS_DAILY_SLOTS;"),
  "shifted_to_next_day must not change trips_dailyv2 cadence"
);

assert.ok(
  orchestrator.includes('DEFAULT_SCHEDULES_CALCULATOR_SLOTS = "A,B,C,D"'),
  "schedules_calculatorv2 must be eligible on every DAY heartbeat slot"
);

assert.ok(
  orchestrator.includes('DEFAULT_LIVE_GROUPS_SLOTS = "A,B,C,D"') &&
    orchestrator.includes('mode === "DAY"') &&
    /run(?:Node|Due)Script\("live_groups_daily\.js"\)/.test(orchestrator),
  "live_groups_daily must run only in DAY mode on every heartbeat slot"
);

assert.ok(
  orchestrator.includes('DEFAULT_LIVE_RINGS_SLOTS = "A,B,C,D"') &&
    orchestrator.includes('mode === "DAY"') &&
    /run(?:Node|Due)Script\("live_rings_daily\.js"\)/.test(orchestrator),
  "live_rings_daily must run only in DAY mode on every heartbeat slot"
);

assert.ok(
  orchestrator.includes("if (!scheduleDueFailed && schedulesCalcDue)") &&
    !orchestrator.includes("} else if (schedulesCalcDue) {"),
  "schedules_calculatorv2 must run whenever its own slot is due, not only when schedules_dailyv2 is due"
);

assert.ok(
  orchestrator.includes('DEFAULT_PUBLISHER_SLOTS = "A,B,C,D"'),
  "publisher must be due on every heartbeat slot so dirty queue records are not stranded"
);

assert.ok(
  /run(?:Node|Due)Script\("trips_dailyv2\.js"\)[\s\S]+if\s*\(!tripsDailyResult\.ok\)/.test(orchestrator),
  "trips downstream work must be blocked when trips_dailyv2 fails"
);

assert.ok(
  /run(?:Node|Due)Script\("schedules_dailyv2\.js"\)[\s\S]+if\s*\(!schedulesDailyResult\.ok\)/.test(orchestrator),
  "schedule downstream work must be blocked when schedules_dailyv2 fails"
);

assert.ok(
  orchestrator.includes('reason: "schedules_dailyv2_failed"') &&
    /if\s*\(scheduleDueFailed\s*&&\s*\(tripsDailyDue\s*\|\|\s*tripsTaggerDue\s*\|\|\s*tripsCalcDue\)\)/.test(orchestrator),
  "trip lanes must be blocked when a due schedule refresh fails"
);

assert.ok(
  /if\s*\(publisherDue\s*&&\s*upstreamOk\)\s*\{[\s\S]+run(?:Node|Due)Script\("publisher\.js"\)/.test(orchestrator),
  "publisher must be blocked when an upstream due lane fails"
);

assert.ok(
  orchestrator.includes('event: "publisher_blocked"'),
  "publisher block events must be logged when upstream lanes fail"
);

assert.ok(
  orchestrator.includes('TABLE_AUTOMATION_ERRS') &&
    orchestrator.includes('"heartbeat_orchestrator_locked"') &&
    orchestrator.includes('"heartbeat_lane_step_overrun"') &&
    orchestrator.includes('"heartbeat_no_active_feeds"'),
  "heartbeat orchestrator must write automation_errs for lock skips, step overruns, and missing active show scope"
);

assert.ok(
  orchestrator.includes('reason: "no_active_show_scope"') &&
    orchestrator.includes('numOrNull(heartbeat?.fields?.app_show_id) === null'),
  "heartbeat orchestrator must not run downstream lanes when latest heartbeat has no active show scope"
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
    heartbeatLane.includes("heartbeat_slot_orchestrator.js") &&
    heartbeatLane.includes("live_rings_daily.js"),
  "heartbeat lane must invoke the slot orchestrator and track live_rings_daily"
);

assert.ok(
  !/method\s*:\s*["'](?:POST|PATCH|PUT|DELETE)["']/i.test(monitor),
  "monitor must be read-only and must not write to Airtable"
);

console.log("heartbeat_slot_orchestrator tests passed");
