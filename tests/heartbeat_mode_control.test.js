const assert = require("assert");
const fs = require("fs");
const path = require("path");

const modeModulePath = path.resolve(__dirname, "..", "lib", "heartbeat_mode.js");
const taggerPath = path.resolve(__dirname, "..", "tagger.js");
const cadenceScriptPath = path.resolve(__dirname, "..", "heartbeat_task_cadence.js");
const heartbeatLanePath = path.resolve(__dirname, "..", "run_tagger_heartbeat_lane.ps1");

assert.ok(fs.existsSync(modeModulePath), "lib/heartbeat_mode.js must exist");
assert.ok(fs.existsSync(cadenceScriptPath), "heartbeat_task_cadence.js must exist");

const {
  normalizeHeartbeatMode,
  isHeartbeatControlMode,
  modeAllowsHeavy,
  resolveHeartbeatCadenceSeconds,
} = require(modeModulePath);

assert.strictEqual(normalizeHeartbeatMode("idle"), "IDLE", "IDLE must be a valid heartbeat mode");
assert.strictEqual(normalizeHeartbeatMode("off"), "OFF", "OFF must be a valid heartbeat mode");
assert.strictEqual(isHeartbeatControlMode("IDLE"), true, "IDLE must be treated as an Airtable control mode");
assert.strictEqual(isHeartbeatControlMode("OFF"), true, "OFF must be treated as an Airtable control mode");
assert.strictEqual(modeAllowsHeavy("IDLE"), false, "IDLE must block heavy SGL lanes");
assert.strictEqual(modeAllowsHeavy("OFF"), false, "OFF must block heavy SGL lanes");
assert.strictEqual(modeAllowsHeavy("DAY"), true, "DAY must still allow normal due-lane work");

assert.strictEqual(
  resolveHeartbeatCadenceSeconds({ cadence: 300, set_intervals: 999, interval: 120, mode: "NIGHT" }),
  300,
  "Airtable cadence must win over legacy interval"
);

assert.strictEqual(
  resolveHeartbeatCadenceSeconds({ set_intervals: 999, interval: 120, mode: "OVERNIGHT" }),
  999,
  "Airtable set_intervals must be used when cadence is absent"
);

assert.strictEqual(
  resolveHeartbeatCadenceSeconds({ interval: 120, mode: "NIGHT" }),
  300,
  "legacy interval must not be mistaken for seconds when mode defaults are available"
);

const tagger = fs.readFileSync(taggerPath, "utf8");
assert.ok(
  tagger.includes("latestHeartbeatModeControl") &&
    tagger.includes("isHeartbeatControlMode"),
  "tagger.js must preserve Airtable IDLE/OFF control from the latest heartbeat"
);

const cadenceScript = fs.readFileSync(cadenceScriptPath, "utf8");
assert.ok(
  cadenceScript.includes("resolveHeartbeatCadenceSeconds") &&
    cadenceScript.includes("Set-ScheduledTask"),
  "heartbeat_task_cadence.js must sync the Windows task from Airtable cadence"
);

const heartbeatLane = fs.readFileSync(heartbeatLanePath, "utf8");
assert.ok(
  heartbeatLane.includes("HEARTBEAT_TASK_CADENCE") &&
    heartbeatLane.includes("heartbeat_task_cadence.js"),
  "heartbeat lane must run the cadence sync after creating heartbeat records"
);

console.log("heartbeat_mode_control tests passed");
