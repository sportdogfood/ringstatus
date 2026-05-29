const assert = require("assert");
const fs = require("fs");
const path = require("path");

const scriptPath = path.resolve(__dirname, "..", "schedules_dailyv2.js");
const script = fs.readFileSync(scriptPath, "utf8");

assert.ok(
  script.includes("emptyPayload = await fetchJson(emptyUrl, {"),
  "schedules_dailyv2 must still attempt the empty schedule ping"
);

assert.ok(
  /catch\s*\(error\)\s*\{[\s\S]*emptyPingError\s*=\s*String\(error\?\.message\s*\|\|\s*error\);[\s\S]*\}/.test(script),
  "empty schedule ping failures must be recorded but must not hard-stop the dated schedule lane"
);

assert.ok(
  script.includes("datedPayload = await fetchJson(datedUrl, {"),
  "dated schedule fetch must remain the authoritative fetch and keep its fail-fast behavior"
);

assert.ok(
  script.includes("endpoint=${endpoint}") &&
    script.includes("body_length=${error?.body_length"),
  "soft payload failures must include endpoint and body length in logs"
);

console.log("schedules_daily_empty_ping tests passed");
