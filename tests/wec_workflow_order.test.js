const assert = require("assert");
const fs = require("fs");
const path = require("path");

const script = fs.readFileSync(
  path.join(__dirname, "..", "docs", "horseshowing", "run-wec-catalyst-workflow.ps1"),
  "utf8"
);

const timeSyncIndex = script.indexOf("Invoke-TimeWorkflowTableSync $heartbeat.focus_day");
const alertIndex = script.indexOf("Write-TimeAlerts $heartbeat.focus_day");

assert.notStrictEqual(timeSyncIndex, -1, "workflow must call Invoke-TimeWorkflowTableSync");
assert.notStrictEqual(alertIndex, -1, "workflow must call Write-TimeAlerts");
assert(
  timeSyncIndex < alertIndex,
  "time workflow tables must refresh before time alerts are evaluated"
);

assert(
  /\[switch\]\$RunMockLiveCheck/.test(script),
  "mock live check must be explicitly gated"
);
assert(
  /if \(\$RunMockLiveCheck\) \{[\s\S]*Invoke-MockLiveCheck/.test(script),
  "mock live check must not run in the default heartbeat path"
);

console.log("wec_workflow_order tests passed");
