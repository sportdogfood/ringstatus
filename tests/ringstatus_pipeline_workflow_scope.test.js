const assert = require("assert");
const fs = require("fs");
const path = require("path");

const workflow = fs.readFileSync(
  path.resolve(__dirname, "..", ".github", "workflows", "ringstatus-pipeline.yml"),
  "utf8"
);

assert.ok(
  !/^\s*CUSTOMER_ID:\s*["']?15["']?\s*$/m.test(workflow),
  "ringstatus pipeline must not hardcode CUSTOMER_ID=15"
);

assert.ok(
  workflow.includes("Resolve focused show scope"),
  "ringstatus pipeline must resolve the focused show before running pipeline scripts"
);

for (const envName of [
  "CUSTOMER_ID",
  "HEARTBEAT_TARGET_SHOW_RECORD_ID",
  "HEARTBEAT_TARGET_APP_SHOW_ID",
  "HEARTBEAT_TARGET_CUSTOMER_ID",
  "HEARTBEAT_TARGET_SQL_DATES",
]) {
  assert.ok(
    workflow.includes(`${envName}=`) && workflow.includes("GITHUB_ENV"),
    `ringstatus pipeline must export ${envName} through GITHUB_ENV`
  );
}

console.log("ringstatus_pipeline_workflow_scope tests passed");
