const assert = require("assert");
const fs = require("fs");
const path = require("path");

const scriptPath = path.resolve(__dirname, "..", "check_sgl_auth_health.ps1");

assert.ok(fs.existsSync(scriptPath), "check_sgl_auth_health.ps1 must exist");

const script = fs.readFileSync(scriptPath, "utf8");

for (const expected of [
  "PUBLIC_OK_AUTH_BAD",
  "AUTH_OK",
  "AUTH_NOT_CONFIGURED",
  "SOFT_THROTTLE",
  "ENDPOINT_CHANGED_OR_SCOPE_BAD",
  "AUTH_STALE_OR_FORBIDDEN",
]) {
  assert.ok(script.includes(expected), `script must classify ${expected}`);
}

assert.ok(
  script.includes("sgl_fetch.ps1"),
  "script must use the existing PowerShell SGL fetch helper"
);

assert.ok(
  /SessionJsonPath\s+''/.test(script),
  "public/no-auth probe must force no session json"
);

assert.ok(
  script.includes("SGL_AUTHORIZATION") &&
    script.includes("SGL_COOKIE_HEADER") &&
    script.includes("SGL_BEARER_TOKEN"),
  "script must inspect and isolate bearer/cookie inputs"
);

console.log("sgl_auth_health_script tests passed");
