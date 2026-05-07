const assert = require("assert");
const fs = require("fs");
const path = require("path");

const scriptPath = path.resolve(__dirname, "..", "runner_pipeline_common.ps1");
const script = fs.readFileSync(scriptPath, "utf8");

for (const name of [
  "SGL_AUTHORIZATION",
  "SGL_BEARER_TOKEN",
  "SGL_COOKIE_HEADER",
  "SGL_FETCH_SESSION_JSON",
]) {
  assert.ok(script.includes(`'${name}'`), `runner must import User-scope ${name} when missing from process env`);
}

assert.ok(
  script.includes("[Environment]::GetEnvironmentVariable($name, 'User')") &&
    script.includes('Set-Item -LiteralPath "Env:$name" -Value $userValue'),
  "runner must copy User-scope SGL env values into the task process env"
);

console.log("runner_pipeline_sgl_env tests passed");
