import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixturePath = resolve(__dirname, "../../webflow/rs-recognition/rs-recognition-test.html");

test("browser fixture uses the wired recognition and session routes", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /id="rs-recognition-test"/);
  assert.match(source, /rs_device_token/);
  assert.match(source, /https:\/\/ringstatus\.webflow\.io\/test\/rs-recognition\/device/);
  assert.match(source, /https:\/\/ringstatus\.webflow\.io\/test\/rs-recognition\/session/);
  assert.match(source, /recordSessionEvent/);
  assert.match(source, /session_event_uid/);
  assert.match(source, /idempotency_key/);
  assert.match(source, /device_record_id/);
  assert.match(source, /person_record_id/);
});

test("browser fixture never contains Airtable credentials or direct Airtable calls", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.doesNotMatch(source, /api\.airtable\.com/);
  assert.doesNotMatch(source, /AIRTABLE_TOKEN/);
  assert.doesNotMatch(source, /Bearer\s+/);
});
