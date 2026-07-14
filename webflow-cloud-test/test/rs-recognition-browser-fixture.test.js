import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixturePath = resolve(__dirname, "../../webflow/rs-recognition/rs-recognition-test.html");

test("browser fixture contains the complete recognition product states", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /id="rs-recognition-test"/);
  assert.match(source, /id="rs-recognition-card"/);
  assert.match(source, /id="rs-recognition-close"/);
  assert.match(source, /id="rs-not-you"/);
  assert.match(source, /id="rs-update-details"/);
  assert.match(source, /id="rs-profile-form"/);
  assert.match(source, /id="rs-new-profile-form"/);
  assert.match(source, /id="rs-members-login-form"/);
  assert.match(source, /id="rs-recovery-form"/);
  assert.match(source, /id="rs-demo-contact"/);
});

test("profile and new-profile forms preserve the approved fields and requirements", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /name="user"[^>]*required/);
  assert.match(source, /name="first"/);
  assert.match(source, /name="last"/);
  assert.match(source, /name="sms"[^>]*required/);
  assert.match(source, /name="email"/);
  assert.match(source, /Update my details/);
  assert.match(source, /Not you\?/);
});

test("members gate includes immediate phone login and fallback recovery", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /We didn.t recognize that phone number/);
  assert.match(source, /full name or email/i);
  assert.match(source, /email we have on file/i);
  assert.match(source, /full barn/i);
  assert.match(source, /\/members\?user=/);
  assert.match(source, /showMembersLogin/);
  assert.match(source, /showRecovery/);
});

test("browser fixture uses the wired recognition and session routes", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /rs_device_token/);
  assert.match(source, /https:\/\/ringstatus\.webflow\.io\/test\/rs-recognition\/device/);
  assert.match(source, /https:\/\/ringstatus\.webflow\.io\/test\/rs-recognition\/session/);
  assert.match(source, /recordSessionEvent/);
  assert.match(source, /session_event_uid/);
  assert.match(source, /idempotency_key/);
  assert.match(source, /device_record_id/);
  assert.match(source, /person_record_id/);
  assert.match(source, /event_type:\s*"visit"/);
  assert.match(source, /event_type:\s*"save"/);
  assert.match(source, /event_type:\s*"new"/);
});

test("browser fixture never contains Airtable credentials or direct Airtable calls", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.doesNotMatch(source, /api\.airtable\.com/);
  assert.doesNotMatch(source, /AIRTABLE_TOKEN/);
  assert.doesNotMatch(source, /Bearer\s+/);
});
