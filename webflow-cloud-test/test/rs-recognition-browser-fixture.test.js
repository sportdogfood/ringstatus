import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixturePath = resolve(__dirname, "../../webflow/rs-recognition/rs-recognition-test.html");
const widgetPath = resolve(__dirname, "../public/rs-recognition.js");

function productSource() {
  return `${readFileSync(fixturePath, "utf8")}\n${readFileSync(widgetPath, "utf8")}`;
}

test("browser fixture contains the complete recognition product states", () => {
  const source = productSource();

  assert.match(source, /root\.id = "rs-recognition-test"/);
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
  const source = productSource();

  assert.match(source, /name="user"[^>]*required/);
  assert.match(source, /name="first"/);
  assert.match(source, /name="last"/);
  assert.match(source, /name="sms"[^>]*required/);
  assert.match(source, /name="email"/);
  assert.match(source, /Update my details/);
  assert.match(source, /Not you\?/);
});

test("members gate includes immediate phone login and fallback recovery", () => {
  const source = productSource();

  assert.match(source, /We didn.t recognize (?:that phone number|your phone)/);
  assert.match(source, /full name or email/i);
  assert.match(source, /email we have on file/i);
  assert.match(source, /full barn/i);
  assert.match(source, /\/members\?user=/);
  assert.match(source, /showMembersLogin/);
  assert.match(source, /showRecovery/);
});

test("browser fixture uses the wired recognition and session routes", () => {
  const source = productSource();

  assert.match(source, /rs_device_token/);
  assert.match(source, /https:\/\/ringstatus\.webflow\.io\/test\/rs-recognition/);
  assert.match(source, /\$\{apiBase\}\/device/);
  assert.match(source, /\$\{apiBase\}\/session/);
  assert.match(source, /\$\{apiBase\}\/identity/);
  assert.match(source, /recordSessionEvent/);
  assert.match(source, /session_event_uid/);
  assert.match(source, /idempotency_key/);
  assert.match(source, /device_record_id/);
  assert.match(source, /person_record_id/);
  assert.match(source, /action:\s*"create_profile"/);
  assert.match(source, /action:\s*"update_profile"/);
  assert.match(source, /action:\s*"phone_login"/);
  assert.match(source, /action:\s*"recovery"/);
  assert.match(source, /action:\s*"confirm_device"/);
  assert.match(source, /action:\s*"retire_device"/);
  assert.doesNotMatch(source, /isLocalTest/);
  assert.doesNotMatch(source, /mockPerson/);
  assert.match(source, /public\/rs-recognition\.js|rs-recognition\.js/);
  assert.match(source, /window\.location\.pathname/);
  assert.match(source, /new URLSearchParams\(window\.location\.search\)/);
});

test("browser fixture never contains Airtable credentials or direct Airtable calls", () => {
  const source = productSource();

  assert.doesNotMatch(source, /api\.airtable\.com/);
  assert.doesNotMatch(source, /AIRTABLE_TOKEN/);
  assert.doesNotMatch(source, /Bearer\s+/);
});
