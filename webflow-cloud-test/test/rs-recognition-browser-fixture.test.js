import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const fixturePath = resolve(__dirname, "../../webflow/rs-recognition/rs-recognition-test.html");

test("fixture is only the self-contained recognition card", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /id="rs-recognition-test"/);
  assert.match(source, /id="rs-recognition-card"/);
  assert.match(source, /id="rs-recognition-close"/);
  assert.match(source, /Hi Lainey/);
  assert.match(source, /id="rs-not-you"/);
  assert.match(source, /id="rs-update-details"/);
  assert.doesNotMatch(source, /rs-preview-controls|rs-test-controls|rs-demo-nav/);
  assert.doesNotMatch(source, /rs-members-login-form|rs-recovery-form/);
});

test("card expands inline to Profile and New Profile with the approved fields", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /id="rs-profile-form"/);
  assert.match(source, /id="rs-new-profile-form"/);
  assert.match(source, /name="user"[^>]*required/);
  assert.match(source, /name="first"/);
  assert.match(source, /name="last"/);
  assert.match(source, /name="sms"[^>]*required/);
  assert.match(source, /name="email"/);
  assert.match(source, /showProfile/);
  assert.match(source, /showNewProfile/);
});

test("card contains no backend, Airtable, session, or recognition wiring", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.doesNotMatch(source, /ringstatus\.webflow\.io|api\.airtable\.com/);
  assert.doesNotMatch(source, /rs-recognition\/(?:device|session|identity)/);
  assert.doesNotMatch(source, /AIRTABLE_TOKEN|recordSessionEvent|session_event_uid|idempotency_key/);
});
