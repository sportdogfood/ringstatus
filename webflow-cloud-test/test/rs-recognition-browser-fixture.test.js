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
  assert.doesNotMatch(source, /We recognized this device\./);
  assert.doesNotMatch(source, /rs-preview-controls|rs-test-controls|rs-demo-nav/);
});

test("card keeps Profile and reuses one member lookup instead of New Profile", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /id="rs-profile-form"/);
  assert.match(source, /name="user"[^>]*required/);
  assert.match(source, /name="sms"[^>]*required/);
  assert.match(source, /name="pin"[^>]*maxlength="4"/);
  assert.match(source, /name="email"/);
  assert.match(source, /showProfile/);
  assert.match(source, /id="rs-recovery-form"/);
  assert.match(source, /id="rs-recovery-first"/);
  assert.match(source, /id="rs-recovery-last"/);
  assert.match(source, /id="rs-recovery-email"/);
  assert.doesNotMatch(source, /id="rs-new-profile-form"|showNewProfile/);
});

test("Not you opens the shared lookup and recovery confirms with thanks", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /rs-not-you"\)\.addEventListener\("click", function/);
  assert.match(source, /rs-not-you[\s\S]*?retire_device[\s\S]*?clearDeviceToken\(\)[\s\S]*?showRecovery\(\)/);
  assert.match(source, /Schedule a demo/);
  assert.match(source, />Contact Me</);
  assert.match(source, /type="submit">Save</);
  assert.match(source, /Thank you\./);
  assert.match(source, /missing_recovery_identity[\s\S]*?Enter your full name or email\./);
});

test("Login presents one phone or PIN action without country-prefix instructions", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /Add your SMS number or PIN\./);
  assert.match(source, /for="rs-login-sms">SMS number or PIN/);
  assert.match(source, /id="rs-login-sms"[^>]*inputmode="tel"[^>]*autocomplete="tel"/);
  assert.match(source, /placeholder="Phone number or 4-digit PIN"/);
  assert.doesNotMatch(source, /\+1/);
  assert.match(source, /type="submit">Continue</);
});

test("local preview can deliberately open Login or member lookup", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /window\.location\.search\.includes\("view=login"\)/);
  assert.match(source, /window\.location\.search\.includes\("view=recovery"\)/);
  assert.match(source, /previewView === "login"[\s\S]*?showLogin\(\)/);
  assert.match(source, /previewView === "recovery"[\s\S]*?showRecovery\(\)/);
  assert.doesNotMatch(source, /rs-preview-controls|rs-test-controls|rs-demo-nav/);
});

test("members gate redirects known phones and moves unknown phones to lookup", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /const memberPath = path === "\/members"/);
  assert.match(source, /action:\s*"phone_login"/);
  assert.match(source, /if \(!result\.recognized\)[\s\S]*?showRecovery\(\)/);
  assert.match(source, /redirectToMembers\(result\.person_uid/);
});

test("card uses the existing recognition/session routes and one action route", () => {
  const source = readFileSync(fixturePath, "utf8");

  assert.match(source, /rs-recognition\/device/);
  assert.match(source, /rs-recognition\/session/);
  assert.match(source, /rs-recognition\/action/);
  assert.match(source, /action:\s*"update_profile"/);
  assert.match(source, /action:\s*"phone_login"/);
  assert.match(source, /action:\s*"recovery"/);
  assert.match(source, /action:\s*"confirm_device"/);
  assert.match(source, /action:\s*"retire_device"/);
  assert.match(source, /id="rs-members-login-form"/);
  assert.match(source, /id="rs-recovery-form"/);
  assert.doesNotMatch(source, /api\.airtable\.com|AIRTABLE_TOKEN|Bearer\s+/);
});
