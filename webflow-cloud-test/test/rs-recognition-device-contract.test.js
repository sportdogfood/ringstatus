import assert from "node:assert/strict";
import test from "node:test";

import { env } from "cloudflare:workers";
import { GET } from "../src/pages/rs-recognition/device.js";

Object.assign(env, {
  AIRTABLE_TOKEN: "pat_test",
  AIRTABLE_BASE_ID: "app_test",
  AIRTABLE_RS_DEVICES_TEST_TABLE: "rs_devices_test",
  AIRTABLE_RS_PEOPLE_TEST_TABLE: "rs_people_test"
});

test("recognizes the live Active device choice and returns its record ID", async () => {
  const calls = [];
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    if (calls.length === 1) {
      return Response.json({
        records: [{
          id: "rec0OtWNkYWs7iGgk",
          fields: {
            device_uid: "device_test_001",
            device_token: "token_test_browser",
            person: ["recxMolAW8UhI3Hph"],
            status: { name: "Active" }
          }
        }]
      });
    }
    return Response.json({
      id: "recxMolAW8UhI3Hph",
      fields: {
        person_name: "Test Member",
        person_uid: "person_test_001",
        first_name: "Test",
        last_name: "Member",
        primary_phone_e164: "+16318752160",
        member_pin: "4826",
        email: "test@example.com",
        status: { name: "test" },
        access_level: { name: "member" }
      }
    });
  };

  const response = await GET({
    request: new Request("https://ringstatus.webflow.io/test/rs-recognition/device?device_token=token_test_browser")
  });
  const body = await response.json();

  assert.equal(response.status, 200);
  assert.equal(body.recognized, true);
  assert.equal(body.recognition_status, "known_device");
  assert.equal(body.device_record_id, "rec0OtWNkYWs7iGgk");
  assert.equal(body.device_uid, "device_test_001");
  assert.equal(body.device_status, "Active");
  assert.equal(body.person_record_id, "recxMolAW8UhI3Hph");
  assert.equal(body.person_uid, "person_test_001");
  assert.equal(body.first_name, "Test");
  assert.equal(body.last_name, "Member");
  assert.equal(body.primary_phone_e164, "+16318752160");
  assert.equal(body.member_pin, "4826");
  assert.equal(body.email, "test@example.com");
  assert.equal(calls.length, 2);
});
