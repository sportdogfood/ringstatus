const assert = require("assert");

const {
  SoftPayloadError,
  inspectSoftPayload,
  assertValidPayload,
  isSoftPayloadError,
} = require("../lib/soft_payload_guard");

function response(headers = {}) {
  return {
    status: 200,
    headers: {
      get(name) {
        return headers[String(name).toLowerCase()] ?? null;
      },
    },
  };
}

assert.deepStrictEqual(
  inspectSoftPayload({
    payload: {},
    text: "{}",
    response: response({ "content-length": "2" }),
    expectedTopLevelKeys: ["show", "trips"],
  }).ok,
  false,
  "empty object payload is not valid"
);

const empty = inspectSoftPayload({
  payload: {},
  text: "{}",
  response: response({ "content-length": "2" }),
  expectedTopLevelKeys: ["show", "trips"],
});
assert.strictEqual(empty.reason, "soft_payload_empty");
assert.strictEqual(empty.content_length, 2);
assert.strictEqual(empty.body_length, 2);

const missingKeys = inspectSoftPayload({
  payload: { ok: true },
  text: '{"ok":true}',
  response: response({ "content-length": "11" }),
  expectedTopLevelKeys: ["show", "trips"],
});
assert.strictEqual(missingKeys.ok, false);
assert.strictEqual(missingKeys.reason, "soft_payload_missing_expected_keys");

const validByKey = inspectSoftPayload({
  payload: { show: { show_id: 15 } },
  text: '{"show":{"show_id":15}}',
  response: response({ "content-length": "22" }),
  expectedTopLevelKeys: ["show", "trips"],
});
assert.strictEqual(validByKey.ok, true);

const validByPredicate = inspectSoftPayload({
  payload: { nested: [{ class_id: 1, entry_id: 2, horse: "A", entryxclasses_uuid: "u" }] },
  text: '{"nested":[{"class_id":1,"entry_id":2,"horse":"A","entryxclasses_uuid":"u"}]}',
  response: response({ "content-length": "78" }),
  expectedPredicate(payload) {
    return Array.isArray(payload.nested);
  },
});
assert.strictEqual(validByPredicate.ok, true);

assert.throws(
  () => assertValidPayload({
    payload: {},
    text: "{}",
    response: response({ "content-length": "2" }),
    lane: "test_lane",
    endpoint: "https://example.test/endpoint",
    expectedTopLevelKeys: ["show"],
  }),
  (error) => {
    assert.ok(error instanceof SoftPayloadError);
    assert.strictEqual(error.reason, "soft_payload_empty");
    assert.strictEqual(error.lane, "test_lane");
    assert.strictEqual(error.endpoint, "https://example.test/endpoint");
    assert.ok(isSoftPayloadError(error));
    return true;
  }
);

console.log("soft_payload_guard tests passed");
