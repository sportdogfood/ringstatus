const assert = require("assert");

const {
  SoftPayloadError,
  inspectSoftPayload,
  assertValidPayload,
  isSoftPayloadError,
  softPayloadMinBodyLengthForEndpoint,
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
  minBodyLength: 2,
  expectedTopLevelKeys: ["show", "trips"],
});
assert.strictEqual(missingKeys.ok, false);
assert.strictEqual(missingKeys.reason, "soft_payload_missing_expected_keys");

const validByKey = inspectSoftPayload({
  payload: { show: { show_id: 15 } },
  text: '{"show":{"show_id":15}}',
  response: response({ "content-length": "22" }),
  minBodyLength: 2,
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

const smallScheduleWithExpectedKey = inspectSoftPayload({
  payload: { show: {} },
  text: '{"show":{}}',
  response: response({ "content-length": "11" }),
  endpoint: "https://sglapi.wellingtoninternational.com/schedule?date=2026-05-03&show_id=200000060&customer_id=15",
  lane: "schedules_dailyv2",
  expectedTopLevelKeys: ["show"],
});
assert.strictEqual(smallScheduleWithExpectedKey.ok, false);
assert.strictEqual(smallScheduleWithExpectedKey.reason, "soft_payload_too_small");
assert.strictEqual(smallScheduleWithExpectedKey.min_body_length, 5000);

const normalSchedule = inspectSoftPayload({
  payload: { show: {}, show_date: "2026-05-03", show_days_list: [] },
  text: "x".repeat(58758),
  response: response({ "content-length": "58758" }),
  endpoint: "https://sglapi.wellingtoninternational.com/schedule?date=2026-05-03&show_id=200000060&customer_id=15",
  lane: "schedules_dailyv2",
  expectedTopLevelKeys: ["show"],
});
assert.strictEqual(normalSchedule.ok, true);

const normalClassEndpoint = inspectSoftPayload({
  payload: { class_related_data: {}, trips: [] },
  text: "x".repeat(10469),
  response: response({ "content-length": "10469" }),
  endpoint: "https://sglapi.wellingtoninternational.com/classes/200024756/?show_id=200000060&customer_id=15",
  lane: "trips_tagger",
  expectedTopLevelKeys: ["class", "class_related_data", "trips"],
});
assert.strictEqual(normalClassEndpoint.ok, true);
assert.strictEqual(
  softPayloadMinBodyLengthForEndpoint({
    endpoint: "https://sglapi.wellingtoninternational.com/classes/200024756/?show_id=200000060&customer_id=15",
  }),
  500
);
assert.strictEqual(
  softPayloadMinBodyLengthForEndpoint({
    endpoint: "https://sglapi.wellingtoninternational.com/classsignup?show_date=2026-05-03&show_id=200000060&customer_id=15",
  }),
  1000
);
assert.strictEqual(
  softPayloadMinBodyLengthForEndpoint({
    endpoint: "https://sglapi.wellingtoninternational.com/entries/200230238?eid=200230238&show_id=200000060&customer_id=15",
  }),
  128
);

const previousGlobalMin = process.env.SOFT_PAYLOAD_MIN_BODY_LENGTH;
process.env.SOFT_PAYLOAD_MIN_BODY_LENGTH = "2";
assert.strictEqual(
  softPayloadMinBodyLengthForEndpoint({
    endpoint: "https://sglapi.wellingtoninternational.com/schedule?date=2026-05-03&show_id=200000060&customer_id=15",
  }),
  5000,
  "legacy global env cannot lower endpoint-specific defaults"
);
if (previousGlobalMin === undefined) {
  delete process.env.SOFT_PAYLOAD_MIN_BODY_LENGTH;
} else {
  process.env.SOFT_PAYLOAD_MIN_BODY_LENGTH = previousGlobalMin;
}

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
