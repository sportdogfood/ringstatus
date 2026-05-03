const assert = require("assert");

const {
  buildClassDetailEndpoint,
  buildClassSignupGroupEndpoint,
  findClassGroupOrderEntry,
  findClassSignupEntry,
  findClassTrip,
  normalizeClassEndpointWithCgid,
} = require("../lib/watch_trips_enrichment");

assert.strictEqual(
  buildClassDetailEndpoint({
    baseUrl: "https://sglapi.wellingtoninternational.com",
    classId: 200024766,
    showId: 200000060,
    customerId: 15,
    classGroupId: 200023612,
  }),
  "https://sglapi.wellingtoninternational.com/classes/200024766?show_id=200000060&customer_id=15&cgid=200023612"
);

assert.strictEqual(
  normalizeClassEndpointWithCgid(
    "https://sglapi.wellingtoninternational.com/classes/200024766?show_id=200000060&customer_id=15",
    200023612
  ),
  "https://sglapi.wellingtoninternational.com/classes/200024766?show_id=200000060&customer_id=15&cgid=200023612"
);

assert.strictEqual(
  normalizeClassEndpointWithCgid(
    "https://sglapi.wellingtoninternational.com/classes/200024766/?show_id=200000060&customer_id=15&cgid=200023612",
    999
  ),
  "https://sglapi.wellingtoninternational.com/classes/200024766/?show_id=200000060&customer_id=15&cgid=200023612"
);

assert.strictEqual(
  buildClassSignupGroupEndpoint({
    baseUrl: "https://sglapi.wellingtoninternational.com",
    classGroupId: 200023629,
    entryId: 200233908,
    showId: 200000060,
    customerId: 15,
  }),
  "https://sglapi.wellingtoninternational.com/classsignup/200023629?eid=200233908&show_id=200000060&customer_id=15"
);

const payload = {
  trips: [
    { entryxclasses_uuid: "abc", entry_id: 10, class_id: 20, order_of_go: 7 },
  ],
  class_group_order_of_go: {
    entries: [
      { entry_id: 10, class_id: 20, order_of_go: 8 },
      { entry_id: 11, class_id: 20, order_of_go: 9 },
    ],
  },
  entry_x_classes: [
    { entry_id: 10, entry_number: 2554, class_id: 20, class_number: 380, order_of_go: 6 },
    { entry_id: 10, entry_number: 2554, class_id: 21, class_number: 381, order_of_go: 12 },
    { entry_id: 11, entry_number: 63, class_id: 20, class_number: 380, order_of_go: 7 },
  ],
};

assert.deepStrictEqual(findClassTrip(payload, { entryxclassesUuid: "abc" }), payload.trips[0]);
assert.strictEqual(findClassTrip(payload, { entryxclassesUuid: "missing" }), null);
assert.deepStrictEqual(
  findClassTrip(payload, { entryxclassesUuid: "missing", entryId: 10, classId: 20 }),
  payload.trips[0]
);
assert.deepStrictEqual(
  findClassGroupOrderEntry(payload, { entryId: 10, classId: 20 }),
  payload.class_group_order_of_go.entries[0]
);
assert.strictEqual(findClassGroupOrderEntry(payload, { entryId: 12, classId: 20 }), null);
assert.deepStrictEqual(
  findClassSignupEntry(payload, { entryId: 10, classNumber: 380 }),
  payload.entry_x_classes[0]
);
assert.deepStrictEqual(
  findClassSignupEntry(payload, { entryId: 10, classNumber: 381 }),
  payload.entry_x_classes[1]
);
assert.deepStrictEqual(
  findClassSignupEntry(payload, { entryNumber: 2554, classNumber: 380 }),
  payload.entry_x_classes[0]
);
assert.strictEqual(findClassSignupEntry(payload, { entryId: 10 }), null);
assert.strictEqual(findClassSignupEntry(payload, { entryId: 12, classNumber: 380 }), null);

console.log("watch_trips_enrichment tests passed");
