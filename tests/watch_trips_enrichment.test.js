const assert = require("assert");

const {
  buildClassDetailEndpoint,
  findClassGroupOrderEntry,
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
};

assert.deepStrictEqual(findClassTrip(payload, { entryxclassesUuid: "abc" }), payload.trips[0]);
assert.strictEqual(findClassTrip(payload, { entryxclassesUuid: "missing" }), null);
assert.deepStrictEqual(
  findClassGroupOrderEntry(payload, { entryId: 10, classId: 20 }),
  payload.class_group_order_of_go.entries[0]
);
assert.strictEqual(findClassGroupOrderEntry(payload, { entryId: 12, classId: 20 }), null);

console.log("watch_trips_enrichment tests passed");
