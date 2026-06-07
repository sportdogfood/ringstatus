const assert = require("assert");

const {
  buildLiveGroupsMap,
  buildLiveClassDataEndpoint,
  findLiveClassTrip,
  normalizeLiveClassDataPayload,
  resolveLiveClassIdsForTrip,
} = require("../lib/liveclassv2_enrichment");

const groups = buildLiveGroupsMap([
  {
    id: "recLive",
    fields: {
      class_group_id: 200023694,
      show_id: 200000061,
      day: "2026-05-07",
      class_ids: "200024875,200025008",
      class_numbers: ["711", "712"],
      has_JSON: "true",
      status: "Underway",
      gone: 27,
      total: 30,
      estimated_start_time: "09:00:00",
      ring_number: 3,
    },
  },
  {
    id: "recNoJson",
    fields: {
      class_group_id: 999,
      show_id: 200000061,
      day: "2026-05-07",
      classes: "200000001",
      has_JSON: "false",
    },
  },
], { app_show_id: 200000061, app_sql_date: "2026-05-07" });

assert.deepStrictEqual([...groups.keys()], ["200023694"]);
assert.deepStrictEqual(groups.get("200023694").class_ids, ["200024875", "200025008"]);
assert.deepStrictEqual(groups.get("200023694").class_numbers, ["711", "712"]);
assert.deepStrictEqual(groups.get("200023694").class_pairs, [
  { class_id: "200024875", class_number: "711" },
  { class_id: "200025008", class_number: "712" },
]);
assert.strictEqual(groups.get("200023694").status, "Underway");
assert.strictEqual(groups.get("200023694").gone, 27);
assert.strictEqual(groups.get("200023694").total, 30);

assert.deepStrictEqual(
  resolveLiveClassIdsForTrip(groups.get("200023694"), { classNumber: 712 }),
  ["200025008"]
);
assert.deepStrictEqual(
  resolveLiveClassIdsForTrip(groups.get("200023694"), { classId: 200024875, classNumber: 712 }),
  ["200024875", "200025008"]
);
assert.deepStrictEqual(
  resolveLiveClassIdsForTrip(groups.get("200023694"), { classNumber: 999 }),
  ["200024875", "200025008"]
);

assert.strictEqual(
  buildLiveClassDataEndpoint({
    showId: 200000061,
    classId: 200024875,
    cacheBuster: 1778153449738,
  }),
  "https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id=200000061&cid=200024875&t=1778153449738"
);

assert.strictEqual(
  buildLiveClassDataEndpoint({
    showId: 200000061,
    classId: 200024900,
    classGroupId: 200023824,
    cacheBuster: null,
  }),
  "https://sgl.wellingtoninternational.com/iphonev2/index.php/esp/liveclassv2/getLiveClassData?show_id=200000061&cid=200024900&cgid=200023824"
);

const live = normalizeLiveClassDataPayload({
  ID: "200024875",
  recs: 34,
  ring_number: "3",
  rows: [
    {
      id: "200383053",
      ENo: "3160",
      Hor: "MARKANTO A",
      Rid: "TANNER KOROTKIN",
      OOG: "30",
      Actual_OOG: "26",
      Gone: "1",
      Pos: "11",
    },
  ],
});

assert.strictEqual(live.class_id, 200024875);
assert.strictEqual(live.total_records, 34);
assert.strictEqual(live.ring_number, 3);

const match = findLiveClassTrip(live, { entryNumber: 3160 });
assert.deepStrictEqual(match, {
  live_trip_id: "200383053",
  entry_id: 200383053,
  entry_number: 3160,
  horse: "MARKANTO A",
  rider_name: "TANNER KOROTKIN",
  order_of_go: 30,
  actual_order: 26,
  gone_in: 1,
  position: 11,
});

console.log("liveclassv2_enrichment tests passed");
