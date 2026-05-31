const assert = require("assert");
const fs = require("fs");
const path = require("path");

const root = path.resolve(__dirname, "..");
const liveRingsPath = path.join(root, "live_rings_daily.js");

assert.ok(fs.existsSync(liveRingsPath), "live_rings_daily.js must exist");

const {
  buildRingKey,
  normalizeLiveRingSnapshots,
} = require("../live_rings_daily");

assert.strictEqual(
  buildRingKey({
    customer_id: 15,
    show_id: 200000063,
    focus_day: "2026-05-31",
    ring_number: 3,
  }),
  "15|200000063|2026-05-31|3",
  "live_rings ring_key must be customer_id|show_id|focus_day|ring_number"
);

const payload = [
  {
    show_id: "200000063",
    show_name: "2026 ESP June I (#5029)",
    live_data: {
      "3": {
        name: "VanKampen Covered Arena",
        livenow: [
          {
            class_group_id: "200024674",
            group_name: "Itty Bitty Jumpers (.60-.65m) II2d",
            day: "2026-05-31",
            group_sequence: "2",
            estimated_start_time: "08:30:00",
            ring_number: "3",
            ring_id: "44",
            ring: "VanKampen Covered Arena",
            classNumbers: ["723"],
            class_numbers: "723",
            status: "Underway",
            gone: 19,
            total: 21,
            is_live: true,
            has_JSON: true,
            curr_updated_at: 1780248232,
          },
        ],
        upcoming: [
          {
            class_group_id: "200024675",
            group_name: "Dover Saddlery/USEF Hunter Seat Medal",
            day: "2026-05-31",
            group_sequence: "3",
            estimated_start_time: "09:45:00",
            ring_number: "3",
            ring_id: "44",
            ring: "VanKampen Covered Arena",
            classNumbers: ["550"],
            class_numbers: "550",
            status: "Upcoming",
            gone: 0,
            total: 8,
            is_live: false,
            has_JSON: true,
          },
          {
            class_group_id: "200024676",
            group_name: "Do Not Expand All Upcoming",
            day: "2026-05-31",
            group_sequence: "4",
            estimated_start_time: "10:30:00",
            ring_number: "3",
            ring_id: "44",
            ring: "VanKampen Covered Arena",
            classNumbers: ["551"],
            class_numbers: "551",
            status: "Upcoming",
          },
        ],
        completed: [],
      },
    },
  },
];

const liveGroupLinks = new Map([
  ["200000063|2026-05-31|3|200024674", "recLiveGroup"],
  ["200000063|2026-05-31|3|200024675", "recNextGroup"],
  ["200000063|2026-05-31|3|200024676", "recUnusedUpcoming"],
]);

const rows = normalizeLiveRingSnapshots(payload, {
  customer_id: 15,
  show_id: 200000063,
  focus_day: "2026-05-31",
  show_record_id: "recShow",
  as_of: "2026-05-31T13:35:00.000Z",
  liveGroupLinks,
});

assert.strictEqual(rows.length, 1, "live_rings must create one row per ring snapshot, not one per upcoming class");

assert.deepStrictEqual(
  pick(rows[0].fields, [
    "ring_key",
    "response_ready",
    "is_latest",
    "show",
    "show_id",
    "focus_day",
    "ring_number",
    "ring_id",
    "ring_name",
    "is_current_scope",
    "dropped_at",
    "as_of",
    "last_seen_at",
    "ring_query_key",
    "live_group",
    "live_class_group_id",
    "live_status",
    "live_start_time",
    "live_gone",
    "live_total",
    "live_progress",
    "next_group",
    "next_class_group_id",
    "next_start_time",
    "ring_state",
  ]),
  {
    ring_key: "15|200000063|2026-05-31|3",
    response_ready: true,
    is_latest: true,
    show: ["recShow"],
    show_id: 200000063,
    focus_day: "2026-05-31",
    ring_number: 3,
    ring_id: 44,
    ring_name: "VanKampen Covered Arena",
    is_current_scope: true,
    dropped_at: null,
    as_of: "2026-05-31T13:35:00.000Z",
    last_seen_at: "2026-05-31T13:35:00.000Z",
    ring_query_key: "15|200000063|2026-05-31|3",
    live_group: ["recLiveGroup"],
    live_class_group_id: 200024674,
    live_status: "Underway",
    live_start_time: "08:30:00",
    live_gone: 19,
    live_total: 21,
    live_progress: "19/21",
    next_group: ["recNextGroup"],
    next_class_group_id: 200024675,
    next_start_time: "09:45:00",
    ring_state: "live",
  },
  "live_rings must store ring state plus one live pointer and one next pointer"
);

assert.ok(rows[0].fields.payload_hash, "live_rings must store a payload hash for change detection");

assert.ok(
  !JSON.stringify(rows[0].fields).includes("200024676"),
  "live_rings must not expand/store the second upcoming class as the next pointer"
);

const source = fs.readFileSync(liveRingsPath, "utf8");
assert.ok(
  source.includes('TABLE_LIVE_RINGS = process.env.TABLE_LIVE_RINGS || "live_rings"'),
  "live_rings_daily must write to live_rings by default"
);
assert.ok(
  source.includes("LiveScoreWidget"),
  "live_rings_daily must use the LiveScoreWidget payload"
);

console.log("live_rings_contract tests passed");

function pick(source, names) {
  const out = {};
  for (const name of names) out[name] = source[name];
  return out;
}
