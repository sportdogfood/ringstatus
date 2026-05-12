const assert = require("node:assert/strict");
const test = require("node:test");

const {
  buildScheduleViewModel,
  renderScheduleHtml,
} = require("./build_schedule_render");

test("buildScheduleViewModel groups schedule rows by ring and attaches trips", () => {
  const payload = {
    meta: { generated_at: "2026-05-12T12:00:00.000Z" },
    lanes: {
      rings: [{ id: "ring:1", ring_number: 1, ringName: "Main" }],
      classes: [{
        schedule_record_id: "sched1",
        schedule_key: "skey",
        schedule_instance_key: "sinst",
        class_number: 101,
        class_name: "Low Adult Jumper",
        class_type: "Jumpers",
      }],
      class_start: [{
        schedule_record_id: "sched1",
        schedule_key: "skey",
        schedule_instance_key: "sinst",
        estimated_start_time: "09:00:00",
      }],
      groups: [{
        schedule_record_id: "sched1",
        schedule_key: "skey",
        schedule_instance_key: "sinst",
        ring_number: 1,
        group_name: "Low Adult Jumper",
        total_trips: 12,
        completed_trips: 3,
      }],
      entries: [{ trips_key: "tkey", trip_instance_key: "tinst", entry_number: 44 }],
      trip_go: [{ schedule_record_id: "sched1", trips_key: "tkey", trip_instance_key: "tinst", estimated_go_time: "09:20:00" }],
      trips: [{ schedule_record_id: "sched1", trips_key: "tkey", trip_instance_key: "tinst", status: "active" }],
      horses: [{ trips_key: "tkey", horse: "Example Horse" }],
      riders: [{ trips_key: "tkey", rider_name: "Example Rider" }],
    },
  };

  const model = buildScheduleViewModel(payload);

  assert.equal(model.rings.length, 1);
  assert.equal(model.rings[0].rows.length, 1);
  assert.equal(model.rings[0].rows[0].trips.length, 1);
  assert.equal(model.rings[0].rows[0].trips[0].rider_name, "Example Rider");
});

test("renderScheduleHtml renders schedule content without raw JSON", () => {
  const html = renderScheduleHtml({
    generated_at: "2026-05-12T12:00:00.000Z",
    summary: { rings: 1, rows: 1, trips: 0 },
    rings: [{
      ring_number: 1,
      ring_name: "Main",
      rows: [{
        start_time: "09:00:00",
        class_number: 101,
        class_name: "Low Adult Jumper",
        class_type: "Jumpers",
        group_name: "Low Adult Jumper",
        completed_trips: 3,
        total_trips: 12,
        trips: [],
      }],
    }],
  });

  assert.match(html, /Low Adult Jumper/);
  assert.match(html, /Main/);
  assert.doesNotMatch(html, /"lanes"/);
});
