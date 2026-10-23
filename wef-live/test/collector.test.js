"use strict";
const test = require("node:test"),
  assert = require("node:assert/strict");
const { runLive } = require("../collector"),
  m = require("../model"),
  { tables, links } = require("../schema");
const clock = () => "2026-10-23T15:00:00.000Z";
function fixture() {
  const g = {
    class_group_id: "20",
    day: "2026-10-23",
    ring_id: "44",
    ring_number: "3",
    ring: "Covered",
    classes: ["30"],
    classNames: ["Open"],
    classNumbers: ["100"],
    has_JSON: true,
    is_live: true,
    total: 1,
    gone: 0,
  };
  return {
    groups: [{ show_id: "75", json_data: [g] }],
    widget: [
      { show_id: "75", live_data: { 3: { name: "Covered", livenow: [g] } } },
    ],
    detail: {
      ID: "30",
      recs: 1,
      rows: [
        {
          id: "400",
          class_id: "30",
          ENo: "123",
          Hor: "Horse",
          Rid: "Rider",
          Tra: "Trainer",
          OOG: "1",
          Scr: "0",
          Gone: "0",
        },
      ],
    },
    decoded: {
      class: { class_id: 30, show_id: 75 },
      class_related_data: { date: "2026-10-23" },
      trips: [
        {
          trip_id: 400,
          entry_id: 200,
          horse_id: 600,
          rider_id: 700,
          trainer_id: 800,
        },
      ],
    },
  };
}
function fake() {
  const db = Object.fromEntries(Object.keys(tables).map((t) => [t, []]));
  db.shows = [
    {
      id: "show",
      fields: {
        "show-key": "15|75",
        last_checked: clock(),
        is_default: true,
        show_id: 75,
        customer_id: 15,
        selected_date: "2026-10-23",
        start_date: "2026-10-22",
        end_date: "2026-10-25",
        available_dates: "2026-10-23, 2026-10-24",
      },
    },
  ];
  db["wef-schedules.schedules"] = [
    {
      id: "schedule-local",
      fields: { "show-id": 75, date: "2026-10-23", "class-id": 30, status: "active", "stage-key": "75|2026-10-23|20_100" },
    },
  ];
  db["wef-entries.entries"] = [
    { id: "entry-local", fields: { "entry-key": "75|2026-10-23|200|30" } },
  ];
  let id = 0;
  return {
    db,
    api: {
      list: async (t) => structuredClone(db[t]),
      upsert: async (t, k, rows) => {
        const allowed = new Set([...tables[t], ...Object.keys(links[t] || {})]);
        for (const fields of rows) {
          for (const field of Object.keys(fields))
            assert(allowed.has(field), "Undocumented " + t + "." + field);
          let r = db[t].find((r) => r.fields[k] === fields[k]);
          if (!r) {
            r = { id: "record" + ++id, fields: {} };
            db[t].push(r);
          }
          Object.assign(r.fields, fields);
        }
        return structuredClone(
          rows.map((f) => db[t].find((r) => r.fields[k] === f[k])),
        );
      },
      retire: async (t, rows) => {
        db[t] = db[t].filter((r) => !rows.some((x) => x.id === r.id));
      },
    },
  };
}
function args(f = fixture(), store = fake()) {
  return {
    store,
    api: store.api,
    clock,
    archive: async (kind, v) => ({
      at: clock(),
      kind,
      outcome: "Created",
      file: "offline.gz",
      sha256: "offline",
      records: Array.isArray(v) ? v.length : 1,
    }),
    get: async (url) =>
      structuredClone(
        url.includes("ListAjax")
          ? f.groups
          : url.includes("Widget")
            ? f.widget
            : url.includes("getLiveClassData")
              ? f.detail
              : f.decoded,
      ),
  };
}
test("repeat collection keeps stable IDs, distinct ring ID/number, exact native links", async () => {
  const a = args();
  const first = await runLive(a);
  assert.equal(first.ok, true, JSON.stringify(first.errors));
  const ids = a.store.db["live-entries"].map((r) => r.id);
  assert.equal(a.store.db["live-rings"][0].fields["ring-id"], 44);
  assert.equal(a.store.db["live-rings"][0].fields["ring-number"], 3);
  assert.equal(a.store.db["live-entries"][0].fields["entry-id"], 200);
  assert.equal(a.store.db["live-entries"][0].fields["trip-id"], 400);
  assert.deepEqual(a.store.db["live-entries"][0].fields.entry, ["entry-local"]);
  assert.deepEqual(a.store.db["live-classes"][0].fields.schedules, [
    "schedule-local",
  ]);
  const second = await runLive(a);
  assert.equal(second.ok, true);
  assert.deepEqual(
    a.store.db["live-entries"].map((r) => r.id),
    ids,
  );
  assert.equal(a.store.db["run-logs"].length, 2);
});
test("untracked live entries are collected without binder matches", async () => {
  const a = args();
  a.store.db["wef-entries.entries"] = [];
  assert.equal((await runLive(a)).ok, true);
  assert.deepEqual(a.store.db["live-entries"][0].fields.entry, []);
});
test("missing schedules do not prevent independent observations", async () => {
  const a = args();
  a.store.db["wef-schedules.schedules"] = [];
  const r = await runLive(a);
  assert.equal(r.ok, true);
  assert(r.warnings.length);
  assert.equal(r.counts.entries, 1);
});
test("confirmed empty is success", async () => {
  const f = fixture();
  f.groups[0].json_data = [];
  f.widget[0].live_data = {};
  const r = await runLive(args(f));
  assert.equal(r.ok, true);
  assert.equal(r.counts.entries, 0);
});
test("wrong show response preserves existing data", async () => {
  const a = args();
  await runLive(a);
  const before = structuredClone(a.store.db["live-entries"]);
  a.get = async () => [{ show_id: "74", json_data: [] }];
  const r = await runLive(a);
  assert.equal(r.ok, false);
  assert.deepEqual(a.store.db["live-entries"], before);
});
test("future source-selected date does not collect live", async () => {
  const a = args();
  a.store.db.shows[0].fields.selected_date = "2026-10-24";
  a.get = async () => {
    throw Error("Must not request source");
  };
  const r = await runLive(a);
  assert.equal(r.ok, true);
  assert.equal(r.counts.entries, 0);
});
test("failed class identity leaves previous entries intact", async () => {
  const f = fixture(),
    a = args(f);
  await runLive(a);
  const before = structuredClone(a.store.db["live-entries"]);
  f.decoded.trips = [];
  const r = await runLive(a);
  assert.equal(r.ok, false);
  assert.deepEqual(a.store.db["live-entries"], before);
});
test("archive failure prevents collection writes and retirement", async () => {
  const a = args();
  a.archive = async () => {
    throw Error("Archive unavailable");
  };
  const r = await runLive(a);
  assert.equal(r.ok, false);
  assert.equal(a.store.db["live-rings"].length, 0);
  assert.equal(a.store.db["live-entries"].length, 0);
});
test("prior dates retire only after successful archive", async () => {
  const a = args();
  a.store.db["live-entries"] = [
    { id: "old", fields: { date: "2026-10-22", "live-entry-key": "old" } },
  ];
  const r = await runLive(a);
  assert.equal(r.ok, true);
  assert(!a.store.db["live-entries"].some((r) => r.id === "old"));
  assert(r.archives.some((x) => x.kind === "retired-live-entries"));
});
test("malformed and wrong date data cannot be empty success", () => {
  assert.throws(() => m.groups({}, { show: 75, date: "2026-10-23" }, clock()));
  const f = fixture();
  f.groups[0].json_data[0].day = "2026-10-22";
  assert.throws(() =>
    m.groups(f.groups, { show: 75, date: "2026-10-23" }, clock()),
  );
});
test("live trips cannot be mistaken for entry IDs", () => {
  const f = fixture();
  f.decoded.trips[0].trip_id = 401;
  assert.throws(() =>
    m.classDetail(
      f.detail,
      f.decoded,
      { show: 75, date: "2026-10-23" },
      { "class-id": 30, "class-key": "75|2026-10-23|30" },
      clock(),
    ),
  );
});
test("Airtable adapter refuses all writes to source and synced tables", async () => {
  const api = require("../airtable").airtable({
    request: async () => {
      throw Error("Unexpected network");
    },
    pause: async () => {},
  });
  await assert.rejects(() =>
    api.call("appSxF4pYMBaxpTZd", "entries", "PATCH", {}),
  );
  await assert.rejects(() =>
    api.upsert("wef-schedules.schedules", "stage-key", []),
  );
  await assert.rejects(() =>
    api.call("appgzosUcVqA4vZdf", "live-entries", "DELETE", {}),
  );
});

test("temporary reference read failure preserves links and does not stop live observations", async () => {
  const a = args();
  await runLive(a);
  const list = a.api.list;
  a.api.list = async (t) => {
    if (t.startsWith("wef-")) throw Error("Sync temporarily unavailable");
    return list(t);
  };
  const r = await runLive(a);
  assert.equal(r.ok, true);
  assert.deepEqual(a.store.db["live-entries"][0].fields.entry, ["entry-local"]);
  assert.deepEqual(a.store.db["live-classes"][0].fields.schedules, [
    "schedule-local",
  ]);
});
test("retirement adapter refuses missing archive and current date", async () => {
  const api = require("../airtable").airtable({
    request: async () => {
      throw Error("Unexpected network");
    },
    pause: async () => {},
  });
  await assert.rejects(
    () =>
      api.retire(
        "live-entries",
        [{ id: "old", fields: { date: "2026-10-22" } }],
        "2026-10-23",
        null,
      ),
    /archive/,
  );
  await assert.rejects(
    () =>
      api.retire(
        "live-entries",
        [{ id: "current", fields: { date: "2026-10-23" } }],
        "2026-10-23",
        null,
      ),
    /scope/,
  );
});

test('multiple live groups link only their own active schedules, preserving repeat IDs',async()=>{
 const f=fixture(),g=structuredClone(f.groups[0].json_data[0]);g.class_group_id='21';f.groups[0].json_data.push(g);
 const a=args(f),original=a.store.db['wef-schedules.schedules'][0];
 a.store.db['wef-schedules.schedules'].push({id:'second',fields:{...original.fields,'stage-key':'75|2026-10-23|21_100'}},
 {id:'dropped',fields:{...original.fields,status:'dropped'}},
 {id:'other-group',fields:{...original.fields,'stage-key':'75|2026-10-23|210_100'}});
 const r=await runLive(a);assert.equal(r.ok,true);assert.deepEqual(r.warnings,[]);
 assert.deepEqual(a.store.db['live-classes'][0].fields.schedules,['schedule-local','second']);
 for(const g of a.store.db['live-groups'])assert.deepEqual(g.fields.schedules,[g.fields['class-group-id']===20?'schedule-local':'second']);
 const ids=a.store.db['live-classes'].map(r=>r.id);assert.equal((await runLive(a)).ok,true);assert.deepEqual(a.store.db['live-classes'].map(r=>r.id),ids);
});
test('canonical authority read is direct, fresh, unique and never falls back to schedules',async()=>{
 const a=args(),read=a.api.list,calls=[];a.api.list=async(t,b)=>{calls.push([b,t]);return read(t,b);};
 const r=await runLive(a);assert.equal(r.ok,true);assert.equal(r.show_source.base,require('../settings').SHOWS_BASE);
 assert(calls.some(([b,t])=>b===require('../settings').SHOWS_BASE&&t==='shows'));
 const before=structuredClone(a.store.db['live-entries']);
 a.store.db.shows[0].fields.last_checked='2026-10-22T15:00:00Z';
 const failed=await runLive(a);assert.equal(failed.ok,false);assert.deepEqual(a.store.db['live-entries'],before);
 assert.throws(()=>m.scope([fake().db.shows[0],fake().db.shows[0]],clock()),/one default/);
});
test('canonical shows cannot be written by live adapter',async()=>{
 const api=require('../airtable').airtable({pause:async()=>{},request:async()=>{throw Error('Must not reach network');}});
 for(const method of ['POST','PATCH','DELETE'])await assert.rejects(api.call(require('../settings').SHOWS_BASE,'shows',method,{}),/Read-only/);
});

test('one mismatched live/decoded snapshot refreshes the pair with both attempts archived',async()=>{
 const a=args(),get=a.get,archives=[];let details=0,decoded=0;
 a.get=async(url,d)=>{const p=await get(url,d);if(url.includes('getLiveClassData'))details++;if(d&&++decoded===1)p.trips=[];return p;};
 const archive=a.archive;a.archive=async(kind,p)=>{if(kind==='class-30')archives.push(structuredClone(p));return archive(kind,p);};
 const r=await runLive(a);assert.equal(r.ok,true);assert.equal(details,2);assert.equal(decoded,2);assert.equal(archives.length,2);assert.equal(r.snapshot_refreshes.length,1);assert.equal(a.store.db['live-entries'][0].fields['entry-id'],200);
});
test('persistent missing identity stays a failure after exactly one fresh pair',async()=>{
 const a=args(),get=a.get;let decoded=0;a.get=async(url,d)=>{const p=await get(url,d);if(d){decoded++;p.trips=[];}return p;};const r=await runLive(a);assert.equal(r.ok,false);assert.equal(decoded,2);assert.equal(a.store.db['live-entries'].length,0);assert(r.errors.some(e=>e.includes('No decoded identity')));
});
