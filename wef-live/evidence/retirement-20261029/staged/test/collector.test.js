"use strict";
const test = require("node:test"),
  assert = require("node:assert/strict");
const { runLive } = require("../collector"),
  m = require("../model"),
  { tables, links } = require("../schema");
const clock = () => "2026-10-23T15:00:00.000Z";
const observations = x => JSON.parse(JSON.stringify(x, (k,v)=>k.startsWith('participation-')?undefined:v));

test('each invocation stamps Running before source work and finalizes the same heartbeat row',async()=>{
 const a=args(),upsert=a.api.upsert,getStatus=a.getStatus,states=[];
 a.api.upsert=async(t,k,rows)=>{if(t==='heartbeat')states.push(structuredClone(rows[0]));return upsert(t,k,rows);};
 a.getStatus=async(...args)=>{assert.equal(a.store.db.heartbeat.at(-1).fields.result,'Running');return getStatus(...args);};
 const first=await runLive(a),second=await runLive(a);
 assert.equal(a.store.db.heartbeat.length,2);assert.notEqual(first.run_id,second.run_id);
 assert.deepEqual(states.map(x=>x.result),['Running','Success','Running','Success']);
 for(const [i,r] of [first,second].entries()){
  assert.equal(states[i*2]['heartbeat-key'],r.run_id);assert.equal(states[i*2+1]['heartbeat-key'],r.run_id);
  const row=a.store.db.heartbeat[i].fields;assert.equal(row['run-id'],r.run_id);assert.equal(row.started,r.started);assert.equal(row.completed,r.completed);
 }
});
test('failed class retains reciprocal group membership when source moves it',async()=>{
 const f=fixture();const a=args(f),get=a.get;await runLive(a);
 const originalClass=structuredClone(a.store.db['live-classes'][0]);
 const originalGroup=structuredClone(a.store.db['live-groups'][0]);
 f.groups[0].json_data.push({...f.groups[0].json_data[0],class_group_id:'21'});
 f.groups[0].json_data[0].classes=['31'];
 a.get=async(url,d)=>{const p=await get(url,d);if(url.includes('cid=30'))throw Error('HTTP 502');
 if(url.includes('cid=31')){p.ID='31';p.rows[0].class_id='31';p.rows[0].id='401';}
 if(d&&url.includes('/31?')){p.class.class_id=31;p.trips[0].trip_id=401;}return p;};
 const r=await runLive(a);assert.equal(r.ok,true);
 assert.deepEqual(a.store.db['live-classes'].find(c=>c.id===originalClass.id),originalClass);
 assert(a.store.db['live-groups'].find(g=>g.id===originalGroup.id).fields.classes.includes(originalClass.id));
 assert(!a.store.db['live-groups'].find(g=>g.fields['class-group-id']===21).fields.classes.includes(originalClass.id));
});
for (const failure of ['HTTP 502', 'timeout', 'malformed', 'scope']) test(`ordinary class ${failure} is partial, preserves links and allows other classes`,async()=>{
 const f=fixture();f.groups[0].json_data.push({...f.groups[0].json_data[0],class_group_id:'21',classes:['31']});
 const a=args(f),get=a.get,archived=[];let fail=false;
 const archive=a.archive;a.archive=async(k,p)=>{archived.push({k,p:structuredClone(p)});return archive(k,p);};
 a.get=async(url,d)=>{const p=await get(url,d);const second=url.includes('cid=31')||url.includes('/31?');
 if(url.includes('cid=31')){p.ID='31';p.rows[0].class_id='31';p.rows[0].id='401';}
 if(d&&second){p.class.class_id=31;p.trips[0].trip_id=401;}
 if(fail&&!second){if(url.includes('getLiveClassData')){if(failure==='HTTP 502'||failure==='timeout')throw Error(failure);if(failure==='malformed')p.rows=null;}if(d&&failure==='scope')p.class.show_id=999;}
 return p;};
 await runLive(a);const before=structuredClone(a.store.db['live-classes'].find(c=>c.fields['class-id']===30));
 const beforeEntry=structuredClone(a.store.db['live-entries'].find(e=>e.fields['trip-id']===400));fail=true;
 const r=await runLive(a);assert.equal(r.ok,true);assert.equal(r.unresolved[0].status,'error');
 assert.equal(a.store.db.heartbeat.at(-1).fields.result,'Partial');assert.equal(r.counts.entries,1);
 assert.deepEqual(a.store.db['live-classes'].find(c=>c.id===before.id),before);
 assert.deepEqual(observations(a.store.db['live-entries'].find(c=>c.id===beforeEntry.id)),observations(beforeEntry));
 assert.equal(a.store.db.collections.find(c=>c.fields['source-id']==='30').fields.status,'error');
 const pair=archived.filter(x=>x.k==='class-30').at(-1).p;assert(pair.decoded);
});
test('run-log outage cannot interrupt collection or heartbeat and does not repeat per archive',async()=>{
 const a=args(),upsert=a.api.upsert;let attempts=0;
 a.api.upsert=async(t,k,rows)=>{if(t==='run-logs'){attempts++;throw Error('Audit timeout');}return upsert(t,k,rows);};
 const r=await runLive(a);assert.equal(r.ok,true);assert.equal(attempts,1);assert.equal(a.store.db['live-entries'].length,1);
 assert.equal(a.store.db.heartbeat.at(-1).fields.result,'Success');assert.match(r.logging_errors[0],/Audit timeout/);assert(r.archives.length>1);
 const id=a.store.db['live-entries'][0].id;assert.equal((await runLive(a)).ok,true);assert.equal(a.store.db['live-entries'][0].id,id);
});
test('heartbeat outage leaves collection and final run log successful',async()=>{
 const a=args(),upsert=a.api.upsert;a.api.upsert=async(t,k,rows)=>{if(t==='heartbeat')throw Error('Heartbeat timeout');return upsert(t,k,rows);};
 const r=await runLive(a);assert.equal(r.ok,true);assert.equal(a.store.db['run-logs'][0].fields.result,'Success');assert.match(r.logging_errors[0],/heartbeat/);
});
test('unresolved class does not prevent a different class from collecting', async()=>{
 const f=fixture();f.groups[0].json_data.push({...f.groups[0].json_data[0],class_group_id:'21',classes:['31']});
 const a=args(f),get=a.get;
 a.get=async(url,d)=>{const p=await get(url,d);if(url.includes('cid=31')){p.ID='31';p.rows[0].class_id='31';p.rows[0].id='401';}if(d){if(url.includes('/31?')){p.class.class_id=31;p.trips[0].trip_id=401;}else p.trips=[];}return p;};
 const r=await runLive(a);assert.equal(r.ok,true);assert.equal(r.unresolved.length,1);
 assert.equal(a.store.db['live-entries'].length,1);assert.equal(a.store.db['live-entries'][0].fields['trip-id'],401);
 assert.equal(a.store.db.collections.find(c=>c.fields['source-id']==='30').fields.status,'unresolved');
});
test('false live status skips source details and preserves records, then resumes when true', async () => {
  const a = args();
  await runLive(a);
  const before = JSON.stringify(a.store.db['live-entries']);
  const skipped = await runLive({...a, getStatus: async () => false, get: async () => { throw Error('No source call expected'); }});
  assert.equal(skipped.skipped, 'live-status-false');
  assert.equal(skipped.ok, true);
  assert.equal(JSON.stringify(a.store.db['live-entries']), before);
  assert.equal(a.store.db.heartbeat.at(-1).fields.result, 'Skipped');
  assert.equal((await runLive(a)).ok, true);
});
test('all TODAY groups completed skips widget and class requests; another day does not affect gate', async () => {
  const f = fixture(); f.groups[0].json_data[0].status = 'Completed';
  f.groups[0].json_data.push({...f.groups[0].json_data[0], day:'2026-10-24', status:'Upcoming'});
  const a = args(f); const get = a.get; const calls = [];
  a.get = async url => { calls.push(url); return get(url); };
  const report = await runLive(a);
  assert.equal(report.skipped, 'all-today-groups-completed');
  assert.equal(report.ok, true);
  assert.equal(calls.length, 1); assert.match(calls[0], /ListAjax/);
  assert.equal(a.store.db['live-classes'].length, 0);
  assert.equal(a.store.db['run-logs'][0].fields.result, 'Skipped');
});
test('empty or mixed groups do not imply completed and invalid status fails visibly', async () => {
  const {allCompleted, statusValue, readStatus} = require('../activity');
  assert.equal(allCompleted([]), false);
  assert.equal(allCompleted([{status:'Completed'}, {status:'Upcoming'}]), false);
  for (const value of [null, {}, '', 'false', {status:null}]) assert.throws(()=>statusValue(value));
  assert.equal(await readStatus({customer:15}, async()=>({ok:true,json:async()=>false})), false);
  await assert.rejects(()=>readStatus({customer:15}, async()=>({ok:false,status:502})), /502/);
  const a = args(); const report = await runLive({...a, getStatus:async()=>null});
  assert.equal(report.ok, false); assert.equal(report.skipped, undefined);
  assert.equal(a.store.db.heartbeat.at(-1).fields.result, 'Failed');
});
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
    { id: "entry-local", fields: { "entry-key": "75|2026-10-23|200|30",'show-id':75,date:'2026-10-23','class-id':30,'entry-id':200,'horse-id':600,'rider-id':700,status:'active','last-seen':clock(),'observation-status':'successful','schedule-key':'75|2026-10-23|20_100' } },
  ];
  Object.assign(db['wef-entries.entries'][0].fields, {
    'schedule-keys':'["75|2026-10-23|20_100"]',
    'schedule-link-status':'linked','schedule-link-basis':'class-date','group-link-status':'unresolved',
    'source-json':JSON.stringify({format:'wef-entry-link-v1',revision:'r1',scope:'15|75|2026-10-23|3|30',
      sourceEvidence:{revision:'r1',scope:'15|75|2026-10-23|3|30'},sourceAsOf:clock(),latestApplicableAttempts:[]})
  });
  db['show-collection']=[{fields:{'show-id':75,date:'2026-10-23',started:clock(),completed:clock(),status:'complete'}}];
  let id = 0;
  return {
    db,
    api: {
      list: async (t) => structuredClone(t==='schedules'?db['wef-schedules.schedules']:db[t]),
      fieldNames: async () => ['last-seen','observation-status','schedule-key'],
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
    getStatus: async () => true,
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
test('empty source arrays preserve current and future records and links across repeated runs', async () => {
  const f = fixture(), a = args(f);
  assert.equal((await runLive(a)).ok, true);
  const protectedTables = ['live-rings','live-groups','live-classes','live-entries','collections'];
  // Current and future observations remain protected on empty runs.
  for (const t of protectedTables) a.store.db[t].push({id:'old-'+t,fields:{date:'2026-10-24','show-id':75,entry:['prior-link'],'scope-status':'historical'}});
  const before = Object.fromEntries(protectedTables.map(t=>[t,structuredClone(a.store.db[t])]));
  f.groups[0].json_data = [];
  f.widget[0].live_data = [];
  for (let i=0;i<2;i++) {
    const r=await runLive(a);
    assert.equal(r.ok,true);assert.deepEqual(r.errors,[]);
    assert.deepEqual(r.counts,{rings:0,groups:0,classes:0,entries:0});
    assert.match(r.summary,/no live activity available/);
    assert.equal(a.store.db.heartbeat.at(-1).fields.result,'Success');
    assert.equal(a.store.db['run-logs'].at(-1).fields.result,'Success');
    for(const t of protectedTables) assert.deepEqual(a.store.db[t],before[t]);
  }
});
test('empty widget with expected rings and malformed empty widgets still fail', async () => {
  const f=fixture();f.widget[0].live_data=[];
  assert.equal((await runLive(args(f))).ok,false);
  f.groups[0].json_data=[];
  for(const bad of [null,'', [1]]) {
    f.widget[0].live_data=bad;
    assert.equal((await runLive(args(f))).ok,false);
  }
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
test('empty successful rollover archives and retires old show once, preserving current rows',async()=>{
 const f=fixture(),a=args(f);await runLive(a);
 const keys=require('../rollover').keys;
 for(const [t,key] of Object.entries(keys)) {
   const row=structuredClone(a.store.db[t][0]);row.id='old-'+t;
   row.fields[key]='old-'+row.fields[key];row.fields['show-id']=74;a.store.db[t].push(row);
 }
 f.groups[0].json_data=[];f.widget[0].live_data=[];
 const first=await runLive(a);assert.equal(first.ok,true);
 for(const t of Object.keys(keys)){
   assert(!a.store.db[t].some(r=>r.id==='old-'+t));assert.equal(a.store.db[t].length,1);
   assert(first.archives.some(x=>x.kind==='retired-'+t));
 }
 const catalogs=structuredClone(a.store.db.collections.filter(r=>r.fields['source-kind']==='archive-catalog'));
 assert.equal(catalogs.length,4);assert(catalogs.every(r=>r.fields.status==='Retired'));
 const second=await runLive(a);assert.equal(second.ok,true);assert.equal(second.retired.length,0);
 assert.deepEqual(a.store.db.collections.filter(r=>r.fields['source-kind']==='archive-catalog'),catalogs);
});

for(const reason of ['false','completed'])test('old rows retire on successful '+reason+' skip',async()=>{
 const f=fixture(),a=args(f);await runLive(a);
 a.store.db['live-entries'].push({id:'old',fields:{'live-entry-key':'old','show-id':75,date:'2026-10-22','scope-status':'historical'}});
 if(reason==='false')a.getStatus=async()=>false;else f.groups[0].json_data[0].status='Completed';
 const result=await runLive(a);assert.equal(result.ok,true);assert(result.skipped);
 assert(!a.store.db['live-entries'].some(r=>r.id==='old'));
});

test('rollover labeling requires valid authority and successful archive',async()=>{
  for(const failure of ['authority','archive']){
    const a=args();await runLive(a);
    a.store.db['live-entries'][0].fields['show-id']=74;
    const before=structuredClone(a.store.db['live-entries']);
    if(failure==='authority')a.store.db.shows[0].fields.last_checked='2026-10-20T00:00:00Z';
    else a.archive=async()=>{throw Error('Archive unavailable');};
    assert.equal((await runLive(a)).ok,false);
    assert.deepEqual(a.store.db['live-entries'],before);
  }
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
test("unresolved class identity leaves previous entries intact", async () => {
  const f = fixture(),
    a = args(f);
  await runLive(a);
  const before = structuredClone(a.store.db["live-entries"]);
  f.decoded.trips = [];
  const r = await runLive(a);
  assert.equal(r.ok, true);
  assert.equal(r.unresolved.length, 1);
  assert.deepEqual(observations(a.store.db["live-entries"]), observations(before));
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
    { id: "old", fields: { "show-id":75, date: "2026-10-22", "live-entry-key": "old" } },
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
        {show:75,date:"2026-10-23"},
        null,
      ),
    /archive/,
  );
  await assert.rejects(
    () =>
      api.retire(
        "live-entries",
        [{ id: "current", fields: { date: "2026-10-23" } }],
        {show:75,date:"2026-10-23"},
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
test('persistent missing identity is unresolved after one fresh pair, preserves links and resumes',async()=>{
 const a=args(),get=a.get;await runLive(a);
 const before=structuredClone(a.store.db['live-entries']),classes=structuredClone(a.store.db['live-classes']);
 let decoded=0,retire=0;a.api.retire=async()=>retire++;
 a.get=async(url,d)=>{const p=await get(url,d);if(d){decoded++;p.trips=[];}return p;};
 const r=await runLive(a);assert.equal(r.ok,true);assert.equal(decoded,2);
 assert.equal(r.unresolved[0].class_id,30);assert.deepEqual(r.errors,[]);
 assert.equal(r.unresolved[0].trip_id,400);assert.equal(r.unresolved[0].group_id,20);
 assert.deepEqual(observations(a.store.db['live-entries']),observations(before));assert.deepEqual(a.store.db['live-classes'],classes);
 assert.equal(a.store.db.heartbeat.at(-1).fields.result,'Partial');assert.equal(retire,0);
 const col=a.store.db.collections.find(r=>r.fields['source-id']==='30');assert.equal(col.fields.status,'unresolved');assert.equal(col.fields['row-count'],1);
 a.get=get;const next=await runLive(a);assert.deepEqual(next.unresolved,[]);assert.equal(a.store.db.collections.find(r=>r.fields['source-id']==='30').fields.status,'complete');
});

test('separate source failures retain separate errors and ambiguous group stays blank',async()=>{
 const f=fixture();f.groups[0].json_data.push({...f.groups[0].json_data[0],class_group_id:'21'});
 const a=args(f),get=a.get;
 a.get=async(url,d)=>{if(url.includes('getLiveClassData'))throw Error('detail HTTP 502');if(d)throw Error('decoded timeout');return get(url,d);};
 const r=await runLive(a);assert.equal(r.ok,true);assert.equal(r.unresolved.length,1);
 assert.deepEqual(r.unresolved[0].messages,['detail HTTP 502','decoded timeout']);
 assert.equal(r.unresolved[0].class_id,30);assert.equal(r.unresolved[0].group_id,undefined);assert.equal(r.unresolved[0].trip_id,undefined);
});

test('published Entries contract avoids upstream schedule reads and schema discovery',async()=>{
 const a=args(),list=a.api.list;
 a.api.list=async t=>{assert(!['schedules','show-collection'].includes(t),'Unexpected upstream replay');return list(t);};
 a.api.fieldNames=async()=>{throw Error('Unexpected schema discovery');};
 const r=await runLive(a);assert.equal(r.ok,true);assert.equal(r.link_issues.length,0);
 assert.equal(a.store.db['live-entries'][0].fields['participation-link-status'],'current');
});
test('stale Entries evidence preserves observations and links across repeat collections',async()=>{
 const a=args();await runLive(a);
 const first=observations(a.store.db['live-entries']);
 const f=a.store.db['wef-entries.entries'][0].fields,e=JSON.parse(f['source-json']);
 e.sourceAsOf='2026-10-23T00:00:00Z';f['source-json']=JSON.stringify(e);
 for(let i=0;i<2;i++){
  const r=await runLive(a);assert.equal(r.ok,true);assert.equal(r.link_issues.length,1);
  assert.deepEqual(observations(a.store.db['live-entries']),first);
  assert.equal(a.store.db['live-entries'][0].fields['participation-link-status'],'historical');
  assert.deepEqual(a.store.db['live-entries'][0].fields.entry,['entry-local']);
  assert.equal(a.store.db['run-logs'].at(-1).fields.result,'Partial');
 }
});
