"use strict";
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path');
const {linkClass,classSchedule} = require('../staged/participation-linker');
function fixture(name = 'positive') {
  const f = JSON.parse(fs.readFileSync(path.join(__dirname, '../../../tests/fixtures/narrow-matcher', name + '.json')));
  const date = f.payload.class_related_data.date.slice(0,10);
  f.show = {show_id:f.payload.show_id, customer_id:15, start_date:date, end_date:date};
  f.observedAt = date + 'T19:52:13.082Z';
  f.options = {sourceEvidence:{revision:'offline-fixture-only'},inventory:{revision:'offline-fixture-only',complete:true,showId:f.show.show_id,customerId:15,groups:f.groups}};
  f.schedules ||= [];
  return f;
}
const run = f => linkClass(f.payload,f.show,f.schedules,f.observedAt,f.options);
const target = rows => rows.find(r => r['entry-id'] === 200245642);

test('shared schedule without group fields keeps unique class link and independently proven group',()=>{
 const f=fixture();for(const row of f.schedules){delete row.fields['group-id'];delete row.fields['row-json'];}
 const row=target(run(f));assert.equal(row['schedule-link-status'],'linked');assert.equal(row['schedule-link-basis'],'class-date');assert.equal(row['group-link-status'],'linked');assert.equal(row['source-group-id'],200029518);
 const active=f.schedules.find(r=>r.fields.status==='active');f.schedules.push({...structuredClone(active),id:'another',fields:{...active.fields,'stage-key':'other-key'}});
 const ambiguous=target(run(f));assert.equal(ambiguous['schedule-link-status'],'Conflict');assert.equal(ambiguous.schedules,undefined);
});
test('saved positive copies actual active schedule and supports observed group association', () => {
  const f = fixture(), row = target(run(f));
  assert.equal(row['entry-key'],'200000075|2026-10-23|200245642|200030347');
  assert.equal(row['schedule-key'],'200000075|2026-10-23|200029518_770');
  assert.deepEqual(row.schedules,['recZE0IZAkr7bG5pQ']);
  assert.equal(row['source-group-id'],200029518);
  assert.equal(row['schedule-link-basis'],'observed-group');
  assert.equal(row['rider-id'],40889); assert.deepEqual(row['source-trainer-ids'],[80970]);
  assert.equal(row['trainer-ids'],undefined); assert.equal(row['owner-key'],undefined);
});
test('saved same-run contradictory group retains supported entry and entity facts', () => {
  const rows = run(fixture('contradiction'));
  const row = rows.find(r => r['source-trips'].some(t => t.trip_id === 200398234));
  assert(row); assert.equal(row['horse-id'],200005304); assert.equal(row['rider-id'],70539);
  assert.equal(row['group-link-status'],'unresolved'); assert.match(row['group-link-reason'],/roster/);
  assert.equal(row['source-group-id'],undefined); assert.equal(row.schedules,undefined);
  assert.equal(row['observation-status'],'successful');
});
test('saved partial roster671 keeps class/date association and flags only group uncertainty', () => {
  const row = run(fixture('partial'))[0];
  assert.equal(row['entry-id'],200246107); assert.equal(row['group-link-status'],'unresolved');
  assert.match(row['group-link-reason'],/incomplete/);
  assert.equal(row['schedule-link-basis'],'class-date');
  assert.equal(row['schedule-key'],'200000076|2026-10-30|200029536_207');
});
test('missing inventory does not gate ordinary class/date linking or entity normalization', () => {
  const f=fixture();delete f.options.inventory;
  const row=target(run(f)); assert.equal(row['schedule-link-status'],'linked');
  assert.equal(row['schedule-link-basis'],'class-date');assert.equal(row['group-link-status'],'unresolved');
});
test('same-revision completeness is required only for group assertion', () => {
  for (const mutate of [f=>f.options.inventory.revision='other',f=>f.options.inventory.complete=false]) {
    const f=fixture();mutate(f);const row=target(run(f));
    assert.equal(row['group-link-status'],'unresolved');assert.equal(row['schedule-link-basis'],'class-date');
  }
});
test('ring ID is not ring number; contradictory ring identity does not become group proof', () => {
  const f=fixture();f.payload.class.schedule_ring_id=1;
  assert.match(target(run(f))['group-link-reason'],/ring-ID/);
  f.payload.class_related_data.ring=10;
  const row=target(run(f));assert.equal(row.schedules,undefined);assert.match(row['schedule-link-reason'],/ring-number/);
});
test('duplicate group rosters are unresolved instead of selecting an arbitrary group', () => {
  const f=fixture();const g=f.groups.find(g=>Number(g.groupId)===200029518);
  f.groups.push({...g,groupId:999});assert.match(target(run(f))['group-link-reason'],/Indistinguishable/);
});
test('duplicate operational occurrences never fan out or erase entity facts', () => {
  const f=fixture(),s=f.schedules.find(s=>s.fields.status==='active');
  f.schedules.push({...structuredClone(s),id:'secondOccurrence'});
  const row=target(run(f));assert.equal(row['group-link-status'],'unresolved');
  assert.equal(row['schedule-link-status'],'Conflict');assert.equal(row.schedules,undefined);assert(row['horse-id']);
});
test('source occurrence conflict and repeated source occurrence are rejected', () => {
  const f=fixture(),g=f.groups.find(g=>Number(g.groupId)===200029518);
  g.occurrences=[{classId:200030347,occurrenceId:1}];
  assert.match(target(run(f))['group-link-reason'],/occurrence conflict/);
  g.occurrences.push({classId:200030347,occurrenceId:200044738});
  assert.match(target(run(f))['group-link-reason'],/Repeated source/);
});
test('exact own-class trip identity matters; another trip or class cannot lend eligibility', () => {
  for (const field of ['class_id','rider_id','horse_id','entry_id','trainer_id','trip_id','entryxclasses_uuid']) {
    const f=fixture();f.payload.class_group_order_of_go.entries[9][field]=field==='entryxclasses_uuid'?'wrong':999;
    assert.equal(target(run(f))['group-link-status'],'unresolved',field);
  }
});
test('one bad trip group match does not stop other supported group links', () => {
  const f=fixture();f.payload.class_group_order_of_go.entries[9].rider_id=999;
  const rows=run(f);assert.equal(target(rows)['group-link-status'],'unresolved');
  assert(rows.some(r=>r['group-link-status']==='linked'));
});
test('empty class succeeds without needing any group or schedule data', () => {
  const f=fixture();f.payload.trips=[];f.payload.total_entry_trips=0;f.schedules=[];f.options={};
  assert.deepEqual(run(f),[]);delete f.payload.trips;assert.throws(()=>run(f),/Incomplete/);
});
test('replay is stable, keeps observation time and does not mutate source or old links', () => {
  const f=fixture(),before=structuredClone(f),a=run(f),b=run(f);
  assert.deepEqual(a,b);assert.deepEqual(f,before);assert(a.every(r=>r['last-seen']===f.observedAt));
});
test('conflicting required identity rejects source facts rather than guessing', () => {
  const f=fixture();f.payload.class.customer_id=99;assert.throws(()=>run(f),/customer/);
  f.payload.class.customer_id=15;f.payload.trips[0].horse_id=null;assert.throws(()=>run(f),/horse ID/);
});
test('unknown trainer and missing trip UUID retain facts but cannot claim group eligibility', () => {
  const f=fixture();delete f.payload.trips[0].trainer_id;delete f.payload.trips[0].entryxclasses_uuid;
  const row=target(run(f));assert.deepEqual(row['source-trainer-ids'],[]);assert.equal(row['group-link-status'],'unresolved');
});

 test('duplicate shared key conflicts only affected classes; healthy key still links',()=>{
 const row=(id,cid,key)=>({id,fields:{'show-id':1,'class-id':cid,date:'2026-10-30',ring:1,status:'active','stage-key':key}});
 const rows=[row('one',100,'duplicate'),row('two',101,'duplicate'),row('healthy',102,'healthy')];
 for(const cid of [100,101]){const result=classSchedule(rows,1,cid,'2026-10-30',1);assert.equal(result.status,'Conflict');assert.match(result.reason,/Duplicate shared schedule key/);}
 assert.equal(classSchedule(rows,1,102,'2026-10-30',1).status,'linked');
 });
