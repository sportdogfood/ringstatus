import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {displayName,projectData,loadSchedule} from './data.mjs';
import {buildSnapshot} from './seed-projection.mjs';
const fixture=JSON.parse(fs.readFileSync(process.env.SCHEDULE_SEED_FIXTURE,'utf8'));
test('unchanged October 30 seed projection retains exact rows and details',()=>{
 const expected=buildSnapshot(fixture),actual=projectData(fixture);
 assert.deepEqual(actual.records,expected.records);assert.deepEqual(actual.days,expected.days);assert.deepEqual(actual.ringDetails,expected.ringDetails);assert.equal(actual.metadata.liveIntegration,'undecided');
});
test('nickname, fallback, name priority including lookup arrays and ambiguity',()=>{
 assert.equal(displayName(['Nick'],['Fallback'],'Name'),'Nick');assert.equal(displayName([' '],['Fallback'],'Name'),'Fallback');assert.equal(displayName(null,[],'Name'),'Name');assert.throws(()=>displayName(['A','B'],null,'Name'));
});
test('lookup nickname changes display while preserving direct trainer and trip identity',()=>{
 const input=structuredClone(fixture);const t=input.trainers[0];t['trainer-nickname (from w-trainers)']=['Nickname'];
 const result=projectData(input);for(const row of result.records)for(const e of row.entries)if(e.trainerId===t.id)assert.equal(e.trainer,'Nickname');
});
test('endpoint refuses missing credentials or invalid show without network',async()=>{
 await assert.rejects(loadSchedule({showId:'bad',token:'test'}),/invalid_show/);
 await assert.rejects(loadSchedule({showId:fixture.shows[0].id}),/missing_credentials/);
});

test('Airtable reader follows linked records, pagination and allowlisted fields',async()=>{
 const input=structuredClone(fixture);
 Object.assign(input.shows[0],{show_name:input.shows[0]['show-name'],customer_id:input.shows[0]['customer-id'],selected_date:input.shows[0]['selected-date']});
 for(const day of input.days){day.show=input.shows[0]['show-key'];day['day-priority']=day.date==='2026-10-30'?0:1;}
 for(const kind of ['rings','schedules','classes','entries'])for(const r of input[kind]){r['w-shows']=r.show;r['w-days']=r.day;}
 input.shows[0].rings=input.rings.map(r=>r.id);input.shows[0].schedules=input.schedules.map(r=>r.id);
 for(const day of input.days)day.schedules=input.schedules.filter(s=>s.day?.includes(day.id)).map(s=>s.id);
 for(const c of input.classes)c.trips=input.trips.filter(t=>t.class?.includes(c.id)).map(t=>t.id);
 for(const e of input.entries)e.___show=e.show;
 for(const r of input.rings)r['w-rings']=input.ringHelper.filter(h=>h.ringId===r['ring-id']).map(h=>h.id);
 input.ringHelpers=input.ringHelper.map(h=>({id:h.id,'ring-key':h.key,'ring-id':h.ringId,'ring-number':h.number,'ring-nickname':h.nickname,active:h.active}));
 const kinds=['shows','days','rings','schedules','groups','classes','trips','entries','horses','riders','trainers','ringHelpers'];
 const tables=['tblGc03icHzudV1v2','tblpRrsNXQdr3KrC9','tblhKkfYXppW3TbTq','tbl6KKlYuMIPHEJWe','tblQObOvktufkyq54','tbl0IFf067w4wUBj5','tblt0GvQ39bHIu930','tblcKPbT4DPbFPu5b','tblkGRRMJxnH55hi8','tblGEzjOJzpn3vxX5','tbl9ROAViHtbKqpRd','tblx3RAYeM52N3zaD'];
 let calls=0,pages=0;
 const fetchImpl=async (url,options)=>{
  assert.equal(options.redirect,'manual');

  calls++;const kind=kinds[tables.indexOf(url.pathname.split('/').at(-1))];assert.ok(kind);
  const ids=[...url.searchParams.get('filterByFormula').matchAll(/rec[A-Za-z0-9]{14}/g)].map(m=>m[0]);
  const fields=url.searchParams.getAll('fields[]');assert.ok(fields.length);
  const found=kind==='days'?input.days:input[kind].filter(r=>ids.includes(r.id));const start=Number(url.searchParams.get('offset')||0);if(start)pages++;
  const records=found.slice(start,start+17).map(r=>({id:r.id,fields:Object.fromEntries(fields.filter(f=>r[f]!==undefined).map(f=>[f,f==='source-json'?JSON.stringify(r[f]):r[f]]))}));
  return Response.json({records,...(start+17<found.length?{offset:String(start+17)}:{})});
 };
 const actual=await loadSchedule({showId:input.shows[0].id,token:'fixture-only',fetchImpl,pause:async()=>{},now:()=>fixture.capturedAt});
 assert.ok(calls>12&&pages>0);assert.deepEqual(actual.records,projectData(fixture).records);assert.deepEqual(actual.days,projectData(fixture).days);
 const fast=await loadSchedule({showId:input.shows[0].id,lane:'day0',token:'fixture-only',fetchImpl,pause:async()=>{},now:()=>fixture.capturedAt});
 assert.deepEqual(fast.records,actual.records.filter(r=>r.date==='2026-10-30'));assert.equal(fast.days.length,1);assert.equal(fast.metadata.package.complete,true);
});

test('source ring numbering survives differing, missing or ambiguous optional master',()=>{
 const input=structuredClone(fixture),ring=input.rings[0],helper=input.ringHelper.find(h=>h.ringId===ring['ring-id']);
 ring['ring-name']='Source ring';ring['ring-number']=41;ring['w-rings']=[helper.id];
 let result=projectData(input);assert.ok(result.records.some(r=>r.ringRecordId===ring.id));
 ring['w-rings']=[];result=projectData(input);assert.ok(result.records.some(r=>r.ringRecordId===ring.id));
 // Last day owns the shared display metadata, while every ring remains in its day.
 assert.ok(result.days.find(d=>ring.day.includes(d.id)).rings.includes(ring['ring-id']));
});

test('saved trip rider identity selects its matching native link, never the first historical link',()=>{
 const input=structuredClone(fixture),visible=projectData(input).records.flatMap(r=>r.entries).find(e=>e.riderId),trip=input.trips.find(t=>t.id===visible.tripId);
 const correct=input.riders.find(r=>r.id===trip.rider[0]),other=input.riders.find(r=>r.id!==correct.id);
 correct['rider-id']=11720;other['rider-id']=1000003220;
 trip['source-json']={...(trip['source-json']||{}),rider_id:correct['rider-id']};trip.rider=[other.id,correct.id];
 const result=projectData(input);const shown=result.records.flatMap(r=>r.entries).filter(e=>e.tripId===trip.id);
 assert.ok(shown.length);assert.ok(shown.every(e=>e.riderId===correct.id));
});
