"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {showRings,resolveHelper}=require('../show-rings');
global.fetch=async()=>{throw Error('Network forbidden');};
const NOW='2026-10-20T16:00:00.000Z';
const ring=(n=1,id=10,name='Derby Field')=>({ring_id:id,ring_number:n,ring_name:name,classes:[]});
function input(){return {show:{fields:{show_id:75,customer_id:15,show_name:'Fall III',start_date:'2026-10-22',end_date:'2026-10-23',sgl_default_day:'2026-10-22',focus_day:'2026-10-20'}},payload:{show:{show_id:75,facility_id:516},show_date:'2026-10-22'},days:[{date:'2026-10-22',ok:true},{date:'2026-10-23',ok:true}],payloads:new Map(['2026-10-22','2026-10-23'].map(date=>[date,{show:{show_id:75,facility_id:516},show_date:date,rings:[ring(),ring(2,3,'Annex')]}])),now:NOW};}
function fixture(){let next=0;const db={'show-details':[],rings:[],'rings-helper':[],schedules:[{id:'sch1',fields:{'show-id':75,date:'2026-10-23',ring:1,'class-name':'preserve','row-json':'original'}},{id:'other',fields:{'show-id':74,date:'2026-10-23',ring:1}}]},calls=[];
 const api={async airtableList(table){return structuredClone(db[table]);},async airtablePatchRecords(table,rows){calls.push(['patch',table]);for(const r of rows)Object.assign(db[table].find(x=>x.id===r.id).fields,structuredClone(r.fields));return {failedRows:[]};}};
 const request=async(url,opts)=>{const table=decodeURIComponent(new URL(url).pathname.split('/').at(-1));assert.equal(opts.method,'POST');calls.push(['post',table]);for(const r of JSON.parse(opts.body).records)db[table].push({id:'new'+(++next),fields:structuredClone(r.fields)});return {ok:true,status:200};};
 const make=(opts={})=>showRings({api,showsTable:'show-details',request,pause:async()=>{},...opts});return {db,calls,api,request,make};}
test('source date wins, daily rings link every schedule, repeated runs preserve IDs and static details',async()=>{
 const x=fixture(),before=structuredClone(x.db.schedules);let result=await x.make().sync(input());
 assert.equal(result.selected_date,'2026-10-22');assert.equal(x.db['show-details'][0].fields.selected_date,'2026-10-22');assert.equal(x.db.rings.length,4);assert.equal(x.db['rings-helper'].length,2);
 assert.equal(x.db.rings.filter(r=>r.fields.is_selected_date).length,2);assert.equal(x.db.rings[0].fields.ring_id,10);assert.equal(x.db.rings[0].fields.ring_number,1);
 const schedule=x.db.schedules[0];assert.equal(schedule.fields['class-name'],'preserve');assert.equal(schedule.fields['row-json'],'original');assert.deepEqual(schedule.fields['show-details'],[x.db['show-details'][0].id]);assert.equal(x.db.rings.find(r=>r.id===schedule.fields.rings[0]).fields.ring_date,'2026-10-23');assert.deepEqual(x.db.schedules[1],before[1]);
 x.db['rings-helper'][0].fields.notes='Owner static data';const ids=Object.fromEntries(Object.entries(x.db).map(([t,rs])=>[t,rs.map(r=>r.id)]));const posts=x.calls.filter(c=>c[0]==='post').length;
 await x.make().sync({...input(),now:'2026-10-20T16:05:00Z'});assert.equal(x.calls.filter(c=>c[0]==='post').length,posts);assert.equal(x.db['rings-helper'][0].fields.notes,'Owner static data');assert.deepEqual(Object.fromEntries(Object.entries(x.db).map(([t,rs])=>[t,rs.map(r=>r.id)])),ids);
});
test('helper lookup accepts each identifier within facility, rejects conflicts and ambiguity',()=>{
 const f={customer_id:15,facility_id:516},records=[{id:'a',fields:{...f,ring_id:10,ring_number:1,ring_name:' Derby Field '}}];
 for(const r of [{ring_id:10},{ring_number:1},{ring_name:'derby field'}])assert.equal(resolveHelper(records,f,r).id,'a');
 assert.equal(resolveHelper(records,{...f,facility_id:999},{ring_number:1}),null);
 assert.throws(()=>resolveHelper([...records,{id:'b',fields:{...f,ring_number:2,ring_name:'Annex'}}],f,{ring_id:10,ring_number:2}),/ambiguous|conflict/i);
 assert.throws(()=>resolveHelper(records,f,{ring_id:10,ring_number:9}),/conflict/i);
});
test('blank helper identity fills from source without overwriting owner fields',async()=>{
 const x=fixture();x.db['rings-helper'].push({id:'static',fields:{'ring-key':'owner-key',customer_id:15,facility_id:516,ring_name:'Derby Field',notes:'keep'}});await x.make().sync(input());assert.equal(x.db['rings-helper'].length,2);assert.equal(x.db['rings-helper'][0].fields.ring_id,10);assert.equal(x.db['rings-helper'][0].fields.ring_number,1);assert.equal(x.db['rings-helper'][0].fields.notes,'keep');
});
test('missing source date never falls back to computer date and invalid sources write nothing',async()=>{
 for(const mutate of [i=>{i.show.fields.sgl_default_day=null;},i=>{i.payloads.get('2026-10-23').show.show_id=999;},i=>{i.payloads.get('2026-10-22').rings.push(ring());},i=>{i.payloads.get('2026-10-22').show_date='2026-10-20';},i=>{i.days[0].ok=false;}]){
  const x=fixture(),i=input();mutate(i);await assert.rejects(x.make().sync(i));assert.equal(x.calls.length,0);
 }
});
test('a committed creation timeout is reconciled once without a second POST',async()=>{
 const x=fixture();let lost=true;await x.make({request:async(...args)=>{const r=await x.request(...args);if(lost){lost=false;throw Error('lost response');}return r;}}).sync(input());assert.equal(x.db['show-details'].length,1);assert.equal(x.calls.filter(c=>c[0]==='post'&&c[1]==='show-details').length,1);
});
test('uncertain uncommitted creation stops and never repeats POST',async()=>{
 const x=fixture();let calls=0;await assert.rejects(x.make({request:async()=>{calls++;throw Error('not committed');}}).sync(input()),/missing|unresolved/i);assert.equal(calls,1);assert.equal(x.db.rings.length,0);
});
test('default show/date rollover clears old selection without deleting old records',async()=>{
 const x=fixture();await x.make().sync(input());const oldIds=x.db.rings.map(r=>r.id),i=input();i.show.fields.show_id=76;i.show.fields.sgl_default_day='2026-10-23';i.payload.show.show_id=76;i.payload.show_date='2026-10-23';for(const p of i.payloads.values())p.show.show_id=76;await x.make().sync(i);
 assert.equal(x.db['show-details'].filter(r=>r.fields.is_default).length,1);assert.equal(x.db['show-details'].find(r=>r.fields.is_default).fields.show_id,76);assert.ok(x.db.rings.filter(r=>oldIds.includes(r.id)).every(r=>r.fields.is_selected_date===false));assert.equal(x.db.rings.filter(r=>r.fields.is_selected_date).length,2);
});
test('missing or ambiguous schedule ring mapping stops without modifying schedule data',async()=>{
 const x=fixture();x.db.schedules[0].fields.ring=99;await assert.rejects(x.make().sync(input()),/schedule.*ring/i);assert.equal(x.db.schedules[0].fields.rings,undefined);
});
test('patch readback mismatch fails the new stage',async()=>{
 const x=fixture();x.api.airtablePatchRecords=async()=>({failedRows:[]});await assert.rejects(x.make().sync(input()),/readback/i);
});
test('conflicting source identities across dates fail before writes',async()=>{
 const x=fixture(),i=input();i.payloads.get('2026-10-23').rings[0].ring_id=99;await assert.rejects(x.make().sync(i),/conflict/i);assert.equal(x.calls.length,0);
});
test('Airtable omitted unchecked boxes are accepted on readback',async()=>{
 const x=fixture(),list=x.api.airtableList;x.api.airtableList=async table=>(await list(table)).map(r=>{for(const k of ['is_default','is_selected_date'])if(r.fields[k]===false)delete r.fields[k];return r;});await x.make().sync(input());await x.make().sync(input());assert.equal(x.db.rings.length,4);
});
test('retained dropped schedules keep historical rings when source removes that ring',async()=>{
 const x=fixture();await x.make().sync(input());const previous=x.db.schedules[0].fields.rings[0];x.db.schedules[0].fields.status='dropped';const i=input();i.payloads.get('2026-10-23').rings.shift();await x.make().sync(i);assert.deepEqual(x.db.schedules[0].fields.rings,[previous]);assert.ok(x.db.rings.some(r=>r.id===previous));
});
test('first adoption does not invent a ring for a dropped row absent from source',async()=>{
 const x=fixture();x.db.schedules[0].fields.status='dropped';const i=input();i.payloads.get('2026-10-23').rings.shift();const result=await x.make().sync(i);assert.deepEqual(x.db.schedules[0].fields.rings,[]);assert.equal(result.dropped_without_ring,1);assert.equal(x.db.schedules[0].fields['row-json'],'original');
});
test('new record batches are paced without repeating POSTs',async()=>{
 const x=fixture(),pauses=[];await x.make({pause:async ms=>pauses.push(ms)}).sync(input());assert.equal(pauses.length,x.calls.filter(c=>c[0]==='post').length);assert.ok(pauses.every(ms=>ms===500));
});
