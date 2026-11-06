'use strict';
globalThis.fetch=async()=>{throw Error('Offline tests: network forbidden');};
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {lane,scheduled,inWindow}=require('./candidate/wef-live/lanes'),{diffs}=require('./candidate/wef-live/lane-store');
function setup(t){
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-lanes-test-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 let now='2026-11-01T14:00:00Z',seq=0;const db={},writes=[],alerts=[],requests=[];
 const row=(table,key,fields)=>{(db[table]||=[]).push({id:'rec'+(++seq),fields:{[key]:fields[key],...fields}});};
 row('shows','show-key',{'show-key':'15|75',customer_id:15,show_id:75,is_default:true,start_date:'2026-11-01',end_date:'2026-11-02',available_dates:'2026-11-01,2026-11-02',selected_date:'2026-11-01',last_checked:now,authority_checked_at:now});
 row('show-days','show-day-key',{'show-day-key':'15|75|2026-11-01','day-priority':0,date:'2026-11-01'});
 row('live-rings','ring-key',{'ring-key':'75|2026-11-01|3','show-id':75,date:'2026-11-01','ring-id':3,'ring-number':1,'ring-name':'Ring 1'});
 const g={day:'2026-11-01',ring_id:3,ring_number:1,ring:'Ring 1',class_group_id:20,classes:[30],classNumbers:[1],classNames:['Class'],has_JSON:true,status:'Underway',is_live:true,total:2,gone:0};
 const fixture={groups:[{show_id:75,json_data:[g]}],widget:[{show_id:75,live_data:{1:{name:'Ring 1',livenow:[g],upcoming:[],completed:[]}}}],detail:{ID:30,recs:2,rows:[{id:400,class_id:30,Hor:'Horse',Rid:'Rider',OOG:1,Actual_OOG:1,Scr:false,Gone:0,Pos:0},{id:401,class_id:30,Hor:'Horse2',Rid:'Rider2',OOG:2,Actual_OOG:2,Scr:false,Gone:0,Pos:0}],gone_trips:'',jo_gone_trips:'',r2_gone_trips:'',curr:400},decoded:{class:{class_id:30,show_id:75},class_related_data:{date:'2026-11-01'},trips:[{trip_id:400,entry_id:100,horse_id:10,rider_id:11,trainer_id:12},{trip_id:401,entry_id:101,horse_id:20,rider_id:21,trainer_id:12}]}};
 const api={list:async table=>structuredClone(db[table]||[]),upsert:async(table,key,rows)=>{
  for(const fields of rows){let r=(db[table]||=[]).find(r=>r.fields[key]===fields[key]);if(!r){row(table,key,fields);r=db[table].at(-1);}else Object.assign(r.fields,fields);}return structuredClone(db[table]||[]);
 },call:async(base,table,method,body)=>{
  if(method==='GET')return{records:structuredClone(db[table]||[])};
  writes.push({table,method,body:structuredClone(body)});
  const records=body.records.map(r=>{if(r.id){const old=db[table].find(x=>x.id===r.id);Object.assign(old.fields,r.fields);return old;}
   const result={id:'rec'+(++seq),fields:structuredClone(r.fields)};(db[table]||=[]).push(result);return result;});return{records:structuredClone(records)};
 }};
 const options={runtime,api,clock:()=>now,getStatus:async()=>true,alert:async e=>alerts.push(e),archive:async(kind,data)=>({kind,file:'offline-evidence',sha256:'offline'}),get:async(url,decoded)=>{
  requests.push(url);if(url.includes('ListAjax'))return structuredClone(fixture.groups);if(url.includes('LiveScoreWidget'))return structuredClone(fixture.widget);return structuredClone(decoded?fixture.decoded:fixture.detail);
 }};
 return{runtime,db,writes,alerts,requests,fixture,options,advance:()=>{now='2026-11-01T14:03:00Z';db.shows[0].fields.authority_checked_at=now;}};
}

test('successful misses cycle Active/Absent/Dropped and reappearance restores Active without changing IDs or links',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('classes',a.options);
 const trip=a.db['live-trips'].find(x=>x.fields['trip-id']===400),before=structuredClone(trip),raw=a.fixture.decoded.trips.shift();
 const legacy=structuredClone(a.db['live-entries'].find(x=>x.fields['trip-id']===400));
 for(const status of ['Absent','Dropped','Dropped']){
  a.advance();const r=await lane('classes',a.options);assert.equal(r.ok,true);assert(!r.issues.some(x=>x.trip_id===400));assert.equal(trip.fields['record-status'],status);
  assert.deepEqual({...trip.fields,'record-status':before.fields['record-status']},before.fields);assert.equal(trip.id,before.id);
 }
 assert.deepEqual(a.db['live-entries'].find(x=>x.id===legacy.id),legacy);
 a.fixture.decoded.trips.unshift(raw);a.advance();await lane('classes',a.options);assert.equal(trip.fields['record-status'],'Active');assert.equal(trip.id,before.id);
 for(const k of ['class','ring','groups','legacy-live-entry'])assert.deepEqual(trip.fields[k],before.fields[k]);assert.equal(a.db['live-trips'].length,2);
 a.writes.length=0;a.advance();await lane('classes',a.options);assert(!a.writes.some(x=>x.table==='live-trips'));
});

test('confirmed empty decoded list ages retained trips without creating guessed UI identities',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('classes',a.options);a.fixture.decoded.trips=[];
 const r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(r.issues.length,0);assert(a.db['live-trips'].every(x=>x.fields['record-status']==='Absent'));assert.equal(a.db['live-trips'].length,2);
});

test('failed or malformed decoded inventory does not advance missing trips; valid trips still save',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('classes',a.options);const trip=a.db['live-trips'][0];
 a.fixture.decoded.trips=[{trip_id:999,entry_id:null},a.fixture.decoded.trips[1]];
 let r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(trip.fields['record-status'],'Active');assert(r.issues.length);
 const get=a.options.get;a.options.get=async(url,d)=>{if(d)throw Error('Decoded unavailable');return get(url,d);};
 r=await lane('classes',a.options);assert.equal(r.ok,false);assert.equal(trip.fields['record-status'],'Active');assert(r.issues.some(x=>x.message==='Decoded unavailable')); // Existing all-class-source failure behavior is preserved.
});

test('other show/date/class trips remain untouched',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('classes',a.options);
 const original=a.db['live-trips'][0];for(const fields of [{'show-id':76},{date:'2026-11-02'},{'class-id':31}])a.db['live-trips'].push({id:'old'+a.db['live-trips'].length,fields:{...structuredClone(original.fields),...fields,'trip-key':'other'+a.db['live-trips'].length}});
 const others=structuredClone(a.db['live-trips'].slice(2));a.fixture.decoded.trips=[];await lane('classes',a.options);assert.deepEqual(a.db['live-trips'].slice(2),others);
});

test('completed:null clears only completed groups, preserves raw null evidence, IDs and other ring links',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('rings',a.options);const ring=a.db['live-rings'][0];ring.fields['completed-groups']=['oldCompleted'];const id=ring.id,current=structuredClone(ring.fields['current-group']);
 a.fixture.widget[0].live_data[1].completed=null;const r=await lane('rings',a.options);assert.equal(r.ok,true);assert.equal(r.issues.length,0);assert.deepEqual(ring.fields['completed-groups'],[]);assert.deepEqual(ring.fields['current-group'],current);assert.equal(ring.id,id);assert.equal(JSON.parse(ring.fields['source-json']).completed,null);
 a.writes.length=0;await lane('rings',a.options);assert(!a.writes.some(x=>x.table==='live-rings'));
});

test('missing or malformed non-null widget list still preserves completed links and logs invalid types',async t=>{
 const a=setup(t);await lane('groups',a.options);a.db['live-rings'][0].fields['completed-groups']=['kept'];
 delete a.fixture.widget[0].live_data[1].completed;let r=await lane('rings',a.options);assert.deepEqual(a.db['live-rings'][0].fields['completed-groups'],['kept']);
 a.fixture.widget[0].live_data[1].completed='broken';r=await lane('rings',a.options);assert(r.issues.some(x=>x.message==='Invalid widget completed for ring 1'));assert.deepEqual(a.db['live-rings'][0].fields['completed-groups'],['kept']);
});



