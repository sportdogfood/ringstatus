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

test('live details collect/save without any decoded request,trip read/write or identity gate',async t=>{
 const a=setup(t);a.db['live-trips']=[{id:'retainedTrip',fields:{'trip-key':'keep','record-status':'Active',class:['existingClass'],groups:['existingGroup']}}];const trips=structuredClone(a.db['live-trips']);
 await lane('groups',a.options);a.requests.length=0;const get=a.options.get;a.options.get=async(url,decoded)=>{assert(!decoded);assert(!new URL(url).pathname.startsWith('/classes/'));return get(url,decoded);};
 const apiList=a.options.api.list;a.options.api.list=async table=>{assert.notEqual(table,'live-trips');return apiList(table);};
 const r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(r.issues.length,0);assert.equal(a.requests.length,1);assert.equal(a.db['live-classes'].length,1);assert.equal(a.db['live-entries'].length,2);assert.deepEqual(a.db['live-trips'],trips);assert(!a.writes.some(x=>x.table==='live-trips'));
 for(const e of a.db['live-entries']){assert(!('entry-id' in e.fields));assert(!('horse-id' in e.fields));assert(e.fields.class.length===1);assert(e.fields['trip-id']);}
 const ids=a.db['live-entries'].map(x=>x.id);a.writes.length=0;a.advance();await lane('classes',a.options);assert.deepEqual(a.db['live-entries'].map(x=>x.id),ids);assert(!a.writes.some(x=>x.table.startsWith('live-')));
});

test('existing entry identity fields and native links survive live-only updates',async t=>{
 const a=setup(t);await lane('groups',a.options);a.db['live-classes']=[{id:'existingClass',fields:{'class-key':'75|2026-11-01|30','show-id':75,date:'2026-11-01','class-id':30,groups:['oldGroup'],schedules:['oldSchedule']}}];
 const baseline=require('./baseline/wef-live/model');const old=baseline.classDetail(a.fixture.detail,a.fixture.decoded,{show:75,customer:15,date:'2026-11-01'},{'class-id':30,'class-key':'75|2026-11-01|30'},'old').entries[0];
 a.db['live-entries']=[{id:'existingEntry',fields:{...old,class:['existingClass'],entry:['oldEntryLink'],'live-trips':['retainedTrip']}}];const fields=structuredClone(a.db['live-entries'][0].fields);
 a.fixture.detail.rows[0].Gone=1;const r=await lane('classes',a.options);assert.equal(r.ok,true);const saved=a.db['live-entries'].find(x=>x.id==='existingEntry');
 for(const key of ['entry-id','entry-key','horse-id','rider-id','trainer-id','class','entry','live-trips'])assert.deepEqual(saved.fields[key],fields[key]);assert.equal(saved.fields.gone,1);assert(a.db['live-classes'][0].fields.groups.includes('oldGroup'));assert.deepEqual(a.db['live-classes'][0].fields.schedules,['oldSchedule']);
});

test('live-only model preserves explicit zeros/false,validates source class and never fabricates identities',()=>{
 const model=require('./candidate/wef-live/model'),s={show:75,customer:15,date:'2026-11-01'},c={'class-id':30,'class-key':'75|2026-11-01|30'};
 const payload={ID:30,recs:1,rows:[{id:400,class_id:30,OOG:0,Actual_OOG:0,Gone:0,Scr:false,Pos:0}]};const result=model.classDetail(payload,null,s,c,'now');const row=result.entries[0];assert.equal(row['order-of-go'],0);assert.equal(row.scratch,false);assert.equal(row.gone,0);assert.equal(row.position,0);assert(!('entry-id' in row));assert(!('entry-key' in row));assert.equal(row['live-entry-key'],'75|2026-11-01|30|400');
 assert.throws(()=>model.classDetail({...payload,ID:31},null,s,c,'now'),/Incomplete/);assert.throws(()=>model.classDetail({...payload,rows:[{id:400,class_id:31}]},null,s,c,'now'),/class mismatch/);assert.throws(()=>model.classDetail(payload,undefined,s,c,'now'),/Decoded class scope mismatch/);
});

test('existing decoded model callers preserve their exact previous fields and validation',async t=>{
 const a=setup(t),before=require('./baseline/wef-live/model'),after=require('./candidate/wef-live/model'),s={show:75,customer:15,date:'2026-11-01'},c={'class-id':30,'class-key':'75|2026-11-01|30'};
 assert.deepEqual(after.classDetail(a.fixture.detail,a.fixture.decoded,s,c,'now'),before.classDetail(a.fixture.detail,a.fixture.decoded,s,c,'now'));
 assert.throws(()=>after.classDetail(a.fixture.detail,{...a.fixture.decoded,trips:[]},s,c,'now'),/No decoded identity/);
});

test('empty live detail succeeds; malformed row logs and other rows/class keep saving',async t=>{
 const a=setup(t);await lane('groups',a.options);a.fixture.detail.rows[0].class_id=31;let r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(a.db['live-entries'].length,1);assert(r.issues.some(x=>x.message==='Live row class mismatch'));
 const before=structuredClone(a.db['live-entries']);a.fixture.detail.rows=[];a.fixture.detail.recs=0;r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(a.db['live-classes'][0].fields.total,0);assert.deepEqual(a.db['live-entries'],before);
});

test('completed:null is an empty list without warnings; record and other ring links remain',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('rings',a.options);const ring=a.db['live-rings'][0],id=ring.id,current=structuredClone(ring.fields['current-group']);ring.fields['completed-groups']=['previousCompleted'];a.fixture.widget[0].live_data[1].completed=null;
 const r=await lane('rings',a.options);assert.equal(r.ok,true);assert.equal(r.issues.length,0);assert.equal(ring.id,id);assert.deepEqual(ring.fields['completed-groups'],[]);assert.deepEqual(ring.fields['current-group'],current);assert.equal(JSON.parse(ring.fields['source-json']).completed,null);
});


