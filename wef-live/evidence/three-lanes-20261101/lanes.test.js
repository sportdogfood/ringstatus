'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {lane,scheduled,inWindow}=require('./candidate/lanes'),{diffs}=require('./candidate/lane-store');
function setup(t){
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-lanes-test-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 let now='2026-11-01T14:00:00Z',seq=0;const db={},writes=[],alerts=[],requests=[];
 const row=(table,key,fields)=>{(db[table]||=[]).push({id:'rec'+(++seq),fields:{[key]:fields[key],...fields}});};
 row('shows','show-key',{'show-key':'15|75',customer_id:15,show_id:75,is_default:true,start_date:'2026-11-01',end_date:'2026-11-02',available_dates:'2026-11-01,2026-11-02',selected_date:'2026-11-01',last_checked:now,authority_checked_at:now});
 row('show-days','show-day-key',{'show-day-key':'15|75|2026-11-01','day-priority':0});
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
test('window follows Eastern including DST and excludes overnight',()=>{
 assert.equal(inWindow('2026-11-01T09:59:00Z'),false);assert.equal(inWindow('2026-11-01T10:00:00Z'),true);assert.equal(inWindow('2026-11-02T00:00:00Z'),true);assert.equal(inWindow('2026-11-02T00:01:00Z'),false);
});
test('diffs include zero, false and activity times; exclude collection clocks and tokens',()=>{
 const d=diffs({gone:2,scratch:true,'estimated-start-time':'10:00','last-seen':'a','source-json':'{"curr_updated_at":1,"total":2}'},{gone:0,scratch:false,'estimated-start-time':'10:30','last-seen':'b','source-json':'{"curr_updated_at":2,"total":2}'});
 assert.deepEqual(d.map(x=>x.field),['gone','scratch','estimated-start-time']);
 assert.deepEqual(diffs({}, {gone:null,scratch:false,groups:[]}),[]);
});
test('unchanged repeat updates coverage evidence without business writes or last-changed',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('classes',a.options);await lane('groups',a.options);
 a.writes.length=0;const before=JSON.parse(fs.readFileSync(path.join(a.runtime,'lane-groups-state.json'))).records;
 a.advance();a.fixture.groups[0].json_data[0].curr_updated_at=99;
 const r=await lane('groups',a.options);assert.equal(r.ok,true);assert.equal(a.writes.filter(x=>x.table.startsWith('live-')).length,0);assert.equal(r.changes.length,0);
 const k='live-groups|75|2026-11-01|20';const after=JSON.parse(fs.readFileSync(path.join(a.runtime,'lane-groups-state.json'))).records[k];assert.equal(after.last_changed,before[k].last_changed);assert.equal(after.last_success,'2026-11-01T14:03:00Z');
});
test('Groups saves final Completed status; class lane then skips details independently',async t=>{
 const a=setup(t);await lane('groups',a.options);a.fixture.groups[0].json_data[0].status='Completed';await lane('groups',a.options);
 assert.equal(a.db['live-groups'][0].fields.status,'Completed');a.requests.length=0;const r=await lane('classes',a.options);assert.equal(r.skipped,'all-groups-completed');assert.equal(a.requests.length,0);
});
test('one malformed group does not block valid group',async t=>{
 const a=setup(t);a.fixture.groups[0].json_data.unshift({...a.fixture.groups[0].json_data[0],ring_id:null,class_group_id:21});
 const r=await lane('groups',a.options);assert.equal(r.ok,true);assert.equal(a.db['live-groups'].length,1);assert(r.issues.length);assert.equal(a.alerts.length,0);
});
test('unresolved trip does not block other entries or class counts; old links stay intact',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('classes',a.options);
 const old=a.db['live-entries'].find(r=>r.fields['trip-id']===400);old.fields.entry=['recExisting'];const oldFields=structuredClone(old.fields);
 a.advance();a.fixture.decoded.trips=a.fixture.decoded.trips.filter(t=>t.trip_id!==400);a.fixture.detail.rows[1].Gone=1;
 const r=await lane('classes',a.options);assert.equal(r.ok,true);assert(r.issues.some(x=>x.trip_id===400));assert.deepEqual(old.fields,oldFields);assert.equal(a.db['live-entries'].find(r=>r.fields['trip-id']===401).fields.gone,1);assert.equal(a.db['live-classes'][0].fields.total,2);
});
test('ring lane collects without group records; records missing links and continues',async t=>{
 const a=setup(t);const r=await lane('rings',a.options);assert.equal(r.ok,true);assert(r.issues.some(x=>x.message.includes('shared group-key')));assert.equal(r.counts['live-rings'],1);
});
test('stage source failure alerts immediately; other lanes finish; no publication/retirement invoked',async t=>{
 const a=setup(t),get=a.options.get;a.options.get=async(url,d)=>{if(url.includes('ListAjax'))throw Error('Source offline');return get(url,d);};
 a.options.api.retire=()=>{throw Error('Forbidden retirement');};a.options.api.retireLogs=()=>{throw Error('Forbidden retention');};
 const r=await scheduled(a.options);assert.equal(r.find(x=>x.lane==='groups').ok,false);assert.equal(r.find(x=>x.lane==='rings').ok,true);assert.equal(a.alerts.length,1);assert.equal(a.alerts[0].stage,'collection/save');
});
test('false live status still stamps and completes each independent heartbeat without source calls',async t=>{
 const a=setup(t);a.options.getStatus=async()=>false;const r=await scheduled(a.options);assert(r.every(x=>x.skipped==='live-status-false'));assert.equal(a.db.heartbeat.length,3);assert.equal(a.requests.length,0);assert(a.db.heartbeat.every(x=>x.fields.result==='Skipped'));
});
test('logging outage does not stop collected business data',async t=>{
 const a=setup(t),call=a.options.api.call;a.options.api.call=async(b,table,...args)=>{if(!table.startsWith('live-'))throw Error('Logs offline');return call(b,table,...args);};const r=await lane('groups',a.options);assert.equal(r.ok,true);assert.equal(a.db['live-groups'].length,1);assert(r.logging_errors.length);
});
test('missing class reference data preserves existing group links',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('classes',a.options);await lane('groups',a.options);
 a.db['live-groups'][0].fields.classes=['recPreservedClass'];
 const before=structuredClone(a.db['live-groups'][0].fields.classes);delete a.fixture.groups[0].json_data[0].classes;
 const r=await lane('groups',a.options);assert.equal(r.ok,true);assert.deepEqual(a.db['live-groups'][0].fields.classes,before);assert(r.issues.length);
});
test('missing trip counters do not clear collected values or infer zero',async t=>{
 const a=setup(t);await lane('groups',a.options);a.fixture.detail.rows[0].Gone=1;await lane('classes',a.options);
 delete a.fixture.detail.rows[0].Gone;delete a.fixture.detail.gone_trips;
 const r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(a.db['live-entries'].find(x=>x.fields['trip-id']===400).fields.gone,1);
});
test('all business saves failing produces a stage alert without repeating writes',async t=>{
 const a=setup(t);a.options.api.call=async()=>{throw Error('Airtable offline');};const r=await lane('groups',a.options);
 assert.equal(r.ok,false);assert.equal(a.alerts.length,1);assert.match(a.alerts[0].stage,/save live-groups/);
});
test('native link order changes are not business diffs',()=>{
 assert.deepEqual(diffs({groups:['recB','recA']},{groups:['recA','recB']}),[]);
});
test('missing group counts preserve values; explicit zero changes are saved and reported',async t=>{
 const a=setup(t);a.fixture.groups[0].json_data[0].gone=2;await lane('groups',a.options);delete a.fixture.groups[0].json_data[0].gone;
 await lane('groups',a.options);assert.equal(a.db['live-groups'][0].fields.gone,2);
 a.fixture.groups[0].json_data[0].gone=0;const r=await lane('groups',a.options);assert.equal(a.db['live-groups'][0].fields.gone,0);
 assert(r.changes.some(c=>c.fields.some(f=>f.field==='gone'&&f.before===2&&f.after===0)));
 assert(a.db['run-logs'].some(x=>x.fields.summary.includes('"field":"gone"')));
});
test('corrupt shared references are logged without failing or stopping ring collection',async t=>{
 const a=setup(t);fs.writeFileSync(path.join(a.runtime,'lane-groups-state.json'),'{invalid');
 const r=await lane('classes',a.options);assert.equal(r.ok,true);assert(r.issues.some(x=>/unreadable/.test(x.message)));assert.equal(a.alerts.length,0);
 assert.equal((await lane('rings',a.options)).ok,true);
});
test('an unavailable alert transport stays explicitly undelivered, never recorded as sent',async t=>{
 const a=setup(t);delete a.options.alert;a.options.getStatus=async()=>{throw Error('Status source offline');};
 const r=await lane('groups',a.options);assert.equal(r.ok,false);assert.equal(r.stage_errors[0].alert,'Undelivered');assert.match(r.stage_errors[0].alert_error,/not configured/);
});
test('all class identity checks being unresolved stays Partial, not a failed stage',async t=>{
 const a=setup(t);await lane('groups',a.options);a.fixture.decoded.class.show_id=999;
 const r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(r.stage_errors.length,0);assert(r.issues.length);assert.equal(a.alerts.length,0);
});
