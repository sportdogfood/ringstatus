'use strict';
globalThis.fetch=async()=>{throw Error('Offline tests: network forbidden');};
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {lane,scheduled,inWindow}=require('./candidate/lanes'),{diffs}=require('./candidate/lane-store');
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

test('day0 class source populates distinct trips/results and preserves existing live-entries links',async t=>{
 const a=setup(t);Object.assign(a.fixture.decoded.trips[0],{entryxclasses_uuid:'uuid400',owner_id:70,Owner:'Owner',placing:2,score1:0});
 await lane('groups',a.options);const r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(a.db['live-trips'].length,2);assert(!a.db['live-trip-results']);assert.equal(a.db['live-entries'].length,2);
 const trip=a.db['live-trips'].find(x=>x.fields['trip-id']===400);assert.equal(trip.fields['owner-id'],70);assert.equal(trip.fields.class.length,1);assert.equal(trip.fields.ring.length,1);assert.equal(trip.fields.groups.length,1);assert.equal(trip.fields['legacy-live-entry'].length,1);
 assert.equal(trip.fields.placing,'2');assert.equal(JSON.parse(trip.fields['results-json']).score1,'0');assert(!a.db['live-trip-results']);assert(!a.db['live-trip-messages']);assert(!a.db['live-trip-comments']);
 const ids=a.db['live-trips'].map(x=>x.id),legacy=structuredClone(a.db['live-entries']);a.writes.length=0;a.advance();await lane('classes',a.options);assert.deepEqual(a.db['live-trips'].map(x=>x.id),ids);assert.deepEqual(a.db['live-entries'],legacy);assert(!a.writes.some(x=>['live-trips','live-trip-results'].includes(x.table)));
});
test('decoded trips continue when UI details fail, with no extra endpoint or helper calls',async t=>{
 const a=setup(t);await lane('groups',a.options);const get=a.options.get;a.options.get=async(url,d)=>{if(url.includes('getLiveClassData'))throw Error('UI detail unavailable');return get(url,d);};
 const r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(a.db['live-trips'].length,2);assert(!a.db['live-trip-results']);assert.equal(a.db['live-classes'].length,1);assert(a.db['live-trips'].every(x=>x.fields.class?.length===1));assert(r.issues.some(x=>x.message.includes('UI detail unavailable')));assert(!a.requests.some(x=>/people|entries/.test(x)));
});
test('trips absent from UI rows are still collected from the class source',async t=>{
 const a=setup(t);a.fixture.detail.rows=[];a.fixture.detail.recs=0;await lane('groups',a.options);await lane('classes',a.options);assert.equal(a.db['live-trips'].length,2);assert.equal((a.db['live-entries']||[]).length,0);
});
test('direct trip result changes retain trip ID and existing class links',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('classes',a.options);const trip=a.db['live-trips'][0],id=trip.id;const links=structuredClone(trip.fields.class);a.fixture.decoded.trips[0].placing=1;a.advance();await lane('classes',a.options);assert.equal(a.db['live-trips'][0].id,id);assert.deepEqual(a.db['live-trips'][0].fields.class,links);assert.equal(a.db['live-trips'][0].fields.placing,'1');assert(!a.db['live-trip-results']);
});
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
test('Completed groups remain collectible without a status gate',async t=>{
 const a=setup(t);await lane('groups',a.options);a.fixture.groups[0].json_data[0].status='Completed';await lane('groups',a.options);
 assert.equal(a.db['live-groups'][0].fields.status,'Completed');a.requests.length=0;const r=await lane('classes',a.options);assert.equal(r.skipped,undefined);assert.equal(a.requests.length,2);
});
test('one malformed group does not block valid group',async t=>{
 const a=setup(t);a.fixture.groups[0].json_data.unshift({...a.fixture.groups[0].json_data[0],ring_id:null,class_group_id:21});
 const r=await lane('groups',a.options);assert.equal(r.ok,true);assert(a.db['live-groups'].some(x=>x.fields['class-group-id']===20&&x.fields.ring?.length===1));assert(r.issues.length);assert.equal(a.alerts.length,0);
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
test('approved false-live rule registers an hourly skipped check without detail collection',async t=>{
 const a=setup(t),before=structuredClone(a.db['live-rings']);a.options.getStatus=async()=>false;const r=await scheduled(a.options);assert(r.every(x=>x.live_status===false));assert.equal(a.db.heartbeat.length,1);assert.equal(a.requests.length,0);assert.equal(a.db['live-groups'],undefined);assert.deepEqual(a.db['live-rings'],before);assert.equal(r[0].minutes,60);
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
 assert.equal(r.ok,false);assert.equal(a.alerts.length,2);assert.deepEqual(a.alerts.map(x=>x.stage),['save live-groups','save live-class-appearances']);
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
test('live-status request failure is logged and does not block collection',async t=>{
 const a=setup(t);delete a.options.alert;a.options.getStatus=async()=>{throw Error('Status source offline');};
 const r=await lane('groups',a.options);assert.equal(r.ok,true);assert.equal(r.stage_errors.length,0);assert(r.issues.some(x=>x.message.includes('Status source offline')));assert.equal(a.db['live-groups'].length,1);
});
test('all class identity checks being unresolved stays Partial, not a failed stage',async t=>{
 const a=setup(t);await lane('groups',a.options);a.fixture.decoded.class.show_id=999;
 const r=await lane('classes',a.options);assert.equal(r.ok,true);assert.equal(r.stage_errors.length,0);assert(r.issues.length);assert.equal(a.alerts.length,0);
});

const membership=require('./candidate/membership');
test('saved payload contains 25 groups and 43 separate appearances, 41 IDs',()=>{
 const payload=JSON.parse(fs.readFileSync(path.join(__dirname,'../payload-examples-20261101/live-groups-full-response-1051.json'),'utf8'));
 const m=membership.appearances(payload,{show:200000076,date:'2026-11-01'});
 assert(m.complete);assert.equal(m.groups.length,25);assert.equal(m.classes.length,43);
 assert.equal(new Set(m.classes.map(c=>c['class-id'])).size,41);
 assert.equal(new Set(m.classes.map(c=>c.key)).size,43);
});
test('two complete misses label absent then dropped; return reactivates with links preserved',()=>{
 const scope={show:75,date:'2026-11-01'},initial=[{key:'75|2026-11-01|20|30','show-id':75,date:scope.date,'record-status':'Active',class:['recClass'],group:['recGroup'],'last-seen':'original'}];
 const a=membership.reconcile(initial,[],{complete:true,scope});assert.equal(a[0]['record-status'],'Absent');
 const b=membership.reconcile(a,[],{complete:true,scope});assert.equal(b[0]['record-status'],'Dropped');assert.equal(b[0]['last-seen'],'original');
 const c=membership.reconcile(b,[{...initial[0],'last-seen':'new'}],{complete:true,scope});assert.equal(c[0]['record-status'],'Active');assert.deepEqual(c[0].class,['recClass']);assert.deepEqual(c[0].group,['recGroup']);
});
test('incomplete inventory never generates absent/dropped updates; other days unchanged',()=>{
 const scope={show:75,date:'2026-11-01'},prior=[{key:'old','show-id':75,date:scope.date,'record-status':'Absent'},{key:'future','show-id':75,date:'2026-11-02','record-status':'Active'}];
 assert.deepEqual(membership.reconcile(prior,[],{complete:false,scope}),[]);
 const r=membership.reconcile(prior,[],{complete:true,scope});assert.equal(r.length,1);assert.equal(r[0].key,'old');
});
test('repeated class appearances transition independently by group',()=>{
 const scope={show:75,date:'2026-11-01'},rows=[20,21].map(g=>({key:`75|2026-11-01|${g}|30`,'show-id':75,date:scope.date,'group-id':g,'class-id':30,'record-status':'Active'}));
 const r=membership.reconcile(rows,[rows[0]],{complete:true,scope});assert.equal(r.find(x=>x['group-id']===20)['record-status'],'Active');assert.equal(r.find(x=>x['group-id']===21)['record-status'],'Absent');
});
test('unavailable detail flag does not block class endpoint collection',async t=>{
 const a=setup(t);a.fixture.groups[0].json_data[0].has_JSON=false;await lane('groups',a.options);a.requests.length=0;
 const r=await lane('classes',a.options);assert.equal(a.requests.length,2);assert.equal(r.counts['live-classes'],1);
});
test('canonical priority zero date overrides wall calendar and selected planning date',async t=>{
 const a=setup(t);a.db.shows[0].fields.selected_date='2026-11-02';a.options.clock=()=> '2026-11-02T14:00:00Z';
 const r=await lane('groups',a.options);assert.equal(r.scope.date,'2026-11-01');assert.equal(r.day_priority,0);assert.equal(r.counts['live-groups'],1);
});

test('native membership follows two successful misses, preserves links/clocks and returns active',async t=>{
 const a=setup(t);await lane('groups',a.options);await lane('classes',a.options);await lane('groups',a.options);
 const group=a.db['live-groups'][0],appearance=a.db['live-class-appearances'][0];
 group.fields.schedules=['recSchedule'];const gid=group.id,aid=appearance.id,seen=appearance.fields['last-seen'],links=structuredClone(appearance.fields);
 a.fixture.groups[0].json_data=[];await lane('groups',a.options);
 assert.equal(group.fields['record-status'],'Absent');assert.equal(appearance.fields['record-status'],'Absent');
 a.advance();await lane('groups',a.options);assert.equal(group.fields['record-status'],'Dropped');assert.equal(appearance.fields['record-status'],'Dropped');
 assert.equal(appearance.fields['last-seen'],seen);assert.deepEqual(appearance.fields.class,links.class);assert.deepEqual(appearance.fields.group,links.group);assert.deepEqual(group.fields.schedules,['recSchedule']);
 a.fixture.groups[0].json_data=[a.fixture.widget[0].live_data[1].livenow[0]];await lane('groups',a.options);
 assert.equal(group.fields['record-status'],'Active');assert.equal(appearance.fields['record-status'],'Active');assert.equal(group.id,gid);assert.equal(appearance.id,aid);assert.equal(a.db['live-class-appearances'].length,1);
});
test('failed or incomplete source does not advance native absence; valid collection continues',async t=>{
 const a=setup(t);await lane('groups',a.options);const group=a.db['live-groups'][0];
 a.fixture.groups[0].json_data=[{day:'2026-11-01',class_group_id:21,classes:null}];await lane('groups',a.options);assert.equal(group.fields['record-status'],'Active');
 a.options.get=async()=>{throw Error('Network failed');};const r=await lane('groups',a.options);assert.equal(r.ok,false);assert.equal(group.fields['record-status'],'Active');
});
test('same class in two groups creates two linked appearance records without duplicate details',async t=>{
 const a=setup(t),g=a.fixture.groups[0].json_data[0];a.fixture.groups[0].json_data.push({...g,class_group_id:21});
 await lane('groups',a.options);await lane('classes',a.options);const r=await lane('groups',a.options);
 assert.equal(r.membership.class_appearances,2);assert.equal(r.membership.unique_classes,1);assert.equal(a.db['live-classes'].length,1);assert.equal(a.db['live-class-appearances'].length,2);
 assert.equal(new Set(a.db['live-class-appearances'].map(x=>x.fields.group[0])).size,2);
 assert(a.db['live-class-appearances'].every(x=>x.fields.class[0]===a.db['live-classes'][0].id));
 const retained=a.db['live-classes'][0].fields.groups.slice();a.fixture.groups[0].json_data.pop();await lane('groups',a.options);await lane('classes',a.options);
 assert.deepEqual(a.db['live-classes'][0].fields.groups,retained);assert.equal(a.db['live-class-appearances'][1].fields['record-status'],'Absent');
});
test('only day zero memberships are updated and no held stage is invoked',async t=>{
 const a=setup(t);a.db['live-groups']=[{id:'historical',fields:{'group-key':'75|2026-10-31|1','show-id':75,date:'2026-10-31','record-status':'Active'}}];
 a.db['live-class-appearances']=[{id:'future',fields:{'appearance-key':'75|2026-11-02|2|3','show-id':75,date:'2026-11-02','record-status':'Active',class:['old']}}];
 const before=structuredClone([a.db['live-groups'][0],a.db['live-class-appearances'][0]]);await lane('groups',a.options);
 assert.deepEqual([a.db['live-groups'][0],a.db['live-class-appearances'][0]],before);
});
test('explicit empty endpoint response succeeds and labels a first missing observation',async t=>{
 const a=setup(t);await lane('groups',a.options);a.fixture.groups=[];const r=await lane('groups',a.options);
 assert.equal(r.ok,true);assert.equal(r.membership.groups,0);assert.equal(a.db['live-groups'][0].fields['record-status'],'Absent');assert.equal(a.db['live-class-appearances'][0].fields['record-status'],'Absent');
});

test('saved 25-group payload creates 43 linked appearances and stable repeat IDs',async t=>{
 const a=setup(t),payload=JSON.parse(fs.readFileSync(path.join(__dirname,'../payload-examples-20261101/live-groups-full-response-1051.json'),'utf8'));
 a.fixture.groups=payload;a.db.shows[0].fields.show_id=200000076;a.db.shows[0].fields['show-key']='15|200000076';a.db['show-days'][0].fields['show-day-key']='15|200000076|2026-11-01';
 const ids=[...new Set(payload[0].json_data.flatMap(g=>g.classes))];
 a.db['live-classes']=ids.map(id=>({id:'class'+id,fields:{'class-key':`200000076|2026-11-01|${id}`,'class-id':Number(id),'show-id':200000076,date:'2026-11-01',schedules:['schedule'+id]}}));
 const before=structuredClone(a.db['live-classes']);const r=await lane('groups',a.options);assert.equal(r.membership.groups,25);assert.equal(r.membership.class_appearances,43);
 const appearances=a.db['live-class-appearances'];assert.equal(appearances.length,43);assert(appearances.every(x=>x.fields.group.length===1&&x.fields.class.length===1&&x.fields['record-status']==='Active'));
 const stable=appearances.map(x=>[x.id,x.fields['appearance-key']]);await lane('groups',a.options);assert.deepEqual(appearances.map(x=>[x.id,x.fields['appearance-key']]),stable);assert.deepEqual(a.db['live-classes'],before);
});

test('malformed null group is logged while valid records continue and absence is not advanced',async t=>{
 const a=setup(t);await lane('groups',a.options);const old=a.db['live-groups'][0];a.fixture.groups[0].json_data=[null,{...a.fixture.widget[0].live_data[1].livenow[0],class_group_id:21}];
 const r=await lane('groups',a.options);assert.equal(r.ok,true);assert.equal(r.membership.complete,false);assert.equal(old.fields['record-status'],'Active');assert(a.db['live-groups'].some(x=>x.fields['class-group-id']===21));assert(r.issues.length);
});
