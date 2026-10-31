'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {evaluate}=require('./calculator');
const {prepare}=require('./worker');
const {stage,preflight}=require('./stage');
const {hash}=require('../wef-timing/pace');
const {adapt}=require('../wef-timing/class-adapter');
const {verify:verifyClass}=require('./class-proof');
const {api,BASE,T}=require('./transport');
const {plan}=require('./retention');
const now='2026-10-30T14:00:00.000Z';
function prove(f){f.phaseProof={phase:'base',occurrence:f.identity.occurrence,sequence_hash:hash(f.rows),observed_at:f.observedAt,
 ref:{source_key:'offline-phase-contract',revision:'fixture'},anchor:{kind:'phase_start',at:f.classTiming.start_at}};return f;}
function fixture(){return prove({identity:{customer:15,show:1,date:'2026-10-30',ring:1,group:2,class:3,occurrence:'15|1|2026-10-30|1|7',phase:'base',entry:11,trip:'b'},now,
 rows:[{key:'a',entry:10,order:1,actual:0,scratch:false,gone:false},{key:'b',entry:11,order:2,actual:0,scratch:false,gone:false}],
 targetKey:'b',observedAt:now,refs:[{source_key:'source',revision:'rev'}],
 classTiming:{start_at:'2026-10-30T14:10:00.000Z',start_kind:'estimated',total_trips:2,completed_trips:0,count_completion_scope:'phase',pace_minutes_per_trip:3,pace_method:'default_3_minutes'}});}
test('proved order estimates from class start, never from entry number',()=>{
 const a=evaluate(fixture()).payload;assert.equal(a.trips_ahead,1);assert.equal(a.entries_ahead,1);assert.equal(a.predicted_go_at,'2026-10-30T14:13:00.000Z');
});
test('scratch and gone are excluded and never get a go time',()=>{
 for(const flag of ['scratch','gone']){const f=fixture();f.rows[1][flag]=true;assert.equal(evaluate(f).payload.predicted_go_at,null);}
 const f=fixture();f.rows[0].scratch=true;f.classTiming.total_trips=1;prove(f);assert.equal(evaluate(f).payload.trips_ahead,0);
});
test('unknown phase withholds derived ETA but preserves independently proved planned queue',()=>{
 const f=fixture();f.phaseProof=null;const p=evaluate(f).payload;
 assert.equal(p.predicted_go_at,null);assert.equal(p.trips_ahead,1);assert.equal(p.order,2);
});
test('missing eligibility and duplicate planned order withhold exact queue',()=>{
 for(const mutate of [f=>f.rows[0].gone=null,f=>f.rows[0].order=2]){
  const f=fixture();mutate(f);assert.equal(evaluate(f).payload.predicted_go_at,null);assert.equal(evaluate(f).payload.trips_ahead,null);
 }
});
test('partial actual execution order does not replace planned queue',()=>{
 const f=fixture();f.rows[0].actual=1;f.rows[0].gone=true;const p=evaluate(f).payload;
 assert.equal(p.trips_ahead,0);assert.equal(p.order,2);assert.equal(p.predicted_go_at,null);
});
test('stale and future observations withhold upcoming calculations without erasing observed status',()=>{
 for(const at of ['2026-10-30T13:30:00.000Z','2026-10-30T14:00:01.000Z']){
  const f=fixture();f.observedAt=at;assert.equal(evaluate(f).payload.eligibility,'stale');assert.equal(evaluate(f).payload.predicted_go_at,null);
  f.rows[1].gone=true;assert.equal(evaluate(f).payload.eligibility,'gone');assert.equal(evaluate(f).payload.observation_quality,'stale');
 }
});
test('countdown-only reevaluation keeps hash; expiry produces a new revision',()=>{
 const f=fixture(),a=evaluate(f),b=evaluate({...f,now:'2026-10-30T14:01:00.000Z'});
 assert.equal(a.fields['content-hash'],b.fields['content-hash']);assert.notEqual(a.payload.minutes_until_go,b.payload.minutes_until_go);
 assert.notEqual(a.fields['content-hash'],evaluate({...f,now:'2026-10-30T14:30:00.000Z'}).fields['content-hash']);
});
test('only today is accepted',()=>assert.throws(()=>evaluate({...fixture(),now:'2026-10-31T14:00:00.000Z'}),/identity\/date/));
function dataFixture(){
 const ref={base:'appdl16HJ5ETm8N69',key:'part',sha256:'a'.repeat(64),record:'rec00000000000001'};
 return{source_lane:'data_live',context:{'customer-id':15,'show-id':1,'show-key':'15|1','selected-date':'2026-10-30','start-date':'2026-10-30','end-date':'2026-10-30'},
 proof:{'show-key':'15|1','run-key':'run','state-sha256':'b'.repeat(64)},inventories:{'2026-10-30':{inventory_complete:true,observed_at:now,refs:[ref],groups:[{identity:{customer:15,show:1,date:'2026-10-30',ring:1,group:2},rows:[{'stage-key':'15|1|2026-10-30|1|7','class-id':3,'total-trips':1,'start-time':'10:10:00',type:'class'}]}]}},
 entry_classes:[{identity:[15,1,'2026-10-30',1,3],observed_at:now,refs:[ref],payload:{class_related_data:{class_group_id:2},total_entry_trips:1,trips:[{entry_id:11,trip_id:50,entryxclasses_uuid:'uuid50',order_of_go:1,actual_order:0,scratch_trip:0,gone_in:0}]}}]};
}
test('actual Data/class adapter preserves missing readback and phase, repeat run writes nothing',async()=>{
 const input={data:dataFixture(),classRows:[],paceRows:[],now,runId:'one'};
 const a=await prepare(input);assert.equal(a.writes.length,1);assert.equal(a.outputs[0].payload.predicted_go_at,null);
 assert.ok(a.outputs[0].payload.reasons.includes('class_readback_missing_or_changed'));
 const b=await prepare({...input,runId:'two',answers:a.writes.map(fields=>({fields}))});assert.equal(b.writes.length,0);assert.equal(b.unchanged,1);
});
test('ambiguous source occurrence is an explicit skip, never guessed',async()=>{
 const data=dataFixture();data.inventories['2026-10-30'].groups[0].rows.push({...data.inventories['2026-10-30'].groups[0].rows[0],'stage-key':'15|1|2026-10-30|1|8'});
 const a=await prepare({data,classRows:[],paceRows:[],now,runId:'one'});assert.equal(a.writes.length,0);assert.ok(a.issues.some(i=>i.reason==='entry_occurrence_ambiguous'));
});
test('publication persists intent before write and independently reads back',async()=>{
 const state={},events=[],transport=memoryAPI((method,table)=>{events.push(method+':'+table);if(method==='write'&&table===T['timing-answers'])assert.ok(state.pending_write);});
 const result=await stage({api:transport,state,save:async outcome=>events.push(outcome),data:dataFixture(),classRows:[],paceRows:[],now,runId:'run'});
 assert.ok(events.indexOf('entry_writing')<events.indexOf('write:'+T['timing-answers']));
 assert.ok(events.indexOf('read:'+T['timing-answers'])<events.indexOf('entry_written'));
 assert.equal(result.saved.length,1);assert.equal(state.pending_write,undefined);assert.equal(state.entry_counts.entries_written,1);
 for(const table of ['engine-checkpoints','run-logs','heartbeat'])assert.equal(transport.db.get(T[table]).length,1);
});
test('write failure preserves durable intent and blocks reuse',async()=>{
 const state={},args={state,save:async()=>{},data:dataFixture(),classRows:[],paceRows:[],now,runId:'run',api:{upsert:async()=>{throw Error('offline injected failure');}}};
 await assert.rejects(stage(args),/injected failure/);assert.ok(state.pending_write);assert.ok(state.entry_failed);
 await assert.rejects(stage(args),/owner review/);
});
test('transport forbids upstream writes and meta writes without HTTP',async()=>{
 let calls=0;const a=api({token:'offline',deadline:Date.now()+10000,request:async()=>{calls++;throw Error('unexpected HTTP');}});
 await assert.rejects(a.list('appgzosUcVqA4vZdf','live-entries'),/Forbidden/);
 await assert.rejects(a.upsert('tblwBvigcmqRlSYUb','Name',[{Name:'x'}]),/Forbidden entry target\/method/);assert.equal(calls,0);
});
test('retention protects today, future, young and synced records, deletion stays disabled',()=>{
 const row=(id,date,at)=>({id:'rec'+String(id).padStart(14,'0'),fields:{'answer-kind':'entry-timing','source-lane':'data_live',date,'evaluated-at':at}});
 const old='2026-10-29T13:59:59.000Z',rows=[row(1,'2026-10-29',old),row(2,'2026-10-30',old),row(3,'2026-10-31',old),row(4,'2026-10-29',now),row(5,'2026-10-29',old)];
 const p=plan({rows,now,protectedIds:[rows[4].id]});assert.deepEqual(p.manifest.records.map(r=>r.id),[rows[0].id]);assert.equal(p.deletion_enabled,false);
});
function memoryAPI(onCall=()=>{}){
 const db=new Map();let next=1;
 return{db,upsert:async(table,key,fields)=>{onCall('write',table);const old=db.get(table)||[],records=fields.map(f=>{
  const match=old.find(r=>r.fields[key]===f[key]);return{id:match?.id||'rec'+String(next++).padStart(14,'0'),fields:structuredClone(f)};
 });db.set(table,[...old.filter(r=>!records.some(n=>n.id===r.id)),...records]);return{records:structuredClone(records)};},
 list:async(base,table,formula)=>{assert.equal(base,BASE);onCall('read',table);const rows=db.get(table)||[];
  return structuredClone(formula?.includes('RECORD_ID()')?rows.filter(r=>formula.includes(r.id)):rows);}};
}
test('matching phase proof supports pre-progress estimate without whole-class completion',()=>{
 const f=fixture();assert.equal(f.classTiming.count_completion_scope,'phase');assert.ok(evaluate(f).payload.predicted_go_at);
 f.classTiming.count_completion_scope='unknown';assert.ok(evaluate(f).payload.predicted_go_at);
 f.phaseProof.phase='jump-off';assert.equal(evaluate(f).payload.predicted_go_at,null);
});
test('observation time cannot become an in-progress anchor',()=>{
 const f=fixture();f.rows[0].gone=true;prove(f);const p=evaluate(f).payload;
 assert.equal(p.trips_ahead,0);assert.equal(p.predicted_go_at,null);assert.ok(p.reasons.includes('phase_start_or_progress_anchor_unproven'));
});
test('malformed second occurrence and unknown raw class prevent false singleton',async()=>{
 for(const extra of [{'stage-key':'15|1|2026-10-30|1|position-2','class-id':3,type:'class'},{'stage-key':'unknown','class-id':null,type:'class'}]){
  const data=dataFixture();data.inventories['2026-10-30'].groups[0].rows.push(extra);
  const r=await prepare({data,classRows:[],paceRows:[],now,runId:'r'});assert.equal(r.outputs.length,0);assert.ok(r.issues.some(i=>i.reason==='entry_occurrence_ambiguous'));
 }
});
test('saved class material and provenance are independently checked',()=>{
 const expected=adapt({data:dataFixture(),now,paceRows:[],liveClasses:new Map()})[0];
 const saved={id:'rec00000000000001',fields:structuredClone(expected.fields)};
 assert.equal(verifyClass(saved,expected,now),true);
 for(const mutate of [r=>{const p=JSON.parse(r.fields['payload-json']);p.start_at='2026-10-30T14:20:00.000Z';r.fields['payload-json']=JSON.stringify(p);},
  r=>r.fields['input-refs-json']='[]',r=>r.fields['answer-key']='wrong',r=>r.fields['observed-at']='2026-10-30T13:00:00.000Z']){
  const r=structuredClone(saved);mutate(r);assert.equal(verifyClass(r,expected,now),false);
 }
});
test('worker accepts verified class bytes but never invents phase from unknown coverage',async()=>{
 const data=dataFixture(),expected=adapt({data,now,paceRows:[],liveClasses:new Map()})[0];
 const classRows=[{id:'rec00000000000001',fields:expected.fields}];
 const r=await prepare({data,classRows,paceRows:[],now,runId:'r'});
 assert.ok(!r.outputs[0].payload.reasons.includes('class_readback_missing_or_changed'));
 assert.ok(r.outputs[0].payload.reasons.includes('phase_unproven'));assert.equal(r.outputs[0].payload.predicted_go_at,null);
});
test('entry preflight rejects saved uncertain intent before business publication',async()=>{
 const a=memoryAPI();await preflight({api:a,state:{}});
 a.db.set(T['engine-checkpoints'],[{id:'rec00000000000001',fields:{'checkpoint-key':'entry-data-runner','policy-version':'entry-timing-v1','last-outcome':'writing','state-json':JSON.stringify({version:1,pending_write:{run_id:'old'}})}}]);
 await assert.rejects(preflight({api:a,state:{}}),/owner review/);
});
test('independent answer readback mismatch leaves intent and failed checkpoint',async()=>{
 const a=memoryAPI(),list=a.list;a.list=async(base,table,formula)=>{
  const rows=await list(base,table,formula);if(table===T['timing-answers']&&rows.length)rows[0].fields['content-hash']='tampered';return rows;
 };
 const state={};await assert.rejects(stage({api:a,state,save:async()=>{},data:dataFixture(),classRows:[],paceRows:[],now,runId:'r'}),/readback mismatch/);
 assert.ok(state.pending_write);assert.ok(state.entry_failed);assert.equal(state.entry_counts.entries_written,0);
 assert.equal(a.db.get(T['engine-checkpoints'])[0].fields['last-outcome'],'failed');assert.equal(a.db.get(T['errs-log']).length,1);
});
test('Live-first matched frozen roster can override stale Data values without renewing their age',async()=>{
 const data=dataFixture(),s=data.entry_classes[0];s.observed_at='2026-10-30T13:00:00.000Z';
 Object.assign(s.payload.trips[0],{horse_id:20,rider_id:30});
 const raw={live:{id:50,class_id:3,OOG:2,Actual_OOG:0,Scr:0,Gone:0},identity:{trip_id:50,entry_id:11}};
 const rows=[{'trip-id':50,'entry-id':11,'horse-id':20,'rider-id':30,_class:'1|2026-10-30|3','order-of-go':2,'actual-order':0,scratch:false,gone:0,'source-json':JSON.stringify(raw)}];
 const fingerprint=require('../wef-timing/pace').digest(JSON.stringify(rows));
 const live={identity:{customer:15,show:1,date:'2026-10-30',ring:1,group:2,class:3},source_lane:'data_live',observed_at:now,
  valid_until:'2026-10-30T14:10:00.000Z',counts:{scope:'class',total:1,completed:0,completion_scope:'unknown'},
  ref:{source_key:'1|2026-10-30|3',revision:'c'.repeat(64),parts:[{archive:'fixture',fingerprint}]}};
 const args={data,liveClasses:new Map([['1|2026-10-30|1|2|3',live]]),classRows:[],paceRows:[],now,runId:'r',readArchive:async()=>structuredClone(rows)};
 const good=await prepare(args);assert.equal(good.outputs[0].payload.source,'live');assert.equal(good.outputs[0].payload.observed_at,now);
 assert.equal(good.outputs[0].payload.input_refs[0].observed_at,s.observed_at);
 rows[0]['order-of-go']=100000;raw.live.OOG=100000;rows[0]['source-json']=JSON.stringify(raw);live.ref.parts[0].fingerprint=require('../wef-timing/pace').digest(JSON.stringify(rows));
 const sentinel=await prepare(args);assert.equal(sentinel.outputs[0].payload.order,null);assert.equal(sentinel.outputs[0].payload.order_observation.raw,100000);
 rows[0]['entry-id']=99;live.ref.parts[0].fingerprint=require('../wef-timing/pace').digest(JSON.stringify(rows));
 const conflict=await prepare(args);assert.equal(conflict.outputs[0].payload.source,'data');assert.equal(conflict.outputs[0].payload.eligibility,'stale');
 assert.ok(conflict.outputs[0].payload.reasons.includes('live_trip_roster_mismatch'));
 live.ref.parts[0].fingerprint='0'.repeat(64);const bad=await prepare(args);assert.equal(bad.outputs.length,0);assert.ok(bad.issues.some(i=>i.reason.includes('archive hash mismatch')));
});
test('real entry stage isolates malformed groups in both orders across repeat runs',async()=>{
 for(const reverse of [false,true])for(const malformed of [null,{identity:{customer:15,show:1,date:'2026-10-30',ring:2,group:9},rows:[null]},
  {identity:{customer:15,show:1,date:'2026-10-30',ring:2,group:9},rows:null}]){
  const data=dataFixture(),good=data.inventories['2026-10-30'].groups[0];
  data.inventories['2026-10-30'].groups=reverse?[malformed,good]:[good,malformed];
  const original=JSON.stringify(data),a=memoryAPI(),state={};
  for(const runId of ['first','repeat']){
   await preflight({api:a,state});
   const result=await stage({api:a,state,save:async()=>{},data,classRows:[],paceRows:[],answers:a.db.get(T['timing-answers'])||[],now,runId,
    excludedGroups:new Set([malformed])});
   assert.equal(result.outputs.length,1);assert.equal(result.status,'partial');assert.equal(state.entry_failed,undefined);
   assert.equal(state.pending_write,undefined);assert.equal(result.writes.length,runId==='first'?1:0);
   assert.ok(result.issues.some(i=>i.reason==='entry_malformed_or_excluded_group'));
  }
  assert.equal(JSON.stringify(data),original);assert.equal(a.db.get(T['run-logs']).length,2);
 }
});
test('duplicate good/bad group excludes both but preserves unrelated scope on repeats',async()=>{
 for(const reverse of [false,true]){
  const data=dataFixture(),groups=data.inventories['2026-10-30'].groups,good=groups[0],bad={identity:{...good.identity},rows:[null]};
  const other=structuredClone(good);other.identity.group=9;other.rows[0]['class-id']=4;other.rows[0]['stage-key']='15|1|2026-10-30|1|8';
  const source=structuredClone(data.entry_classes[0]);source.identity[4]=4;source.payload.class_related_data.class_group_id=9;source.payload.trips[0].trip_id=51;source.payload.trips[0].entryxclasses_uuid='uuid51';data.entry_classes.push(source);
  data.inventories['2026-10-30'].groups=reverse?[bad,good,other]:[other,good,bad];
  const a=memoryAPI(),state={};
  for(const runId of ['first','repeat']){
   await preflight({api:a,state});const r=await stage({api:a,state,save:async()=>{},data,classRows:[],paceRows:[],now,runId,
    answers:a.db.get(T['timing-answers'])||[],excludedGroups:new Set([good,bad])});
   assert.deepEqual(r.outputs.map(o=>o.payload.identity.class),[4]);assert.equal(state.entry_failed,undefined);
   assert.equal(r.issues.filter(i=>i.reason==='entry_duplicate_group_excluded').length,2);assert.equal(r.writes.length,runId==='first'?1:0);
  }
 }
});
test('excluded raw occurrence still prevents a false unique entry match',async()=>{
 const data=dataFixture(),good=data.inventories['2026-10-30'].groups[0];
 const bad={identity:{...good.identity,group:9},rows:[{...good.rows[0]},null]};data.inventories['2026-10-30'].groups.push(bad);
 const r=await prepare({data,classRows:[],paceRows:[],now,runId:'r',excludedGroups:new Set([bad])});
 assert.equal(r.outputs.length,0);assert.ok(r.issues.some(i=>i.reason==='entry_occurrence_ambiguous'));
});

test('sentinel target and peer never acquire rank or exact queue through any input type',()=>{
 for(const sentinel of [100000,'100000',0,null,''])for(const target of [false,true]){
  const f=fixture();f.rows[target?1:0].order=sentinel;f.rows[target?1:0].actual=100000;f.phaseProof=null;
  const p=evaluate(f).payload;assert.equal(p.trips_ahead,null);assert.equal(p.workload_minutes,null);assert.equal(p.predicted_go_at,null);
  assert.equal(p.order,target?null:2);assert.equal(p.eligibility,'eligible');
 }
});
test('unknown peer eligibility cannot erase target status or invent queue',()=>{
 const f=fixture();f.rows[0].scratch=null;f.phaseProof=null;const p=evaluate(f).payload;
 assert.equal(p.eligibility,'eligible');assert.equal(p.order,2);assert.equal(p.trips_ahead,null);
});
test('gone sentinel peers are not pending; actual order never ranks the remaining target',()=>{
 const f=fixture();f.rows[0].order=100000;f.rows[0].actual=1;f.rows[0].gone=true;f.phaseProof=null;
 const p=evaluate(f).payload;assert.equal(p.order,2);assert.equal(p.trips_ahead,0);assert.equal(p.predicted_go_at,null);
});
test('provider estimate is independent sourced evidence, never a derived or completed upcoming ETA',()=>{
 const f=fixture();f.phaseProof=null;f.rows[0].order=100000;
 f.rows[1].providerEstimate={raw:'10:15:00',observed_at:now,ref:{source_key:'trip-b',revision:'source'}};
 let p=evaluate(f).payload;assert.equal(p.provider_estimate.at,'2026-10-30T14:15:00.000Z');assert.equal(p.provider_estimate.upcoming,true);
 assert.equal(p.predicted_go_at,null);assert.equal(p.trips_ahead,null);
 f.rows[1].gone=true;p=evaluate(f).payload;assert.equal(p.provider_estimate.upcoming,false);assert.equal(p.eligibility,'gone');
 f.observedAt='2026-10-30T13:00:00.000Z';p=evaluate(f).payload;assert.equal(p.eligibility,'gone');assert.equal(p.observation_quality,'stale');
 for(const raw of ['00:00:00','bad','25:00:00','2026-10-31T14:00:00Z']){f.rows[1].providerEstimate.raw=raw;assert.equal(evaluate(f).payload.provider_estimate.at,null);}
});
test('provider local time rejects DST ambiguity and nonexistent wall time',()=>{
 const {providerTime}=require('./calculator');assert.equal(providerTime('01:30:00','2026-11-01'),null);
 assert.equal(providerTime('02:30:00','2026-03-08'),null);
});
test('worker normalizes retained sentinel and carries original provider provenance through real stage',async()=>{
 const data=dataFixture(),t=data.entry_classes[0].payload.trips[0];Object.assign(t,{order_of_go:'100000',actual_order:1,gone_in:1,estimated_go_time:'10:15:00'});
 const a=memoryAPI(),state={};const r=await stage({api:a,state,save:async()=>{},data,classRows:[],paceRows:[],now,runId:'sentinel'});
 const p=JSON.parse(r.saved[0].fields['payload-json']);assert.equal(p.order,null);assert.equal(p.actual_order,1);assert.equal(p.eligibility,'gone');
 assert.equal(p.order_observation.raw,'100000');assert.equal(p.provider_estimate.upcoming,false);assert.equal(p.provider_estimate.ref.parts[0].sha256,'a'.repeat(64));
});
test('Class component proof preserves matching pace when only end/count material changes, rejects tampering',()=>{
 const {components,material}=require('./class-proof');const expected=adapt({data:dataFixture(),now,paceRows:[],liveClasses:new Map()})[0];
 const p=structuredClone(expected.payload);p.total_trips=null;p.completed_trips=null;p.duration_minutes=null;p.estimated_end_at=null;p.end_kind='unknown';
 const fields={...expected.fields,'payload-json':JSON.stringify(p),'content-hash':hash(material(p)),availability:'unavailable'};
 fields['answer-key']=fields['scope-key']+'|'+fields['content-hash'];const saved={id:'rec00000000000001',fields};
 assert.equal(components(saved,expected,now).pace_minutes_per_trip,2.5746);assert.equal(verifyClass(saved,expected,now),false);
 const bad=structuredClone(saved);bad.fields['pace-minutes-per-trip']=6;assert.equal(components(bad,expected,now),null);
 const wrong=structuredClone(saved);wrong.fields['payload-json']=wrong.fields['payload-json'].replace('"class":3','"class":4');assert.equal(components(wrong,expected,now),null);
});
test('malformed trip class is scoped; unrelated class still publishes without persistent failure',async()=>{
 const data=dataFixture(),other=structuredClone(data.entry_classes[0]),group=structuredClone(data.inventories['2026-10-30'].groups[0]);
 group.identity.group=9;group.rows[0]['class-id']=4;group.rows[0]['stage-key']='15|1|2026-10-30|1|8';data.inventories['2026-10-30'].groups.push(group);
 other.identity[4]=4;other.payload.class_related_data.class_group_id=9;other.payload.trips[0].entry_id=null;data.entry_classes.push(other);
 const a=memoryAPI(),state={};const r=await stage({api:a,state,save:async()=>{},data,classRows:[],paceRows:[],now,runId:'local'});
 assert.equal(r.saved.length,1);assert.equal(r.issues.length,1);assert.equal(state.entry_failed,undefined);
});
test('isolated runner uses real stage twice with read-only upstream and own journal',async()=>{
 const {run}=require('./runner'),{SHOWS,TIMING,DATA_TABLES,CLASS_TIMING,CLASS_ANSWERS}=require('../wef-timing/api');
 const data=dataFixture(),a=memoryAPI(),state={version:1};let saves=0;
 const show={id:'rec00000000000999',fields:{is_default:true,customer_id:15,show_id:1,'show-key':'15|1',last_checked:now,start_date:'2026-10-30',end_date:'2026-10-30',selected_date:'2026-10-30'}};
 const args={deadline:Date.now()+160000,clock:()=>now,state,save:async()=>{saves++;},destination:a,
  source:{list:async(base,table)=>{if(base===SHOWS&&table==='shows')return[show];assert.equal(base,TIMING);assert.ok([DATA_TABLES.answers,require('../wef-timing/api').SYNC_GROUPS,require('../wef-timing/api').SYNC_RINGS].includes(table));return[];}},
  classSource:{list:async(base,table)=>{assert.equal(base,CLASS_TIMING);assert.equal(table,CLASS_ANSWERS);return[];}},
  project:async()=>data,readPointer:()=>({head:'pinned'}),readLive:async()=>({classes:new Map()})};
 const first=await run({...args,runId:'first'}),second=await run({...args,runId:'second'});
 assert.equal(first.ok,true);assert.equal(second.ok,true);assert.equal(first.counts.entries_written,1);assert.equal(second.counts.entries_written,0);
 assert.equal(second.counts.entries_unchanged,1);assert.ok(saves>0);assert.equal(a.db.get(T['run-logs']).length,2);
 const failed=await run({...args,runId:'bad',project:async()=>{throw Error('Airtable HTTP 503');}});assert.equal(failed.status,'deferred');assert.equal(state.entry_failed,undefined);
 const recovered=await run({...args,runId:'recovered'});assert.equal(recovered.ok,true);
 state.pending_write={run_id:'uncertain'};
 let sourceCalls=0;const blocked=await run({...args,runId:'blocked',source:{list:async()=>{sourceCalls++;throw Error('must not reach');}}});
 assert.equal(blocked.ok,false);assert.equal(sourceCalls,0);
});

test('explicit Data group binds one occurrence without assigning its trips to another group',async()=>{
 const data=dataFixture(),second=structuredClone(data.inventories['2026-10-30'].groups[0]);
 second.identity.group=9;second.rows[0]['stage-key']='15|1|2026-10-30|1|8';data.inventories['2026-10-30'].groups.push(second);
 const r=await prepare({data,classRows:[],paceRows:[],now,runId:'r'});
 assert.equal(r.outputs.length,1);assert.equal(r.outputs[0].payload.identity.group,2);assert.equal(r.issues.length,1);
 assert.equal(r.issues[0].identity.group,9);
});

test('proven prewrite fence failure defers, logs and permits next ordinary invocation; reporting uncertainty blocks',async()=>{
 for(const controlFailure of [false,true]){
 const a=memoryAPI(),state={};if(controlFailure){const upsert=a.upsert;a.upsert=async(...args)=>{if(args[0]===T['run-logs'])throw Error('control HTTP 503');return upsert(...args);};}
 const args={api:a,state,save:async()=>{},data:dataFixture(),classRows:[],paceRows:[],now,runId:'fence'};
 await assert.rejects(stage({...args,beforePublish:async()=>{throw Error('Entry sync source changed');}}),/sync source changed/);
 assert.equal(state.pending_write,undefined);assert.equal((a.db.get(T['timing-answers'])||[]).length,0);
 if(controlFailure){assert.ok(state.entry_failed);await assert.rejects(preflight({api:a,state}),/owner review/);}
 else{assert.equal(state.entry_failed,undefined);await preflight({api:a,state});const good=await stage({...args,runId:'next-normal'});assert.equal(good.saved.length,1);}
 }
});

test('real retained chain feeds captured final pace through Class proof, Entry saved readback and repeat',async()=>{
 const fs=require('fs'),{execFileSync}=require('child_process'),{run}=require('./runner'),A=require('../wef-timing/api');
 const stage='C:/Users/gombc/Documents/Codex/staging/wef-entry-workingday-20261031';
 const captured=JSON.parse(fs.readFileSync('C:/Users/gombc/Documents/Codex/staging/wef-ring-sync-final-20261030/scheduled-readback-93554292-c1a8-4175-8423-289b0f767856.json'));
 const fixture=JSON.parse(execFileSync('wef-data/.venv/Scripts/python.exe',['-B','wef-entry-calculator/retained-fixture.py',stage,'captured','2026-10-30'],{maxBuffer:8*1024*1024}));
 assert.equal(fixture.data.proof['state-sha256'],fixture.pointer['run-sha256']);assert.equal(fixture.data.proof['run-key'],fixture.pointer.head);
 const finals=captured.ring_rows.filter(r=>JSON.parse(r.fields['payload-json']).group_progress?.quality==='final');
 const unique=values=>[...new Map(values.map(r=>[r.id,r])).values()];
 const groups=unique(finals.map(r=>JSON.parse(r.fields['payload-json']).group_progress.ref.material.group)),rings=unique(finals.map(r=>JSON.parse(r.fields['payload-json']).group_progress.ref.material.ring));
 const c=fixture.context,show={id:'rec00000000000999',fields:{is_default:true,customer_id:c['customer-id'],show_id:c['show-id'],'show-key':c['show-key'],last_checked:captured.completed,start_date:c['start-date'],end_date:c['end-date'],selected_date:c['selected-date']}};
 const a=memoryAPI(),state={version:1},args={runId:'retained-captured',deadline:Date.now()+160000,clock:()=>captured.completed,state,save:async()=>{},destination:a,source:{list:async(base,table)=>table==='shows'?[show]:table===A.SYNC_GROUPS?groups:table===A.SYNC_RINGS?rings:captured.ring_rows},classSource:{list:async()=>captured.class_rows},project:async()=>structuredClone(fixture.data),readPointer:()=>fixture.pointer,readLive:async()=>({classes:new Map()})};
 const out=await run(args);assert.equal(out.ok,true,JSON.stringify(out));const saved=a.db.get(T['timing-answers'])||[];assert.ok(saved.length>0);
 let checked=0;
 for(const ring of finals){const p=JSON.parse(ring.fields['payload-json']);if(!(p.selected_pace_minutes_per_trip>0))continue;
 const matching=saved.map(r=>JSON.parse(r.fields['payload-json'])).filter(p2=>p2.identity.group===p.identity.group&&p2.identity.ring===p.identity.ring&&captured.class_rows.some(c=>JSON.parse(c.fields['payload-json']).identity.class===p2.identity.class));
 assert.ok(matching.length>0,'final group '+p.identity.group);for(const p2 of matching)assert.equal(p2.pace_minutes_per_trip,p.selected_pace_minutes_per_trip);checked++;
 }
 assert.ok(checked>0);const repeat=await run({...args,runId:'retained-repeat'});assert.equal(repeat.counts.entries_written,0);assert.equal(repeat.counts.entries_unchanged,saved.length);
});

test('canonical planning tomorrow retains TODAY trip selection, sync filter, controls and authority fence',async()=>{
 const {execFileSync}=require('child_process'),{run}=require('./runner'),A=require('../wef-timing/api');
 const fixture=JSON.parse(execFileSync('wef-data/.venv/Scripts/python.exe',['-B','wef-entry-calculator/retained-fixture.py','C:/Users/gombc/Documents/Codex/staging/wef-entry-workingday-20261031','planning','2026-10-30'],{maxBuffer:8*1024*1024}));
 assert.equal(fixture.context['selected-date'],'2026-10-31');assert.ok(fixture.data.entry_classes.length>0);assert.ok(fixture.data.entry_classes.every(c=>c.identity[2]==='2026-10-30'));
 const c=fixture.context,at='2026-10-31T03:00:00.000Z',show={id:'rec00000000000999',fields:{is_default:true,customer_id:c['customer-id'],show_id:c['show-id'],'show-key':c['show-key'],last_checked:at,start_date:c['start-date'],end_date:c['end-date'],selected_date:c['selected-date']}};
 const a=memoryAPI(),state={version:1};let syncReads=0;
 const args={runId:'planning-tomorrow',clock:()=>at,deadline:Date.now()+160000,state,save:async()=>{},destination:a,source:{list:async(base,table,filter)=>{if(table==='shows')return[show];if([A.SYNC_GROUPS,A.SYNC_RINGS].includes(table)){assert.ok(filter.includes("2026-10-30"));syncReads++;}return[];}},classSource:{list:async()=>[]},project:async(context,deadline,day)=>{assert.deepEqual(context,c);assert.equal(day,'2026-10-30');return structuredClone(fixture.data);},readPointer:()=>fixture.pointer,readLive:async()=>({classes:new Map()})};
 const out=await run(args);assert.equal(out.ok,true,JSON.stringify(out));assert.ok(syncReads>2);const rows=a.db.get(T['timing-answers'])||[];assert.ok(rows.length>0);assert.ok(rows.every(r=>r.fields.date==='2026-10-30'));assert.equal(a.db.get(T['run-logs'])[0].fields.date,'2026-10-30');
 const before=rows.length;const changed=await run({...args,runId:'changed',readPointer:(()=>{let n=0;return()=>++n<3?fixture.pointer:{...fixture.pointer,head:'changed'};})()});assert.equal(changed.ok,false);assert.equal((a.db.get(T['timing-answers'])||[]).length,before);
});
