"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {run,scheduleFingerprint}=require('./runner');const {ENGINE,SCHEDULES,LIVE,TABLES}=require('./api');
const instant='2026-10-29T09:25:00Z';
function fixture(){
  const raw=[{key:'g_c',fields:{show_id:76,show_date:'2026-10-30',ring_number:6,class_group_id:19,class_id:30,estimated_start_time:'08:06:00',estimated_end_time:'08:39:00',total_trips:11,class_type:'Jumpers'}}];
  const row={'stage-key':'76|2026-10-30|g_c','dataset-id':'dataset',status:'active','show-id':76,date:'2026-10-30',ring:6,'group-id':19,'class-id':30,'start-time':'08:06:00','end-time':'08:39:00','total-trips':11,type:'Jumpers','row-json':JSON.stringify(raw[0])};
  const sources={shows:[{id:'show',fields:{show_id:76,customer_id:15,is_default:true,last_checked:'2026-10-29T09:24:00Z'}}],
    rings:[{id:'ring',fields:{show_id:76,ring_date:'2026-10-30',ring_number:6}}],
    schedules:[{id:'schedule',fields:row}],
    'show-collection':[{id:'scope',fields:{'show-id':76,date:'2026-10-30',status:'complete',completed:'2026-10-29T09:24:00Z','dataset-id':'dataset','row-count':1,fingerprint:scheduleFingerprint(raw)}}]};
  const data={[TABLES.answers]:[],[TABLES.checkpoint]:[]},writes=[];
  const api={clockVerified:true,list:async(base,table)=>structuredClone(base===ENGINE?data[table]:base===LIVE?[]:sources[table]),
    upsert:async(table,key,fields)=>{writes.push(table);for(const f of fields){const old=data[table].find(r=>r.fields[key]===f[key]);if(old)old.fields=structuredClone(f);else data[table].push({id:'rec'+data[table].length,fields:structuredClone(f)});}return{};}};
  return {api,data,sources,writes,raw,readArchive:()=>structuredClone(raw),clock:()=>instant};
}
test('normal repeat retains snapshot identities and writes only runner status on unchanged input',async()=>{
  const f=fixture();const first=await run({...f,runId:'one'});assert(first.ok,JSON.stringify(first));assert.equal(first.counts.written,1);
  const original=structuredClone(f.data[TABLES.answers]);const repeat=await run({...f,runId:'two'});
  assert(repeat.ok);assert.equal(repeat.counts.written,0);assert.equal(repeat.counts.unchanged,1);assert.deepEqual(f.data[TABLES.answers],original);
  assert(f.writes.every(t=>[TABLES.answers,TABLES.checkpoint].includes(t)));
});
test('bad structured projection cannot borrow frozen numeric pace',async()=>{
  const f=fixture();f.sources.schedules[0].fields['total-trips']=100;
  const r=await run({...f,runId:'mismatch'});assert(r.ok);const answer=f.data[TABLES.answers][0].fields;
  assert.equal(answer.availability,'unavailable');assert.match(answer['reason-codes-json'],/schedule_consistency_unproven/);
});
test('an active projection cannot admit a frozen dropped schedule',async()=>{
  const f=fixture();f.raw[0].inactive=true;f.raw[0].dropped_at='2026-10-30';
  f.sources.schedules[0].fields['row-json']=JSON.stringify(f.raw[0]);f.sources['show-collection'][0].fields.fingerprint=scheduleFingerprint(f.raw);
  const r=await run({...f});assert(r.ok);assert.equal(f.data[TABLES.answers][0].fields.availability,'unavailable');
  assert.match(f.data[TABLES.answers][0].fields['reason-codes-json'],/schedule_consistency_unproven/);
});
test('changing producer collection defers publication without inventing snapshot',async()=>{
  const f=fixture(),list=f.api.list;let calls=0;f.api.list=async(base,table)=>{const rows=await list(base,table);if(table==='show-collection'&&++calls===2)rows[0].fields.completed='2026-10-29T09:24:01Z';return rows;};
  const r=await run({...f});assert(r.ok);assert.equal(f.data[TABLES.answers].length,0);assert.equal(f.data[TABLES.checkpoint][0].fields['last-outcome'],'deferred_source_changed');
});
test('scope failure is recorded, no automatic successful retry or baseline advancement',async()=>{
  const f=fixture();f.api.upsert=async(table)=>{if(table===TABLES.answers)throw Error('unknown response');return{};};
  const r=await run({...f});assert(!r.ok);assert.equal(f.data[TABLES.answers].length,0);assert.match(r.errors[0],/unknown response/);
});
test('budget deferral is explicit rather than partial snapshot publication',async()=>{
  const f=fixture();const r=await run({...f,deadline:Date.now()+10000});assert(r.ok);assert.equal(r.counts.deferred,1);assert.equal(f.data[TABLES.answers].length,0);
});
test('new evidence creates a snapshot even when pace is unchanged',async()=>{
  const f=fixture();await run({...f,runId:'first'});
  f.sources['show-collection'][0].fields.completed='2026-10-29T09:24:30Z';
  const r=await run({...f,runId:'second'});assert(r.ok);assert.equal(r.counts.written,1);assert.equal(f.data[TABLES.answers].length,2);
});
test('checkpoint failure cannot report successful execution',async()=>{
  const f=fixture(),upsert=f.api.upsert;f.api.upsert=async(table,key,fields)=>{if(table===TABLES.checkpoint)throw Error('checkpoint write lost');return upsert(table,key,fields);};
  const r=await run({...f});assert.equal(r.ok,false);assert.match(r.errors.join(' '),/checkpoint write lost/);
});
function liveFixture(){
  const f=fixture(),list=f.api.list;
  const group={'show-id':76,'customer-id':15,date:'2026-10-30','scope-status':'current','last-seen':'2026-10-29T09:23:00Z','class-group-id':19,total:11,gone:3,status:'Live',_classes:['76|2026-10-30|30'],'source-json':JSON.stringify({ring_number:6})};
  const collection={id:'live-collection',fields:{'show-id':76,date:'2026-10-30',status:'complete',completed:'2026-10-29T09:24:00Z','row-count':1,'archive-file':'live.json.gz',fingerprint:'hash'}};
  const live=[group];f.api.list=async(base,table)=>base===LIVE?structuredClone([collection]):list(base,table);
  f.readArchive=file=>file==='live.json.gz'?structuredClone(live):structuredClone(f.raw);
  return{...f,group,collection,live};
}
test('normal interval retains both endpoints and absolute counts; same observation is not a second interval',async()=>{
  const f=liveFixture();await run({...f,runId:'first'});
  f.group['last-seen']='2026-10-29T09:24:00Z';f.group.gone=5;
  const r=await run({...f,runId:'next',clock:()=> '2026-10-29T09:25:10Z'});assert(r.ok);
  const answer=f.data[TABLES.answers].at(-1).fields,p=JSON.parse(answer['payload-json']);
  assert.equal(answer['pace-method'],'observed_interval');assert.equal(answer['pace-minutes-per-trip'],0.5);
  assert.deepEqual(p.interval.endpoints.map(x=>x.gone),[3,5]);assert(p.interval.endpoints.every(x=>x.ref.record==='live-collection'));
  const repeat=await run({...f,runId:'repeat',clock:()=> '2026-10-29T09:25:20Z'});assert.equal(repeat.counts.written,0);
});
test('future observation and mismatched class roster cannot yield interval pace',async()=>{
  for(const mutate of [g=>g['last-seen']='2026-10-29T09:26:00Z',g=>g._classes=['76|2026-10-30|31']]){
    const f=liveFixture();await run({...f,runId:'first'});mutate(f.group);
    const r=await run({...f,runId:'next'});assert(r.ok);const p=JSON.parse(f.data[TABLES.answers].at(-1).fields['payload-json']);assert.equal(p.observed_interval_pace_minutes_per_trip,null);
  }
});
test('malformed later archive member invalidates the entire candidate collection',async()=>{
  const f=liveFixture();await run({...f,runId:'first'});f.group['last-seen']='2026-10-29T09:24:00Z';f.group.gone=5;
  f.live.push({...f.group,'class-group-id':20,_classes:null});f.collection.fields['row-count']=2;
  const r=await run({...f,runId:'next'});assert(r.ok);const p=JSON.parse(f.data[TABLES.answers].at(-1).fields['payload-json']);assert.equal(p.observed_interval_pace_minutes_per_trip,null);
});
function purgeFixture(){
  const f=fixture(),list=f.api.list;
  f.data[TABLES.checkpoint].push({id:'checkpoint',fields:{'checkpoint-key':'ring-pace-runner','last-outcome':'complete','state-json':JSON.stringify({version:1,baselines:{},cursor:'',current_show:75})}});
  Object.assign(f.sources.shows[0].fields,{'show-key':'15|76',selected_date:'2026-10-29',start_date:'2026-10-29',end_date:'2026-11-01'});
  f.sources['show-collection'].push({id:'today-scope',fields:{'show-id':76,date:'2026-10-29',status:'complete',completed:'2026-10-29T09:24:00Z','dataset-id':'empty','row-count':0,fingerprint:scheduleFingerprint([])}});
  f.readArchive=file=>file.includes('empty.json.gz')?[]:structuredClone(f.raw);
  f.api.list=async(base,table,formula)=>{const rows=await list(base,table);return base===SCHEDULES&&table==='show-collection'&&formula?.includes("{date}='2026-10-29'")?rows.filter(r=>r.fields.date==='2026-10-29'):rows;};
  f.removed=[];f.api.removeSnapshots=async ids=>{
    const pending=JSON.parse(f.data[TABLES.checkpoint][0].fields['state-json']).pending_purge;
    assert.deepEqual(pending.records.map(x=>x.id),ids);f.removed.push(...ids);
    f.data[TABLES.answers]=f.data[TABLES.answers].filter(r=>!ids.includes(r.id));return ids;
  };
  f.addOld=(show,date,id)=>{const scope=['schedules_live',show,date,6,19,'base'].join('|');f.data[TABLES.answers].push({id,fields:{'answer-key':scope+'|old|hash','scope-key':scope,'show-id':show,date,'ring-number':6,'group-id':19,'answer-kind':'ring-pace','source-lane':'schedules_live','content-hash':'old','evaluated-at':'2026-10-28T09:00:00Z','payload-json':JSON.stringify({identity:{customer:15,show,date,ring:6,group:19}})}});};
  return f;
}
test('two normal fixture invocations purge only prior show/past day and keep today/future',async()=>{
  const f=purgeFixture();f.addOld(75,'2026-11-01','old-show');f.addOld(76,'2026-10-28','past-day');f.addOld(76,'2026-10-29','today');f.addOld(76,'2026-10-31','future');
  const first=await run({...f,runId:'one'});assert(first.ok,JSON.stringify(first));assert.equal(f.removed.length,0);
  const second=await run({...f,runId:'two'});assert(second.ok,JSON.stringify(second));assert.equal(second.counts.purged,2);
  assert.deepEqual(f.removed.sort(),['old-show','past-day']);assert(f.data[TABLES.answers].some(r=>r.id==='today'));assert(f.data[TABLES.answers].some(r=>r.id==='future'));
});
test('a source change before delete fences purge and requires new confirmation',async()=>{
  const f=purgeFixture();f.addOld(75,'2026-10-28','old');await run({...f,runId:'one'});
  const list=f.api.list;let calls=0;f.api.list=async(base,table,formula)=>{const rows=await list(base,table,formula);if(base===SCHEDULES&&table==='shows'&&++calls===3)rows[0].fields.selected_date='2026-10-30';return rows;};
  const r=await run({...f,runId:'two'});assert(r.ok);assert.equal(f.removed.length,0);assert.equal(r.counts.purge_deferred,1);
  const state=JSON.parse(f.data[TABLES.checkpoint][0].fields['state-json']);assert.equal(state.purge_status,'source_changed_before_delete');assert(!state.purge_authority);
});
test('unknown deletion stops with exact retained intent, without retry or business source mutation',async()=>{
  const f=purgeFixture();f.addOld(75,'2026-10-28','old');await run({...f,runId:'one'});
  let calls=0;f.api.removeSnapshots=async()=>{calls++;throw Error('Unknown deletion response');};
  const r=await run({...f,runId:'two'});assert.equal(r.ok,false);assert.equal(calls,1);assert.equal(f.data[TABLES.checkpoint][0].fields['last-outcome'],'failed');
  const state=JSON.parse(f.data[TABLES.checkpoint][0].fields['state-json']);assert.equal(state.pending_purge.records[0].id,'old');assert(f.data[TABLES.answers].some(r=>r.id==='old'));
});
test('pending partial deletion blocks replanning and preserves original intent',async()=>{
  const f=purgeFixture();f.addOld(75,'2026-10-28','remaining');
  const state=JSON.parse(f.data[TABLES.checkpoint][0].fields['state-json']);
  state.pending_purge={run_id:'original',context:{show:76},records:[{id:'already-absent',key:'absent',hash:'absent'},{id:'remaining',key:f.data[TABLES.answers][0].fields['answer-key'],hash:'old'}]};
  f.data[TABLES.checkpoint][0].fields['state-json']=JSON.stringify(state);
  const r=await run({...f,runId:'resume'});assert.equal(r.ok,false);assert.equal(f.removed.length,0);
  assert.deepEqual(JSON.parse(f.data[TABLES.checkpoint][0].fields['state-json']).pending_purge,state.pending_purge);
});
test('a fully absent pending batch reconciles before new work without another delete',async()=>{
  const f=purgeFixture(),state=JSON.parse(f.data[TABLES.checkpoint][0].fields['state-json']);
  state.pending_purge={run_id:'original',records:[{id:'already-absent',key:'absent',hash:'absent'}]};
  f.data[TABLES.checkpoint][0].fields['state-json']=JSON.stringify(state);
  const r=await run({...f,runId:'resume'});assert(r.ok);assert.equal(f.removed.length,0);
  const saved=JSON.parse(f.data[TABLES.checkpoint][0].fields['state-json']);assert(!saved.pending_purge);assert(saved.last_purge.reconciled_at);
});
test('confirmed retired days cannot be recreated after authority becomes stale',async()=>{
  const f=purgeFixture(),pastRaw={...structuredClone(f.raw[0]),fields:{...f.raw[0].fields,show_date:'2026-10-28'}};
  f.sources.schedules.push({id:'past-source',fields:{...f.sources.schedules[0].fields,'stage-key':'76|2026-10-28|g_c',date:'2026-10-28','dataset-id':'past','row-json':JSON.stringify(pastRaw)}});
  f.sources['show-collection'].push({id:'past-scope',fields:{'show-id':76,date:'2026-10-28',status:'complete',completed:'2026-10-29T09:24:00Z','dataset-id':'past','row-count':1,fingerprint:scheduleFingerprint([pastRaw])}});
  const read=f.readArchive;f.readArchive=file=>file.includes('past.json.gz')?[pastRaw]:read(file);
  f.addOld(76,'2026-10-28','past-engine');
  assert((await run({...f,runId:'one'})).ok);assert((await run({...f,runId:'two'})).ok);
  assert(!f.data[TABLES.answers].some(r=>r.fields.date==='2026-10-28'));
  f.sources.shows[0].fields.last_checked='2026-10-28T00:00:00Z';
  assert((await run({...f,runId:'stale'})).ok);assert(!f.data[TABLES.answers].some(r=>r.fields.date==='2026-10-28'));
  const state=JSON.parse(f.data[TABLES.checkpoint][0].fields['state-json']);assert.equal(state.retirement_cutoffs['15|76'],'2026-10-29');
});
