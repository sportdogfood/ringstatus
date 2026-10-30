"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {run}=require('./data-runner'),{TIMING,SHOWS,LIVE,DATA_TABLES:T,api}=require('./api');
const {evaluate}=require('./pace'),purge=require('./purge');
function fixture(){
  const now='2026-10-29T14:25:00Z',show={id:'canonical',fields:{'show-key':'15|76',show_id:76,customer_id:15,is_default:true,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-29',last_checked:'2026-10-29T14:24:00Z'}};
  const identity={customer:15,show:76,date:'2026-10-29',ring:6,group:19},row={'stage-key':'15|76|2026-10-29|6|occ','class-id':30,'start-time':'08:06:00','end-time':'08:39:00','total-trips':11,type:'class'};
  const source={proof:{'run-key':'head','state-sha256':'hash'},inventories:{'2026-10-29':{observed_at:'2026-10-29T14:24:00Z',inventory_complete:true,refs:[{base:'appdl16HJ5ETm8N69',table:'collections',record:'retained',sha256:'hash'}],groups:[{identity,rows:[row],class_keys:['76|2026-10-29|30']}]}}};
  const db={[T.answers]:[],[T.checkpoint]:[]},calls=[],live=[];
  const transport={clockVerified:true,list:async(base,table)=>{calls.push({base,table,method:'GET'});if(base===TIMING)return structuredClone(db[table]);if(base===SHOWS)return[structuredClone(show)];if(base===LIVE)return structuredClone(live);throw Error('Unexpected source');},
    upsert:async(table,key,rows)=>{calls.push({base:TIMING,table,method:'PATCH'});for(const f of rows){const old=db[table].find(r=>r.fields[key]===f[key]);if(old)old.fields=structuredClone(f);else db[table].push({id:'rec'+db[table].length,fields:structuredClone(f)});}},
    removeSnapshots:async(ids)=>{calls.push({base:TIMING,table:T.answers,method:'DELETE'});db[T.answers]=db[T.answers].filter(r=>!ids.includes(r.id));return ids;}};
  return{api:transport,clock:()=>now,show,source,db,calls,live,readData:async()=>structuredClone(source),readPointer:()=>({head:'head'}),identity,row};
}

test('every discovered group is evaluated on consecutive runs',async()=>{
  const f=fixture(), inventory=f.source.inventories['2026-10-29'];
  inventory.groups=Array.from({length:227},(_,n)=>({identity:{...f.identity,group:n+1},rows:[{...f.row,'stage-key':'group-'+n}],class_keys:['76|2026-10-29|30']}));
  for(const id of ['first','second']){const r=await run({...f,runId:id});assert(r.ok,JSON.stringify(r));assert.equal(r.counts.evaluated,inventory.groups.length);assert.equal(r.counts.deferred,0);}
  assert.equal(new Set(f.db[T.answers].map(r=>r.fields['scope-key'])).size,inventory.groups.length);
});
test('Data and Schedules use identical arithmetic but isolated output identities',()=>{
  const f=fixture(),args={identity:f.identity,rows:[f.row],observedAt:f.clock(),inputRefs:[],now:f.clock()};
  const a=evaluate(args).fields,b=evaluate({...args,sourceLane:'data_live'}).fields;
  assert.equal(a['pace-minutes-per-trip'],3);assert.equal(b['pace-minutes-per-trip'],3);assert.notEqual(a['scope-key'],b['scope-key']);
});
test('Data inventory covers missing Live classes and repeats without duplicate snapshots',async()=>{
  const f=fixture(),a=await run({...f,runId:'one'});assert(a.ok,JSON.stringify(a));assert.equal(a.counts.written,1);assert.equal(f.db[T.answers][0].fields['pace-method'],'schedule_window');
  const b=await run({...f,runId:'two'});assert(b.ok);assert.equal(b.counts.written,0);assert.equal(b.counts.unchanged,1);
  assert(f.calls.every(c=>c.method==='GET'||c.base===TIMING));assert(!f.calls.some(c=>c.base==='appam4opFT4NeL4mn'));
});
test('Data original source time remains stale despite a new completed head',async()=>{
  const f=fixture();f.source.inventories['2026-10-29'].observed_at='2026-10-29T12:00:00Z';
  const r=await run({...f});assert(r.ok);assert.equal(f.db[T.answers][0].fields.availability,'unavailable');assert.match(f.db[T.answers][0].fields['reason-codes-json'],/stale_observation/);
});
test('changed pinned head withholds output and never picks another show',async()=>{
  const f=fixture();let calls=0;f.readPointer=()=>({head:++calls===1?'head':'different'});
  const r=await run({...f});assert(r.ok);assert.equal(f.db[T.answers].length,0);assert.equal(JSON.parse(f.db[T.checkpoint][0].fields['state-json']).input_status,'source_changed');
});
test('new-show handoff absence is deferred rather than a collector invocation or old-head fallback',async()=>{
  const f=fixture();f.readPointer=()=>{const e=Error('Waiting for current Data authority');e.deferred=true;throw e;};
  const r=await run({...f});assert(r.ok);assert.equal(f.db[T.answers].length,0);assert.equal(f.db[T.checkpoint][0].fields['last-outcome'],'input_unavailable');
});
test('Data purge cannot remove the original Schedules lane',()=>{
  const f=fixture(),base={identity:f.identity,rows:[f.row],observedAt:f.clock(),inputRefs:[],now:f.clock()};
  const oldIdentity={...f.identity,date:'2026-10-28'};
  const original=evaluate({...base,identity:oldIdentity}).fields,data=evaluate({...base,identity:oldIdentity,sourceLane:'data_live'}).fields;
  const context={customer:15,show:76,day:'2026-10-29',source_lane:'data_live',retired_shows:[]};
  assert(!purge.eligible({fields:original},context));assert(purge.eligible({fields:data},context));
});
test('Data transport cannot write Engine, Data, Live or Shows',async()=>{
  let calls=0;const a=api({token:'test',deadline:Date.now()+10000,lane:'data_live',request:async()=>{calls++;throw Error('Network should not run');}});
  await assert.rejects(a.upsert('tblHnV5iEJ13DOsBF','key',[{key:'x'}]),/Forbidden/);
  await assert.rejects(a.list('appdl16HJ5ETm8N69','collections'),/Forbidden/);assert.equal(calls,0);
});

test('Data Live exact roster creates interval pace; a different roster falls back',async()=>{
  const f=fixture();let at='2026-10-29T14:24:00Z',gone=1;
  f.live.push({id:'live',fields:{status:'complete',date:'2026-10-29',completed:'2026-10-29T14:25:00Z','row-count':1,'archive-file':'inert',fingerprint:'fixture'}});
  f.readArchive=()=>[{'show-id':76,'customer-id':15,date:'2026-10-29','scope-status':'current','class-group-id':19,_classes:['76|2026-10-29|30'],'last-seen':at,total:11,gone,status:'active','source-json':JSON.stringify({ring_number:6})}];
  assert((await run({...f,runId:'baseline'})).ok);
  at='2026-10-29T14:25:00Z';gone=3;
  assert((await run({...f,runId:'interval'})).ok);
  assert.equal(f.db[T.answers].at(-1).fields['pace-method'],'observed_interval');
  assert.equal(f.db[T.answers].at(-1).fields['pace-minutes-per-trip'],0.5);
  f.source.inventories['2026-10-29'].groups[0].class_keys=['76|2026-10-29|31'];
  assert((await run({...f,runId:'roster-change'})).ok);
  assert.equal(f.db[T.answers].at(-1).fields['pace-method'],'schedule_window');
});

test('Data purge confirms across runs, preserves current/future and unrelated lanes',async()=>{
  const f=fixture(),args={identity:f.identity,rows:[f.row],observedAt:f.clock(),inputRefs:[],now:f.clock(),sourceLane:'data_live'};
  for(const [id,date,lane]of [['past','2026-10-28','data_live'],['future','2026-10-30','data_live'],['other','2026-10-28','schedules_live']])f.db[T.answers].push({id,fields:evaluate({...args,identity:{...f.identity,date},sourceLane:lane}).fields});
  assert((await run({...f,runId:'first'})).ok);assert(f.db[T.answers].some(r=>r.id==='past'));
  const second=await run({...f,runId:'second'});assert(second.ok);assert.equal(second.counts.purged,1);
  assert(!f.db[T.answers].some(r=>r.id==='past'));assert(f.db[T.answers].some(r=>r.id==='future'));assert(f.db[T.answers].some(r=>r.id==='other'));
});

test('unrelated completed Data head advancement does not duplicate consumed evidence',async()=>{
 const f=fixture();assert((await run({...f,runId:'initial'})).ok);
 f.source.proof={'run-key':'new-head','state-sha256':'new-head-hash'};f.readPointer=()=>({head:'new-head'});
 const next=await run({...f,runId:'head-only'});assert(next.ok);assert.equal(next.counts.written,0);assert.equal(next.counts.unchanged,1);
 assert.equal(JSON.parse(f.db[T.checkpoint][0].fields['state-json']).source_head['run-key'],'new-head');
});
