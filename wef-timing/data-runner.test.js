"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {run,writeAnswers}=require('./data-runner'),{TIMING,SHOWS,LIVE,DATA_TABLES:T,api}=require('./api');
const {evaluate}=require('./pace'),purge=require('./purge');
function fixture(){
  const now='2026-10-29T14:25:00Z',show={id:'canonical',fields:{'show-key':'15|76',show_id:76,customer_id:15,is_default:true,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-29',last_checked:'2026-10-29T14:24:00Z'}};
  const identity={customer:15,show:76,date:'2026-10-29',ring:6,group:19},row={'stage-key':'15|76|2026-10-29|6|occ','class-id':30,'start-time':'08:06:00','end-time':'08:39:00','total-trips':11,type:'class'};
  const source={proof:{'run-key':'head','state-sha256':'hash'},inventories:{'2026-10-29':{observed_at:'2026-10-29T14:24:00Z',inventory_complete:true,refs:[{base:'appdl16HJ5ETm8N69',table:'collections',record:'retained',sha256:'hash'}],groups:[{identity,rows:[row],class_keys:['76|2026-10-29|30']}]}}};
  const db={[T.answers]:[],[T.checkpoint]:[]},calls=[],live=[];
  const transport={clockVerified:true,list:async(base,table,formula)=>{calls.push({base,table,method:'GET',formula});if(base===TIMING){const rows=structuredClone(db[table]);if(!formula)return rows;assert.match(formula,/^OR\(RECORD_ID\(\)=/);const ids=[...formula.matchAll(/RECORD_ID\(\)='(rec[A-Za-z0-9]{14})'/g)].map(m=>m[1]);return rows.filter(r=>ids.includes(r.id));}if(base===SHOWS)return[structuredClone(show)];if(base===LIVE)return structuredClone(live);throw Error('Unexpected source');},
    upsert:async(table,key,rows)=>{calls.push({base:TIMING,table,method:'PATCH'});for(const f of rows){const old=db[table].find(r=>r.fields[key]===f[key]);if(old)old.fields=structuredClone(f);else db[table].push({id:'rec'+String(db[table].length).padStart(14,'0'),fields:structuredClone(f)});}return{records:structuredClone(rows.map(f=>db[table].find(r=>r.fields[key]===f[key])))};},
    removeSnapshots:async(ids)=>{calls.push({base:TIMING,table:T.answers,method:'DELETE'});db[T.answers]=db[T.answers].filter(r=>!ids.includes(r.id));return ids;}};
  return{api:transport,clock:()=>now,show,source,db,calls,live,readData:async()=>structuredClone(source),readPointer:()=>({head:'head'}),readLive:async()=>({classes:new Map(),issues:[]}),prepareClassBatch:()=>({outputs:[],writes:[],unchanged:0}),identity,row};
}
test('written-record readback covers all batches without scanning unrelated history',async()=>{
 const f=fixture(),writes=Array.from({length:227},(_,i)=>({'answer-key':'new-'+i,'content-hash':'hash-'+i}));
 f.db[T.answers].push({id:'historical',fields:{'answer-key':'history'}});
 const saved=await writeAnswers(f.api,writes);assert.equal(saved.length,227);
 assert.equal(f.calls.filter(c=>c.method==='PATCH').length,23);
 const reads=f.calls.filter(c=>c.method==='GET');assert.equal(reads.length,3);assert(reads.every(c=>c.formula));
 assert(!saved.some(r=>r.id==='historical'));
});

test('independent readback rejects missing, altered, duplicate and unexpected records',async()=>{
 for(const kind of ['missing','altered','duplicate','unexpected']){
  const f=fixture(),list=f.api.list;
  f.api.list=async(...args)=>{const rows=await list(...args);if(kind==='missing')return [];if(kind==='altered')rows[0].fields['content-hash']='wrong';if(kind==='duplicate')rows.push(structuredClone(rows[0]));if(kind==='unexpected')rows.push({id:'rec99999999999999',fields:{'answer-key':'extra'}});return rows;};
  await assert.rejects(writeAnswers(f.api,[{'answer-key':'new','content-hash':'expected'}]),/readback/);
 }
});

test('filtered readback follows all offsets with the same exact-record filter',async()=>{
 const urls=[],formula="OR(RECORD_ID()='rec00000000000001')";
 const transport=api({token:'inert',lane:'data_live',deadline:Date.now()+10000,request:async(url)=>{
  urls.push(new URL(url));return{ok:true,headers:{get:()=>new Date().toUTCString()},json:async()=>urls.length===1?{records:[],offset:'next-page'}:{records:[{id:'rec00000000000001',fields:{}}]}};
 }});
 assert.equal((await transport.list(TIMING,T.answers,formula)).length,1);
 assert.equal(urls.length,2);assert(urls.every(u=>u.searchParams.get('filterByFormula')===formula));assert.equal(urls[1].searchParams.get('offset'),'next-page');
});

test('initial complete inventory rejects historical duplicate keys before answer writes',async()=>{
 const f=fixture();f.db[T.answers].push({id:'old1',fields:{'answer-key':'duplicate'}},{id:'old2',fields:{'answer-key':'duplicate'}});
 const r=await run({...f});assert.equal(r.ok,false);assert.match(r.errors[0],/Duplicate Data snapshot/);
 assert(!f.calls.some(c=>c.table===T.answers&&c.method==='PATCH'));
 assert.equal(f.calls.filter(c=>c.table===T.answers&&c.method==='GET'&&!c.formula).length,1);
});

test('failed checkpoint only resumes the specifically authorized run, never pending purge',async()=>{
 const f=fixture();f.db[T.checkpoint].push({id:'ck',fields:{'checkpoint-key':'ring-pace-data-runner','run-id':'failed-run','last-outcome':'failed','state-json':JSON.stringify({version:1,baselines:{},cursor:''})}});
 const blocked=await run({...f,runId:'blocked',resumeRunId:'other'});assert.equal(blocked.ok,false);
 // This mock failure is intentionally isolated; restore the verified saved checkpoint.
 f.db[T.checkpoint][0].fields={'checkpoint-key':'ring-pace-data-runner','run-id':'failed-run','last-outcome':'failed','state-json':JSON.stringify({version:1,baselines:{},cursor:''})};
 const allowed=await run({...f,runId:'authorized',resumeRunId:'failed-run'});assert.equal(allowed.ok,true,JSON.stringify(allowed));
 const g=fixture();g.db[T.checkpoint].push({id:'ck',fields:{'checkpoint-key':'ring-pace-data-runner','run-id':'failed-run','last-outcome':'failed','state-json':JSON.stringify({version:1,baselines:{},cursor:'',pending_purge:{records:[]}})}});
 const pending=await run({...g,runId:'pending',resumeRunId:'failed-run'});assert.equal(pending.ok,false);
});

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
test('one normal Data invocation saves pace and each eligible class in the same destination',async()=>{
 const f=fixture();delete f.prepareClassBatch;
 f.source.source_lane='data_live';f.source.context={'customer-id':15,'show-id':76,'show-key':'15|76','start-date':'2026-10-29','end-date':'2026-11-01','selected-date':'2026-10-29'};
 f.source.proof={'run-key':'head','show-key':'15|76','state-sha256':'d'.repeat(64)};
 f.source.inventories['2026-10-29'].refs=[{base:'appdl16HJ5ETm8N69',table:'collections',key:'part',record:'rec1',sha256:'a'.repeat(64)}];
 const result=await run({...f,runId:'integration'});assert(result.ok,JSON.stringify(result));
 assert.equal(result.counts.evaluated,1);assert.equal(result.counts.classes_evaluated,1);
 assert.equal(f.db[T.answers].filter(r=>r.fields['answer-kind']==='ring-pace').length,1);
 assert.equal(f.db[T.answers].filter(r=>r.fields['answer-kind']==='class-timing').length,1);
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
test('source crossing freshness limit during a run cannot authorize pace or purge',async()=>{
 const f=fixture();let current='2026-10-29T14:25:00Z',calls=0;f.clock=()=>current;
 f.readPointer=()=>{if(++calls===2)current='2026-10-29T14:55:00Z';return {head:'head'};};
 const r=await run({...f,runId:'stale'});assert(r.ok,JSON.stringify(r));assert.equal(f.db[T.answers][0].fields.availability,'unavailable');
 assert.match(f.db[T.answers][0].fields['reason-codes-json'],/stale_observation/);assert.equal(r.counts.purged,0);
});
test('expired source cannot advance retirement authority when no old rows exist',async()=>{
 const f=fixture();let current='2026-10-29T14:25:00Z';f.clock=()=>current;
 assert((await run({...f,runId:'prior'})).ok);
 const original=f.api.list;f.api.list=async(base,table,...rest)=>{const result=await original(base,table,...rest);if(base===TIMING&&table===T.answers)current='2026-10-29T14:55:00Z';return result;};
 const r=await run({...f,runId:'expired'});assert(r.ok,JSON.stringify(r));
 const state=JSON.parse(f.db[T.checkpoint][0].fields['state-json']);assert.equal(state.retirement_cutoffs,undefined);assert.equal(r.counts.purged,0);
});
test('Live class source changing defers class work without losing recorded ring counts',async()=>{
 const f=fixture();f.readLive=async()=>{const e=Error('Live changed');e.code='SOURCE_CHANGED';throw e;};
 const r=await run({...f,runId:'changed-live'});assert(r.ok);assert.equal(r.counts.written,1);
 assert.equal(f.db[T.checkpoint][0].fields['last-outcome'],'input_deferred');
});
test('a class-only change records a complete checkpoint',async()=>{
 const f=fixture();assert((await run({...f,runId:'first'})).ok);
 f.prepareClassBatch=()=>({outputs:[{fields:{availability:'partial'}}],unchanged:0,writes:[{'answer-key':'class-answer','scope-key':'class-scope','answer-kind':'class-timing'}]});
 const r=await run({...f,runId:'second'});assert(r.ok,JSON.stringify(r));assert.equal(r.counts.written,0);assert.equal(r.counts.classes_written,1);
 assert.equal(f.db[T.checkpoint][0].fields['last-outcome'],'complete');
});
test('changed pinned head withholds output and never picks another show',async()=>{
  const f=fixture();let calls=0;f.readPointer=()=>({head:++calls===1?'head':'different'});
  const r=await run({...f});assert(r.ok);assert.equal(f.db[T.answers].length,0);const state=JSON.parse(f.db[T.checkpoint][0].fields['state-json']);assert.equal(state.input_status,'data_head_changed');assert.equal(state.fence_diagnostic.data_head_after,'different');assert.equal(r.fence_diagnostic.reason,'data_head_changed');assert.notEqual(r.fence_diagnostic.pointer_hash_before,r.fence_diagnostic.pointer_hash_after);
});
test('fresh Shows heartbeat refresh does not interrupt unchanged authority',async()=>{
 const f=fixture(),list=f.api.list;let reads=0;f.api.list=async(base,table,...args)=>{if(base===SHOWS&&++reads===2)f.show.fields.last_checked='2026-10-29T14:24:30Z';return list(base,table,...args);};
 const r=await run({...f,runId:'shows-refreshed'});assert(r.ok,JSON.stringify(r));assert.equal(r.counts.evaluated,1);assert.equal(r.counts.written,1);
});
test('changed Shows selected date defers with explicit reason',async()=>{
 const f=fixture(),list=f.api.list;let reads=0;f.api.list=async(base,table,...args)=>{if(base===SHOWS&&++reads===2)f.show.fields.selected_date='2026-10-30';return list(base,table,...args);};
 const r=await run({...f,runId:'date-changed'});assert(r.ok);assert.equal(f.db[T.answers].length,0);
 const s=JSON.parse(f.db[T.checkpoint][0].fields['state-json']);assert.equal(s.input_status,'show_authority_changed');assert.equal(s.fence_diagnostic.show_after.context['selected-date'],'2026-10-30');
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

test('archive prerequisite defers cleanup on repeat runs and protects every show date',async()=>{
 const f=fixture(),args={identity:f.identity,rows:[f.row],observedAt:f.clock(),inputRefs:[],now:f.clock(),sourceLane:'data_live'};
 for(const [id,show,date,lane]of [['past',76,'2026-10-28','data_live'],['current',76,'2026-10-29','data_live'],['future',76,'2026-10-30','data_live'],['retired-current',75,'2026-10-29','data_live'],['retired-future',75,'2026-10-30','data_live'],['other',76,'2026-10-28','schedules_live']]){
  f.db[T.answers].push({id,fields:evaluate({...args,identity:{...f.identity,show,date},sourceLane:lane}).fields});
 }
 assert((await run({...f,runId:'confirm'})).ok);
 const state=JSON.parse(f.db[T.checkpoint][0].fields['state-json']);state.retired_shows=[75];f.db[T.checkpoint][0].fields['state-json']=JSON.stringify(state);
 f.prepareClassBatch=()=>({outputs:[{fields:{availability:'unavailable'}}],writes:[],unchanged:1});
 for(const runId of ['second','third']){
  const r=await run({...f,runId});assert(r.ok,JSON.stringify(r));assert.equal(r.counts.evaluated,1);assert.equal(r.counts.classes_evaluated,1);
  assert.equal(r.counts.purged,0);assert.equal(r.counts.purge_deferred,1);assert.equal(r.purge_status,'archive_required');
  assert.equal(f.db[T.checkpoint][0].fields['last-outcome'],'purging_deferred');
 }
 for(const id of ['past','current','future','retired-current','retired-future','other'])assert(f.db[T.answers].some(r=>r.id===id));
 assert(!f.calls.some(c=>c.method==='DELETE'));
});

test('failed class readback prevents rollover deletion',async()=>{
 const f=fixture(),old={...f.identity,date:'2026-10-28'};
 f.db[T.answers].push({id:'past',fields:evaluate({identity:old,rows:[f.row],observedAt:f.clock(),inputRefs:[],now:f.clock(),sourceLane:'data_live'}).fields});
 assert((await run({...f,runId:'confirm'})).ok);
 f.source.inventories['2026-10-29'].groups[0].rows[0]['total-trips']=12;
 f.prepareClassBatch=()=>({outputs:[{fields:{availability:'partial'}}],unchanged:0,writes:[{'answer-key':'class-new','scope-key':'class','answer-kind':'class-timing'}]});
 const list=f.api.list;let answerReads=0;f.api.list=async(base,table,...args)=>{const rows=await list(base,table,...args);if(base===TIMING&&table===T.answers&&++answerReads===3)return rows.filter(r=>r.fields['answer-key']!=='class-new');return rows;};
 const r=await run({...f,runId:'readback-failure'});assert.equal(r.ok,false);assert.match(r.errors[0],/readback missing/);
 assert(f.db[T.answers].some(r=>r.id==='past'));assert(!f.calls.some(c=>c.method==='DELETE'));
});
test('unresolved deletion intent blocks additional deletions',async()=>{
 const f=fixture();const id='old',fields=evaluate({identity:{...f.identity,date:'2026-10-28'},rows:[f.row],observedAt:f.clock(),inputRefs:[],now:f.clock(),sourceLane:'data_live'}).fields;
 f.db[T.answers].push({id,fields});f.db[T.checkpoint].push({id:'ck',fields:{'checkpoint-key':'ring-pace-data-runner','run-id':'before','last-outcome':'purging','state-json':JSON.stringify({version:1,baselines:{},cursor:'',pending_purge:{records:[{id,key:fields['answer-key'],hash:fields['content-hash']}]}})}});
 const r=await run({...f,runId:'reconcile'});assert.equal(r.ok,false);assert.match(r.errors[0],/Pending Data purge requires owner resolution/);
 assert(!f.calls.some(c=>c.method==='DELETE'));assert(f.db[T.answers].some(x=>x.id==='old'));
});

test('unrelated completed Data head advancement does not duplicate consumed evidence',async()=>{
 const f=fixture();assert((await run({...f,runId:'initial'})).ok);
 f.source.proof={'run-key':'new-head','state-sha256':'new-head-hash'};f.readPointer=()=>({head:'new-head'});
 const next=await run({...f,runId:'head-only'});assert(next.ok);assert.equal(next.counts.written,0);assert.equal(next.counts.unchanged,1);
 assert.equal(JSON.parse(f.db[T.checkpoint][0].fields['state-json']).source_head['run-key'],'new-head');
});
