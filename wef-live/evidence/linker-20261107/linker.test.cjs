'use strict';
globalThis.fetch=async()=>{throw Error('Offline: network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {plan,link,TABLES}=require('./candidate/wef-live/linker');
const scope={customer:15,show:200000077,date:'2026-11-07'},prefix='15|200000077|2026-11-07|5';
function fixture(){
 const common={'show-id':scope.show,'customer-id':15,date:scope.date};
 return{
 'wef-shows.show':[{id:'show',fields:{'show-key':'15|200000077'}}],
 'wef-shows.show-days':[{id:'day',fields:{'show-day-key':'15|200000077|2026-11-07'}}],
 'wef-data.schedules':[{id:'schedule',fields:{'schedule-key':prefix+'|200043091',ring:prefix,group:prefix+'|200028531',class:prefix+'|200031195','record-status':'Active'}}],
 'live-rings':[{id:'ring',fields:{...common,'ring-key':'200000077|2026-11-07|49','ring-id':49,'ring-number':5,'current-group':['group']}}],
 'live-groups':[{id:'group',fields:{...common,'group-key':'200000077|2026-11-07|200028531','class-group-id':200028531,'source-json':JSON.stringify({ring_id:'49',ring_number:'5',classes:['200031195']})}}],
 'live-classes':[{id:'class',fields:{...common,'class-key':'200000077|2026-11-07|200031195','class-id':200031195}}],
 'live-entries':[{id:'entry',fields:{...common,'live-entry-key':'200000077|2026-11-07|200031195|200400517','trip-id':200400517,'entry-id':200249999,'source-json':JSON.stringify({live:{class_id:'200031195'}})}}],
 'live-trips':[{id:'trip',fields:{...common,'trip-key':'15|200000077|2026-11-07|49|200031195|200249999|uuid','trip-id':200400517,'class-id':200031195,'ring-id':49,'entry-id':200249999,'record-status':'Dropped',schedules:['oldSchedule']}}],
 };}
function setup(t,data=fixture()){
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-linker-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));let seq=0;const calls=[];
 const api={list:async table=>{calls.push({table,method:'LIST'});assert(!table.includes('wef-schedules'));return structuredClone(data[table]||[]);},call:async(base,table,method,body)=>{
  calls.push({table,method,body:structuredClone(body)});
  if(method==='GET')return{records:structuredClone(data[table]||[])};
  if(method==='PATCH'){for(const p of body.records){const r=data[table].find(x=>x.id===p.id);Object.assign(r.fields,structuredClone(p.fields));}return{records:structuredClone(body.records)};}
  assert.equal(method,'POST');assert(['run-logs','errs-log','heartbeat'].includes(table));const records=body.records.map(x=>({id:'log'+(++seq),fields:x.fields}));(data[table]||=[]).push(...records);return{records:structuredClone(records)};
 }};
 return{data,calls,options:{scope,runtime,api,clock:()=> '2026-11-07T14:00:00Z'}};
}
test('exact keys link all five tables to Shows/day/Data; ring source ID is not ring number',()=>{
 const d=fixture(),issues=[],p=plan(d,scope,(...x)=>issues.push(x));assert.equal(issues.length,0);assert.equal(p.length,5);
 for(const row of p){assert.deepEqual(row.fields.show,['show']);assert.deepEqual(row.fields['show-day'],['day']);assert.deepEqual(row.fields['data-schedules'],['schedule']);}
 assert.deepEqual(p.find(x=>x.table==='live-classes').fields.groups,['group']);assert.deepEqual(p.find(x=>x.table==='live-trips').fields['legacy-live-entry'],['entry']);
 assert(!p.some(x=>'schedules' in x.fields||'classes' in x.fields||'live-classes' in x.fields));
});
test('populated fields remain untouched, even if wrong; no auditing or relinking',()=>{
 const d=fixture();for(const table of TABLES)for(const r of d[table])for(const field of ['show','show-day','data-schedules','ring','class','groups','legacy-live-entry'])r.fields[field]=['leaveAlone'];
 const issues=[];assert.deepEqual(plan(d,scope,x=>issues.push(x)),[]);assert.deepEqual(issues,[]);
});
test('no names/fuzzy matching or guessing; ambiguous Shows remains empty, other relationships continue',()=>{
 const d=fixture();d['wef-shows.show'].push({id:'duplicate',fields:{'show-key':'15|200000077'}});delete d['wef-data.schedules'][0].fields.class;
 const issues=[],p=plan(d,scope,x=>issues.push(x));assert(p.every(x=>!('show' in x.fields)));assert(p.every(x=>x.fields['show-day'][0]==='day'));assert(!p.find(x=>x.table==='live-classes').fields['data-schedules']);assert(issues.length);
});
test('multiple exact Data occurrences link together; dropped and duplicate keys do not guess',()=>{
 const d=fixture(),base=d['wef-data.schedules'][0];d['wef-data.schedules'].push({id:'second',fields:{...base.fields,'schedule-key':prefix+'|differentOccurrence'}});
 let p=plan(d,scope);assert.deepEqual(p[0].fields['data-schedules'],['schedule','second']);d['wef-data.schedules'][1].fields['record-status']='Dropped';assert.deepEqual(plan(d,scope)[0].fields['data-schedules'],['schedule']);
 d['wef-data.schedules'].push({id:'duplicate',fields:{...base.fields}});assert(!plan(d,scope)[0].fields['data-schedules']);
});
test('only canonical day0 rows are filled; absent/dropped facts are never edited',()=>{
 const d=fixture();d['live-trips'].push({id:'historical',fields:{...d['live-trips'][0].fields,date:'2026-11-06'}});const p=plan(d,scope);assert(!p.some(x=>x.id==='historical'));assert(p.every(x=>!('record-status' in x.fields)));assert.equal(d['live-trips'][0].fields['record-status'],'Dropped');
});
test('normal linker fills empty fields, logs diffs, preserves old fields and makes zero repeat patches',async t=>{
 const a=setup(t),before=structuredClone(a.data['live-trips'][0]);let r=await link(a.options);assert.equal(r.ok,true);assert.equal(r.issues.length,0);assert.equal(r.changes.length,5);assert.equal(r.logging_errors.length,0);
 for(const [key,v]of Object.entries(before.fields))assert.deepEqual(a.data['live-trips'][0].fields[key],v);assert.equal(a.data['live-trips'][0].id,before.id);
 assert(a.data['run-logs'].some(x=>x.fields['run-id'].includes('|diff|')));a.calls.length=0;r=await link(a.options);assert.equal(r.changes.length,0);assert(!a.calls.some(x=>x.method==='PATCH'));assert.equal(a.data.heartbeat.length,2);
});
test('field filled after planning is skipped immediately before PATCH; never overwrite populated links',async t=>{
 const a=setup(t);for(const table of TABLES)for(const r of a.data[table])for(const f of ['show','show-day','data-schedules','ring','class','groups','legacy-live-entry'])r.fields[f]=['kept'];delete a.data['live-rings'][0].fields.show;
 const call=a.options.api.call;a.options.api.call=async(b,t,m,...args)=>{if(t==='live-rings'&&m==='GET')a.data['live-rings'][0].fields.show=['filledElsewhere'];return call(b,t,m,...args);};
 const r=await link(a.options);assert.equal(r.changes.length,0);assert.deepEqual(a.data['live-rings'][0].fields.show,['filledElsewhere']);assert(!a.calls.some(x=>x.method==='PATCH'));
});
test('uncertain write is not repeated; failure is logged for rollback decision',async t=>{
 const a=setup(t),call=a.options.api.call;let patches=0;a.options.api.call=async(b,t,m,...args)=>{if(m==='PATCH'){patches++;throw Error('Response lost');}return call(b,t,m,...args);};
 const r=await link(a.options);assert.equal(r.ok,false);assert.equal(patches,1);assert(r.stage_errors.some(x=>x.message==='Response lost'));assert(a.data['errs-log'].some(x=>x.fields.message.includes('Response lost')));
});
test('one missing reference does not stop other links and one error is recorded per row/field',async t=>{
 const a=setup(t);a.data['wef-shows.show-days']=[];const r=await link(a.options);assert.equal(r.ok,true);assert.equal(r.issues.length,5);assert.equal(a.data['errs-log'].length,5);assert(a.data['live-rings'][0].fields.show);assert(!a.data['live-rings'][0].fields['show-day']);
});
test('adapter reads only prepared local references; old Schedules target is not used and refs stay read-only',async()=>{
 const {airtable}=require('./candidate/wef-live/airtable');const requests=[];const api=airtable({token:'offline',pause:async()=>{},request:async(url)=>{requests.push(url);return{ok:true,json:async()=>({records:[]})};}});
 for(const name of ['wef-shows.show','wef-shows.show-days','wef-data.schedules'])await api.list(name);
 await assert.rejects(api.list('wef-schedules.schedules'),/Unapproved read target/);await assert.rejects(api.call('appgzosUcVqA4vZdf','wef-data.schedules','PATCH',{records:[]}),/Read-only source/);assert.equal(requests.length,3);
});
test('scheduled hook runs after all collection lanes; quiet/status-false checks do not run linker',async t=>{
 const a=setup(t),{scheduled}=require('./candidate/wef-live/lanes');let linked=0;
 a.options.api.list=async name=>name==='shows'?[{id:'canonicalShow',fields:{customer_id:15,show_id:scope.show,is_default:true}}]:name==='show-days'?[{fields:{'show-day-key':'15|200000077|2026-11-07','day-priority':0,date:scope.date}}]:[];
 const opts={...a.options,getStatus:async()=>true,get:async()=>[],archive:async()=>({file:'offline'}),linker:async()=>{linked++;for(const n of ['groups','rings','classes']){const r=JSON.parse(fs.readFileSync(path.join(opts.runtime,'lane-'+n+'-last-run.json')));assert(r.completed);}return{run_id:'linker-id',ok:true};},cleanup:async({protectedRuns})=>{assert.equal(protectedRuns.length,4);return{result:'Skipped'};}};
 const r=await scheduled(opts);assert.equal(linked,1);assert.equal(r.length,4);
 const b=setup(t);b.options.api.list=a.options.api.list;await scheduled({...b.options,getStatus:async()=>false,linker:async()=>{throw Error('Must not run');},cleanup:async()=>({result:'Skipped'})});
});
