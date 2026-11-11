'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),crypto=require('node:crypto');
const {cleanup,choose,localLogs,pruneLocal}=require('./candidate/cleanup-lane');
const tmp=()=>fs.mkdtempSync(path.join(os.tmpdir(),'simple-cleanup-'));
const row=(id,fields={},n=0)=>({id,fields,createdTime:new Date(1700000000000+n*1000).toISOString()});
function fixture(){
 const runtime=tmp(),deletes=[],summaries=[],data={
  'run-logs':[row('done',{completed:'2026-11-06',status:'Succeeded'}),row('current',{completed:'2026-11-07',status:'Succeeded'}),row('active',{status:'Started'})],
  'err-logs':[row('old-error',{run:['done']}),row('active-error',{run:['active']}),row('current-error',{run:['current']})],
  heartbeat:[row('hb',{'last-run':['current']})],runs:[row('r-done',{completed:'2026-11-06'}),row('r-active',{})],
  schedules:[row('schedule')],horses:[row('horse')],'w-horses':[row('reference')],'cleanup-logs':[row('history')]
 };
 const tables=Object.keys(data).map(name=>({id:name,name}));
 data.tblIhyIiYgvHHeChy=tables.map(t=>row('control-'+t.name,{'\uFEFFName':t.name,cleanup:'checked'}));
 const api={schema:async()=>tables,list:async t=>structuredClone(data[t]||[]),remove:async(t,ids)=>{
  deletes.push({table:t,ids});data[t]=data[t].filter(r=>!ids.includes(r.id));
  if(t==='run-logs')for(const e of data['err-logs'])e.fields.run=e.fields.run.filter(id=>!ids.includes(id));
  return {records:ids.map(id=>({id,deleted:true}))};},summary:async(id,fields)=>summaries.push({id,fields})};
 return {runtime,api,data,deletes,summaries};
}
async function main(){
 let count=0;
 const test=async(name,fn)=>{await fn();count++;console.log('PASS '+name);};
 await test('calendar day deletes completed logs, protects current/active, references and show rows',async()=>{
  const f=fixture();try{const r=await cleanup({trigger:'calendar-day',event_id:'day' },f);assert.equal(r.result,'Success');assert.deepEqual(f.deletes.flatMap(d=>d.ids),['done','old-error','r-done']);assert.equal(f.data.schedules.length,1);assert.equal(f.data['w-horses'].length,1);assert.equal(f.data['cleanup-logs'].length,1);assert.equal(f.summaries.length,4);}finally{fs.rmSync(f.runtime,{recursive:true});}
 });
 await test('new show clears owned tables but not references/history',async()=>{
  const f=fixture();try{const r=await cleanup({trigger:'new-show',event_id:'show'},f);assert.equal(r.result,'Success');assert.equal(f.data.schedules.length,0);assert.equal(f.data.horses.length,0);assert.equal(f.data['w-horses'].length,1);assert.equal(f.data['cleanup-logs'].length,1);}finally{fs.rmSync(f.runtime,{recursive:true});}
 });
 await test('new show-day does nothing without API calls',async()=>{const r=await cleanup({trigger:'new-show-day'},{api:{},runtime:'unused'});assert.equal(r.result,'Skipped');});
 await test('threshold 999 none, 1000 oldest 500 eligible; same rule applies to runs',async()=>{
  const rows=Array.from({length:1000},(_,n)=>row(String(n),{},n)).reverse();assert.equal(choose(rows.slice(1),()=>true,'log-check').length,0);const selected=choose(rows,r=>r.id!=='0','log-check');assert.equal(selected.length,500);assert.equal(selected[0].id,'1');assert.equal(selected.at(-1).id,'500');
 });
 await test('batch error records exact cause and continues tables and later batches',async()=>{
  const f=fixture();f.data['run-logs']=Array.from({length:22},(_,i)=>row('d'+i,{completed:'2026-11-06'},i));const remove=f.api.remove;let first=true;
  f.api.remove=async(t,ids)=>{if(t==='run-logs'&&first){first=false;throw Error('HTTP 422 exact source reason');}return remove(t,ids);};
  try{const r=await cleanup({trigger:'calendar-day',event_id:'error'},f);assert.equal(r.result,'Error');assert.equal(r.errors[0].error,'HTTP 422 exact source reason');assert.equal(r.errors[0].record_ids.length,10);assert.equal(r.actions.find(a=>a.table==='run-logs').deleted,12);assert.equal(f.data.runs.length,1);assert(!fs.existsSync(path.join(f.runtime,'cleanup','cleanup-state.json')));}finally{fs.rmSync(f.runtime,{recursive:true});}
 });
 await test('same successful Records event cannot delete another 500',async()=>{
  const f=fixture();try{await cleanup({trigger:'calendar-day',event_id:'same'},f);const n=f.deletes.length;const r=await cleanup({trigger:'calendar-day',event_id:'same'},f);assert(r.duplicate);assert.equal(f.deletes.length,n);assert.equal(fs.readFileSync(path.join(f.runtime,'cleanup/history.jsonl'),'utf8').trim().split('\n').length,1);}finally{fs.rmSync(f.runtime,{recursive:true});}
 });
 await test('logging read error does not stop new-show cleanup or hide its cause',async()=>{
  const f=fixture(),list=f.api.list;f.api.list=async t=>{if(t==='run-logs')throw Error('HTTP 502 original cause');return list(t);};
  try{const r=await cleanup({trigger:'new-show',event_id:'read-error'},f);assert.equal(r.result,'Error');assert.equal(f.data.schedules.length,0);assert.equal(f.data.horses.length,0);assert.equal(f.data['err-logs'].length,3);assert(r.errors.some(e=>e.error==='HTTP 502 original cause'));assert(fs.existsSync(path.join(f.runtime,'cleanup/history.jsonl')));}finally{fs.rmSync(f.runtime,{recursive:true});}
 });
 await test('successful event updates existing Records show/day/three-hour checkpoint',async()=>{
  const f=fixture();try{const event={trigger:'calendar-day',event_id:'checkpoint',show_id:77,calendar_day:'2026-11-07',issued_at:'2026-11-07T14:00:00Z'};await cleanup(event,f);const s=JSON.parse(fs.readFileSync(path.join(f.runtime,'cleanup/cleanup-state.json')));assert.equal(s.show,77);assert.equal(s.calendar_day,'2026-11-07');assert.equal(s.last_log_check,event.issued_at);assert.equal(s.event_id,event.event_id);assert(!fs.existsSync(path.join(f.runtime,'cleanup/simple-state.json')));}finally{fs.rmSync(f.runtime,{recursive:true});}
 });
 await test('local deletes delivered completed family; keeps pending and current',async()=>{
  const runtime=tmp(),root=path.join(runtime,'logging/wef-data');for(const d of ['run-logs','err-logs','acks'])fs.mkdirSync(path.join(root,d),{recursive:true});
  const hash=v=>crypto.createHash('sha256').update(JSON.stringify(v)).digest('hex');
  for(const [i,id]of ['old','pending','new'].entries()){
   const a={id,workflow:'wef-data',started:new Date(1700000000000+i*1000).toISOString(),status:'Succeeded',stage:'Collection',completed:'2026-11-06'};
   fs.writeFileSync(path.join(root,'run-logs',id+'.json'),JSON.stringify(a));
   const fields={'attempt-id':id,workflow:a.workflow,started:a.started,status:a.status,stage:a.stage,completed:a.completed};
   if(id!=='pending')fs.writeFileSync(path.join(root,'acks','run-logs-'+id+'.json'),JSON.stringify({id:'native-'+id,hash:hash(fields)}));
  }
  try{const local=localLogs(runtime);assert(local.pending.has('pending'));assert(local.pending.has('new'));const out=pruneLocal(local.families,'calendar-day');assert.equal(out.runs,1);assert(!fs.existsSync(path.join(root,'run-logs/old.json')));assert(fs.existsSync(path.join(root,'run-logs/pending.json')));assert(fs.existsSync(path.join(root,'run-logs/new.json')));}finally{fs.rmSync(runtime,{recursive:true});}
 });
 console.log(count+' checks passed; no live calls');
}
main().catch(e=>{console.error(e);process.exitCode=1;});
