'use strict';
// Separate Class cleanup lane. No timer, Records alerts or collector invocation.
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib');
const {isDeepStrictEqual}=require('node:util'),{hash}=require('./pace');
const {BASE,T,airtable}=require('./class-snapshot');
const LOGS={'run-logs':T.runs,heartbeat:T.heartbeat,'errs-log':T.errors};
const OWNED={...LOGS,'timing-answers':T.answers,'engine-checkpoints':T.checkpoints};
const terminal=new Set(['Success','Skipped','Partial','Failed','Error']);
const {decode}=require('./checkpoint-state');
const read=(file,fallback)=>fs.existsSync(file)?JSON.parse(fs.readFileSync(file,'utf8')):fallback;
// Windows scanners can hold a rename handle. A durable in-place write avoids rename retries;
// interrupted/invalid state cannot be parsed on the next run and therefore cannot authorize deletion.
const save=(file,data)=>{const body=JSON.stringify(data),fd=fs.openSync(file,'w');try{fs.writeFileSync(fd,body);fs.fsyncSync(fd);}finally{fs.closeSync(fd);}if(fs.readFileSync(file,'utf8')!==body)throw Error('Cleanup state readback mismatch');};
function due(state,show,now){const p=Object.fromEntries(new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',hourCycle:'h23'}).formatToParts(new Date(now)).map(p=>[p.type,p.value])),day=`${p.year}-${p.month}-${p.day}`;
 if(Number.isSafeInteger(state.show)&&state.show!==show)return {kind:'new-show',oldShow:state.show,show,day};
 if(Number(p.hour)>=19&&state.dailyDay!==day)return {kind:'daily',show,day};
 if(!Number.isFinite(Date.parse(state.quickAt))||Date.parse(now)-Date.parse(state.quickAt)>=10800000)return {kind:'quick',show,day};return null;}
function plan({name,rows,event,now,protectedIds=[],protectedRuns=[],resolvedErrors=[],retiredCheckpoints=[],referencedAnswers=[]}){
 if(!OWNED[name]||!event||(event.kind!=='new-show'&&!LOGS[name])||(event.kind==='quick'&&rows.length<1000))return [];
 const selected=rows.filter(r=>{const f=r.fields||{},created=Date.parse(r.createdTime);if(!Number.isFinite(created)||created>Date.parse(now)||protectedIds.includes(r.id)||protectedRuns.includes(f['run-id']))return false;
  if(name==='engine-checkpoints'){const proof=retiredCheckpoints.find(p=>p.id===r.id);return event.kind==='new-show'&&proof?.show===event.oldShow&&/^\d{4}-\d{2}-\d{2}$/.test(proof.date||'')&&proof.date<event.day&&Date.parse(now)-created>=86400000;}
  if(event.kind==='new-show'&&f['show-id']!==event.oldShow)return false;
  if(name==='timing-answers'){
   const identity=(()=>{try{return JSON.parse(f['payload-json']).identity;}catch{return null;}})();
   return event.kind==='new-show'&&identity?.customer===15&&identity.show===event.oldShow&&identity.date===f.date&&/^\d{4}-\d{2}-\d{2}$/.test(f.date||'')&&f.date<event.day&&Date.parse(now)-Math.max(created,Date.parse(f['evaluated-at']))>=86400000&&typeof f['answer-key']==='string'&&!referencedAnswers.some(ref=>ref===f['answer-key']||ref===r.id||ref.endsWith('/'+r.id));
  }
  if(name==='errs-log')return resolvedErrors.includes(r.id)&&Number.isFinite(Date.parse(f['occurred-at']))&&Date.parse(f['occurred-at'])<=Date.parse(now);
  return terminal.has(f.result)&&Number.isFinite(Date.parse(f.completed))&&Date.parse(f.completed)<=Date.parse(now);
 }).sort((a,b)=>Date.parse(a.createdTime)-Date.parse(b.createdTime)||a.id.localeCompare(b.id));return event.kind==='quick'?selected.slice(0,500):selected;
}
async function archive(api,name,records,runtime,now){
 const body={base:BASE,table:OWNED[name],records},digest=hash(body),file=path.join(runtime,digest+'.json.gz');
 if(!fs.existsSync(file))fs.writeFileSync(file,zlib.gzipSync(JSON.stringify(body)),{flag:'wx'});
 if(hash(JSON.parse(zlib.gunzipSync(fs.readFileSync(file))))!==digest)throw Error('Archive file/hash readback mismatch');
 const key=`${BASE}|${OWNED[name]}|${digest}`,fields={'archive-key':key,'source-table':OWNED[name],'archive-file':file,'content-hash':digest,'record-count':records.length,'scope-json':JSON.stringify({base:BASE,ids:records.map(r=>r.id)}),'verified-at':now,'cleanup-status':'verified','policy-version':'class-cleanup'};
 const existing=await api.list(T.catalog,`{archive-key}=${JSON.stringify(key)}`);if(existing.length&&Number.isFinite(Date.parse(existing[0].fields['verified-at'])))fields['verified-at']=existing[0].fields['verified-at'];if(!existing.length)await api.create(T.catalog,fields);
 const saved=await api.list(T.catalog,`{archive-key}=${JSON.stringify(key)}`);if(saved.length!==1||Object.entries(fields).some(([k,v])=>JSON.stringify(saved[0].fields[k])!==JSON.stringify(v)))throw Error('Archive catalog readback mismatch');
 return file;
}
async function run({deadline=Date.now()+160000,api=airtable({deadline}),now=new Date().toISOString(),runtime=path.join(__dirname,'runtime/class-cleanup')}={}){
 if(deadline-Date.now()<30000)return {ok:true,deleted:0,reason:'Cleanup deferred; calculation used the available runner budget'};
 fs.mkdirSync(runtime,{recursive:true});const file=path.join(runtime,'state.json'),state=read(file,{});if(state.pending||state.runningEvent)return {ok:false,reason:'Unfinished cleanup event requires reconciliation',deleted:0};
 const defaults=(await api.list(T.shows)).filter(s=>s.fields.is_default===true);if(defaults.length!==1)return {ok:false,reason:'Default show unavailable',deleted:0};
 const show=Number(defaults[0].fields.show_id),customer=Number(defaults[0].fields.customer_id);if(!Number.isSafeInteger(show)||show<=0||!Number.isSafeInteger(customer)||customer<=0)return {ok:false,deleted:0,reason:'Default show/customer identity is invalid'};
 const event=due(state,show,now);if(!event)return {ok:true,deleted:0,reason:'Not due'};
 const controls=await api.list(T.cleanup),rows={};for(const [name,table]of Object.entries(OWNED))rows[name]=await api.list(table);
 const checkpoints=rows['engine-checkpoints'],states=checkpoints.map(c=>decode(c.fields['state-json']||'{}'));if(states.some(s=>s.pending_write||s.pending_purge||s.class_retention?.pending_purge))return {ok:false,deleted:0,reason:'Pending control work is protected'};
 const protectedRuns=[...rows['run-logs'],...rows.heartbeat].filter(r=>!terminal.has(r.fields.result)||!r.fields.completed).map(r=>r.fields['run-id']);
 const heartbeatRuns=rows.heartbeat.map(r=>r.fields['run-id']).filter(Boolean);protectedRuns.push(...heartbeatRuns);
 const protectedIds=checkpoints.filter(r=>!(state.retiredCheckpoints||[]).some(proof=>proof.id===r.id)).map(r=>r.id),snapshotRefs=await api.list(T.snapshots),referencedAnswers=[];
 const collect=x=>{if(typeof x==='string'){referencedAnswers.push(x);try{collect(JSON.parse(x));}catch{}}else if(Array.isArray(x))x.forEach(collect);else if(x&&typeof x==='object')Object.values(x).forEach(collect);};collect(states);collect(snapshotRefs);
 for(const answer of rows['timing-answers']){collect(answer.fields['input-refs-json']);collect(answer.fields['payload-json']);}
 // Errors keep their parent runs until explicitly resolved; retained children are not orphaned.
 for(const error of rows['errs-log'])if(!(state.resolvedErrors||[]).includes(error.id)&&error.fields['run-id'])protectedRuns.push(error.fields['run-id']);
 let deleted=0;const actions=[];state.runningEvent=event;state.eventDeleted={};save(file,state);
 for(const control of controls){const name=control.fields['\uFEFFName'];if(control.fields.cleanup!=='checked'||!OWNED[name])continue;
  const selected=plan({name,rows:rows[name],event,now,protectedIds,protectedRuns,resolvedErrors:state.resolvedErrors||[],retiredCheckpoints:state.retiredCheckpoints||[],referencedAnswers});let removed=0;
  for(let i=0;i<selected.length;i+=10){const batch=selected.slice(i,i+10),filter='OR('+batch.map(r=>`RECORD_ID()=${JSON.stringify(r.id)}`).join(',')+')';
   const fresh=await api.list(OWNED[name],filter);if(hash(fresh.sort((a,b)=>a.id.localeCompare(b.id)))!==hash([...batch].sort((a,b)=>a.id.localeCompare(b.id))))throw Error('Selected cleanup records changed');
   const archiveFile=name==='timing-answers'||name==='engine-checkpoints'?await archive(api,name,batch,runtime,now):null;
   const final=await api.list(OWNED[name],filter);if(hash(final.sort((a,b)=>a.id.localeCompare(b.id)))!==hash([...batch].sort((a,b)=>a.id.localeCompare(b.id))))throw Error('Cleanup records changed during archive verification');
   state.pending={name,ids:batch.map(r=>r.id),records:batch,archiveFile,event};save(file,state);
   await api.remove(OWNED[name],batch.map(r=>r.id));if((await api.list(OWNED[name],filter)).length)throw Error('Deletion absence unverified');
   (state.eventDeleted[name]||=[]).push(...batch.map(r=>r.id));delete state.pending;save(file,state);removed+=batch.length;deleted+=batch.length;
  }
  if(removed){const fields={records:rows[name].length,started:now,completed:new Date().toISOString(),result:'Success','records-deleted':removed};await api.update(T.cleanup,control.id,fields);const check=await api.list(T.cleanup,`RECORD_ID()=${JSON.stringify(control.id)}`);if(check.length!==1||!isDeepStrictEqual(Object.fromEntries(Object.keys(fields).map(k=>[k,check[0].fields[k]])),fields))throw Error('Cleanup summary readback mismatch');}
  actions.push({name,deleted:removed});
 }
 state.show=show;if(event.kind==='daily')state.dailyDay=event.day;if(event.kind==='quick')state.quickAt=now;state.lastCompleted={event,deleted:state.eventDeleted};delete state.runningEvent;delete state.eventDeleted;save(file,state);return {ok:true,event,deleted,actions};
}
module.exports={LOGS,OWNED,due,plan,archive,run};
