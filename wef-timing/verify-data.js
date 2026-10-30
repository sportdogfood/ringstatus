'use strict';
// Read-only acceptance: verifies a completed scheduled receipt against Airtable
// and the producer's pinned local immutable handoff; never starts a workflow.
const fs=require('node:fs'),path=require('node:path'),{execFile}=require('node:child_process');
const {api,TIMING,DATA_TABLES:T}=require('./api');
const {hash,ageReason}=require('./pace');
function pinnedData(pointer,context,deadline){return new Promise((resolve,reject)=>{
 const remaining=deadline-Date.now();if(remaining<2000)return reject(Error('Pinned verification budget exhausted'));
 const child=execFile(path.resolve(__dirname,'../wef-data/.venv/Scripts/python.exe'),['-B',path.join(__dirname,'verify-data-input.py')],{windowsHide:true,timeout:remaining,maxBuffer:8*1024*1024,env:{SystemRoot:process.env.SystemRoot,WINDIR:process.env.WINDIR,TEMP:process.env.TEMP,TMP:process.env.TMP,PYTHONDONTWRITEBYTECODE:'1'}},(error,stdout,stderr)=>{
  if(error)return reject(Error(String(stderr||error.message)));try{resolve(JSON.parse(stdout));}catch{reject(Error('Pinned source verification unreadable'));}
 });child.stdin.end(JSON.stringify({pointer,context}));
});}
function validate(row,inventory,group){
 const f=row.fields,p=JSON.parse(f['payload-json']),refs=JSON.parse(f['input-refs-json']);
 if(f['source-lane']!=='data_live')throw Error('Wrong source lane');
 const id=p.identity,g=group.identity;
 if(!id||!['customer','show','date','ring','group'].every(k=>id[k]===g[k])||f['show-id']!==id.show||f.date!==id.date||f['ring-number']!==id.ring||f['group-id']!==id.group)throw Error('Snapshot identity mismatch');
 if(f['answer-kind']==='class-timing'){
  const scope=JSON.stringify(['data_live','class-timing',id.customer,id.show,id.date,id.ring,id.group,id.class,id.occurrence,id.phase]);
  if(scope!==f['scope-key']||!group.rows.some(r=>r['stage-key']===id.occurrence&&r['class-id']===id.class))throw Error('Class occurrence mismatch');
  const {evaluated_at,minutes_until_start,minutes_since_start,...material}=p;
  material.input_refs=p.input_refs.map(({producer_proof,...ref})=>ref);
  if(hash(material)!==f['content-hash']||hash(refs)!==hash(p.input_refs))throw Error('Class snapshot hash/provenance mismatch');
  if(Math.round(p.pace_minutes_per_trip*10000)/10000!==f['pace-minutes-per-trip'])throw Error('Class pace mismatch');
  const evidence=refs.find(x=>x.role==='data_evidence');
  if(evidence){if(hash(evidence.parts)!==hash(inventory.refs))throw Error('Class Data evidence mismatch');}
  else if(!p.reasons?.includes('data_observation_unusable')||!ageReason(inventory.observed_at,p.evaluated_at,1800000)||f.availability!=='unavailable'||p.start_at!==null||p.total_trips!==null)throw Error('Unusable class Data evidence not justified');
 }else if(f['answer-kind']==='ring-pace'){
  if(f['scope-key']!==['data_live',id.show,id.date,id.ring,id.group,'base'].join('|'))throw Error('Pace scope mismatch');
  const member=r=>({'stage-key':r['stage-key'],'class-id':r['class-id']??null,'start-time':r['start-time']??null,'end-time':r['end-time']??null,'total-trips':r['total-trips']??null,type:r.type??null});
  const sort=v=>[...v].sort((a,b)=>String(a['stage-key']).localeCompare(String(b['stage-key'])));
  if(hash(sort(p.schedule_members))!==hash(sort(group.rows.map(member))))throw Error('Pace membership mismatch');
  const dataRefs=refs.filter(x=>x.base==='appdl16HJ5ETm8N69'&&x.table==='collections');
  const original=dataRefs.map(({observed_at,...v})=>v);
  if(hash(original)!==hash(inventory.refs))throw Error('Pace Data evidence mismatch');
  if(hash({policy:'ring-pace-v1',payload:p,inputRefs:refs,observedAt:inventory.observed_at})!==f['content-hash'])throw Error('Pace snapshot hash mismatch');
  if(p.selected_pace_minutes_per_trip!==null&&Math.round(p.selected_pace_minutes_per_trip*10000)/10000!==f['pace-minutes-per-trip'])throw Error('Pace field mismatch');
 }else throw Error('Unexpected answer kind');
}
function validateCoverage(expected,answers){
 const latest=new Map();for(const r of answers){const f=r.fields;if(!expected.has(f['scope-key']))continue;
  const previous=latest.get(f['scope-key']);if(previous&&previous.fields['evaluated-at']===f['evaluated-at']&&previous.fields['content-hash']!==f['content-hash'])throw Error('Ambiguous latest answer');
  if(!previous||String(f['evaluated-at'])>String(previous.fields['evaluated-at']))latest.set(f['scope-key'],r);
 }
 let groups=0,classes=0;for(const [scope,required]of expected){const row=latest.get(scope);if(!row||row.fields['answer-kind']!==required.kind)throw Error('Missing expected '+required.kind+' scope');validate(row,required.inventory,required.group);if(required.kind==='ring-pace')groups++;else classes++;}
 return{groups,classes,validated:latest.size};
}
async function verify(){
 const deadline=Date.now()+160000,receipt=JSON.parse(fs.readFileSync(path.join(__dirname,'runtime/data/last-run.json'),'utf8'));
 const a=api({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline,lane:'data_live'});
 const answers=await a.list(TIMING,T.answers),checkpoints=await a.list(TIMING,T.checkpoint);
 const keys=answers.map(r=>r.fields['answer-key']);if(new Set(keys).size!==keys.length)throw Error('Duplicate snapshot keys');
 const current=checkpoints.find(r=>r.fields['checkpoint-key']==='ring-pace-data-runner'),state=JSON.parse(current?.fields['state-json']||'{}');
 if(!receipt.ok||receipt.errors.length||current?.fields['run-id']!==receipt.run_id||!['complete','noop','purging_deferred'].includes(current.fields['last-outcome'])||state.input_status!=='verified'||state.pending_purge)throw Error('Completed verified-input scheduled run required');
 const pin=state.source_pointer,context=state.source_context;
 if(!pin||!context||pin.head!==state.source_head?.['run-key']||pin['run-sha256']!==state.source_head?.['state-sha256'])throw Error('Pinned producer head missing from checkpoint');
 const data=await pinnedData(pin,context,deadline);if(data.proof['run-key']!==pin.head||data.proof['state-sha256']!==pin['run-sha256'])throw Error('Pinned producer input differs');
 const expected=new Map();
 for(const [day,inv] of Object.entries(data.inventories)){
  if(state.evaluation_cutoff&&day<state.evaluation_cutoff)continue;
  for(const group of inv.groups){const i=group.identity,groupScope=['data_live',i.show,day,i.ring,i.group,'base'].join('|');
   if(expected.has(groupScope))throw Error('Duplicate expected group');expected.set(groupScope,{kind:'ring-pace',inventory:inv,group});
   for(const row of group.rows){if(['schooling','break'].includes(String(row.type).toLowerCase()))continue;
    if(!Number.isSafeInteger(row['class-id'])||row['class-id']<=0||typeof row['stage-key']!=='string'||/\|position-\d+$/.test(row['stage-key']))throw Error('Unresolved expected class identity');
    const scope=JSON.stringify(['data_live','class-timing',i.customer,i.show,day,i.ring,i.group,row['class-id'],row['stage-key'],'base']);
    if(expected.has(scope))throw Error('Duplicate expected class');expected.set(scope,{kind:'class-timing',inventory:inv,group});
   }
  }
 }
 const {groups,classes,validated}=validateCoverage(expected,answers);
 if(receipt.counts.evaluated!==groups||receipt.counts.classes_evaluated!==classes||receipt.counts.deferred!==0||state.scope_count!==groups||state.class_scope_count!==classes)throw Error('Scheduled run coverage differs from pinned source');
 const created=answers.filter(r=>r.fields['answer-key'].includes('|'+receipt.run_id+'|'));
 if(created.length!==receipt.counts.written+receipt.counts.classes_written)throw Error('Written count/readback mismatch');
 const result={verified_at:new Date().toISOString(),run:receipt.run_id,started:receipt.started,completed:receipt.completed,ok:true,counts:receipt.counts,groups,classes,validated_latest:validated,total_snapshots:answers.length,checkpoint_outcome:current.fields['last-outcome'],purge_status:state.purge_status};
 fs.mkdirSync(path.join(__dirname,'evidence'),{recursive:true});fs.writeFileSync(path.join(__dirname,'evidence','data-normal-'+receipt.run_id+'.json'),JSON.stringify(result,null,2));
 fs.writeFileSync(path.join(__dirname,'evidence','data-receipt-'+receipt.run_id+'.json'),JSON.stringify(receipt,null,2));console.log(JSON.stringify(result));
}
if(require.main===module)verify().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={validate,validateCoverage};
