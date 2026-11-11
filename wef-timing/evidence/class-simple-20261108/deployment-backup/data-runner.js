'use strict';
const {api:makeApi,TIMING,SHOWS,DATA_TABLES:T,CLASS_TIMING,CLASS_ANSWERS,SYNC_GROUPS,SYNC_RINGS,syncToken}=require('./api');
const {hash}=require('./pace'),{verifyRows}=require('./runner'),{decode,encode}=require('./checkpoint-state');
const source=require('./data-source'),core=require('./ring-workflow'),{prepare}=require('./class-worker'),{readLiveClasses}=require('./class-live-source'),{localDay}=require('./purge');
const quoted=x=>JSON.stringify(x),orKeys=keys=>'OR('+keys.map(k=>'{answer-key}='+quoted(k)).join(',')+')';
const ringCadence=require('./ring-cadence');
async function run({api,classApi,ringApi,readRingHistory=core.savedRingHistory,readLiveReceipt=ringCadence.readReceipt,clock=()=>new Date().toISOString(),deadline=Date.now()+160000,runId=require('node:crypto').randomUUID(),readData=source.loadData,readPointer=source.pointer,readLive=readLiveClasses,prepareClassBatch=prepare}={}){
 api=api||makeApi({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline,lane:'data_live'});classApi=classApi||makeApi({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline,lane:'class_live'});
 const started=clock(),day=localDay(started),checkpointKey='ring-pace-data-runner|'+day,began=Date.now(),report={run_id:runId,started,ok:false,source_lane:'data_live',destination:TIMING,errors:[],issues:[],counts:{evaluated:0,written:0,unchanged:0,purged:0,classes_evaluated:0,classes_written:0,classes_unchanged:0}};
 const ringWarnings=[];const ringTransport=ringApi||makeApi({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline,lane:'ring_state',onClockIssue:message=>ringWarnings.push(message)});
 report.ring=await require('./ring-snapshot').run({api:ringTransport,now:started,runId});
 report.ring.issues.push(...[...new Set(ringWarnings)].map(message=>({message})));
 report.counts.evaluated=report.ring.rings.length;report.counts.written=report.ring.written;
 let state={version:1},loaded=false,blocked=false,stage='checkpoint_read';
 async function save(outcome,error=''){
  const f={'checkpoint-key':checkpointKey,'run-id':runId,'started-at':started,'completed-at':clock(),'last-outcome':outcome,'duration-ms':Date.now()-began,'counts-json':JSON.stringify(report.counts),'state-json':encode(state),'error-summary':error,'policy-version':'live-ring-pace-v2'};
  await api.upsert(T.checkpoint,'checkpoint-key',[f]);verifyRows(await api.list(TIMING,T.checkpoint,"{checkpoint-key}="+quoted(checkpointKey)),[f],'checkpoint-key');
 }
 async function publish(transport,writes,base,table,nextRing){
  if(!writes.length){if(nextRing)state.ring_workflow=nextRing;return;}
  stage=base===TIMING?'ring_publication':'class_publication';state.pending_write={base,table,run_id:runId,answers:writes.map(f=>({key:f['answer-key'],hash:f['content-hash']})),...(nextRing?{next_ring_workflow:nextRing}:{})};
  await save('writing');await source.writeAnswers(transport,writes,base,table);if(nextRing)state.ring_workflow=nextRing;delete state.pending_write;await save('written');
 }
 async function deferClass(reason){state.class_status=reason;report.class_status='deferred';report.status='deferred';report.deferred_reason=reason;report.completed=clock();await save('class_deferred');return report;}
 try{
  const checkpoints=await api.list(TIMING,T.checkpoint,"{checkpoint-key}="+quoted(checkpointKey));if(checkpoints.length>1)throw Error('Duplicate shared checkpoint');
  if(checkpoints.length){state=decode(checkpoints[0].fields['state-json']);if(state.version!==1)throw Error('Unsupported shared checkpoint');if(state.pending_write||state.pending_purge||state.class_retention?.pending_purge||state.entry_failed||(checkpoints[0].fields['last-outcome']==='failed'&&!state.retry_safe)){blocked=true;throw Error('Uncertain prior intent requires owner review');}}
  loaded=true;stage='ring_source_read';const selected=source.selectShow(await api.list(SHOWS,'shows'),clock()),context=selected.context;
  if(day<context['start-date']||day>context['end-date'])throw source.defer('TODAY outside show dates');
  const filter=`AND({show-id}=${context['show-id']},{customer-id}=${context['customer-id']},{date}='${day}',{scope-status}='current')`;
  const rings=await api.list(TIMING,SYNC_RINGS,filter),groups=await api.list(TIMING,SYNC_GROUPS,filter);
  const stable=async()=>{const again=source.selectShow(await api.list(SHOWS,'shows'),clock());return source.sameAuthority(selected,again)&&localDay(clock())===day&&syncToken(rings)===syncToken(await api.list(TIMING,SYNC_RINGS,filter))&&syncToken(groups)===syncToken(await api.list(TIMING,SYNC_GROUPS,filter));};
  // Preserve the existing Class stage exactly once; its Data dependency is downstream of Ring publication.
  stage='class_source_read';report.counts.class_stage_attempts=1;let pinned,data;
  try{pinned=readPointer(context);data=await readData(context,deadline);}catch(e){if(e.deferred)return await deferClass(e.message);throw e;}
  if(data.proof?.['run-key']!==pinned.head||hash(readPointer(context))!==hash(pinned))return await deferClass('Data head changed');
  if(!data.inventories?.[day])throw Error('Today Class Data inventory missing');
  for(const inv of Object.values(data.inventories))inv.synced_live={rings,groups};
  let live;try{live=await readLive({api,customer:context['customer-id'],show:context['show-id'],now:clock()});}catch(e){if(e.code==='SOURCE_CHANGED')return await deferClass('Live Class source changed');throw e;}
  const [ringAnswers,classAnswers]=await Promise.all([api.list(TIMING,T.answers,`AND({answer-kind}='ring-pace',{show-id}=${context['show-id']},DATETIME_FORMAT({date},'YYYY-MM-DD')='${day}')`),classApi.list(CLASS_TIMING,CLASS_ANSWERS,`AND({show-id}=${context['show-id']},{date}='${day}')`)]);
  const inventory=data.inventories[day],identityCounts=new Map(),excluded=new Set();
  for(const g of inventory.groups){const i=g?.identity;if(!i||i.customer!==context['customer-id']||i.show!==context['show-id']||i.date!==day||!Number.isSafeInteger(i.ring)||i.ring<=0||!Number.isSafeInteger(i.group)||i.group<=0)throw Error('Data projected identity mismatch');const k=[i.show,i.date,i.ring,i.group].join('|');identityCounts.set(k,(identityCounts.get(k)||0)+1);}
  for(const g of inventory.groups){const k=[g.identity.show,g.identity.date,g.identity.ring,g.identity.group].join('|');if(identityCounts.get(k)>1||!Array.isArray(g.rows)||g.rows.some(r=>!r||typeof r!=='object')||!Array.isArray(g.class_keys)){excluded.add(g);const message=identityCounts.get(k)>1?'Data group identity repeated':'Malformed group rows or class membership';report.errors.push(message);report.issues.push({stage,identity:g.identity,message});}}
  const classData={...data,inventories:{[day]:{...inventory,groups:inventory.groups.filter(g=>!excluded.has(g))}}},batch=prepareClassBatch({data:classData,liveClasses:live.classes,answers:[...ringAnswers,...classAnswers],now:clock(),runId,onIssue:(identity,message)=>{report.issues.push({stage,identity,message});report.errors.push(message);}});
  report.counts.classes_evaluated=batch.outputs.length;report.counts.classes_unchanged=batch.unchanged;report.counts.classes_unavailable=batch.outputs.filter(x=>x.fields.availability==='unavailable').length;
  if(!await stable()||hash(readPointer(context))!==hash(pinned))return await deferClass('Class source changed before publication');
  await publish(classApi,batch.writes,CLASS_TIMING,CLASS_ANSWERS);report.counts.classes_written=batch.writes.length;
  state.class_live_issues=live.issues;state.class_scope_count=batch.outputs.length;state.class_status='complete';state.purge_status='archive_required';report.purge_status='archive_required';
  report.class_status='complete';report.completed=clock();report.status=report.errors.length?'partial':'complete';report.ok=!report.errors.length;state.retry_safe=true;await save(report.status);return report;
 }catch(error){
  report.status=blocked?'blocked':error.deferred?'deferred':'failed';report.errors.push(error.message);report.issues.push({stage,message:error.message});report.completed=clock();
  if(loaded&&!blocked){state.retry_safe=!state.pending_write&&!state.pending_purge&&!state.class_retention?.pending_purge;try{await save(report.status,error.message);}catch(e){report.errors.push('Failure reporting: '+e.message);}}
  return report;
 }
}
module.exports={run,...source};
