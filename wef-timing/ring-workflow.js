'use strict';
// Offline new Ring workflow. No production imports, network or task execution.
const crypto=require('node:crypto');
const canonical=x=>Array.isArray(x)?x.map(canonical):x&&typeof x==='object'?Object.fromEntries(Object.keys(x).sort().map(k=>[k,canonical(x[k])])):x;
const hash=x=>crypto.createHash('sha256').update(JSON.stringify(canonical(x))).digest('hex');
const links=v=>(v||[]).map(x=>typeof x==='string'?x:x.id);
const count=x=>Number.isSafeInteger(x)&&x>=0;
function normalize(saved,day,skipped=[]){
 const rings=saved.records||saved.tables['wef-live.rings'];
 const native=saved.tables?.['wef-live.groups'];
 return rings.flatMap(r=>{
  try {
   if(r.fields.date!==day||r.fields['scope-status']!=='current')return [];
   const f=r.fields,raw=JSON.parse(f['source-json']);
   const embedded=native?[]:['livenow','upcoming','completed'].flatMap(k=>(raw[k]||[]).map(g=>({fields:{'group-key':[f['show-id'],g.day,g.class_group_id].join('|'),'show-id':f['show-id'],'customer-id':f['customer-id'],date:g.day,'class-group-id':Number(g.class_group_id),'last-seen':f['last-seen'],'source-updated-at':String(g.curr_updated_at),status:g.status,'is-live':g.is_live,total:Number(g.total),gone:Number(g.gone),'jo-total':Number(g.jo_total),'jo-gone':Number(g.jo_gone),'r2-total':Number(g.r2_total),'r2-gone':Number(g.r2_gone),'estimated-start-time':g.estimated_start_time,ring:[r.id],'source-json':JSON.stringify(g)},origin:'captured ring source-json.'+k})));
   const groups=(native||embedded).filter(g=>g?.fields&&g.fields.date===day&&g.fields['show-id']===f['show-id']&&g.fields['customer-id']===f['customer-id']&&links(g.fields.ring).includes(r.id));
   const key=JSON.stringify([f['customer-id'],f['show-id'],day,f['ring-id']]);
   const pointer=String(f['current-group']||'').split(',').map(x=>x.trim()).filter(Boolean);
   const matches=groups.filter(g=>pointer.includes(g.fields['group-key']));
   const underway=groups.filter(g=>g.fields.status==='Underway'||g.fields['is-live']===true);
   let reason=null,g=null;
   if(pointer.length===0)reason=underway.length?'selection_conflict':'no_active_group';
   else if(pointer.length!==1||matches.length!==1)reason='selection_conflict';
   else{g=matches[0];if(g.fields.status!=='Underway'||g.fields['is-live']!==true||underway.some(x=>x.fields['group-key']!==g.fields['group-key'])){g=null;reason='selection_conflict';}}
   let selected=null;
   if(g){const gf=g.fields,gr=JSON.parse(gf['source-json']);const members=(gr.classes||[]).map(Number).sort((a,b)=>a-b);
    if(!members.length||!members.every(x=>Number.isSafeInteger(x)&&x>0)||new Set(members).size!==members.length)throw Error('Invalid group membership');
    if(gr.JOUnderway===true&&gr.R2Underway===true)throw Error('Conflicting round flags');
    const phase=gr.JOUnderway===true?'jump_off':gr.R2Underway===true?'round_two':'base';
    const total=phase==='jump_off'?gf['jo-total']:phase==='round_two'?gf['r2-total']:gf.total;
    const gone=phase==='jump_off'?gf['jo-gone']:phase==='round_two'?gf['r2-gone']:gf.gone;
    if(!count(total)||total===0||!count(gone)||gone>total)throw Error('Invalid group counts');
    selected={key:gf['group-key'],id:gf['class-group-id'],status:gf.status,isLive:gf['is-live'],phase,total,gone,members,membership:hash(members),observedAt:gf['last-seen'],sourceUpdatedAt:gf['source-updated-at']??null,estimatedStart:gf['estimated-start-time']??null,rounds:{joTotal:gf['jo-total'],joGone:gf['jo-gone'],r2Total:gf['r2-total'],r2Gone:gf['r2-gone']}};
   }
   return [{key,ringId:f['ring-id'],ringName:f['ring-name'],day,observedAt:f['last-seen'],rawRingSourceUpdatedAt:f['source-updated-at']??null,selected,reason,context:{current:pointer,next:f['next-group']||null,completed:f['completed-groups']||null},proof:{ring:r,groups}}];
  }catch(error){skipped.push({ringId:r?.fields?.['ring-id']??null,reason:error.message});return [];}
 });
}
function create(){return{version:'offline-prototype',current:new Map(),history:new Map(),checkpoint:{version:1,rings:{}},skipped:[]};}
function step(state,observations){
 for(const obs of observations){
  const g=obs.selected,at=Date.parse(g?.observedAt??obs.observedAt),original=Date.parse(obs.observedAt);
  if(!Number.isFinite(original)||!Number.isFinite(at)||at>original||(g&&(!count(g.total)||g.total<=0||!count(g.gone)||g.gone>g.total))){state.skipped.push({key:obs.key,reason:'invalid_snapshot'});continue;}
  const historyKey=JSON.stringify(['ring-observation',obs.key,obs.observedAt,hash(obs)]);
  if(state.history.has(historyKey))continue;
  const current=state.current.get(obs.key),prior=state.checkpoint.rings[obs.key];
  const same=!!g&&prior?.groupKey===g.key&&prior.phase===g.phase&&prior.membership===g.membership&&prior.total===g.total;
  let rejected=null;
  if(current&&original<Date.parse(current.observedAt))rejected='older_snapshot';
  else if(current&&original===Date.parse(current.observedAt))rejected='same_time_conflict';
  else if(same&&(at<Date.parse(prior.at)||g.gone<prior.gone||(at===Date.parse(prior.at)&&g.gone!==prior.gone)))rejected='regressed_or_conflicting_count';
  if(rejected){state.history.set(historyKey,{key:historyKey,observation:structuredClone(obs),result:null,ignored:rejected});continue;}
  let pace=null,method='unavailable',reason=obs.reason,interval=null,paceQuality=null;
  let measurement=null,hold=false;
  if(g){
   if(same){
    const elapsed=at-Date.parse(prior.at),increase=g.gone-prior.gone;
    if(elapsed>0&&increase>0){pace=elapsed/60000/increase;method='observed_interval';reason=null;paceQuality='measured';interval={from:prior.at,to:g.observedAt,goneFrom:prior.gone,goneTo:g.gone,total:g.total,fromHistory:prior.historyKey,toHistory:historyKey};measurement={pace,interval};}
    else{hold=true;reason='no_new_completions';measurement=prior.measurement??null;if(measurement){pace=measurement.pace;interval=structuredClone(measurement.interval);method='observed_interval';paceQuality='retained';}}
   }else reason=prior?.groupKey===g.key?'baseline_changed':'baseline_required';
  }
  const result={key:obs.key,ringId:obs.ringId,ringName:obs.ringName,day:obs.day,observedAt:obs.observedAt,sourceUpdatedAt:g?.sourceUpdatedAt??null,rawRingSourceUpdatedAt:obs.rawRingSourceUpdatedAt,status:g?.status??null,groupKey:g?.key??null,groupId:g?.id??null,phase:g?.phase??null,total:g?.total??null,gone:g?.gone??null,members:g?.members??[],estimatedStart:g?.estimatedStart??null,rounds:g?.rounds??null,pace,method,reason,interval,paceQuality,paceAsOf:interval?.to??null,context:obs.context};
  state.history.set(historyKey,{key:historyKey,observation:structuredClone(obs),result:structuredClone(result)});
  state.current.set(obs.key,structuredClone(result));
  state.checkpoint.rings[obs.key]=hold?prior:{historyKey,groupKey:g?.key??null,phase:g?.phase??null,membership:g?.membership??null,total:g?.total??null,gone:g?.gone??null,at:g?.observedAt??obs.observedAt,measurement};
 }
 return state;
}

// Saved-record and Class compatibility boundary for the reviewed Ring core.
function verifyObservation(obs){
 const problems=[];const rebuilt=normalize({records:[obs.proof.ring],tables:{'wef-live.groups':obs.proof.groups}},obs.day,problems);
 if(problems.length||rebuilt.length!==1||hash(rebuilt[0])!==hash(obs)||!obs.selected)throw Error('Ring observation proof mismatch');
 const f=obs.proof.ring.fields,g=obs.proof.groups.find(r=>r.fields['group-key']===obs.selected.key)?.fields,raw=JSON.parse(g?.['source-json']||'null');
 if(!g||!raw||Number(raw.class_group_id)!==g['class-group-id']||Number(raw.ring_number)!==f['ring-number']||Number(raw.ring_id)!==f['ring-id']||raw.day!==obs.day||String(raw.status)!==String(g.status)||Number(raw.total)!==g.total||Number(raw.gone)!==g.gone)throw Error('Native group projection mismatch');
 if(!Number.isFinite(Date.parse(obs.observedAt))||!Number.isFinite(Date.parse(obs.selected.observedAt))||Date.parse(obs.selected.observedAt)>Date.parse(obs.observedAt))throw Error('Ring proof clock mismatch');
 return obs.selected;
}
function fieldsFor(result,kind,key,scope,payload,now,observed,refs=[]){
 const identity=JSON.parse(result.key);const f={
  'answer-key':key,'scope-key':scope,'answer-kind':kind,'source-lane':'data_live','show-id':identity[1],date:result.day,
  'ring-number':result.ringNumber,'group-id':result.groupId??null,phase:result.phase??null,
  availability:result.pace==null?'unavailable':'available','pace-method':result.pace==null?'unavailable':'observed_interval',
  'pace-minutes-per-trip':result.pace==null?null:Math.round(result.pace*10000)/10000,'evaluated-at':now,'observed-at':observed??null,
  'reason-codes-json':JSON.stringify(result.reason?[result.reason]:[]),'input-refs-json':JSON.stringify(refs),
  'payload-json':JSON.stringify(payload),'content-hash':hash(payload)};
 if(f['payload-json'].length>95000)throw Error('Ring snapshot field too large; no evidence truncated');
 return f;
}
function records(state,now,runId,onlyKeys=null){
 const history=[],pace=[],current=[];const latest=new Map();
 for(const [key,entry] of state.history){
  if(onlyKeys&&!onlyKeys.has(key))continue;
  const obs=entry.observation,number=obs.proof.ring.fields['ring-number'];const result={...(entry.result??{key:obs.key,day:obs.day,groupId:null,phase:null,pace:null,reason:entry.ignored}),ringNumber:number};
  history.push(fieldsFor(result,'ring-observation',key,'ring-observation|'+obs.key,{schema_version:2,policy:'ring-observation-v2',history:entry},now,obs.observedAt));
  if(!entry.result)continue;
  latest.set(obs.key,key);
  if(entry.result.pace==null||!entry.result.interval)continue;
  const v=entry.result.interval,a=state.history.get(v.fromHistory)?.observation,b=state.history.get(v.toHistory)?.observation;
  if(!a||!b)throw Error('Missing saved interval observation');
  const ga=verifyObservation(a),gb=verifyObservation(b),g=verifyObservation(obs),id=JSON.parse(obs.key);
  const payload={schema_version:2,policy:'live-ring-pace-v2',identity:{customer:id[0],show:id[1],date:obs.day,ring:number,group:g.id},phase:g.phase,
   member_class_ids:g.members,pace_method:'observed_interval',selected_pace_minutes_per_trip:entry.result.pace,observed_interval_pace_minutes_per_trip:entry.result.pace,
   interval:{from:ga.observedAt,to:gb.observedAt,new_completions:gb.gone-ga.gone,endpoints:[a,b]},latest_observation:obs,quality:entry.result.paceQuality};
  const scope=['data_live',id[1],obs.day,number,g.id,g.phase].join('|');
  const f=fieldsFor(result,'ring-pace',scope+'|'+runId+'|'+hash(payload).slice(0,16),scope,payload,now,v.to);
  f['content-hash']=hash({policy:payload.policy,payload});pace.push(f);
 }
 for(const [key,result]of state.current){
  const entry=state.history.get(latest.get(key));if(!entry)continue;
  const number=entry.observation.proof.ring.fields['ring-number'];
  const row={...result,ringNumber:number};
  current.push(fieldsFor(row,'ring-current','ring-current|'+key,'ring-current|'+key,{schema_version:2,policy:'ring-current-v2',current:row,history_key:entry.key},now,result.observedAt));
 }
 return{history,pace,current};
}
function consume(record,identity,inventory,now){
 const f=record.fields,p=JSON.parse(f['payload-json']);
 if(p.policy!=='live-ring-pace-v2'||p.schema_version!==2||p.phase!=='base'||f.phase!=='base'||f['answer-kind']!=='ring-pace'||f['source-lane']!=='data_live'||f.availability!=='available'
  ||!['customer','show','date','ring','group'].every(k=>p.identity?.[k]===identity[k])||f.date!==identity.date||f['show-id']!==identity.show||f['ring-number']!==identity.ring||f['group-id']!==identity.group)throw Error('Ring v2 identity mismatch');
 if(hash({policy:p.policy,payload:p})!==f['content-hash']||!Array.isArray(p.interval?.endpoints)||p.interval.endpoints.length!==2)throw Error('Ring v2 content mismatch');
 const [a,b]=p.interval.endpoints,ga=verifyObservation(a),gb=verifyObservation(b),latest=verifyObservation(p.latest_observation);
 for(const obs of [a,b,p.latest_observation]){const r=obs.proof.ring.fields,g=obs.selected;if(r['customer-id']!==identity.customer||r['show-id']!==identity.show||obs.day!==identity.date||r['ring-number']!==identity.ring||g.id!==identity.group||g.phase!==p.phase||Date.parse(obs.observedAt)>Date.parse(now))throw Error('Ring v2 scope/clock mismatch');}
 const elapsed=Date.parse(gb.observedAt)-Date.parse(ga.observedAt),increase=gb.gone-ga.gone;
 if(elapsed<=0||increase<=0||ga.total!==gb.total||ga.membership!==gb.membership||latest.membership!==gb.membership||latest.total!==gb.total||latest.gone<gb.gone
  ||Date.parse(latest.observedAt)<Date.parse(gb.observedAt)||p.interval.from!==ga.observedAt||p.interval.to!==gb.observedAt||f['observed-at']!==gb.observedAt||p.interval.new_completions!==increase
  ||elapsed/60000/increase!==p.selected_pace_minutes_per_trip||p.observed_interval_pace_minutes_per_trip!==p.selected_pace_minutes_per_trip
  ||f['pace-minutes-per-trip']!==Math.round(p.selected_pace_minutes_per_trip*10000)/10000||hash(p.member_class_ids)!==hash(gb.members)||!gb.members.includes(identity.class))throw Error('Ring v2 interval mismatch');
 const synced=inventory.synced_live,gs=synced?.groups.filter(r=>r.fields['class-group-id']===identity.group&&r.fields.date===identity.date&&r.fields['show-id']===identity.show),rs=synced?.rings.filter(r=>r.fields['ring-number']===identity.ring&&r.fields.date===identity.date&&r.fields['show-id']===identity.show);
 if(gs?.length!==1||rs?.length!==1)throw Error('Ring v2 native identity missing');
 const gf=gs[0].fields,rf=rs[0].fields,raw=JSON.parse(gf['source-json']);
 if(gf['customer-id']!==identity.customer||rf['customer-id']!==identity.customer||!links(gf.ring).includes(rs[0].id)||gf['scope-status']!=='current'||rf['scope-status']!=='current'
  ||Number(raw.ring_id)!==rf['ring-id']||Number(raw.ring_number)!==identity.ring||Number(raw.class_group_id)!==identity.group||hash(raw.classes.map(Number).sort((x,y)=>x-y))!==hash(gb.members)
  ||!Number.isFinite(Date.parse(gf['last-seen']))||Date.parse(gf['last-seen'])>Date.parse(now))throw Error('Ring v2 native scope mismatch');
 if(raw.JOUnderway===true&&raw.R2Underway===true)throw Error('Ring v2 conflicting current phase');
 const currentPhase=raw.JOUnderway===true?'jump_off':raw.R2Underway===true?'round_two':'base';
 if(currentPhase!==p.phase||String(raw.status)!==String(gf.status)||Number(raw.total)!==gf.total||Number(raw.gone)!==gf.gone||Boolean(raw.is_live)!==Boolean(gf['is-live'])||raw.day!==identity.date||gf['group-key']!==[identity.show,identity.date,identity.group].join('|'))throw Error('Ring v2 current native projection/phase mismatch');
 const nativeAt=Date.parse(gf['last-seen']),ringAt=Date.parse(rf['last-seen']);
 if(!Number.isFinite(ringAt)||ringAt>Date.parse(now)||nativeAt<Date.parse(latest.observedAt)||ringAt<Date.parse(p.latest_observation.observedAt)||nativeAt<Date.parse(gb.observedAt))throw Error('Ring v2 native chronology regressed');
 const final=gf.status==='Completed'&&raw.status==='Completed';
 if(!final&&(String(rf['current-group'])!==gf['group-key']||gf.status!=='Underway'||gf.total!==latest.total||gf.gone<latest.gone))throw Error('Ring v2 current selection mismatch');
 const end=Date.parse(gb.observedAt),n=Date.parse(now);
 if(end>n||(!final&&n-end>=600000))throw Error('Ring v2 observation expired');
 const scope=['data_live',identity.show,identity.date,identity.ring,identity.group,'base'].join('|');
 if(f['scope-key']!==scope||!f['answer-key'].startsWith(scope+'|'))throw Error('Ring v2 answer key mismatch');
 return{identity:{...p.identity,phase:p.phase},source_lane:'data_live',observed_at:gb.observedAt,valid_until:new Date(end+600000).toISOString(),
  ref:{base:'appYOykKIy1kAquhP',table:'timing-answers',record:record.id,source_key:scope,revision:f['content-hash'],input_refs:JSON.parse(f['input-refs-json'])},
  lifecycle:final?'final':'active_or_unknown',final_verified:final,answer_key:f['answer-key'],member_class_ids:gb.members,pace_minutes_per_trip:p.selected_pace_minutes_per_trip,method:'observed_interval'};
}
module.exports={normalize,create,step,hash,records,consume};
