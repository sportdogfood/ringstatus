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

// Ring-only snapshot path. Class's existing normalize/consume contract stays above.
const RING_INPUTS={shows:'tblbIE6RygVMtiEmS',days:'tblgRdGfOmwLRUlrT',schedules:'tbl02nCQk1caPoeO2',rings:'tbleertfMY7ISGZCh',groups:'tblSmVxVsHlpudxlQ',classes:'tblREU7auaqFoeKZ8'};
const number=v=>v===null||v===undefined||v===''?null:Number.isFinite(Number(v))?Number(v):null;
function easternTime(day,time){
 if(!/^\d{4}-\d{2}-\d{2}$/.test(day)||!/^\d{2}:\d{2}:\d{2}$/.test(time||'')||time==='00:00:00')return null;
 const target=Date.parse(day+'T'+time+'Z');if(!Number.isFinite(target))return null;
 let at=target;for(let i=0;i<3;i++){const f=new Intl.DateTimeFormat('sv-SE',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'}).format(new Date(at));at+=target-Date.parse(f.replace(' ','T')+'Z');}
 return at;
}
function sourceTime(value){const n=number(value);const at=n!==null&&n>1000000000?n*(n<100000000000?1000:1):Date.parse(value);return Number.isFinite(at)?at:null;}
function groupObservation(raw,{show,day,ringId,ringNumber,at,ref},issues){
 if(!raw||raw.day!==day)return null;
 const group=number(raw.class_group_id);if(!group||number(raw.ring_id)!==ringId||number(raw.ring_number)!==ringNumber){issues.push('group_identity_mismatch');return null;}
 const phase=raw.JOUnderway===true?'jump_off':raw.R2Underway===true?'round_two':'base';
 const total=number(phase==='jump_off'?raw.jo_total:phase==='round_two'?raw.r2_total:raw.total),gone=number(phase==='jump_off'?raw.jo_gone:phase==='round_two'?raw.r2_gone:raw.gone);
 const observed=at; // measurement uses saved collector time; raw source clock stays evidence
 if(!Number.isSafeInteger(total)||total<=0||!Number.isSafeInteger(gone)||gone<0||gone>total||!Number.isFinite(observed)){issues.push('invalid_group_counts_or_time:'+group);return null;}
 if(raw.JOUnderway===true&&raw.R2Underway===true){issues.push('conflicting_phase:'+group);return null;}
 return {show,day,ringId,ringNumber,group,phase,total,gone,at:new Date(observed).toISOString(),source_updated_at:raw.curr_updated_at??null,status:raw.status,is_live:raw.is_live===true,start:raw.estimated_start_time??null,name:raw.group_name??'',sequence:number(raw.group_sequence),members:(raw.classes||[]).map(Number).filter(Number.isSafeInteger),class_names:raw.classNames||[],ref};
}
function savedRingHistory({show,day,root,issues=[]}){
 const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),{localDay}=require('./purge');
 const runtime=path.join(root,'wef-live/runtime'),sources=new Map(),out=[];
 let names;try{names=fs.readdirSync(runtime);}catch(e){issues.push('saved_live_history_unavailable:'+e.message);return out;}
 for(const name of names.filter(n=>/^lane-rings-.*\.json$/.test(n)&&n!=='lane-rings-last-run.json')){try{
  const receipt=JSON.parse(fs.readFileSync(path.join(runtime,name),'utf8'));
  if(receipt.ok!==true||receipt.scope?.show!==show||localDay(receipt.started)!==day)continue;
  for(const a of receipt.archives||[]){if(a.kind!=='source-widget')continue;const at=Date.parse(a.at||receipt.timings?.collection_completed||receipt.completed);if(!Number.isFinite(at)||localDay(new Date(at).toISOString())!==day)continue;
   const previous=sources.get(a.file);if(!previous||at<previous.at)sources.set(a.file,{at,receipt:name});
  }
 }catch(e){issues.push('saved_receipt_unreadable:'+name+':'+e.message);}}
 for(const [file,reference]of sources){try{
  const payload=JSON.parse(zlib.gunzipSync(fs.readFileSync(file)));if(!Array.isArray(payload))continue;
  for(const source of payload){if(Number(source.show_id)!==show)continue;
   for(const [rn,r]of Object.entries(source.live_data||{}))for(const g of [...(r.livenow||[]),...(r.completed||[])]){
    if(g.day!==day||g.JOUnderway===true||g.R2Underway===true)continue;
    const o=groupObservation(g,{show,day,ringId:number(g.ring_id),ringNumber:Number(rn),at:reference.at,ref:'wef-live/runtime/'+reference.receipt+' -> '+file},issues);if(o)out.push(o);
   }
  }
 }catch(e){issues.push('saved_history_unreadable:'+file+':'+e.message);}}
 return out;
}
function calculateRingState({show,customer,day,now,rings,groups=[],classes=[],schedules=[],history=[]}){
 const results=[],issues=[],nowMs=Date.parse(now),dayKey=[customer,show,day].join('|');
 const report=(ring,message)=>issues.push({ring_id:ring,message});
 for(const row of rings){const f=row.fields,ringId=number(f['ring-id']),rn=number(f['ring-number']);if(f.date!==day||number(f['show-id'])!==show||number(f['customer-id'])!==customer)continue;
  const problems=[];let raw={};try{raw=JSON.parse(f['source-json']||'{}');}catch{problems.push('malformed_ring_source_json');}
  const original=sourceTime(f['last-seen']);if(original===null||original>nowMs)problems.push('invalid_ring_observation_time');
  const embedded=['livenow','completed','upcoming'].flatMap(k=>(raw[k]||[]).map(g=>({raw:g,ref:'ring-source-json:'+row.id})));
  const native=groups.filter(g=>g.fields.date===day&&g.fields['group-key']?.startsWith(show+'|'+day+'|')&&links(g.fields.ring).includes(row.id));
  const nativeProjected=native.map(g=>{try{const gf=g.fields,r=JSON.parse(gf['source-json']);if(number(r.total)!==number(gf.total)||number(r.gone)!==number(gf.gone)||String(r.status)!==String(gf.status))problems.push('group_projection_conflict:'+gf['group-key']);return{raw:{...r,total:gf.total,gone:gf.gone,status:gf.status,is_live:gf['is-live']===true,curr_updated_at:gf['source-updated-at']??r.curr_updated_at},ref:'wef-live.groups:'+g.id};}catch{problems.push('malformed_group_source_json:'+g.id);return null;}}).filter(Boolean);
  const live=new Map();for(const g of [...embedded,...nativeProjected]){const o=groupObservation(g.raw,{show,day,ringId,ringNumber:rn,at:original,ref:g.ref},problems);if(o&&Date.parse(o.at)<=nowMs)live.set(o.group,o);else if(o)problems.push('future_group_observation:'+o.group);}
  const latest=[...live.values()];const localSchedules=schedules.filter(s=>s.fields['w-days']===dayKey&&s.fields['w-shows']===customer+'|'+show&&number(s.fields.ring?.split('|').at(-1))===rn&&!String(s.fields['record-status']||'').startsWith('Dropped'));
  const scheduleFor=g=>localSchedules.filter(s=>number(s.fields.group?.split('|').at(-1))===g.group&&s.fields.kind==='class');
  const scheduleTimes=g=>{const ss=scheduleFor(g),starts=ss.map(s=>easternTime(day,s.fields['start-time'])).filter(Number.isFinite),ends=ss.map(s=>easternTime(day,s.fields['end-time'])).filter(Number.isFinite);return{start:starts.length?Math.min(...starts):easternTime(day,g.start),end:ends.length?Math.max(...ends):null};};
  const saved=history.filter(o=>o.day===day&&o.show===show&&o.ringId===ringId&&o.ringNumber===rn);
  const measuredKey=o=>JSON.stringify([o.group,o.phase,o.total,o.gone,o.status,[...o.members].sort((a,b)=>a-b)]),byMeasurement=new Map();
  const receipt=o=>String(o.ref||'').startsWith('wef-live/runtime/');
  for(const o of [...saved,...latest].sort((a,b)=>Number(receipt(b))-Number(receipt(a))||Date.parse(a.at)-Date.parse(b.at))){const key=measuredKey(o);if(!byMeasurement.has(key))byMeasurement.set(key,o);}
  const combined=[...byMeasurement.values()];
  const sequences=new Map();for(const o of combined){if(!['Underway','Completed'].includes(o.status)||Date.parse(o.at)>nowMs||!Number.isFinite(Date.parse(o.at))||!count(o.gone)||!count(o.total)||o.total<=0||o.gone>o.total)continue;const k=[o.group,o.phase,JSON.stringify([...o.members].sort((a,b)=>a-b))].join('|');if(!sequences.has(k))sequences.set(k,[]);sequences.get(k).push(o);}
  let minutes=0,completions=0,paceAt=null;const intervals=[],estimatedStarts=[];
  for(let list of sequences.values()){const originals=list.filter(receipt);if(originals.length)list=originals;list.sort((a,b)=>Date.parse(a.at)-Date.parse(b.at)||a.gone-b.gone);let base=null;for(const o of list){if(!base){base=o;continue;}const elapsed=(Date.parse(o.at)-Date.parse(base.at))/60000,delta=o.gone-base.gone;
   if(o.total!==base.total){problems.push('group_total_changed:'+o.group);base=o;continue;}
   if(elapsed<0||delta<0||(elapsed===0&&delta!==0)){problems.push('conflicting_or_regressed_history:'+o.group);continue;}
   if(elapsed>0&&delta>0){minutes+=elapsed;completions+=delta;intervals.push({group:o.group,phase:o.phase,from:base.at,to:o.at,gone_from:base.gone,gone_to:o.gone,from_ref:base.ref,to_ref:o.ref,from_status:base.status,to_status:o.status,elapsed_minutes:elapsed,completed_trips:delta});paceAt=!paceAt||o.at>paceAt?o.at:paceAt;base=o;}
  }}
  let pace=completions>0?minutes/completions:null,method=pace===null?'unavailable':'observed_history';
  if(pace===null){let elapsed=0,gone=0;for(const g of latest){const start=scheduleTimes(g).start,at=Date.parse(g.at);if(['Underway','Completed'].includes(g.status)&&g.phase==='base'&&g.gone>0&&Number.isFinite(start)&&at>start){elapsed+=(at-start)/60000;gone+=g.gone;estimatedStarts.push({group:g.group,from:new Date(start).toISOString(),to:g.at,elapsed_minutes:(at-start)/60000,completed_trips:g.gone,status:g.status});paceAt=!paceAt||g.at>paceAt?g.at:paceAt;}}if(gone>0){pace=elapsed/gone;minutes=elapsed;completions=gone;method='estimated_start';problems.push('pace_assumes_estimated_start');}}
  const currentKeys=String(f['current-group']||'').split(',').map(s=>s.trim());
  const currents=latest.filter(g=>g.status==='Underway'&&(currentKeys.includes([show,day,g.group].join('|'))||native.some(n=>number(n.fields['class-group-id'])===g.group&&links(n.fields['current-in-ring']).includes(row.id))||g.is_live));
  if(currents.length>1)problems.push('multiple_current_groups');
  const current=currents.length===1?currents[0]:null;
  const nextKeys=String(f['next-group']||'').split(',').map(s=>s.trim());
  const upcoming=latest.filter(g=>g.status==='Upcoming').sort((a,b)=>(nextKeys.includes([show,day,b.group].join('|'))?1:0)-(nextKeys.includes([show,day,a.group].join('|'))?1:0)||(a.sequence??Infinity)-(b.sequence??Infinity));
  const next=upcoming[0]??null,completed=latest.filter(g=>g.status==='Completed').sort((a,b)=>(b.sequence??-Infinity)-(a.sequence??-Infinity)),subject=current??completed[0]??next;
  let projected=null,offset=null,calculation=null,state='unavailable',asOf=original!==null&&original<=nowMs?new Date(original).toISOString():null;
  if(subject){const times=scheduleTimes(subject),at=Date.parse(subject.at);let expected=times.end??(times.start!==null?times.start+subject.total*2.95*60000:null);
   calculation={group_id:subject.group,group_name:subject.name,status:subject.status,is_live:subject.is_live,total:subject.total,gone:subject.gone,estimated_start:subject.start,source_updated_at:subject.source_updated_at,scheduled_start:times.start===null?null:new Date(times.start).toISOString(),scheduled_end:times.end===null?null:new Date(times.end).toISOString(),comparison_end:expected===null?null:new Date(expected).toISOString(),finish_basis:subject.status==='Completed'?'first_seen_completed':subject===current?'pace_projection':'unavailable'};
   if(subject.status==='Completed'){
    const evidence=combined.filter(o=>receipt(o)&&measuredKey(o)===measuredKey(subject)&&Date.parse(o.at)<=nowMs).sort((a,b)=>Date.parse(a.at)-Date.parse(b.at));
    projected=evidence.length?Date.parse(evidence[0].at):null;if(projected===null)problems.push('completed_time_unavailable:'+subject.group);
    if(subject.gone!==subject.total)problems.push('completed_count_conflict:'+subject.group);
   }
   else if(subject===current&&pace!==null&&asOf){projected=Date.parse(asOf)+(subject.total-subject.gone)*pace*60000;}
   else if(subject.status==='Upcoming'&&asOf&&times.start!==null){offset=(Date.parse(asOf)-times.start)/60000;}
   if(projected!==null&&expected!==null)offset=(projected-expected)/60000;
   if(offset!==null){const rounded=Math.round(offset);state=rounded>0?'late':rounded<0?'early':'on_time';}
  }
  const classInfo=g=>{if(!g)return null;return{group_id:g.group,group_name:g.name,classes:g.members.map((id,i)=>{const c=classes.find(c=>c.fields['class-key']===[show,day,id].join('|'));return{id,name:c?.fields['class-name']??g.class_names[i]??null,number:c?.fields['class-number']??null};})};};
  if(!asOf)problems.push('ring_as_of_unavailable');if(pace===null)problems.push('pace_evidence_unavailable');
  results.push({identity:{customer,show,date:day,ring:rn,ring_id:ringId},ring_name:f['ring-name'],as_of:asOf,evaluated_at:now,pace_minutes_per_trip:pace,pace_as_of:paceAt,pace_method:method,ring_state:state,offset_minutes:offset===null?null:Math.round(offset*10000)/10000,projected_finish:projected===null?null:new Date(projected).toISOString(),current_class:classInfo(current),next_class:classInfo(next),completed_groups:completed.map(g=>g.group),calculation,observations:latest,measurement:{minutes,completions,intervals,estimated_starts:estimatedStarts},issues:[...new Set(problems)]});for(const p of new Set(problems))report(ringId,p);
 }
 return{results,issues};
}


function ringCalculationFields(result){
 const c=result.calculation??{},round=v=>v===null||v===undefined?null:Math.round(v*10000)/10000;
 const time=at=>at?new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23',timeZoneName:'short'}).format(new Date(at)):null;
 const classNames=group=>group?group.classes.map(c=>[c.number,c.name].filter(v=>v!==null&&v!==undefined).join(' ')).join('\n'):null;
 return{
  'calculation-group-id':c.group_id??null,'calculation-group':c.group_name??null,status:c.status??null,'is-live':c.is_live===undefined?null:String(c.is_live),
  total:c.total??null,gone:c.gone??null,'remaining-trips':c.total===undefined?null:c.total-c.gone,'source-updated-at':c.source_updated_at===null||c.source_updated_at===undefined?null:String(c.source_updated_at),
  'estimated-start-time':c.estimated_start??null,'scheduled-start-time':time(c.scheduled_start),'scheduled-end-time':time(c.scheduled_end),'comparison-end-time':time(c.comparison_end),
  'projected-finish':time(result.projected_finish),'finish-basis':c.finish_basis??null,
  'pace-elapsed-minutes':round(result.measurement.minutes),'pace-completed-trips':result.measurement.completions,'pace-as-of':time(result.pace_as_of),
  'ring-state':result.ring_state,'minutes-early-late':result.offset_minutes,'current-class':classNames(result.current_class),'next-class':classNames(result.next_class),
  'pace-evidence':result.pace_method==='estimated_start'?(result.measurement.estimated_starts||[]).map(i=>'Group '+i.group+' | estimated start '+time(i.from)+' -> '+time(i.to)+' '+i.status+' | '+round(i.elapsed_minutes)+' minutes / '+i.completed_trips+' trips | assumed start').join('\n'):result.measurement.intervals.map(i=>'Group '+i.group+' | '+i.phase+' | '+time(i.from)+' '+i.from_status+' gone '+i.gone_from+' -> '+time(i.to)+' '+i.to_status+' gone '+i.gone_to+' | '+round(i.elapsed_minutes)+' minutes / '+i.completed_trips+' trips').join('\n')
 };
}

function ringSourceLinks({showRow,dayRows,ring,result,groups,schedules,classes}){
 const {customer,show,date:day,ring:rn}=result.identity,dayKey=[customer,show,day].join('|');
 const ringGroups=groups.filter(g=>g.fields.date===day&&g.fields['group-key']?.startsWith(show+'|'+day+'|')&&links(g.fields.ring).includes(ring.id)&&number(g.fields['class-group-id'])===result.calculation?.group_id);
 const selected=result.observations.find(g=>g.group===result.calculation?.group_id);
 const memberIds=new Set([...(selected?.members||[]),...(result.next_class?.classes||[]).map(c=>c.id)]);
 const source={
  'w-shows':[showRow.id],
  'w-days':dayRows.filter(d=>d.fields['show-day-key']===dayKey).map(d=>d.id),
  'wef-live.rings':[ring.id],
  'wef-live.groups':ringGroups.map(g=>g.id),
  'wef-data.schedules':schedules.filter(s=>s.fields['w-days']===dayKey&&s.fields['w-shows']===customer+'|'+show&&number(s.fields.ring?.split('|').at(-1))===rn&&number(s.fields.group?.split('|').at(-1))===result.calculation?.group_id&&s.fields.kind==='class'&&!String(s.fields['record-status']||'').startsWith('Dropped')).map(s=>s.id),
  'wef-live.classes':classes.filter(c=>c.fields.date===day&&memberIds.has(number(c.fields['class-id']))&&c.fields['class-key']===[show,day,c.fields['class-id']].join('|')).map(c=>c.id)
 };
 return Object.fromEntries(Object.entries(source).map(([field,ids])=>[field,[...new Set(ids)].sort()]));
}

async function runRingSnapshots({api,now,runId,root=require('node:path').resolve(__dirname,'..'),readHistory=savedRingHistory}){
 const {TIMING,DATA_TABLES:T}=require('./api'),{writeAnswers}=require('./data-source'),{localDay}=require('./purge');const day=localDay(now),report={day,run_id:runId,started:now,ok:false,rings:[],issues:[],written:0};
 const warn=message=>report.issues.push({message});
 const read=async(table,formula)=>{try{return await api.list(TIMING,table,formula);}catch(e){warn('source_read:'+table+':'+e.message);return[];}};
 try{
  const shows=await read(RING_INPUTS.shows),defaults=shows.filter(s=>s.fields.is_default===true);if(defaults.length!==1){warn('current_show_identity_unavailable');return report;}
  const show=number(defaults[0].fields.show_id),customer=number(defaults[0].fields.customer_id);if(!show||!customer){warn('invalid_show_identity');return report;}
  const dayRows=await read(RING_INPUTS.days,`{show-day-key}='${customer}|${show}|${day}'`);if(dayRows.length!==1)warn('today_show_day_missing_or_duplicate');
  const rings=await read(RING_INPUTS.rings,`AND({show-id}=${show},{customer-id}=${customer},{date}='${day}')`),groups=await read(RING_INPUTS.groups,`{date}='${day}'`),classes=await read(RING_INPUTS.classes,`{date}='${day}'`),schedules=await read(RING_INPUTS.schedules,`{w-days}='${customer}|${show}|${day}'`);
  const retained=await read(T.answers,`AND({answer-kind}='ring-observation',{show-id}=${show},DATETIME_FORMAT({date},'YYYY-MM-DD')='${day}')`),history=readHistory({show,day,root,issues:report.issues});
  for(const r of retained){try{const p=JSON.parse(r.fields['payload-json']);if(p.policy==='ring-state-v1')history.push(...p.result.observations);else if(p.policy==='ring-observation-v2'){const o=p.history.observation,s=o.selected;if(s)history.push({show,day,ringId:o.ringId,ringNumber:o.proof.ring.fields['ring-number'],group:s.id,phase:s.phase,total:s.total,gone:s.gone,at:s.observedAt,members:s.members,ref:'timing-answers:'+r.id});}}catch{warn('unreadable_saved_observation:'+r.id);}}
  for(const ring of rings){try{
   const batch=calculateRingState({show,customer,day,now,rings:[ring],groups,classes,schedules,history});report.issues.push(...batch.issues);const result=batch.results[0];if(!result)continue;
   const sourceLinks=ringSourceLinks({showRow:defaults[0],dayRows,ring,result,groups,schedules,classes});
   const key=[customer,show,day,result.identity.ring_id].join('|'),payload={schema_version:1,policy:'ring-state-v1',run_id:runId,result};
   const normalizeLinks=row=>({...row,fields:{...Object.fromEntries(Object.keys(sourceLinks).map(field=>[field,[]])),...row.fields}});
   const answerApi={list:async(...args)=>(await api.list(...args)).map(normalizeLinks),upsert:async(...args)=>{const response=await api.upsert(...args);return{...response,records:response.records?.map(normalizeLinks)};}};
   const base={...sourceLinks,...ringCalculationFields(result),'scope-key':'ring-state|'+key,'source-lane':'data_live','show-id':show,date:day,'ring-number':result.identity.ring,'group-id':result.current_class?.group_id??null,phase:'base',availability:result.pace_minutes_per_trip===null?'unavailable':'available','pace-method':result.pace_method,'pace-minutes-per-trip':result.pace_minutes_per_trip===null?null:Math.round(result.pace_minutes_per_trip*10000)/10000,'evaluated-at':now,'observed-at':result.as_of,'reason-codes-json':JSON.stringify(result.issues),'input-refs-json':JSON.stringify([{table:RING_INPUTS.rings,record:ring.id}]),'payload-json':JSON.stringify(payload),'content-hash':hash(payload)};
   if(base['payload-json'].length>95000){warn('ring_payload_too_large:'+result.identity.ring_id);continue;}
   await writeAnswers(answerApi,[{...base,'answer-key':'ring-snapshot|'+key+'|'+runId,'answer-kind':'ring-observation'}]);
   await writeAnswers(answerApi,[{...base,'answer-key':'ring-current|'+JSON.stringify([customer,show,day,result.identity.ring_id]),'answer-kind':'ring-current'}]);
   report.written+=2;report.rings.push({ring:result.identity.ring,name:result.ring_name,as_of:result.as_of,pace:result.pace_minutes_per_trip,state:result.ring_state,offset_minutes:result.offset_minutes,current_class:result.current_class,next_class:result.next_class});
  }catch(e){warn('ring_failed:'+ring.id+':'+e.message);}}
  if(!rings.length)warn('today_rings_unavailable');report.ok=rings.length>0&&report.rings.length===rings.length;
 }catch(e){warn(e.message);}
 const messages=[...new Set(report.issues.map(i=>typeof i==='string'?i:(i.ring_id??'')+':'+i.message))];
 for(let i=0;i<messages.length;i+=10){try{await api.upsert('tbliHowblBdSL51ov','error-key',messages.slice(i,i+10).map((message,j)=>({'error-key':'ring-state|'+runId+'|'+(i+j),'run-id':runId,'occurred-at':now,date:day,message})));}catch(e){console.error('Ring issue logging:',e.message);break;}}
 report.completed=new Date().toISOString();return report;
}
module.exports={...module.exports,RING_INPUTS,easternTime,groupObservation,savedRingHistory,calculateRingState,ringCalculationFields,ringSourceLinks,runRingSnapshots};
