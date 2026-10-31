'use strict';
const {hash}=require('../wef-timing/pace');
const {instant}=require('../wef-timing/class-timing');
function material(payload){
 const {evaluated_at,minutes_until_start,minutes_since_start,...value}=payload;
 if(!Array.isArray(payload.input_refs))throw Error('Class provenance missing');
 value.input_refs=payload.input_refs.map(({producer_proof,...ref})=>ref);
 return value;
}
// Recompute both saved bytes and the current class adapter result. A copied hash
// field is not proof, and a valid old hash is not proof of current source inputs.
function verify(saved,expected,now){
 try{
  const f=saved?.fields,e=expected.fields;
  if(!/^rec[A-Za-z0-9]{14}$/.test(saved?.id||'')||!f) return false;
  const p=JSON.parse(f['payload-json']),refs=JSON.parse(f['input-refs-json']);
  const value=material(p),at=instant(f['evaluated-at']);
  if(at===null||at>instant(now)||p.evaluated_at!==f['evaluated-at']
   ||hash(value)!==f['content-hash']||hash(material(expected.payload))!==f['content-hash']
   ||hash(refs)!==hash(p.input_refs)||hash(JSON.parse(f['reason-codes-json']))!==hash(p.reasons))return false;
  for(const name of ['scope-key','answer-kind','source-lane','show-id','date','ring-number','group-id','phase','availability','pace-method','pace-minutes-per-trip','observed-at'])
   if(f[name]!==e[name])return false;
  return typeof f['answer-key']==='string'&&f['answer-key'].startsWith(e['scope-key']+'|')
   &&(f['answer-key']===e['scope-key']+'|'+f['content-hash']||f['answer-key'].endsWith('|'+f['content-hash'].slice(0,16)));
 }catch{return false;}
}
// Verify persisted bytes/identity first, then compare only the dependencies of
// each consumed component against the current authoritative recomputation.
function components(saved,expected,now){
 try{
  const f=saved?.fields,p=JSON.parse(f['payload-json']),e=expected.payload;
  if(!/^rec[A-Za-z0-9]{14}$/.test(saved.id)||p.policy!=='class-timing-v1'||p.source_lane!=='data_live'
   ||hash(p.identity)!==hash(e.identity)||p.timezone!==e.timezone)return null;
  const at=instant(p.evaluated_at);
  if(at===null||at>instant(now)||f['evaluated-at']!==p.evaluated_at||hash(material(p))!==f['content-hash']
   ||hash(JSON.parse(f['input-refs-json']))!==hash(p.input_refs)||hash(JSON.parse(f['reason-codes-json']))!==hash(p.reasons))return null;
  const i=p.identity,projected={'scope-key':expected.fields['scope-key'],'answer-kind':'class-timing','source-lane':'data_live',
   'show-id':i.show,date:i.date,'ring-number':i.ring,'group-id':i.group,phase:i.phase,
   availability:p.estimated_end_at?'partial':'unavailable','pace-method':p.pace_method,
   'pace-minutes-per-trip':Math.round(p.pace_minutes_per_trip*10000)/10000,
   'observed-at':p.input_refs.length?p.input_refs.map(r=>r.observed_at).sort((a,b)=>instant(a)-instant(b))[0]:''};
  if(Object.entries(projected).some(([k,v])=>f[k]!==v)||typeof f['answer-key']!=='string'
   ||!f['answer-key'].startsWith(projected['scope-key']+'|')
   ||!(f['answer-key']===projected['scope-key']+'|'+f['content-hash']||f['answer-key'].endsWith('|'+f['content-hash'].slice(0,16))))return null;
  const refs=(x,role)=>x.input_refs.filter(r=>r.role===role).map(({producer_proof,...r})=>r);
  const same=(keys,role)=>keys.every(k=>hash(p[k])===hash(e[k]))&&hash(refs(p,role))===hash(refs(e,role));
  const pace=same(['pace_minutes_per_trip','pace_method','pace_answer_key'],'pace');
  const start=same(['start_at','start_kind','start_source'],'start');
  if(!pace&&!start)return null;
  return{pace_minutes_per_trip:pace?p.pace_minutes_per_trip:null,pace_method:pace?p.pace_method:'unknown',
   start_at:start?p.start_at:null,start_kind:start?p.start_kind:'unknown'};
 }catch{return null;}
}
module.exports={material,verify,components};
