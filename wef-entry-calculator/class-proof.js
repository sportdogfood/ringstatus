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
module.exports={material,verify};
