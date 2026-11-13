// Schedule extension of Profile's device-first pending-save pattern. Astro confirms the input log only.
import {validHorseFields} from './horse-fields.js';
export function createHorseEdits({endpoint,storage=globalThis.localStorage,fetchImpl=fetch,onChange=()=>{},onStatus=()=>{}}){
 let uid=null,device=null,generation=0,values={},pending=[];const running=new Map();
 const key=id=>'rs_schedule_horse_inputs_v1:'+id;
 function retain(){storage.setItem(key(uid),JSON.stringify({uid,device,values,pending}));}
 function notifyStatus(){onStatus(pending.length?'Saved on device · Pending':Object.keys(values).length?'Submitted':'');}
 function setIdentity(identity){
  const next=identity?.uid||null,fingerprint=identity?.fingerprint||null;if(next===uid&&fingerprint===device)return;
  generation++;uid=next;device=fingerprint;values={};pending=[];
  if(uid)try{const saved=JSON.parse(storage.getItem(key(uid))||'null');if(saved?.uid===uid){pending=Array.isArray(saved.pending)?saved.pending.filter(p=>p.uid===uid&&p.body?.requestId&&validHorseFields(p.body.fields)):[];if(saved.device===device)values=saved.values||{};}}catch{}
  notifyStatus();onChange();if(uid)void flush();
 }
 function fields(horse){return {...values[horse.horseId],nickname:values[horse.horseId]?.nickname??horse.name};}
 function apply(rows){return rows.map(row=>({...row,entries:row.entries.map(e=>{const v=values[e.horseId];return {...e,...v,name:v?.nickname||e.name,canEdit:Boolean(uid&&device)};})}));}
 function save(horse,showId,input){
  if(!uid||!device)throw Error('Sign in from Profile to edit this horse.');
  if(!validHorseFields(input))throw Error('Check the horse details.');
  const prior=fields(horse),changed=Object.fromEntries(Object.entries(input).map(([k,v])=>[k,v.trim()]).filter(([k,v])=>(prior[k]||'')!==v));
  if(!Object.keys(changed).length)return;
  const oldValues=values,oldPending=pending,body={requestId:crypto.randomUUID(),horseId:horse.horseId,name:horse.name,showId,fields:changed,submittedAt:new Date().toISOString()};
  values={...values,[horse.horseId]:{...values[horse.horseId],...changed}};pending=[...pending,{uid,body}];
  try{retain();}catch{values=oldValues;pending=oldPending;throw Error('The device could not retain this edit. Try again.');}
  notifyStatus();onChange();void drain(horse.horseId);
 }
 function drain(horseId){
  const actor=uid,check=generation,scope=actor+'|'+horseId;if(!actor)return Promise.resolve();if(running.has(scope))return running.get(scope);
  const work=(async()=>{while(actor===uid&&check===generation){const entry=pending.find(p=>p.uid===actor&&p.body.horseId===horseId);if(!entry)return;
   try{
    const response=await fetchImpl(endpoint,{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-RS-Actor-Scope':actor},body:JSON.stringify(entry.body),signal:AbortSignal.timeout(20000)});
    const result=await response.json();if(actor!==uid||check!==generation)return;
    if(!response.ok||!result.ok||result.requestId!==entry.body.requestId||result.personUid!==actor||!result.logId){if([401,403].includes(response.status))setIdentity(null);return;}
    pending=pending.filter(p=>p.body.requestId!==entry.body.requestId);retain();notifyStatus();
   }catch{return;}
  }})();running.set(scope,work);void work.finally(()=>{if(running.get(scope)===work)running.delete(scope);});return work;
 }
 function flush(){return Promise.all([...new Set(pending.map(p=>p.body.horseId))].map(drain));}
 return {get ready(){return Boolean(uid&&device);},get pendingCount(){return pending.length;},setIdentity,fields,apply,save,flush};
}
