// Same Astro identity and recoverable save journal as Profile. Log inputs only; never patch synced sources.
import {remembered} from '../rs-recognition-iam.js';
import {readTenantSave,runTenantSave,payloadHash} from '../rs-tenant-save-operation.js';
import {InputError,digest} from '../rs-inputs.js';
import {sharedBase,recordId,equals} from './shared-base.mjs';
import {validHorseFields} from '../../../public/wef-schedule-v2/horse-fields.js';
const fail=(code,status=400)=>{throw new InputError(code,status);};
export async function logHorseInput(request,env,{fetchImpl=fetch,identify=remembered,journal={read:readTenantSave,run:runTenantSave},api=sharedBase({token:env.AIRTABLE_TOKEN,fetchImpl,writable:true})}={}){
 const identity=await identify(request,env);
 if(!identity||identity.identity_only!==false)fail('authentication_required',401);
 const actor=identity.person_uid;
 if(request.headers.get('X-RS-Actor-Scope')!==actor)fail('actor_changed',409);
 const origin=request.headers.get('Origin');if(origin&&origin!==new URL(request.url).origin)fail('invalid_origin',403);
 const text=await request.text();if(text.length>6000)fail('input_too_large',413);
 let body;try{body=JSON.parse(text);}catch{fail('invalid_input');}
 if(!body||typeof body!=='object'||Array.isArray(body))fail('invalid_input');
 if(!/^[a-f0-9-]{36}$/i.test(body.requestId||'')||!recordId(body.horseId)||!recordId(body.showId)||!validHorseFields(body.fields)||!Number.isFinite(Date.parse(body.submittedAt))||typeof body.name!=='string'||body.name.length>200)fail('invalid_horse_input');
 const input={requestId:body.requestId,horseId:body.horseId,showId:body.showId,fields:body.fields,submittedAt:body.submittedAt,name:body.name};
 const inputHash=await payloadHash({actor,input}),eventId=await digest('schedule-horse-input|'+actor+'|'+input.requestId),saved=await journal.read(env,eventId);
 if(saved.row){const prior=JSON.parse(saved.row.result_json||'{}');if(prior.actorId!==actor||prior.payloadHash!==inputHash)fail('request_id_reused',409);if(saved.row.state==='complete')return {ok:true,requestId:input.requestId,personUid:actor,logId:prior.record.id};}
 const horse=await api.read('horses',`RECORD_ID()='${input.horseId}'`);if(horse.length!==1)fail('horse_unavailable',404);
 const fields={'change-key':eventId,'request-id':input.requestId,'person-uid':actor,'entity-type':'horse','record-id':input.horseId,'record-name':input.name,'show-id':input.showId,'changes-json':JSON.stringify(input.fields),'submitted-at':input.submittedAt,source:'wef-schedule-app',status:'Submitted'};
 const result=await journal.run({env,key:saved.key,prior:saved.row,context:{actorId:actor,payloadHash:inputHash,envelope:{event:{eventId,inputHash}}},work:async op=>{
  const record=await op.step('schedule-input-log',async()=>{const rows=await api.read('changes',equals('change-key',eventId));if(!rows.length)return null;if(rows.length!==1||Object.entries(fields).some(([k,v])=>rows[0][k]!==v))fail('change_log_conflict',409);return rows[0];},()=>api.upsert('changes',fields));
  return {record:{id:record.id}};
 }});
 return {ok:true,requestId:input.requestId,personUid:actor,logId:result.record.id};
}
