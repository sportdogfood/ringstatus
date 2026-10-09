// Native personal alert preferences. Reuses subscription storage and reservations;
// these records do not choose an engine or activate message delivery.
import { InputError, digest } from './rs-inputs.js';
import { withSubscriptionReservation } from './rs-input-subscription-control.js';
const fail=(code,status=400)=>{throw new InputError(code,status)};
export function validatePreferences(value,definitions){
 if(!value||value.version!==1||typeof value.enabled!=='boolean'||typeof value.phone!=='string'||typeof value.timeZone!=='string'||!value.variables||Object.keys(value).some(k=>!['version','enabled','phone','timeZone','variables'].includes(k)))fail('invalid_preferences');
 try{new Intl.DateTimeFormat('en',{timeZone:value.timeZone}).format();}catch{fail('invalid_time_zone');}
 if(!value.timeZone||value.timeZone.length>80||value.phone.length>30||Object.keys(value.variables).length!==definitions.length)fail('invalid_preferences');
 const variables={};
 for(const def of definitions){
  const p=value.variables[def.key];
  if(!p||typeof p.enabled!=='boolean'||typeof p.value!=='string'||Object.keys(p).some(k=>!['enabled','value'].includes(k)))fail('invalid_preferences');
  if(p.value&&(def.presets?!def.presets.map(String).includes(p.value):def.input==='time'?!/^([01]\d|2[0-3]):[0-5]\d$/.test(p.value):true))fail('invalid_alert_value');
  if(value.enabled&&p.enabled&&def.input&&!p.value)fail('alert_value_required');
  variables[def.key]={enabled:p.enabled,value:p.value};
 }
 let phone=value.phone.trim();
 if(phone){
  if(!/^\+?[\d ()-]+$/.test(phone))fail('invalid_phone');
  let digits=phone.replace(/\D/g,'');if(!phone.startsWith('+')&&digits.length===10)digits='1'+digits;
  phone='+'+digits;if(!/^\+[1-9]\d{7,14}$/.test(phone))fail('invalid_phone');
 }
 if(value.enabled&&(!phone||!Object.values(variables).some(p=>p.enabled)))fail('phone_and_alert_required');
 return {version:1,enabled:value.enabled,phone,timeZone:value.timeZone,variables};
}
export async function personalPreferences({request,payload,actor,store,env,definitions,clock}){
 const id='rs_'+(await digest('sms-preferences|'+actor.id)).slice(0,32);
 const owned=row=>{if(row&&(row.owner_uid!==actor.id||row.target_type!=='Person'||row.target_uid!==actor.id||!row.preferences_json||row.record_mode!=='Test'))fail('preference_scope_mismatch',409);return row;};
 const output=row=>({ok:true,preferences:row?validatePreferences(JSON.parse(row.preferences_json),definitions):null,revision:row?.revision||0});
 if(request.method==='GET')return output(owned(await store.byId(id)));
 if(!actor.permissions.includes('inputs:write'))fail('permission_denied',403);
 if(Object.keys(payload).some(k=>!['action','requestId','expectedRevision','preferences'].includes(k))||!/^\w[\w-]{7,127}$/.test(payload.requestId||''))fail('invalid_payload');
 const preferences=validatePreferences(payload.preferences,definitions);
 const eventId=await digest('subscriptions|'+actor.id+'|'+payload.requestId);
 const fingerprint=await digest(JSON.stringify({preferences,expectedRevision:payload.expectedRevision??null}));
 return withSubscriptionReservation(env,{actorId:actor.id,eventId,fingerprint},async guard=>{
  const current=owned(await store.byId(id));
  const replay=await store.event(eventId);
  if(replay){
   if(replay.inputHash!==fingerprint)fail('request_id_reused',409);
   if(replay.record.id!==id||replay.record.ownerUid!==actor.id||replay.record.targetType!=='Person'||!current)fail('invalid_audit_record',502);
   return output(current);
  }
  const audit=async record=>{
   guard.markWriteAttempted();
   const at=clock();if(!Number.isFinite(Date.parse(at)))fail('invalid_server_time',503);
   await store.appendEvent({eventId,actorId:actor.id,requestId:payload.requestId,inputHash:fingerprint,occurredAt:at,
    action:preferences.enabled?'consent_granted':'consent_revoked',
    record:{id,ownerUid:actor.id,targetType:'Person',targetUid:actor.id,revision:record.revision,status:record.status,consentState:record.consent_state,consentAt:record.consent_at,consentSource:record.consent_source,noticeVersion:record.notice_version,revokedAt:record.revoked_at||null}});
  };
  if(current?.request_uid===eventId){if(current.request_hash!==fingerprint)fail('request_id_reused',409);await audit(current);return output(current);}
  if(guard.recoveryOnly)fail('subscription_reconciliation_required',503);
  if(current){
   const prior=await store.event(current.request_uid);
   if(!prior||prior.inputHash!==current.request_hash||prior.record.id!==id||prior.record.revision!==current.revision||prior.record.ownerUid!==actor.id)fail('prior_audit_pending',409);
  }
  if(current?payload.expectedRevision!==current.revision:![undefined,0].includes(payload.expectedRevision))fail('record_changed',409);
  const at=clock();if(!Number.isFinite(Date.parse(at)))fail('invalid_server_time',503);
  const record={entity_uid:id,target_type:'Person',target_uid:actor.id,owner_uid:actor.id,
   preferences_json:JSON.stringify(preferences),phone_e164:preferences.phone,
   status:preferences.enabled?'Draft':'Revoked',consent_state:preferences.enabled?'Granted':'Revoked',
   consent_at:at,consent_source:'/rs-barn-onboarding-alerts:explicit-save',notice_version:'sms-preferences-v1',
   revoked_at:preferences.enabled?null:at,revision:(current?.revision||0)+1,request_uid:eventId,request_hash:fingerprint,record_mode:'Test',alert_types:''};
  guard.markWriteAttempted();const saved=await store.put(record,current?.revision);await audit(saved);return output(saved);
 });
}
