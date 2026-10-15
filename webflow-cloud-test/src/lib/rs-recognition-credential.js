import {createInputRecognition} from './rs-inputs-recognition.js';
import {createInputAccess, accessHash} from './rs-inputs-access.js';
import {requireClaimDatabase, claimOnce, completeClaim} from './rs-recognition-claims.js';

const fail=(code,status=400)=>{const e=new Error(code);e.code=code;e.status=status;throw e;};
export async function handleRecognitionCredential({request,env,payload,fetchImpl=fetch}) {
  if(request.headers.get('Origin')!==new URL(request.url).origin)fail('origin_denied',403);
  if(env.RS_INPUTS_WRITE_MODE!=='isolated-trial')fail('recognition_writes_disabled',503);
  const {kind,value,phone,request_id}=payload;
  if(!/^[a-f0-9-]{36}$/i.test(request_id||''))fail('invalid_request_id');
  if(!['phone','pin','invite'].includes(kind)||typeof value!=='string')fail('invalid_credential');
  const normalize=v=>{const digits=String(v||'').replace(/\D/g,'');const local=digits.length===11&&digits[0]==='1'?digits.slice(1):digits;if(!/^\d{10}$/.test(local))fail('invalid_phone');return '+1'+local;};
  const normalized=kind==='phone'?normalize(value):value;
  if(kind==='pin'&&!/^\d{4}$/.test(value))fail('invalid_pin');
  if(kind==='invite'&&!/^\d{7}$/.test(value))fail('invalid_invitation');
  const associatedPhone=phone?normalize(phone):'';
  // Existing D1 claims provide durable same-operation replay, not another store.
  const identity=await accessHash(JSON.stringify([kind,normalized,associatedPhone]));
  const claim=await claimOnce(env,'recognition-credential',[request_id,identity]);
  if(!claim.won) {
    if(claim.result)return claim.result;
    fail('credential_outcome_unknown',409);
  }
  const db=requireClaimDatabase(env);
  const bucket=await accessHash('credential-rate:'+String(request.headers.get('cf-connecting-ip')||'unknown')+':'+Math.floor(Date.now()/600000));
  const rate=await db.prepare(`INSERT INTO recognize_claims(claim_key,state,result_json) VALUES (?,'complete','{"count":1}')
    ON CONFLICT(claim_key) DO UPDATE SET result_json=json_set(result_json,'$.count',json_extract(result_json,'$.count')+1)
    WHERE json_extract(result_json,'$.count')<5 RETURNING result_json`).bind(bucket).first();
  if(!rate)return completeClaim(env,claim,{ok:true,verified:false,reason:'retry_limit'});
  let candidate;
  if(kind==='invite') {
    // Existing hash/expiry and durable one-use claim remain the authority.
    try {candidate={personUid:(await createInputAccess({env,fetchImpl}).redeem(request,value)).actor.id};}
    catch(error){if(error.code==='invalid_invitation')return completeClaim(env,claim,{ok:true,verified:false,reason:'invalid_invitation'});throw error;}
  } else {
    const entries=(request.headers.get('Cookie')||'').split(';').map(v=>v.trim()).filter(v=>v.startsWith('__Host-rs_recognition_device='));
    const token=entries.length===1?entries[0].split('=')[1]:null;
    try {candidate=await createInputRecognition({env,fetchImpl,verifiedInputAccess:true}).credentialCandidate({
      phone:kind==='phone'?normalized:associatedPhone,pin:kind==='pin'?value:'',deviceToken:token});}
    catch(error){if(error.code==='ambiguous_pin')return completeClaim(env,claim,{ok:true,verified:false,reason:'phone_required'});throw error;}
  }
  if(!candidate)return completeClaim(env,claim,{ok:true,verified:false,reason:'not_matched'});
  const deviceToken=crypto.randomUUID();
  const bound=await createInputRecognition({env,fetchImpl,verifiedInputAccess:true,principalPersonUid:candidate.personUid}).action({
    action:'confirm_device',requestId:request_id,device_token:deviceToken,values:{}},request);
  if(!bound.recognized)fail('credential_binding_unconfirmed',503);
  // Successful recognition must not exhaust a shared venue's failed-guess budget.
  await db.prepare(`UPDATE recognize_claims SET result_json=json_set(result_json,'$.count',MAX(0,json_extract(result_json,'$.count')-1)) WHERE claim_key=?`).bind(bucket).run();
  return completeClaim(env,claim,{ok:true,verified:true,device_token:deviceToken});
}
