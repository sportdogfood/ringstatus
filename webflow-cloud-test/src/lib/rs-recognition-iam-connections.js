import {claimOnce,completeClaim,requireClaimDatabase} from './rs-recognition-claims.js';
import {loadIamSnapshot} from './rs-recognition-iam-refresh.js';
import {recordRecognitionSession,recordRecognitionSummary} from './rs-recognition-session.js';
import {syncIamHistoryDevice} from './rs-recognition-action.js';
import {accessHash} from './rs-inputs-access.js';

export async function queueIamRefresh({env,operationId,executionContext,fetchImpl=fetch}){
 if(!env.RS_RECOGNITION_CONTROL_DB)return;
 const claim=await claimOnce(env,'iam-refresh',[operationId]);
 if(claim.won)await completeClaim(env,claim,{work:'iam-refresh',pending:true});
 if(executionContext?.waitUntil)executionContext.waitUntil(drainIamRefresh({env,fetchImpl}).catch(()=>console.error('[rs-recognition] IAM refresh retained for retry')));
}
export async function drainIamRefresh({env,fetchImpl=fetch}){
 const db=requireClaimDatabase(env),rows=(await db.prepare("SELECT claim_key,result_json FROM recognize_claims WHERE state='complete' AND json_extract(result_json,'$.work')='iam-refresh' AND json_extract(result_json,'$.pending')=1 LIMIT 20").all()).results;
 if(!rows.length)return {refreshed:false};
 await db.prepare("DELETE FROM recognize_claims WHERE claim_key='iam-refresh-lock-v1' AND json_extract(result_json,'$.expiresAt')<=?").bind(Date.now()).run();
 const lock={owner:crypto.randomUUID(),expiresAt:Date.now()+60000},won=await db.prepare("INSERT INTO recognize_claims(claim_key,state,result_json) VALUES ('iam-refresh-lock-v1','claimed',?) ON CONFLICT(claim_key) DO NOTHING").bind(JSON.stringify(lock)).run();
 if(won.meta.changes!==1)return {refreshed:false,pending:true};
 try{const deadline=AbortSignal.timeout(20000);const boundedFetch=(url,options={})=>fetchImpl(url,{...options,signal:options.signal?AbortSignal.any([deadline,options.signal]):deadline});await loadIamSnapshot({env,fetchImpl:boundedFetch});await db.batch(rows.map(row=>db.prepare("UPDATE recognize_claims SET result_json=? WHERE claim_key=? AND result_json=?").bind(JSON.stringify({work:'iam-refresh',pending:false}),row.claim_key,row.result_json)));return {refreshed:true};}
 finally{await db.prepare("DELETE FROM recognize_claims WHERE claim_key='iam-refresh-lock-v1' AND result_json=?").bind(JSON.stringify(lock)).run();}
}
// Separate existing session operation: recognition/sign-out themselves never call Airtable.
export async function exportIamHistory({env,request,eventUid,fetchImpl=fetch}){
 if(!/^[a-f0-9-]{36}$/i.test(eventUid||''))throw Error('invalid_history_event');
 const db=requireClaimDatabase(env),key=await accessHash(JSON.stringify(['iam-audit',[eventUid]]));
 const row=await db.prepare("SELECT result_json FROM recognize_claims WHERE claim_key=? AND state='complete'").bind(key).first();
 if(!row)throw Error('history_pending');const entry=JSON.parse(row.result_json);
 if(entry.exported)return {exported:true,duplicate:true};
 if(!entry.session_uid)throw Error('history_session_missing');
 const lockKey='iam-history-lock:'+await accessHash(entry.device_token||eventUid),lock=JSON.stringify({owner:crypto.randomUUID(),expiresAt:Date.now()+60000});
 await db.prepare("DELETE FROM recognize_claims WHERE claim_key=? AND json_extract(result_json,'$.expiresAt')<=?").bind(lockKey,Date.now()).run();
 const acquired=await db.prepare("INSERT INTO recognize_claims(claim_key,state,result_json) VALUES (?,'claimed',?) ON CONFLICT(claim_key) DO NOTHING").bind(lockKey,lock).run();
 if(acquired.meta.changes!==1)throw Error('history_pending');
 const originalFetch=fetchImpl,deadline=AbortSignal.timeout(20000);fetchImpl=(url,options={})=>originalFetch(url,{...options,signal:options.signal?AbortSignal.any([deadline,options.signal]):deadline});
 try{
 let device;
 const tokenHash=entry.device_token?await accessHash(entry.device_token):null;
 const state=tokenHash?await db.prepare('SELECT retired FROM recognize_iam_devices WHERE token_hash=?').bind(tokenHash).first():null;
 if(entry.person_record_id&&entry.device_token)device=await syncIamHistoryDevice({env,fetchImpl,deviceToken:entry.device_token,personRecordId:entry.person_record_id,retired:state?.retired===1});
 const payload={session_event_uid:entry.event_uid,session_uid:entry.session_uid,idempotency_key:'iam:'+entry.event_uid,event_type:entry.event_type,event_at:entry.at,event_result:entry.person_record_id?'matched':'not_matched',recognition_status:entry.person_record_id?'confirmed':'rejected',matched_by:entry.matched_by||'device_token',person_record_id:entry.person_record_id,device_record_id:device?.id,page_path:entry.page_path,detail:{source:'cloudflare_iam'}};
 await recordRecognitionSession({env,fetchImpl,geoFetchImpl:fetchImpl,request,payload});
 if(device)try{await recordRecognitionSummary({env,fetchImpl,sessionUid:entry.session_uid,tokenFingerprint:tokenHash,personRecordId:entry.person_record_id,deviceRecordId:device.id,recognized:entry.event_type!=='device_retired',retired:entry.event_type==='device_retired',now:()=>Date.parse(entry.at)});}catch(error){if(error.code!=='session_closed')throw error;}
 await db.prepare("UPDATE recognize_claims SET result_json=? WHERE claim_key=? AND result_json=?").bind(JSON.stringify({...entry,exported:true}),key,row.result_json).run();
 return {exported:true};
 }finally{await db.prepare('DELETE FROM recognize_claims WHERE claim_key=? AND result_json=?').bind(lockKey,lock).run();}
}
