import { InputError } from './rs-inputs.js';
const fail=(code,status=503)=>{throw new InputError(code,status)};
export async function withSubscriptionReservation(env,{actorId,eventId,fingerprint},work){
 const db=env?.RS_RECOGNITION_CONTROL_DB;
 if(!db||typeof db.prepare!=='function')fail('subscription_control_database_required');
 if(!/^[A-Za-z0-9_-]{1,128}$/.test(actorId||'')||!/^[a-f0-9]{64}$/.test(eventId)||!/^[a-f0-9]{64}$/.test(fingerprint))fail('invalid_subscription_reservation',400);
 const token=crypto.randomUUID();let recoveryOnly=false;
 try{
  const inserted=await db.prepare("INSERT INTO subscription_write_reservations (actor_uid, request_uid, request_hash, worker_token, state) VALUES (?, ?, ?, ?, 'working') ON CONFLICT(actor_uid) DO UPDATE SET request_uid=excluded.request_uid, request_hash=excluded.request_hash, worker_token=excluded.worker_token, state='working', updated_at=CURRENT_TIMESTAMP WHERE subscription_write_reservations.state='idle'").bind(actorId,eventId,fingerprint,token).run();
  if(inserted.meta.changes!==1){
   const row=await db.prepare('SELECT request_uid, request_hash, state FROM subscription_write_reservations WHERE actor_uid=?').bind(actorId).first();
   if(!row)fail('subscription_control_unavailable');
   if(row.request_uid===eventId&&row.request_hash!==fingerprint)fail('request_id_reused',409);
   if(row.state==='working')fail('subscription_request_in_progress',409);
   if(row.state!=='uncertain'||row.request_uid!==eventId)fail('subscription_reconciliation_required');
   const recovered=await db.prepare("UPDATE subscription_write_reservations SET state='working', worker_token=?, updated_at=CURRENT_TIMESTAMP WHERE actor_uid=? AND request_uid=? AND request_hash=? AND state='uncertain'").bind(token,actorId,eventId,fingerprint).run();
   if(recovered.meta.changes!==1)fail('subscription_request_in_progress',409);
   recoveryOnly=true;
  }
 }catch(error){if(error instanceof InputError)throw error;fail('subscription_control_unavailable');}
 let attempted=false;
 const finish=async state=>{
  try{
   const result=await db.prepare("UPDATE subscription_write_reservations SET state=?, updated_at=CURRENT_TIMESTAMP WHERE actor_uid=? AND worker_token=? AND state='working'").bind(state,actorId,token).run();
   if(result.meta.changes!==1)fail('subscription_control_unavailable');
  }catch{fail('subscription_control_unavailable');}
 };
 let result;
 try{result=await work({recoveryOnly,markWriteAttempted(){attempted=true;}});}
 catch(error){
  // No lease expiry or blind retry. An uncertain external effect retains actor scope.
  await finish(attempted||recoveryOnly?'uncertain':'idle');
  throw error;
 }
 await finish('idle');
 return result;
}

