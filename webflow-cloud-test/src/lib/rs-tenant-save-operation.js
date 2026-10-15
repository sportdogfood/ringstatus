// Reuse recognition's exact-JSON claim/readback pattern for the existing save.
// No lease expiry: a concurrent or interrupted executor is never overwritten.
import {claimOnce, requireClaimDatabase} from './rs-recognition-claims.js';
import {InputError,digest} from './rs-inputs.js';
const fail=code=>{throw new InputError(code,503)};
export const payloadHash=payload=>digest(JSON.stringify(payload,(_key,value)=>value&&typeof value==='object'&&!Array.isArray(value)?Object.fromEntries(Object.keys(value).sort().map(k=>[k,value[k]])):value));
export async function readTenantSave(env,eventId){
 const key=await digest(JSON.stringify(['tenant-barn-direct-operation',eventId]));
 return {key,row:await requireClaimDatabase(env).prepare('SELECT state, result_json FROM recognize_claims WHERE claim_key = ?').bind(key).first()};
}
export async function runTenantSave({env,key,prior,context,work}){
 const db=requireClaimDatabase(env),token=crypto.randomUUID();
 let journal=prior?.result_json?JSON.parse(prior.result_json):null;
 if(prior&&(!journal?.envelope||journal.busy))fail('tenant_save_in_progress');
 let serialized=prior?.result_json??null;
 if(!prior){const claim=await claimOnce(env,'tenant-barn-direct-operation',context.envelope.event.eventId);if(!claim.won)fail('tenant_save_in_progress');}
 journal=journal||{...context,steps:{},revisionKey:null};
 async function save(next,state='claimed'){
  const result=await db.prepare('UPDATE recognize_claims SET state = ?, result_json = ? WHERE claim_key = ? AND state = \'claimed\' AND result_json IS ?').bind(state,JSON.stringify(next),key,serialized).run();
  if(result.meta.changes!==1)fail('tenant_save_in_progress');
  journal=next;serialized=JSON.stringify(next);
 }
 await save({...journal,busy:token});
 const operation={
  context:journal,
  async reserveRevision(identity){
   if(journal.revisionKey)return;
   const claim=await claimOnce(env,'tenant-barn-entity-revision',identity);
   if(!claim.won)throw new InputError('record_changed',409);
   await save({...journal,revisionKey:claim.key});
  },
  async step(name,read,apply){
   const old=journal.steps[name];
   // A completed or uncertain external effect is read back before continuing.
   const existing=await read(journal.references?.[name]);
   if(existing){await save({...journal,steps:{...journal.steps,[name]:'complete'}});return existing;}
   if(old==='pending'||old==='complete')fail(old==='pending'?'write_outcome_unknown':'record_changed');
   await save({...journal,steps:{...journal.steps,[name]:'pending'}});
   try{const applied=await apply();if(applied?.id)await save({...journal,references:{...journal.references,[name]:applied.id}});}catch(error){
    // A provider rejection explicitly confirms no mutation for this request.
    if(error.noWrite===true)await save({...journal,steps:{...journal.steps,[name]:'ready'}});
    throw error;
   }
   const confirmed=await read(journal.references?.[name]);if(!confirmed)fail('target_readback_mismatch');
   await save({...journal,steps:{...journal.steps,[name]:'complete'}});return confirmed;
  }
 };
 try{
  const result=await work(operation);
  const record={...result.record};delete record.pin;
  await save({actorId:journal.actorId,inputHash:journal.envelope.event.inputHash,payloadHash:journal.payloadHash,record},'complete');
  return result;
 }catch(error){
  const effected=Object.values(journal.steps).some(s=>s==='pending'||s==='complete');
  if(!effected){
   if(journal.revisionKey)await db.prepare("DELETE FROM recognize_claims WHERE claim_key = ? AND state = 'claimed' AND result_json IS NULL").bind(journal.revisionKey).run();
   await db.prepare("DELETE FROM recognize_claims WHERE claim_key = ? AND state = 'claimed' AND result_json = ?").bind(key,serialized).run();
  }else await save({...journal,busy:null});
  throw error;
 }
}
