import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
const module=await import('../src/lib/rs-input-subscription-control.js').catch(()=>null);
function db(){
 const sql=new DatabaseSync(':memory:');sql.exec(readFileSync(new URL('../migrations/recognize-control/0002_subscriptions.sql',import.meta.url),'utf8'));
 return {sql,prepare(text){return {bind(...values){return {async run(){return {meta:{changes:sql.prepare(text).run(...values).changes}}},async first(){return sql.prepare(text).get(...values)||null}}}}}};
}
const identity={actorId:'synthetic_actor',eventId:'a'.repeat(64),fingerprint:'b'.repeat(64)};
function control(binding){assert.equal(typeof module?.withSubscriptionReservation,'function','concrete SQL reservation missing');return (id,work)=>module.withSubscriptionReservation({RS_RECOGNITION_CONTROL_DB:binding},id,work);}
test('real SQLite excludes a concurrent writer and releases only the owning token',async()=>{
 const binding=db(),run=control(binding);let unblock;const wait=new Promise(r=>unblock=r);let entered=0;
 const first=run(identity,async()=>{entered++;await wait;return 'done'});
 while(!entered)await new Promise(r=>setTimeout(r,1));
 await assert.rejects(run({...identity,eventId:'c'.repeat(64)},async()=>{entered++}),{code:'subscription_request_in_progress'});
 unblock();assert.equal(await first,'done');assert.equal(entered,1);assert.equal(binding.sql.prepare('SELECT state FROM subscription_write_reservations').get().state,'idle');binding.sql.close();
});
test('uncertain writes retain scope; only same request can enter reconciliation mode',async()=>{
 const binding=db(),run=control(binding);
 await assert.rejects(run(identity,async guard=>{guard.markWriteAttempted();throw Object.assign(Error(),{code:'write_outcome_unknown'})}),{code:'write_outcome_unknown'});
 await assert.rejects(run({...identity,eventId:'c'.repeat(64)},async()=>{}),{code:'subscription_reconciliation_required'});
 await assert.rejects(run({...identity,fingerprint:'c'.repeat(64)},async()=>{}),{code:'request_id_reused'});
 let mode;await run(identity,async guard=>{mode=guard.recoveryOnly});assert.equal(mode,true);
 assert.equal(binding.sql.prepare('SELECT state FROM subscription_write_reservations').get().state,'idle');binding.sql.close();
});
test('pre-write validation releases reservation and missing database fails before callback',async()=>{
 const binding=db(),run=control(binding);
 await assert.rejects(run(identity,async()=>{throw Object.assign(Error(),{code:'invalid_payload'})}),{code:'invalid_payload'});
 assert.equal(binding.sql.prepare('SELECT state FROM subscription_write_reservations').get().state,'idle');let called=false;
 await assert.rejects(module.withSubscriptionReservation({},identity,async()=>{called=true}),{code:'subscription_control_database_required'});assert.equal(called,false);binding.sql.close();
});
test('migration absent and crashed working reservations remain fail closed',async()=>{
 const binding=db(),run=control(binding);
 binding.sql.prepare("INSERT INTO subscription_write_reservations(actor_uid,request_uid,request_hash,worker_token,state) VALUES(?,?,?,?, 'working')").run(identity.actorId,identity.eventId,identity.fingerprint,'dead-worker');
 await assert.rejects(run(identity,async()=>{}),{code:'subscription_request_in_progress'});
 binding.sql.exec('DROP TABLE subscription_write_reservations');await assert.rejects(run(identity,async()=>{}),{code:'subscription_control_unavailable'});binding.sql.close();
});

