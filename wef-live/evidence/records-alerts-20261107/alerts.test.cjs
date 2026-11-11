"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),{EventEmitter}=require('node:events'),{PassThrough}=require('node:stream');
const a=require('./candidate/wef-live/records-alerts');
const temp=t=>{const p=fs.mkdtempSync(path.join(os.tmpdir(),'live-alert-test-'));t.after(()=>fs.rmSync(p,{recursive:true,force:true}));return p;};
test('status/parser failure remains alertable even though report says ok true / Skipped',()=>{
 const rows=a.failures([{lane:'live-check',run_id:'r',ok:true,issues:[{message:'Live-status check: Unreadable live status: object keys []'}]}]);assert.equal(rows.length,1);assert.equal(rows[0].run_id,'r');
});
test('collection/logging/linker stage and cleanup failures included; routine warnings/link checks excluded',()=>{
 assert.equal(a.failures([{lane:'rings',stage_errors:[{message:'Save failed'}],logging_errors:['logs failed']},{lane:'classes',issues:[{message:'Source HTTP 403'}]},{lane:'linker',issues:[{message:'No exact match'}],stage_errors:[{message:'Linker read failed'}]},{lane:'live-check',issues:[{message:'Warning: date mismatch'}]},{cleanup:{result:'Blocked'}}]).length,5);
});
test('first normal invocation sends honest connection notice once; healthy repeats silent; later failures report',async t=>{
 const runtime=temp(t),prompts=[];const send=async p=>{prompts.push(p);};
 let r=await a.notify({skipped:'next-check-not-due'},{runtime,send});assert.equal(r.result,'Delivered');assert.match(prompts[0],/connection check, not a fabricated collection failure/);assert.equal(r.target,a.TARGET);
 assert.equal((await a.notify([],{runtime,send})).result,'Not needed');
 r=await a.notify([{lane:'live-check',run_id:'x',issues:[{message:'Live-status check: timeout'}]}],{runtime,send});assert.equal(r.kind,'Failure alert');assert.equal(prompts.length,2);
 const receipt=JSON.parse(fs.readFileSync(path.join(runtime,'records-alert-'+r.alert_id+'.json')));assert.equal(receipt.result,'Delivered');assert.equal(receipt.errors[0].run_id,'x');
});
test('delivery failure is honest, does not throw into collection, and is not retried automatically',async t=>{
 const runtime=temp(t);let calls=0;const send=async()=>{calls++;throw Error('CLI exited 1');};
 const r=await a.notify([],{runtime,send});assert.equal(r.result,'Failed');assert.equal(r.error,'CLI exited 1');assert.equal((await a.notify([],{runtime,send})).result,'Not needed');assert.equal(calls,1);
});
test('existing CLI route exact target, hidden window, no secret env; requires acknowledgement and zero exit',async()=>{
 function spawnProcess(cli,args,opts){assert.equal(cli,'mock.exe');assert.deepEqual(args,['exec','--sandbox','read-only','resume',a.TARGET,'-','--json']);assert.equal(opts.windowsHide,true);assert.equal(Object.keys(opts.env).some(k=>/TOKEN|COOKIE|API_KEY|PASSWORD|SECRET/i.test(k)),false);const c=new EventEmitter();c.stdin=new PassThrough();c.stdout=new PassThrough();c.stderr=new PassThrough();c.kill=()=>{};c.stdin.on('finish',()=>{c.stdout.write(JSON.stringify({type:'item.completed',item:{type:'agent_message',text:'WEF_LIVE_ALERT_RECEIVED'}})+'\n');c.emit('close',0);});return c;}
 await a.deliver('receipt only',{spawnProcess,cli:'mock.exe'});
 const rejectSpawn=()=>{const c=new EventEmitter();c.stdin=new PassThrough();c.stdout=new PassThrough();c.stderr=new PassThrough();c.kill=()=>{};c.stdin.on('finish',()=>c.emit('close',1));return c;};
 await assert.rejects(a.deliver('test',{spawnProcess:rejectSpawn,cli:'mock.exe'}),/exited 1 without acknowledgement/);
});
