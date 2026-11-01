const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path'),{EventEmitter}=require('node:events');
const code=fs.readFileSync(path.join(__dirname,'../entry.js'),'utf8');
function fixture(linking,report={ok:true,counts:{saved:3},'item-errors':[]}){
 const calls=[],locks=[],reports=[],roots=[],events=[];let released=0;const module={exports:{}};
 const logger={readAttempt(root){roots.push(root);return {}},atomic(){},recordError(root,id,e){events.push(e)},finish(root,id,r){events.push(r)},async deliver(root,config){assert.equal(config.workflow,linking?'wef-data-linking':'wef-data');return {ok:true}}};
 function spawn(exe,args,options){calls.push({exe,args,options});const c=new EventEmitter();c.stdout=new EventEmitter();c.stderr=new EventEmitter();queueMicrotask(()=>{c.stdout.emit('data',JSON.stringify(report));c.emit('close',0)});return c;}
 const process={argv:['node','entry.js',...(linking?['--linking']:[])],env:{WEF_LOG_ATTEMPT_ID:'fake-attempt'}};
 const fakeFs={mkdirSync(){},readFileSync(){return JSON.stringify({workflows:{'wef-data':{workflow:'wef-data'},'wef-data-linking':{workflow:'wef-data-linking'}}})}};
 const collect={acquireLock(file){locks.push(file);return()=>released++},async atomicReport(file,body){reports.push({file,body})}};
 vm.runInNewContext(code,{module,process,__dirname:'C:/fixture/wef-data',console:{log(){},error(){}},require(n){return n==='node:fs'?fakeFs:n==='node:path'?path:n==='node:child_process'?{spawn}:n==='./attempt-log'?logger:collect}});
 return {run:module.exports.scheduled,calls,locks,reports,roots,events,get released(){return released}};
}
test('fast and saved-link modes use independent locks/reports/outboxes and same private runner',async()=>{
 const fast=fixture(false),link=fixture(true);await fast.run();await link.run();
 assert.notEqual(fast.locks[0],link.locks[0]);assert.notEqual(fast.roots[0],link.roots[0]);assert.notEqual(fast.reports[0].file,link.reports[0].file);
 assert.equal(fast.calls[0].exe,link.calls[0].exe);assert.ok(fast.calls[0].args[1].endsWith('scheduled.py'));assert.ok(!fast.calls[0].args[1].endsWith('linking-scheduled.py'));assert.ok(link.calls[0].args[1].endsWith('linking-scheduled.py'));
 for(const f of [fast,link]){assert.equal(f.released,1);assert.equal(f.calls[0].options.windowsHide,true);assert.equal(f.events[0].status,'Succeeded')}
});
test('both stages preserve actual item errors in existing logging path',async()=>{
 for(const linking of [false,true]){const f=fixture(linking,{ok:false,state:'Partial',counts:{saved:7},'item-errors':[{key:'example',error:'TypeError: fetch failed\ncause: reset'}]});await f.run();assert.equal(f.events[0].message,'TypeError: fetch failed\ncause: reset');assert.equal(f.events[1].status,'Partial');assert.equal(f.events[1].counts.saved,7)}
});
