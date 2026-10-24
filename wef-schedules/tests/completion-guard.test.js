"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),path=require('node:path'),{spawnSync}=require('node:child_process');
const guard=path.resolve(__dirname,'../completion-guard.js');
function fixture(work){return spawnSync(process.execPath,['-e',`const {runWithCompletion}=require(${JSON.stringify(guard)});runWithCompletion(()=>{${work}});`],{encoding:'utf8',timeout:5000,env:{...process.env,WEF_SCHEDULES_AIRTABLE_TOKEN:''}});}
test('unfinished promise fails visibly instead of silent exit 0',()=>{const r=fixture('return new Promise(()=>{});');assert.equal(r.status,1);assert.match(r.stderr,/incomplete_run/);assert.doesNotMatch(r.stdout,/RUN_FINISHED/);});
test('explicit zero exit while unfinished cannot claim success',()=>{const r=fixture('process.exit(0);');assert.equal(r.status,1);assert.match(r.stderr,/incomplete_run/);});
test('verified terminal success permits exit zero',()=>{const r=fixture('return Promise.resolve({ok:true,days:[{ok:true}]});');assert.equal(r.status,0,r.stderr);assert.match(r.stdout,/RUN_FINISHED result=Success/);assert.doesNotMatch(r.stderr,/incomplete_run/);});
test('delayed terminal completion waits then succeeds',()=>{const r=fixture('return new Promise(resolve=>setTimeout(()=>resolve({ok:true}),20));');assert.equal(r.status,0);assert.match(r.stdout,/RUN_FINISHED/);});
test('reported failed collection remains failed',()=>{const r=fixture('return {ok:false,days:[{ok:false,error:"fixture"}]};');assert.equal(r.status,1);assert.match(r.stdout,/RUN_FINISHED result=Failed/);});
test('rejected completion remains failed with reason',()=>{const r=fixture('return Promise.reject(Error("fixture final log failed"));');assert.equal(r.status,1);assert.match(r.stderr,/fixture final log failed/);});
test('sync startup error remains failed',()=>{const r=fixture('throw Error("fixture startup");');assert.equal(r.status,1);assert.match(r.stderr,/fixture startup/);});
test('missing success result cannot permit exit zero',()=>{const r=fixture('return {};');assert.equal(r.status,1);assert.match(r.stderr,/terminal report/);});

// Compile the real entry point against inert adapters; no source/Airtable calls.
function entryFixture(source,body){
 const script=`const fs=require('node:fs'),Module=require('node:module');const file=${JSON.stringify(path.resolve(__dirname,'../collect.js'))};const m=new Module(file);m.filename=file;m.paths=Module._nodeModulePaths(require('node:path').dirname(file));const original=m.require.bind(m);m.require=id=>({ './settings':{AIRTABLE_TOKEN:'offline-fixture'},'./collector-core':{},'./store':{runStagedCollector:()=>{${body}}},'./run-logs':{runLogs:()=>({})},'./heartbeat':{heartbeat:()=>({})},'./show-index':{showIndex:()=>({})},'./show-rings':{showRings:()=>({})} }[id]||original(id));process.mainModule=m;m._compile(fs.readFileSync(${JSON.stringify(source)},'utf8'),file);`;
 return spawnSync(process.execPath,['-e',script],{encoding:'utf8',timeout:5000});
}
test('actual entry point rejects unresolved finalization',()=>{const r=entryFixture(path.resolve(__dirname,'../collect.js'),'return new Promise(()=>{});');assert.equal(r.status,1);assert.match(r.stderr,/incomplete_run/);});
test('actual entry point accepts terminal verified report',()=>{const r=entryFixture(path.resolve(__dirname,'../collect.js'),'return Promise.resolve({ok:true});');assert.equal(r.status,0,r.stderr);assert.match(r.stdout,/RUN_FINISHED result=Success/);});
