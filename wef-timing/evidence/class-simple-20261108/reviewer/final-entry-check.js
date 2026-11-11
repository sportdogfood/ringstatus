'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../candidate/entry-data.js'),'utf8');
const calls=[],files=new Map(),fakeFs={mkdirSync:()=>{},openSync:p=>{calls.push('lock');return 1;},writeFileSync:(p,v)=>files.set(p,v),renameSync:(a,b)=>{files.set(b,files.get(a));files.delete(a);},closeSync:()=>calls.push('close'),unlinkSync:()=>calls.push('unlock')};
const fakeProcess={pid:1,exitCode:null,exit:n=>{throw Error('Unexpected process exit '+n);}};
const context={require:n=>n==='node:fs'?fakeFs:n==='node:path'?path:n==='node:crypto'?{randomUUID:()=> 'fake-run'}:n==='./data-runner'?{run:async()=>{calls.push('calculate');return {ok:true};}}:n==='./ring-cleanup'?{cleanup:async()=>{calls.push('ring-cleanup');return {};}}:n==='./class-cleanup'?{run:async({deadline})=>{assert(deadline>Date.now());calls.push('class-cleanup');return {ok:true,deleted:0};}}:(()=>{throw Error('Unexpected dependency '+n);})(),__dirname:'C:/offline-review-only',process:fakeProcess,console:{log:()=>calls.push('receipt')},setTimeout:()=>1,clearTimeout:()=>{},Date};
vm.runInNewContext(source,context,{filename:'staged-entry-data.js'});
setImmediate(()=>{try{assert.deepEqual(calls,['lock','calculate','ring-cleanup','class-cleanup','receipt','close','unlock']);assert.equal(fakeProcess.exitCode,0);console.log(JSON.stringify({staged_entry:'PASS',calls,production_files_written:0}));}catch(e){console.error(e);process.exitCode=1;}});
