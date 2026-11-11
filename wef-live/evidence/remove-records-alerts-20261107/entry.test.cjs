"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const file=path.join(__dirname,'candidate/wef-live/entry.js'),code=fs.readFileSync(file,'utf8');
async function run(reports,error){const module={exports:{}},calls=[],logs=[],errors=[],process={};const scheduled=async()=>{if(error)throw error;return reports;};const require=name=>{calls.push(name);assert.equal(name,'./lanes');return{scheduled};};require.main=module;vm.runInNewContext(code,{module,require,process,console:{log:s=>logs.push(s),error:s=>errors.push(s)}});await new Promise(r=>setImmediate(r));return{calls,logs,errors,process,module,scheduled};}
test('proven original entry restored byte-for-byte;Records alert module absent',()=>{
 assert.deepEqual(fs.readFileSync(file),fs.readFileSync(path.join(__dirname,'../records-alerts-20261107/baseline/wef-live/entry.js')));assert.equal(fs.existsSync(path.join(__dirname,'candidate/wef-live/records-alerts.js')),false);
});
test('normal reports unchanged,noalertrequire/noCLI,and preserved module export',async()=>{const reports=[{ok:true}];const r=await run(reports);assert.deepEqual(r.calls,['./lanes']);assert.deepEqual(JSON.parse(r.logs[0]),reports);assert.equal(r.process.exitCode,0);assert.equal(r.module.exports.scheduled,r.scheduled);});
test('failed report still returns1 without notification transport',async()=>{const r=await run([{ok:false}]);assert.equal(r.process.exitCode,1);assert.equal(r.errors.length,0);assert.deepEqual(r.calls,['./lanes']);});
test('uncaught scheduled failure still returns1 and logs original error without alerts',async()=>{const r=await run(null,Error('scheduled failure'));assert.equal(r.process.exitCode,1);assert.deepEqual(r.errors,['scheduled failure']);assert.deepEqual(r.calls,['./lanes']);});
