const fs=require('node:fs'),vm=require('node:vm'),path=require('node:path'),{createRequire}=require('node:module');
const testPath=path.resolve('test/events.test.js');
const prefix=fs.readFileSync(testPath,'utf8').split("test('gone-in arms")[0];
const sandbox={require:createRequire(testPath),structuredClone,console}; vm.createContext(sandbox);
vm.runInContext(prefix+'\nglobalThis.api={mock,cleanResults,now};',sandbox);
(async()=>{
 const {mock,cleanResults,now}=sandbox.api;
 const b=mock(); b.add('w-shows',{'show-key':'27|333',show_id:333,customer_id:27});
 b.add('w-days',{'show-day-key':'27|333|2030-02-10','day-priority':0});
 const run=b.add('run-logs',{'run-id':'priority-zero-run','show-id':333,result:'Complete',completed:'2030-02-10T20:00:00Z'});
 const err=b.add('errs-log',{'error-key':'priority-zero-error','run-id':'priority-zero-run','occurred-at':'2030-02-10T19:59:00Z'});
 const snap=b.add('snapshot',{'snapshot-key':'X|27|333|2030-02-10|3|555|666|UUID','customer-id':27,'show-id':333,date:'2030-02-10','check-status':'Complete'});
 await cleanResults(b,{reason:'new-show',now});
 console.log(JSON.stringify({case:'old show retains matching priority-zero day',runRetained:b.data['run-logs'].some(r=>r.id===run.id),errorRetained:b.data['errs-log'].some(r=>r.id===err.id),snapshotRetained:b.data.snapshot.some(r=>r.id===snap.id),deletes:b.calls.filter(c=>c.type==='delete')}));
 const c=mock(); c.add('run-logs',{'run-id':'legacy-success',result:'Success',completed:'2030-02-10T20:00:00Z'});
 await cleanResults(c,{reason:'daily',now}); console.log(JSON.stringify({case:'retained original runner known completed Success',remaining:c.data['run-logs'].length,deletes:c.calls.filter(c=>c.type==='delete')}));
})().catch(e=>{console.error(e);process.exitCode=1});
