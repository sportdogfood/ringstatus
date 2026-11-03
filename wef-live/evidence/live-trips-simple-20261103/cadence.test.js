'use strict';
globalThis.fetch=async()=>{throw Error('Offline tests: network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {check,decision}=require('./candidate/live-cadence'),{scheduled}=require('./candidate/lanes');
function setup(t){
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-cadence-offline-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 let now='2026-11-02T15:00:00.000Z',status=false,statusCalls=0,cleanupCalls=0,seq=0;
 const requests=[],writes=[],reads=[],db={shows:[{id:'recShow',fields:{show_id:77,customer_id:15,is_default:true}}],'show-days':[{id:'recDay',fields:{'show-day-key':'15|77|2026-11-02','day-priority':0,date:'2026-11-02'}}]};
 const api={list:async(table)=>{reads.push(table);return structuredClone(db[table]||[]);},call:async(base,table,method,body)=>{
  if(method==='GET')return{records:structuredClone(db[table]||[])};
  writes.push({table,method,body:structuredClone(body)});const records=body.records.map(p=>{if(p.id){const r=db[table].find(r=>r.id===p.id);Object.assign(r.fields,p.fields);return r;}
   const r={id:'rec'+(++seq),fields:structuredClone(p.fields)};(db[table]||=[]).push(r);return r;});return{records:structuredClone(records)};
 }};
 const options={runtime,api,clock:()=>now,getStatus:async()=>{statusCalls++;return status;},get:async url=>{requests.push(url);return[];},archive:async()=>({file:'offline',sha256:'offline'}),cleanup:async()=>{cleanupCalls++;return{run_id:'offline-cleanup',result:'Success'};}};
 return{runtime,db,reads,writes,requests,options,setNow:v=>{now=v;},setStatus:v=>{status=v;},statusCalls:()=>statusCalls,cleanupCalls:()=>cleanupCalls,setDate:v=>{db['show-days'][0].fields.date=v;db['show-days'][0].fields['show-day-key']='15|77|'+v;}};
}
test('false status is hourly and registers heartbeat/run-log without warning or details',async t=>{
 const a=setup(t);const [r]=await scheduled(a.options);assert.equal(r.reason,'live-false');assert.equal(r.minutes,60);assert.equal(a.statusCalls(),1);assert.equal(a.requests.length,0);
 assert.equal(a.db.heartbeat.length,1);assert.equal(a.db['run-logs'].length,1);assert.equal(a.db['run-logs'][0].fields.result,'Skipped');assert.equal(a.db['errs-log'],undefined);assert.equal(a.cleanupCalls(),1);
});
test('quiet dispatcher makes no source, Airtable or collection log calls and evaluates local cleanup',async t=>{
 const a=setup(t);await scheduled(a.options);const reads=a.reads.length,writes=a.writes.length;a.setNow('2026-11-02T15:03:00.000Z');
 const r=await scheduled(a.options);assert.equal(r.skipped,'next-check-not-due');assert.equal(a.statusCalls(),1);assert.equal(a.reads.length,reads);assert.equal(a.writes.length,writes);assert.equal(a.cleanupCalls(),2);
});
test('hourly due time is inclusive; repeated false checks each receive their own run record',async t=>{
 const a=setup(t);await scheduled(a.options);a.setNow('2026-11-02T15:59:59.999Z');assert.equal((await check(a.options)).due,false);
 a.setNow('2026-11-02T16:00:00.000Z');const r=await check(a.options);assert.equal(r.due,true);assert.equal(a.statusCalls(),2);assert.equal(a.db.heartbeat.length,2);assert.equal(new Set(a.db['run-logs'].map(r=>r.fields['run-id'])).size,2);
});
test('true with future priority date is hourly, warns once per check and never fetches details',async t=>{
 const a=setup(t);a.setStatus(true);a.setDate('2026-11-05');const [r]=await scheduled(a.options);assert.equal(r.reason,'priority-date-mismatch');assert.equal(r.minutes,60);assert.equal(a.requests.length,0);
 assert.equal(a.db['errs-log'].length,1);assert.match(a.db['errs-log'][0].fields.message,/today 2026-11-02.*priority-0 date 2026-11-05.*show 77/);assert.equal(a.db.heartbeat[0].fields.date,'2026-11-05');
});
test('true with a past priority date is also hourly and warns',async t=>{
 const a=setup(t);a.setStatus(true);a.setDate('2026-11-01');const r=await check(a.options);assert.equal(r.reason,'priority-date-mismatch');assert.equal(a.db['errs-log'].length,1);
});
test('true today inside window runs three lanes with one live-status request',async t=>{
 const a=setup(t);a.setStatus(true);const r=await scheduled(a.options);assert.equal(r.length,3);assert(r.every(r=>r.ok&&r.live_status===true&&r.scope.date==='2026-11-02'));
 assert.equal(a.statusCalls(),1);assert.equal(a.db.heartbeat.length,3);assert.equal(a.db['run-logs'].length,3);assert.equal(a.requests.length,2);assert.equal(a.cleanupCalls(),1);
});
test('false-to-true transition occurs at the next hourly check, then uses three-minute checks',async t=>{
 const a=setup(t);await scheduled(a.options);a.setStatus(true);a.setNow('2026-11-02T16:00:00.000Z');await scheduled(a.options);assert.equal(a.statusCalls(),2);
 a.setNow('2026-11-02T16:02:59.999Z');assert.equal((await scheduled(a.options)).skipped,'next-check-not-due');a.setNow('2026-11-02T16:03:00.000Z');await scheduled(a.options);assert.equal(a.statusCalls(),3);
});
test('active-to-false immediately changes next check to hourly and stops detail calls',async t=>{
 const a=setup(t);a.setStatus(true);await scheduled(a.options);const n=a.requests.length;a.setStatus(false);a.setNow('2026-11-02T15:03:00.000Z');const [r]=await scheduled(a.options);
 assert.equal(r.minutes,60);assert.equal(a.requests.length,n);a.setNow('2026-11-02T15:06:00.000Z');await scheduled(a.options);assert.equal(a.statusCalls(),2);
});
test('hourly false checks continue overnight and register runs',async t=>{
 const a=setup(t);a.setNow('2026-11-03T03:00:00.000Z');let r=await check(a.options);assert.equal(r.reason,'live-false');assert.equal(r.today,'2026-11-02');
 a.setNow('2026-11-03T04:00:00.000Z');r=await check(a.options);assert.equal(r.due,true);assert.equal(a.db.heartbeat.length,2);
});
test('true today outside window is hourly and does not fetch details',async t=>{
 const a=setup(t);a.setStatus(true);a.setNow('2026-11-03T03:00:00.000Z');const [r]=await scheduled(a.options);assert.equal(r.reason,'outside-collection-window');assert.equal(r.minutes,60);assert.equal(a.requests.length,0);
});
test('Eastern window boundaries and DST use local today, not UTC date',()=>{
 assert.equal(decision(true,{date:'2026-11-01'},'2026-11-01T09:59:00Z').collect,false);assert.equal(decision(true,{date:'2026-11-01'},'2026-11-01T10:00:00Z').collect,true);
 assert.equal(decision(true,{date:'2026-11-02'},'2026-11-03T00:00:00Z').collect,true);assert.equal(decision(true,{date:'2026-11-02'},'2026-11-03T00:01:00Z').collect,false);
 assert.equal(decision(true,{date:'2026-11-01'},'2026-11-02T03:00:00Z').today,'2026-11-01');
});
test('known live-today outside window can enter collection at05:00 without waiting until05:30',async t=>{
 const a=setup(t);a.setStatus(true);a.setNow('2026-11-02T09:30:00Z');assert.equal((await check(a.options)).reason,'outside-collection-window');
 a.setNow('2026-11-02T10:00:00Z');assert.equal((await check(a.options)).collect,true);assert.equal(a.statusCalls(),2);
});
test('after19:00 an active run waits one hour rather than polling again three minutes later',async t=>{
 const a=setup(t);a.setStatus(true);a.setNow('2026-11-03T00:00:00Z');await check(a.options);
 a.setNow('2026-11-03T00:03:00Z');assert.equal((await check(a.options)).due,false);a.setNow('2026-11-03T01:00:00Z');assert.equal((await check(a.options)).reason,'outside-collection-window');assert.equal(a.statusCalls(),2);
});
test('launch-time jitter cannot make active collection miss the next three-minute tick',async t=>{
 const a=setup(t);a.setStatus(true);a.setNow('2026-11-02T15:00:05.500Z');const first=await check(a.options);assert.equal(first.next_check_at,'2026-11-02T15:03:00.000Z');
 a.setNow('2026-11-02T15:03:03.100Z');assert.equal((await check(a.options)).due,true);assert.equal(a.statusCalls(),2);
});
test('launch-time jitter cannot defer an hourly check by another dispatcher interval',async t=>{
 const a=setup(t);a.setNow('2026-11-02T15:00:05.500Z');await check(a.options);a.setNow('2026-11-02T16:00:03.100Z');assert.equal((await check(a.options)).due,true);assert.equal(a.statusCalls(),2);
});
test('status request errors remain unknown, are logged and skip details with hourly next check',async t=>{
 const a=setup(t);a.options.getStatus=async()=>{throw Error('HTTP503');};const [r]=await scheduled(a.options);assert.equal(r.live_status,undefined);assert.equal(r.reason,'live-status-unavailable');assert.equal(r.minutes,60);assert.equal(a.requests.length,0);assert.match(a.db['errs-log'][0].fields.message,/HTTP503/);
});
test('malformed status is not accepted as false or true',async t=>{
 const a=setup(t);a.setStatus('true');const r=await check(a.options);assert.equal(r.reason,'live-status-unavailable');assert.match(a.db['errs-log'][0].fields.message,/explicit boolean/);
});
test('missing canonical context logs the exact problem and skips detail collection',async t=>{
 const a=setup(t);a.setStatus(true);a.db['show-days']=[];const [r]=await scheduled(a.options);assert.equal(r.reason,'show-context-unavailable');assert.match(a.db['errs-log'][0].fields.message,/one canonical day-priority 0/);assert.equal(a.requests.length,0);
});
test('active collection uses the exact approved scope, without a second authority read changing its day',async t=>{
 const a=setup(t);a.setStatus(true);let calls=0;const list=a.options.api.list;a.options.api.list=async table=>{if(table==='show-days'){calls++;if(calls>1)return[{fields:{'show-day-key':'15|77|2026-11-05','day-priority':0,date:'2026-11-05'}}];}return list(table);};
 const rows=await scheduled(a.options);assert(rows.every(r=>r.scope.date==='2026-11-02'));assert.equal(calls,1);
});
test('inactive logging failures are reported without turning the check into collection or retrying writes',async t=>{
 const a=setup(t);const call=a.options.api.call;let failed=0;a.options.api.call=async(base,table,...args)=>{if(table==='heartbeat'){failed++;throw Error('heartbeat unavailable');}return call(base,table,...args);};
 const [r]=await scheduled(a.options);assert.equal(r.ok,true);assert.equal(failed,1);assert.equal(a.requests.length,0);assert(r.logging_errors.length);assert.match(a.db['errs-log'][0].fields.message,/heartbeat unavailable/);
});
test('unreadable local due state is logged and cannot silently prevent the next real check',async t=>{
 const a=setup(t);fs.writeFileSync(path.join(a.runtime,'live-cadence-state.json'),'not-json');const r=await check(a.options);assert.equal(r.due,true);assert.equal(a.statusCalls(),1);assert(a.db['errs-log'].some(r=>r.fields.message.includes('cadence state')));
});
test('prepared task changes only its repetition duration and does not change action/account/interval',()=>{
 const before=fs.readFileSync(path.join(__dirname,'task-before.xml'),'utf8').trim(),after=fs.readFileSync(path.join(__dirname,'task-prepared.xml'),'utf8').trim();
 assert.equal(after,before.replace('<Duration>PT14H</Duration>','<Duration>PT24H</Duration>').replace('<StopAtDurationEnd>true</StopAtDurationEnd>','<StopAtDurationEnd>false</StopAtDurationEnd>'));assert.match(after,/<Interval>PT3M<\/Interval>/);assert.match(after,/<MultipleInstancesPolicy>IgnoreNew<\/MultipleInstancesPolicy>/);
});
