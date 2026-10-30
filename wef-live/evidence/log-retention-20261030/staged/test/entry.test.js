const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { scheduled } = require('../entry');
test('logging outages do not fail a completed collection or prevent next invocation',async()=>{
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-live-test-'));let calls=0,releases=0;
 try {const options={runtime,lock:()=>()=>releases++,run:async()=>({run_id:'logging-'+ ++calls,ok:true,errors:[],completed:new Date().toISOString(),logging_errors:['run-logs timeout']}),api:{upsert:async()=>{throw Error('Airtable down');}}};
 for(let i=0;i<2;i++){const r=await scheduled(options);assert.equal(r.ok,true);assert(r.logging_errors.some(e=>e.includes('Airtable down')));}
 assert.equal(releases,2);assert.match(fs.readFileSync(path.join(runtime,'last-run.json'),'utf8'),/Airtable down/);
 // A file used as runtime makes receipt/log writes fail without changing collection outcome.
 const blocked=path.join(runtime,'not-a-directory');fs.writeFileSync(blocked,'test');
 const r=await scheduled({...options,runtime:blocked});assert.equal(r.ok,true);assert(r.logging_errors.some(e=>e.includes('Local receipt')));
 }finally{process.exitCode=0;fs.rmSync(runtime,{recursive:true});}
});
test('partial run writes unresolved identity to errs-log without failing the invocation',async()=>{
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-live-test-'));const rows=[];
 try {const r=await scheduled({runtime,lock:()=>()=>{},api:{upsert:async(t,k,v)=>rows.push(...v)},run:async()=>({run_id:'partial',completed:new Date().toISOString(),ok:true,errors:[],unresolved:[{class_id:30,message:'No decoded identity for trip 400'},{class_id:31,status:'error',message:'HTTP 502'}]})});
 assert.equal(r.ok,true);assert.match(rows[0].message,/Unresolved class 30/);
 assert.match(rows[1].message,/Class error 31: HTTP 502/);
 }finally{process.exitCode=0;fs.rmSync(runtime,{recursive:true});}
});

test('failed scheduled invocation logs errors and does not block the next invocation', async () => {
  const runtime = fs.mkdtempSync(path.join(os.tmpdir(), 'wef-live-test-'));
  const rows = [];
  let calls = 0, releases = 0;
  const options = { runtime, lock: () => () => releases++, api: { upsert: async (table, key, values) => rows.push(...values) },
    run: async () => ({ run_id: 'run-' + ++calls, completed: '2026-10-23T18:00:00Z', ok: calls > 1, errors: calls === 1 ? ['Source HTTP 502'] : [] }) };
  try {
    fs.writeFileSync(path.join(runtime, 'halt.json'), 'historical failure');
    assert.equal((await scheduled(options)).ok, false);
    assert.equal((await scheduled(options)).ok, true);
    assert.equal(calls, 2); assert.equal(releases, 2);
    assert.equal(rows[0].message, 'Source HTTP 502');
    assert.equal(fs.readFileSync(path.join(runtime, 'halt.json'), 'utf8'), 'historical failure');
  } finally { process.exitCode = 0; fs.rmSync(runtime, { recursive: true }); }
});

test('each error gets its own record and exact optional IDs without changing messages',async()=>{
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-live-test-')),rows=[];
 try {
  await scheduled({runtime,lock:()=>()=>{},api:{upsert:async(t,k,v)=>rows.push(...v)},run:async()=>({run_id:'ids',completed:'2026-10-25T14:00:00Z',ok:true,errors:['General error'],unresolved:[
   {class_id:200030349,trip_id:200398787,group_id:123,message:'No decoded identity for trip 200398787'},
   {class_id:200030315,status:'error',message:'HTTP 502; timeout',messages:['HTTP 502','timeout']}
  ],link_issues:[{entry_key:'200000075|2026-10-25|200241392|200030375',class_id:200030375,trip_id:456,message:'Missing, stale or incomplete dated schedule collection'}]})});
  assert.equal(rows.length,5);assert.equal(new Set(rows.map(x=>x['error-key'])).size,5);
  assert.equal(rows[0]['err-class'],null);assert.equal(rows[1]['err-class'],200030349);assert.equal(rows[1]['err-trip'],200398787);assert.equal(rows[1]['err-group'],123);
  assert.equal(rows[1].message,'Unresolved class 200030349: No decoded identity for trip 200398787');
  assert.equal(rows[2].message,'Class error 200030315: HTTP 502');assert.equal(rows[3].message,'Class error 200030315: timeout');assert.equal(rows[2]['err-trip'],null);assert.equal(rows[2]['err-group'],null);
  assert.equal(rows[4]['err-class'],200030375);assert.equal(rows[4]['err-trip'],456);assert.equal(rows[4]['err-group'],null);
  assert.equal(fs.readFileSync(path.join(runtime,'errs-log.jsonl'),'utf8').trim().split('\n').length,5);
 }finally{process.exitCode=0;fs.rmSync(runtime,{recursive:true});}
});

test('unexpected collection and Airtable logging failures retain local evidence and release lock', async () => {
  const runtime = fs.mkdtempSync(path.join(os.tmpdir(), 'wef-live-test-'));
  let released = false;
  try {
    const report = await scheduled({ runtime, lock: () => () => { released = true; },
      run: async () => { throw Error('collection unavailable'); },
      api: { upsert: async () => { throw Error('Airtable unavailable'); } } });
    assert.equal(report.ok, false); assert.equal(released, true);
    assert.match(fs.readFileSync(path.join(runtime, 'errs-log.jsonl'), 'utf8'), /collection unavailable/);
    assert.match(fs.readFileSync(path.join(runtime, 'last-run.json'), 'utf8'), /Airtable unavailable/);
    assert.equal(fs.existsSync(path.join(runtime, 'halt.json')), false);
  } finally { process.exitCode = 0; fs.rmSync(runtime, { recursive: true }); }
});
