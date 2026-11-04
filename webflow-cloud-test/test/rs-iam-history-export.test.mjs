import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {renewHistorySession,flushIamHistory} from '../src/assets/rs-recognition/native-client.js';
const storage=()=>{const m=new Map();return {getItem:k=>m.get(k)||null,setItem:(k,v)=>m.set(k,v),removeItem:k=>m.delete(k)};};
async function fixture(code){
 let entry={event_uid:crypto.randomUUID(),session_uid:'old-session',person_record_id:'person',device_token:'test-token',event_type:'recognition',at:'2026-11-02T00:00:00Z'},events=0,summaries=0;
 const db={prepare(sql){let args;return {bind(...values){args=values;return this;},async first(){return sql.includes('recognize_iam_devices')?{retired:0}:{result_json:JSON.stringify(entry)};},async run(){if(sql.startsWith('UPDATE recognize_claims SET result_json'))entry=JSON.parse(args[0]);return {meta:{changes:1}};}};}};
 globalThis.__historyTest={requireClaimDatabase:()=>db,accessHash:async()=> 'hash',syncIamHistoryDevice:async()=>({id:'device'}),recordRecognitionSession:async()=>{events++;},recordRecognitionSummary:async()=>{summaries++;if(code)throw Object.assign(Error(code),{code});}};
 let source=readFileSync(new URL('../src/lib/rs-recognition-iam-connections.js',import.meta.url),'utf8').replace(/^import .*;\r?\n/gm,'');
 source='const {requireClaimDatabase,accessHash,syncIamHistoryDevice,recordRecognitionSession,recordRecognitionSummary}=globalThis.__historyTest;\n'+source;
 const module=await import('data:text/javascript;base64,'+Buffer.from(source+'\n//'+crypto.randomUUID()).toString('base64'));
 return {run:()=>module.exportIamHistory({env:{},request:new Request('https://ringstatus.com'),eventUid:entry.event_uid}),state:()=>({entry,events,summaries})};
}
test('confirmed Footprint survives summary identity conflict without rebinding or blocking the queue',async()=>{
 const f=await fixture('session_identity_conflict');const result=await f.run();assert.equal(result.exported,true);assert.equal(result.renew_session_uid,'old-session');assert.equal(f.state().entry.summary_status,'session_identity_conflict');assert.equal(f.state().events,1);assert.equal(f.state().summaries,1);
 const duplicate=await f.run();assert.equal(duplicate.duplicate,true);assert.equal(duplicate.renew_session_uid,'old-session');assert.equal(f.state().events,1);
});
test('real export failures remain pending',async()=>{const f=await fixture('session_summary_write_uncertain');await assert.rejects(f.run(),/session_summary_write_uncertain/);assert.notEqual(f.state().entry.exported,true);});
test('history renewal affects only the matching tab session, never credentials',()=>{const s=storage();s.setItem('rs_native_recognition_session_v1','current');renewHistorySession({exported:true,renew_session_uid:'old'},s,()=> 'new');assert.equal(s.getItem('rs_native_recognition_session_v1'),'current');renewHistorySession({exported:true,renew_session_uid:'current'},s,()=> 'new');assert.equal(s.getItem('rs_native_recognition_session_v1'),'new');});
test('confirmed legacy event advances to the next queued event; unavailable event stays queued',async()=>{const s=storage(),key='rs_recognition_pending_history_v1',ids=[crypto.randomUUID(),crypto.randomUUID()],seen=[];s.setItem(key,JSON.stringify(ids));await flushIamHistory({endpoint:'https://ringstatus.com/test/rs-inputs/native-recognition',persistentStorage:s,fetchImpl:async(_url,o)=>{seen.push(JSON.parse(o.body).history_event_uid);return seen.length===1?Response.json({exported:true,summary_status:'session_identity_conflict'}):Response.json({exported:false},{status:503});}});assert.deepEqual(seen,ids);assert.deepEqual(JSON.parse(s.getItem(key)),[ids[1]]);});
test('new credential attempts have distinct history sessions while retry keeps its original operation',async()=>{
 const persistent=storage(),visit=storage(),sent=[];let fail=true;
 globalThis.__widgetHistoryTest={persistent,visit,fetch:async(_url,o)=>{sent.push(JSON.parse(o.body));if(fail){fail=false;throw Error('lost response');}return Response.json({ok:true,verified:true,device_token:'fixture-token'});}};
 let source=readFileSync(new URL('../src/assets/rs-recognition/widget.js',import.meta.url),'utf8').replace(/^import .*;\r?\n/gm,'');
 source=`const {persistent,visit,fetch}=globalThis.__widgetHistoryTest;const location={origin:'https://ringstatus.com'},navigator={};const safeStorage=s=>s,browserStorage=n=>n==='localStorage'?persistent:visit,runSilentRecognition=async o=>o.onOutcome({outcome:'recognized',data:{person_uid:'fixture-person',device_fingerprint:'fixture-fingerprint'}}),flushIamHistory=()=>{};\n`+source;
 const {recognitionAdapter}=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
 await assert.rejects(recognitionAdapter.recognize('2025550101'),/lost response/);await recognitionAdapter.recognize('2025550101');assert.equal(sent[0].session_uid,sent[1].session_uid);assert.equal(sent[0].request_id,sent[1].request_id);assert.equal(visit.getItem('rs_native_recognition_session_v1'),sent[1].session_uid);
 await recognitionAdapter.recognize('2025550102');assert.notEqual(sent[2].session_uid,sent[1].session_uid);assert.equal(visit.getItem('rs_native_recognition_session_v1'),sent[2].session_uid);
});
