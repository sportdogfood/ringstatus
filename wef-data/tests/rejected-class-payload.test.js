'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const source=require('../fast-source');
const route='/classes/200031425',params={customer_id:15,show_id:200000078};
const once=body=>source.once(route,params,'iphone',null,async()=>Response.json(body));
test('wrong ID is recorded independently while full rows are retained',async()=>{
 const payload={ID:'200031435',rows:[{id:'200403147',Hor:'SEGER'}]};
 const r=await once(payload);
 assert.equal(r.ok,false);assert.deepEqual(r.metric.validation.failed,['class_id_match']);
 assert.equal(r.metric.validation.expected_id,'200031425');assert.equal(r.metric.validation.actual_id,'200031435');
 assert.equal(r.metric.validation.rows_type,'array');assert.deepEqual(r.metric.rejected_payload,payload);
});
test('matching ID with missing rows reports only the rows check',async()=>{
 const r=await once({ID:'200031425',recs:0});
 assert.deepEqual(r.metric.validation.failed,['rows_array']);assert.equal(r.metric.validation.rows_type,'undefined');
 assert.deepEqual(r.metric.rejected_payload,{ID:'200031425',recs:0});
});
test('null or object rows are distinguished without losing the payload',async()=>{
 for(const rows of [null,{}]){
  const r=await once({ID:'200031425',rows});assert.deepEqual(r.metric.validation.failed,['rows_array']);
  assert.equal(r.metric.validation.rows_type,rows===null?'null':'object');assert.deepEqual(r.metric.rejected_payload,{ID:'200031425',rows});
 }
});
test('empty or null payload records both failed checks',async()=>{
 for(const body of [{},null]){
  const r=await once(body);assert.deepEqual(r.metric.validation.failed,['class_id_match','rows_array']);
  assert.equal(r.metric.validation.actual_id,null);assert.deepEqual(r.metric.rejected_payload,body);
 }
});
test('non JSON class response retains the full redacted response text',async()=>{
 const raw='<html>'+ 'x'.repeat(5000)+' Bearer private-bearer</html>';
 const r=await source.once(route,params,'iphone',null,async()=>new Response(raw,{status:200}));
 assert.deepEqual(r.metric.validation.failed,['json_parse']);assert.equal(typeof r.metric.rejected_payload,'string');
 assert.ok(r.metric.rejected_payload.includes('x'.repeat(5000)));assert.doesNotMatch(JSON.stringify(r),/private-bearer/);
});
test('HTTP rejection preserves payload and original status without class reclassification',async()=>{
 const r=await source.once(route,params,'iphone',null,async()=>Response.json({message:'Unavailable',note:'source error'},{status:503}));
 assert.equal(r.metric.http,503);assert.deepEqual(r.metric.validation.failed,['http_status']);
 assert.deepEqual(r.metric.rejected_payload,{message:'Unavailable',note:'source error'});
});
test('rejected JSON keeps all nonsecret data without diagnostic truncation',async t=>{
 const prior=process.env.SGL_TOKEN;process.env.SGL_TOKEN='configured-private';t.after(()=>{if(prior===undefined)delete process.env.SGL_TOKEN;else process.env.SGL_TOKEN=prior;});
 const payload={ID:'other',rows:[{Hor:'SEGER',note:'a'.repeat(5000),comment:'Bearer private-bearer configured-private',_t:'private-token',cookie:'private-cookie',nested:{authorization:'private-auth',value:'safe'}}]};
 const r=await once(payload);assert.equal(r.metric.rejected_payload.rows[0].note.length,5000);
 assert.equal(r.metric.rejected_payload.rows[0].Hor,'SEGER');assert.equal(r.metric.rejected_payload.rows[0].nested.value,'safe');
 assert.doesNotMatch(JSON.stringify(r),/private-bearer|configured-private|private-token|private-cookie|private-auth/);
});
test('each existing preparation profile retains its own rejected payload and validation',async()=>{
 let calls=0;const r=await source.read(route,params,true,null,async()=>{
  calls++;return Response.json(calls===1?{ID:'wrong',rows:[]}:calls===2?{ID:'200031425',rows:null}:{});
 });
 assert.equal(calls,3);assert.equal(r.attempts.length,3);assert.deepEqual(r.attempts.map(a=>a.profile),['iphone','android','windows']);
 assert.deepEqual(r.attempts.map(a=>a.validation.failed),[['class_id_match'],['rows_array'],['class_id_match','rows_array']]);
 assert.deepEqual(r.attempts[0].rejected_payload,{ID:'wrong',rows:[]});assert.deepEqual(r.attempts[1].rejected_payload,{ID:'200031425',rows:null});
});
test('valid data and legitimate empty rows remain successful single requests',async()=>{
 for(const rows of [[],[{id:'200403147'}]]){
  let calls=0;const payload={ID:'200031425',rows};const r=await source.once(route,params,'iphone',null,async()=>{calls++;return Response.json(payload);});
  assert.equal(calls,1);assert.equal(r.ok,true);assert.deepEqual(JSON.parse(Buffer.from(r.body,'base64')),payload);
  assert.equal(r.metric.validation,undefined);assert.equal(r.metric.rejected_payload,undefined);
 }
});
test('schedule validation and retries have no class diagnostic additions',async()=>{
 let calls=0;const r=await source.read('/schedule',{...params,date:'2026-11-14'},true,null,async()=>{calls++;return Response.json({message:'Deferred'},{status:502});});
 assert.equal(calls,2);assert.ok(r.attempts.every(a=>a.validation===undefined&&a.rejected_payload===undefined));
});

