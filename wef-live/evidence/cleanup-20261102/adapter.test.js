'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const settings=require('./candidate/settings');
async function setup(t,fields={'show-id':76,groups:['recGone','recKept']}){
 const root=path.join(settings.RUNTIME,'history'),unique='offline-'+process.pid+'-'+Math.random().toString(36).slice(2);
 const rows=[{id:'recOld',createdTime:'2026-11-01T00:00:00Z',fields}],receipt=await require('./candidate/archive').archiveFile(root,unique,rows);
 t.after(()=>{fs.rmSync(path.dirname(receipt.file),{recursive:true,force:true});});
 let current=structuredClone(rows),show=77,deletes=0;
 const request=async(url,options)=>{
  if(url.includes('/shows?'))return{ok:true,json:async()=>({records:[{id:'recShow',fields:{customer_id:15,is_default:true,show_id:show}}]})};
  assert(url.includes('/live-groups?')||url.includes('/heartbeat?'));
  if(options.method==='DELETE'){deletes++;const result=current.map(r=>({id:r.id,deleted:true}));current=[];return{ok:true,json:async()=>({records:result})};}
  return{ok:true,json:async()=>({records:structuredClone(current)})};
 };
 const api=require('./candidate/airtable').airtable({token:'offline-token',pause:async()=>{},request});
 const policy={show:77,trigger:'new-show',deletedIds:new Set(['recGone']),protectedRuns:new Set()};
 return{api,rows,receipt,policy,setCurrent:v=>{current=v;},setShow:v=>{show=v;},deletes:()=>deletes};
}
test('actual adapter accepts archived reciprocal-link removals and confirms exact deleted IDs',async t=>{
 const a=await setup(t);a.setCurrent([{...a.rows[0],fields:{'show-id':76,groups:['recKept']}}]);let count=0;
 await a.api.retireCleanup('live-groups',a.rows,a.policy,a.receipt,ids=>{count+=ids.length;});assert.equal(count,1);assert.equal(a.deletes(),1);
});
test('actual adapter rejects changed business values before DELETE',async t=>{
 const a=await setup(t);a.setCurrent([{...a.rows[0],fields:{'show-id':77,groups:['recKept']}}]);
 await assert.rejects(a.api.retireCleanup('live-groups',a.rows,a.policy,a.receipt),/changed before deletion/);assert.equal(a.deletes(),0);
});
test('actual adapter rejects removing retained links',async t=>{
 const a=await setup(t);a.setCurrent([{...a.rows[0],fields:{'show-id':76,groups:[]}}]);
 await assert.rejects(a.api.retireCleanup('live-groups',a.rows,a.policy,a.receipt),/changed before deletion/);assert.equal(a.deletes(),0);
});
test('actual adapter protects canonical new-show records, archives and non-log quick cleanup',async t=>{
 const a=await setup(t);await assert.rejects(a.api.retireCleanup('collections',a.rows,a.policy,a.receipt),/Forbidden cleanup/);
 await assert.rejects(a.api.retireCleanup('live-groups',a.rows,{...a.policy,show:76},a.receipt),/Forbidden cleanup/);
 await assert.rejects(a.api.retireCleanup('live-groups',a.rows,{...a.policy,trigger:'quick-cleanup'},a.receipt),/Forbidden cleanup/);
 assert.equal(a.deletes(),0);
});
test('actual adapter stops on default-show change or archive tampering',async t=>{
 const a=await setup(t);a.setShow(78);await assert.rejects(a.api.retireCleanup('live-groups',a.rows,a.policy,a.receipt),/Default show changed/);
 await assert.rejects(a.api.retireCleanup('live-groups',a.rows,a.policy,{...a.receipt,sha256:'bad'}),/hash mismatch/);assert.equal(a.deletes(),0);
});
test('scheduled integration always finishes collection before cleanup and isolates cleanup failures',async t=>{
 const {scheduled}=require('./candidate/lanes');const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-cleanup-scheduled-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 let invoked=0;const options={runtime,clock:()=> '2026-11-02T15:00:00Z',api:{list:async()=>[],call:async()=>({records:[]})},cleanup:async({protectedRuns})=>{
  invoked++;assert.equal(protectedRuns.length,3);for(const name of ['groups','rings','classes'])assert(fs.existsSync(path.join(runtime,'lane-'+name+'-last-run.json')));throw Error('cleanup unavailable');
 }};
 const r=await scheduled(options);assert.equal(invoked,1);assert.equal(r.length,3);assert(r.every(r=>r.completed));assert.equal(r[0].cleanup.result,'Error');
});
