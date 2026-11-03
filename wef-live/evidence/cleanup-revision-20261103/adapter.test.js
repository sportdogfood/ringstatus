'use strict';
globalThis.fetch=async()=>{throw Error('Offline tests: network forbidden');};
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os');
const settings=require('./candidate/settings');
async function setup(t,fields={'show-id':76,groups:['recGone','recKept']}){
 const rows=[{id:'recOld',createdTime:'2026-11-01T00:00:00Z',fields}];
 let current=structuredClone(rows),show=77,deletes=0;
 const request=async(url,options)=>{
  if(url.includes('/shows?'))return{ok:true,json:async()=>({records:[{id:'recShow',fields:{customer_id:15,is_default:true,show_id:show}}]})};
  assert(url.includes('/live-groups?')||url.includes('/heartbeat?'));
  if(options.method==='DELETE'){deletes++;const result=current.map(r=>({id:r.id,deleted:true}));current=[];return{ok:true,json:async()=>({records:result})};}
  return{ok:true,json:async()=>({records:structuredClone(current)})};
 };
 const api=require('./candidate/airtable').airtable({token:'offline-token',pause:async()=>{},request});
 const policy={show:77,trigger:'new-show',deletedIds:new Set(['recGone']),protectedRuns:new Set()};
 return{api,rows,policy,setCurrent:v=>{current=v;},setShow:v=>{show=v;},deletes:()=>deletes};
}
test('actual adapter accepts expected reciprocal-link removals after deletion and confirms exact deleted IDs',async t=>{
 const a=await setup(t);a.setCurrent([{...a.rows[0],fields:{'show-id':76,groups:['recKept']}}]);let count=0;
 await a.api.retireCleanup('live-groups',a.rows,a.policy,ids=>{count+=ids.length;});assert.equal(count,1);assert.equal(a.deletes(),1);
});
test('actual adapter rejects changed business values before DELETE',async t=>{
 const a=await setup(t);a.setCurrent([{...a.rows[0],fields:{'show-id':77,groups:['recKept']}}]);
 await assert.rejects(a.api.retireCleanup('live-groups',a.rows,a.policy),/changed before deletion/);assert.equal(a.deletes(),0);
});
test('actual adapter rejects removing retained links',async t=>{
 const a=await setup(t);a.setCurrent([{...a.rows[0],fields:{'show-id':76,groups:[]}}]);
 await assert.rejects(a.api.retireCleanup('live-groups',a.rows,a.policy),/changed before deletion/);assert.equal(a.deletes(),0);
});
test('actual adapter protects canonical new-show records, archives and non-log quick cleanup',async t=>{
 const a=await setup(t);await assert.rejects(a.api.retireCleanup('collections',a.rows,a.policy),/Forbidden cleanup/);
 
 await assert.rejects(a.api.retireCleanup('live-groups',a.rows,{...a.policy,trigger:'quick-cleanup'}),/Forbidden cleanup/);
 assert.equal(a.deletes(),0);
});
test('actual adapter stops on default-show change without needing an archive',async t=>{
 const a=await setup(t);a.setShow(78);await assert.rejects(a.api.retireCleanup('live-groups',a.rows,a.policy),/Default show changed/);
 assert.equal(a.deletes(),0);
});
test('scheduled integration always finishes collection before cleanup and isolates cleanup failures',async t=>{
 const {scheduled}=require('./candidate/lanes');const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-cleanup-scheduled-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 let invoked=0;const options={runtime,clock:()=> '2026-11-02T15:00:00Z',getStatus:async()=>true,get:async()=>[],api:{list:async table=>table==='shows'?[{id:'recShow',fields:{show_id:77,customer_id:15,is_default:true}}]:table==='show-days'?[{id:'recDay',fields:{'show-day-key':'15|77|2026-11-02','day-priority':0,date:'2026-11-02'}}]:[],call:async()=>({records:[]})},cleanup:async({protectedRuns})=>{
  invoked++;assert.equal(protectedRuns.length,3);for(const name of ['groups','rings','classes'])assert(fs.existsSync(path.join(runtime,'lane-'+name+'-last-run.json')));throw Error('cleanup unavailable');
 }};
 const r=await scheduled(options);assert.equal(invoked,1);assert.equal(r.length,3);assert(r.every(r=>r.completed));assert.equal(r[0].cleanup.result,'Error');
});

test('adapter permits calendar-day deletion of completed current-show logs',async t=>{
 const a=await setup(t,{'show-id':77,result:'Success',completed:'2026-11-03T12:00:00Z'});
 await a.api.retireCleanup('heartbeat',a.rows,{...a.policy,trigger:'calendar-day'});assert.equal(a.deletes(),1);
});
test('adapter protects current-run diff children and Running rows during full clears',async t=>{
 const a=await setup(t,{'show-id':77,'run-id':'current|diff|1',result:'Success',completed:'2026-11-03T12:00:00Z'});
 await assert.rejects(a.api.retireCleanup('heartbeat',a.rows,{...a.policy,trigger:'calendar-day',protectedRuns:new Set(['current'])}),/Forbidden cleanup/);
 const b=await setup(t,{'show-id':77,result:'Running'});await assert.rejects(b.api.retireCleanup('heartbeat',b.rows,{...b.policy,trigger:'new-show'}),/Forbidden cleanup/);assert.equal(a.deletes()+b.deletes(),0);
});
test('adapter permits new-show clear of matching new-show rows',async t=>{
 const a=await setup(t,{'show-id':77});await a.api.retireCleanup('live-groups',a.rows,a.policy);assert.equal(a.deletes(),1);
});
