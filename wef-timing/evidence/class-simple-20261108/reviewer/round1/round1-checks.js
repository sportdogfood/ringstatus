'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
// Read the writer's helper declarations without invoking its test entrypoint.
const source=fs.readFileSync(path.join(__dirname,'../test.js'),'utf8').split('let passed=0;')[0];
const exported={};vm.runInNewContext(source+'\nObject.assign(exported,{example,fake});',{require:n=>n.startsWith('../../')?require('../../../'+n.slice(6)):require(n),__dirname:path.resolve(__dirname,'..'),structuredClone,console,exported,Date,process},{filename:'reviewer/fixture-helper.js'});
const S=require('../../../class-snapshot'),C=require('../../../class-cleanup'),{example,fake}=exported;
const tests=[];async function check(name,fn){try{await fn();tests.push({name,result:'PASS'});}catch(e){tests.push({name,result:'FAIL',error:e.message});}}
(async()=>{
 await check('empty current Ring pointer must not revive old group backlink',()=>{const e=example();e.input.rings[0].fields['current-group']=[];e.input.groups[0].fields['current-in-ring']=[{id:e.input.rings[0].id}];const out=S.build(e);assert.equal(out.filter(x=>x.fields.status==='Underway').length,0);});
 await check('per-class phase-only counts must not mark every group member live',()=>{const e=example();e.input.groups[0].fields['source-json']=JSON.stringify({is_live:false});assert.equal(S.build(e)[0].fields['is-live'],'false');});
 await check('count decrease cannot produce negative measured pace from old actual start',()=>{const e=example();e.input.live[0].fields.gone=0;const out=S.build(e);assert(out[0].fields['pace-used']>0);});
 await check('unknown current class observation cannot borrow an older phase actual start as fresh evidence',()=>{const e=example();e.input.live[0].fields['last-seen']='bad';const out=S.build(e);assert.equal(out[0].fields['actual-start'],null);});
 await check('new-show cleanup can identify Class run logs and heartbeat by show ID',async()=>{const e=example(),api=fake(e.input);const report=await S.run({api,now:e.now,runId:'review-round1',readHistory:()=>e.history});assert.equal(report.ok,true);assert.equal(api.data[S.T.runs][0].fields['show-id'],e.show);assert.equal(api.data[S.T.heartbeat][0].fields['show-id'],e.show);});
 await check('explicit retired checkpoint can be cleaned on verified default-show change',async()=>{const e=example(),api=fake(e.input),os=require('node:os');const dir=fs.mkdtempSync(path.join(os.tmpdir(),'class-review-retired-'));try{
  fs.writeFileSync(path.join(dir,'state.json'),JSON.stringify({show:200000076,retiredCheckpoints:['rec00000000000090']}));
  api.data[S.T.checkpoints]=[{id:'rec00000000000090',createdTime:'2026-11-01T10:00:00Z',fields:{'checkpoint-key':'retired','show-id':200000076,date:'2026-11-01','state-json':'{}'}}];
  api.data[S.T.cleanup]=[{id:'rec00000000000091',fields:{'\uFEFFName':'engine-checkpoints',cleanup:'checked'}}];
  const originalList=api.list;api.list=async(t,f)=>{const rows=await originalList(t,f);if(t===S.T.catalog&&f?.includes('archive-key'))return rows.filter(r=>f.includes(r.fields['archive-key']));if(f?.startsWith('OR('))return rows.filter(r=>f.includes(r.id));return rows;};
  const result=await C.run({api,now:e.now,runtime:dir});assert.equal(result.deleted,1);
 }finally{fs.rmSync(dir,{recursive:true,force:true});}});
 for(const result of tests)console.log(JSON.stringify(result));fs.writeFileSync(path.join(__dirname,'round1-results.json'),JSON.stringify(tests,null,2));process.exitCode=tests.some(t=>t.result==='FAIL')?1:0;
})().catch(e=>{console.error(e);process.exitCode=1;});
