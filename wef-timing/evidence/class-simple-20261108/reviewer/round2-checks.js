'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'round1/test.js'),'utf8').split('let passed=0;')[0];
const exported={};vm.runInNewContext(source+'\nObject.assign(exported,{example,fake});',{require:n=>n.startsWith('../../')?require('../../../'+n.slice(6)):require(n),__dirname:path.resolve(__dirname,'..'),structuredClone,console,exported,Date,process},{filename:'reviewer/round1-fixture-helper.js'});
const S=require('../../../class-snapshot'),C=require('../../../class-cleanup'),{example,fake}=exported;
const tests=[];async function check(name,fn){try{await fn();tests.push({name,result:'PASS'});}catch(e){tests.push({name,result:'FAIL',error:e.message});}}
(async()=>{
 await check('empty current Ring pointer cannot revive an old group backlink',()=>{const e=example();e.input.rings[0].fields['current-group']=[];e.input.groups[0].fields['current-in-ring']=[{id:e.input.rings[0].id}];assert.equal(S.build(e).filter(x=>x.fields.status==='Underway').length,0);});
 await check('source false is-live remains false',()=>{const e=example();e.input.groups[0].fields['source-json']=JSON.stringify({is_live:false});assert.equal(S.build(e)[0].fields['is-live'],'false');});
 await check('Class logs and heartbeat retain verified default-show ID',async()=>{const e=example(),api=fake(e.input);const report=await S.run({api,now:e.now,runId:'review-round2',readHistory:()=>e.history});assert.equal(report.ok,true);assert.equal(api.data[S.T.runs][0].fields['show-id'],e.show);assert.equal(api.data[S.T.heartbeat][0].fields['show-id'],e.show);});
 await check('invalid default identity is failed rather than successful empty output',async()=>{const e=example(),api=fake(e.input);delete api.data[S.T.shows][0].fields.show_id;const report=await S.run({api,now:e.now,runId:'review-invalid-show',readHistory:()=>[]});assert.equal(report.ok,false);assert.equal(report.written,0);});
 await check('record-ID reference protects an otherwise eligible old timing answer',()=>{const e=example(),row={id:'rec00000000000090',createdTime:'2026-11-01T10:00:00Z',fields:{'show-id':200000076,date:'2026-11-01','evaluated-at':'2026-11-01T10:00:00Z','answer-key':'old-key','payload-json':JSON.stringify({identity:{customer:15,show:200000076,date:'2026-11-01'}})}};const selected=C.plan({name:'timing-answers',rows:[row],event:{kind:'new-show',oldShow:200000076,show:e.show,day:e.day},now:e.now,referencedAnswers:[row.id]});assert.equal(selected.length,0);});
 await check('explicit retired checkpoint is cleaned through full offline new-show executor',async()=>{const e=example(),api=fake(e.input),os=require('node:os');const dir=fs.mkdtempSync(path.join(os.tmpdir(),'class-review-round2-'));try{
  fs.writeFileSync(path.join(dir,'state.json'),JSON.stringify({show:200000076,retiredCheckpoints:[{id:'rec00000000000090',show:200000076,date:'2026-11-01'}]}));
  api.data[S.T.checkpoints]=[{id:'rec00000000000090',createdTime:'2026-11-01T10:00:00Z',fields:{'checkpoint-key':'retired','state-json':'{}'}}];
  api.data[S.T.cleanup]=[{id:'rec00000000000091',fields:{'\uFEFFName':'engine-checkpoints',cleanup:'checked'}}];
  const originalList=api.list;api.list=async(t,f)=>{const rows=await originalList(t,f);if(t===S.T.catalog&&f?.includes('archive-key'))return rows.filter(r=>f.includes(r.fields['archive-key']));if(f?.startsWith('OR('))return rows.filter(r=>f.includes(r.id));return rows;};
  const result=await C.run({api,now:e.now,runtime:dir});assert.equal(result.deleted,1);assert.equal(api.data[S.T.checkpoints].length,0);assert.equal(api.data[S.T.catalog].length,1);
 }finally{fs.rmSync(dir,{recursive:true,force:true});}});
 await check('shared current group fallback does not run workload in parallel',()=>{const e=example();e.history=[];
  e.input.live[0].fields.total=20;e.input.live[0].fields.gone=10;e.input.groups[0].fields.total=40;e.input.groups[0].fields.gone=20;
  const second=structuredClone(e.input.classes[0]);second.id='rec00000000000070';second.fields['class-key']=second.fields['class-key'].replace('|101','|103');second.fields['class-id']=103;second.fields.schedules=[{id:'rec00000000000071'}];e.input.classes.push(second);
  const live=structuredClone(e.input.live[0]);live.id='rec00000000000072';live.fields['class-id']=103;e.input.live.push(live);
  const schedule=structuredClone(e.input.schedules[0]);schedule.id='rec00000000000071';schedule.fields.class=second.fields['class-key'];schedule.fields.total_trips=20;schedule.fields.completed_trips=10;e.input.schedules.push(schedule);
  e.input.schedules[1].fields['start-time']='14:30:00';
  const output=S.build(e),next=output.find(r=>r.fields['class-key'].endsWith('|102')).fields;
  const minimum=Date.parse(e.now)+20*2.5746*60000;
  assert(next['estimated-start']===null||Date.parse(next['estimated-start'])>=minimum,'Shared remaining20trips must not be reduced to10trips');
 });
 await check('invalid default show cannot trigger new-show deletion',async()=>{const e=example(),api=fake(e.input),os=require('node:os');delete api.data[S.T.shows][0].fields.show_id;const dir=fs.mkdtempSync(path.join(os.tmpdir(),'class-review-invalid-cleanup-'));try{
  fs.writeFileSync(path.join(dir,'state.json'),JSON.stringify({show:200000076}));
  api.data[S.T.runs]=[{id:'rec00000000000080',createdTime:'2026-11-01T10:00:00Z',fields:{'run-id':'old-finished',result:'Success',completed:'2026-11-01T10:01:00Z','show-id':200000076}}];
  api.data[S.T.cleanup]=[{id:'rec00000000000081',fields:{'\uFEFFName':'run-logs',cleanup:'checked'}}];
  const result=await C.run({api,now:e.now,runtime:dir});assert.equal(result.deleted,0);assert.equal(result.ok,false);
 }finally{fs.rmSync(dir,{recursive:true,force:true});}});
 for(const result of tests)console.log(JSON.stringify(result));fs.writeFileSync(path.join(__dirname,'round2-results.json'),JSON.stringify(tests,null,2));process.exitCode=tests.some(t=>t.result==='FAIL')?1:0;
})().catch(e=>{console.error(e);process.exitCode=1;});
