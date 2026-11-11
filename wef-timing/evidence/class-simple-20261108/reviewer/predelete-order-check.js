'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),os=require('node:os');
const source=fs.readFileSync(path.join(__dirname,'round1/test.js'),'utf8').split('let passed=0;')[0],exported={};
vm.runInNewContext(source+'\nObject.assign(exported,{example,fake});',{require:n=>n.startsWith('../../')?require('../../../'+n.slice(6)):require(n),__dirname:path.resolve(__dirname,'..'),structuredClone,console,exported,Date,process});
const S=require('../../../class-snapshot'),C=require('../../../class-cleanup');
(async()=>{const e=exported.example(),api=exported.fake(e.input),dir=fs.mkdtempSync(path.join(os.tmpdir(),'class-review-predelete-'));try{
 fs.writeFileSync(path.join(dir,'state.json'),JSON.stringify({show:200000076}));
 const answer={id:'rec00000000000090',createdTime:'2026-11-01T00:00:00Z',fields:{'show-id':200000076,date:'2026-11-01','evaluated-at':'2026-11-01T10:00:00Z','answer-key':'old-key','payload-json':JSON.stringify({identity:{customer:15,show:200000076,date:'2026-11-01'}})}};
 api.data[S.T.answers]=[answer];api.data[S.T.cleanup]=[{id:'rec00000000000091',fields:{'\uFEFFName':'timing-answers',cleanup:'checked'}}];
 const create=api.create;api.create=async(t,f)=>{const result=await create(t,f);if(t===S.T.catalog){answer.fields.date='2026-11-09';answer.fields['payload-json']=JSON.stringify({identity:{customer:15,show:200000076,date:'2026-11-09'}});}return result;};
 let error;try{await C.run({api,now:e.now,runtime:dir});}catch(e){error=e.message;}
 const protectedRecordSurvives=api.data[S.T.answers].some(r=>r.id===answer.id);
 const result={case:'record changes to future scope during archive/catalog readback',protected_record_survives:protectedRecordSurvives,error:error||null};fs.writeFileSync(path.join(__dirname,'predelete-order-result.json'),JSON.stringify(result,null,2));console.log(JSON.stringify(result));assert(protectedRecordSurvives,'Future record changed during archive work was deleted');
}finally{fs.rmSync(dir,{recursive:true,force:true});}})().catch(e=>{console.error(e);process.exitCode=1;});
