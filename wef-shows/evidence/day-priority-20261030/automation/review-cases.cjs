const fs=require('fs'),path=require('path'),os=require('os'),assert=require('assert/strict');
const {updatePriority}=require(path.resolve(__dirname,'../../../staged/day-priority-20261030/wef-shows/day-priority'));
const temp=fs.mkdtempSync(path.join(os.tmpdir(),'priority-review-'));
const now='2026-10-30T23:04:00Z';
function fixture(name,links=['canonical']){
 const show={id:'canonical',fields:{'show-key':'15|76',show_id:76,customer_id:15,is_default:true,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-31'}};
 const rows=[{id:'day',fields:{'show-day-key':'15|76|2026-10-30',show:links,date:'2026-10-30'}}];let calls=0;
 const api={list:async table=>structuredClone(table==='shows'?[show]:rows),call:async(base,table,method,body)=>{calls++;for(const r of body.records)Object.assign(rows.find(x=>x.id===r.id).fields,r.fields);}};
 return{show,rows,api,statePath:path.join(temp,name+'.json'),calls:()=>calls};
}
(async()=>{
 const ambiguous=fixture('ambiguous',['canonical','other-show']);
 await assert.rejects(updatePriority({...ambiguous,now}),/ambiguous|multiple|Invalid/i);
 assert.equal(ambiguous.calls(),0);
 console.log(JSON.stringify({case:'ambiguous-parent',status:'PASS',patches:ambiguous.calls(),row:ambiguous.rows[0]}));
 const partial=fixture('partial'),call=partial.api.call;
 partial.api.call=async(...args)=>{await call(...args);throw Error('fixture response lost after write');};
 await assert.rejects(updatePriority({...partial,now}),/response lost/);
 assert(fs.existsSync(partial.statePath+'.stopped.json'));assert(!fs.existsSync(partial.statePath));
 await assert.rejects(updatePriority({...partial,now}),/stopped/);assert.equal(partial.calls(),1);
 console.log(JSON.stringify({case:'uncertain-write',status:'PASS',patches:partial.calls(),stopPreserved:true}));
 const empty=fixture('empty');empty.rows.length=0;
 const result=await updatePriority({...empty,now});assert.equal(result.verified,0);assert.equal(empty.calls(),0);
 console.log(JSON.stringify({case:'empty',status:'PASS',result}));
})().catch(e=>{console.error(e.stack);process.exitCode=1;}).finally(()=>{
 if(path.dirname(temp)!==path.resolve(os.tmpdir()))throw Error('Unexpected temporary path');
 fs.rmSync(temp,{recursive:true,force:true});
});
