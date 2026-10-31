const assert=require('node:assert/strict'),path=require('node:path');
const root=path.resolve(__dirname,'../../staged/direct-counts-20261031/wef-shows');
const {countDays}=require(path.join(root,'direct-counts')),{publishDays}=require(path.join(root,'show-days'));
(async()=>{
 const show={id:'s',fields:{'show-key':'15|76',customer_id:15,show_id:76,start_date:'2026-10-31',end_date:'2026-10-31',available_dates:'2026-10-31'}};
 const result=await countDays({show,runId:'offline',clock:()=>new Date().toISOString(),fetchCount:async url=>{
  if(new URL(url).pathname.startsWith('/classes'))throw Error('Class response unavailable');
  return{sha256:'a'.repeat(64),payload:{show:{show_id:76,start_date:'2026-10-31',end_date:'2026-10-31'},show_date:'2026-10-31',rings:[{ring_number:1,classes:[{class_id:123}]}]}};
 }});
 const rows=[];const api={list:async()=>structuredClone(rows),upsert:async(t,k,values)=>{for(const value of values){let row=rows.find(x=>x.fields[k]===value[k]);if(!row){row={id:'d',fields:{}};rows.push(row)}Object.assign(row.fields,value)}return structuredClone(rows)}};
 const report=await publishDays({api,show,readSummaries:async()=>result});
 assert.equal(result.diagnostics[0].complete,false);assert(result.diagnostics[0].errors.length);
 assert.equal(report.ok,false);assert.equal(rows[0].fields['latest-attempt-status'],'Failed');
 assert.equal(rows[0].fields['unique-entry-count'],undefined);
 console.log(JSON.stringify({sourceIncomplete:true,publisherOk:report.ok,status:rows[0].fields['publication-status'],errors:report.errors}));
})().catch(e=>{console.error(e);process.exitCode=1});
