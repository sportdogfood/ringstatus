const assert=require('node:assert/strict'),path=require('node:path');
const {reader}=require('./review-isolated/wef-shows/day-source');
const {publishDays,status}=require('./review-isolated/wef-shows/show-days');
(async()=>{
 const show={id:'s',fields:{'show-key':'15|76',customer_id:15,show_id:76,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-31',available_dates:'2026-10-29,2026-10-30,2026-10-31,2026-11-01'}};
 let captured;
 const result=await reader({list:()=>{throw Error('Unexpected Airtable read')}},{savedRoot:__dirname,replay:async input=>{captured=input;return {mode:'saved-day',days:[]}}})(show);
 assert.equal(captured.mode,'saved-day');assert.equal(captured.context['selected-date'],'2026-10-31');
 const rows=captured.context['canonical-days'].map((date,i)=>({id:'d'+i,fields:{date,'show-day-key':'15|76|'+date,show:['s'],'accepted-summary':'prior accepted bytes','unique-entry-count':7}}));
 const before=JSON.stringify(rows);
 const report=await publishDays({show,api:{list:async()=>rows,upsert:async()=>{throw Error('Unexpected historical write')}},readSummaries:async()=>result});
 assert.equal(report.ok,true);assert.equal(report.rows,0);assert.equal(JSON.stringify(rows),before);
 assert.equal(status({completeness:'complete','accepted-as-of':'2026-10-31T12:00:00Z'},'2026-10-31T12:05:00Z'),'Stale');
 console.log('PASS saved dispatch performs no legacy fetch; omitted existing days retained byte-for-byte; five-minute source age remains Stale. Mock API only.');
})().catch(e=>{console.error(e);process.exitCode=1});
