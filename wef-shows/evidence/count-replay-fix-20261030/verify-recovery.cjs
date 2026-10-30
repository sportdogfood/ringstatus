// GET-only acceptance of a completed scheduled run. Never calls a collector.
const fs=require('fs'),path=require('path'),assert=require('assert/strict'),zlib=require('zlib');
const shows=path.resolve(__dirname,'../..'),settings=require(path.join(shows,'settings'));
const {hash,readAccepted}=require(path.join(shows,'show-days'));
async function main(){
 const receiptFile=path.join(settings.RUNTIME,'last-run.json'),receipt=JSON.parse(fs.readFileSync(receiptFile)),deployment=JSON.parse(fs.readFileSync(path.join(__dirname,'deployment.json')));
 assert.ok(Date.parse(receipt.started)>Date.parse(deployment.at),'Need a post-deployment scheduled run');
 const audit=await require(path.join(shows,'verify-audit')).verifyAudit();
 const api=require(path.join(shows,'airtable')).airtable(),rows=await api.list('show-days'),showRows=await api.list('shows'),before=JSON.parse(fs.readFileSync(path.join(__dirname,'days-before.json')));
 const days=[];
 for(const reported of receipt.show_days.days){
  const prior=before.find(r=>r.fields.date===reported.date),row=rows.find(r=>r.id===prior?.id);assert.ok(row,'Existing day identity must survive');
  const show=showRows.find(r=>r.id===row.fields.show?.[0]);assert.ok(show);assert.equal(row.fields['show-day-key'],show.fields['show-key']+'|'+reported.date);
  assert.ok(Date.parse(row.fields['latest-attempt-at'])>=Date.parse(receipt.started));
  if(reported.status==='Unknown'){
   assert.equal(row.fields['publication-status'],'Unknown');assert.equal(row.fields['latest-attempt-status'],'Unknown');
   assert.equal(row.fields['latest-attempt-reason'],'Data has not accepted the current canonical show/date context');
   for(const field of ['accepted-summary','unique-entry-count','trip-count','count-observed-at','data-evidence-ref'])assert.deepEqual(row.fields[field],prior.fields[field],field+' must be preserved');
  }else await readAccepted(()=>api.list('show-days'),row.fields['show-day-key'],show,receipt.completed);
  days.push({id:row.id,date:reported.date,status:row.fields['publication-status'],attempt:row.fields['latest-attempt-at'],reason:row.fields['latest-attempt-reason']||'',show:show.id,selectedDate:show.fields.selected_date});
 }
 assert.equal(days.length,4);
 for(const item of receipt.archives){const decoded=zlib.gunzipSync(fs.readFileSync(item.file));assert.equal(hash(decoded),item.sha256);JSON.parse(decoded);}
 assert.equal(fs.existsSync(path.join(settings.RUNTIME,'show-days-stopped.json')),false);
 assert.equal(JSON.parse(fs.readFileSync(receiptFile)).run_id,receipt.run_id,'Receipt changed during readback');
 const result={...audit,days,archivesVerified:receipt.archives.length,manualRun:false};
 fs.writeFileSync(path.join(__dirname,'normal-'+receipt.run_id+'.json'),JSON.stringify(result,null,2));
 console.log(JSON.stringify(result));
}
main().catch(error=>{console.error(error.message);process.exitCode=1;});
