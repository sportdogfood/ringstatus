"use strict";
// GET-only verification of normal-run publications. No collector entry points.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const settings=require('./settings'),{readAccepted,dates,canonical,hash}=require('./show-days'),{reader}=require('./day-source');
async function verifyDays(){
 const underlying=require('./airtable').airtable(),api={list:underlying.list};
 const receipt=JSON.parse(fs.readFileSync(path.join(settings.RUNTIME,'last-run.json'),'utf8'));
 assert.equal(receipt.ok,true,JSON.stringify(receipt.errors));assert.equal(receipt.discovery_ok,true);assert.equal(receipt.show_days?.ok,true);
 const shows=await api.list('shows'),defaults=shows.filter(r=>r.fields.is_default);assert.equal(defaults.length,1);const show=defaults[0];assert.equal(show.fields.show_id,receipt.show_id);
 const logs=await api.list('run-logs');assert.equal(logs.find(r=>r.fields['run-id']===receipt.run_id)?.fields.result,'Success');
 const read=()=>api.list('show-days'),rows=await read(),out=[],replayed=new Map();
 for(const day of dates(show)){
  const key=show.fields['show-key']+'|'+day,matches=rows.filter(r=>r.fields['show-day-key']===key);assert.equal(matches.length,1);const row=matches[0],reported=receipt.show_days.days.find(d=>d.date===day);assert.ok(reported);assert.deepEqual(row.fields.show,[show.id]);
  if(!row.fields['accepted-summary']){assert.equal(row.fields['publication-status'],'Unknown');assert.equal(row.fields['unique-entry-count']??null,null);out.push({date:day,id:row.id,status:'Unknown',reason:'No accepted summary'});continue;}
  const accepted=await readAccepted(read,key,show,new Date().toISOString()),head=accepted.body.evidence['run-key'];
  if(!replayed.has(head))replayed.set(head,await reader(api,{headKey:head})(show));
  const source=replayed.get(head).days.find(d=>d.date===day);assert.ok(source);assert.equal(canonical(source),canonical(accepted.body));assert.equal(reported.revision,head);assert.equal(reported.digest,hash(canonical(accepted.body)));
  out.push({date:day,id:row.id,show:show.id,revision:head,digest:reported.digest,sourceAsOf:source['accepted-as-of'],coverage:source.coverage,completeness:source.completeness,uniqueEntries:source['unique-entry-count'],trips:source['proven-trip-count'],status:accepted.status});
 }
 const after=JSON.parse(fs.readFileSync(path.join(settings.RUNTIME,'last-run.json'),'utf8'));assert.equal(after.run_id,receipt.run_id,'Scheduled run changed during verification; evidence not saved');
 const result={at:new Date().toISOString(),result:'PASS',showsRun:receipt.run_id,started:receipt.started,completed:receipt.completed,days:out};
 const dir=path.join(__dirname,'evidence/show-days-build-20261029');fs.mkdirSync(dir,{recursive:true});fs.writeFileSync(path.join(dir,'normal-'+receipt.run_id+'.json'),JSON.stringify(result,null,2));return result;
}
if(require.main===module)verifyDays().then(r=>console.log(JSON.stringify(r))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verifyDays};
