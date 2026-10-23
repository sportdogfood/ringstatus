"use strict";
// Read-only verification. Does not call discovery, schedules or consumer entry points.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),zlib=require('node:zlib');
const settings=require('./settings');
let readQueue=Promise.resolve();
async function list(base,table){let rows=[],offset;do{const q=new URLSearchParams({pageSize:'100',...(offset?{offset}:{})});readQueue=readQueue.then(()=>new Promise(r=>setTimeout(r,250)));await readQueue;const r=await fetch(`https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}?${q}`,{headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});if(!r.ok)throw Error('Read HTTP '+r.status);const d=await r.json();rows.push(...d.records);offset=d.offset;}while(offset);return rows;}
async function producer(){
 const receipt=JSON.parse(fs.readFileSync(path.join(settings.RUNTIME,'last-run.json')));assert.equal(receipt.ok,true,receipt.errors.join('; '));
 const [shows,logs,hb]=await Promise.all(['shows','run-logs','heartbeat'].map(t=>list(settings.BASE_ID,t)));
 const defaults=shows.filter(r=>r.fields.is_default);assert.equal(defaults.length,1);const show=defaults[0];assert.equal(show.fields.show_id,receipt.show_id);assert.equal(show.fields.selected_date,receipt.selected_date);
 const payload=require('../wef-schedules/show-source').verifiedPayload(show.fields);assert.equal(Number(payload.show.show_id),show.fields.show_id);
 assert.equal(logs.find(r=>r.fields['run-id']===receipt.run_id)?.fields.result,'Success');assert.equal(hb[0].fields.result,'Success');
 for(const e of receipt.archives){const data=zlib.gunzipSync(fs.readFileSync(e.file));assert.equal(crypto.createHash('sha256').update(data).digest('hex'),e.sha256);}
 const result={at:new Date().toISOString(),result:'PASS',show,receipt};fs.writeFileSync(path.join(__dirname,'evidence','producer-'+receipt.run_id+'.json'),JSON.stringify(result,null,2));return result;
}
async function consumer(){
 const p=await producer(),cutover=JSON.parse(fs.readFileSync(path.join(__dirname,'evidence/cutover.json')));
 const focus=JSON.parse(fs.readFileSync(path.join(__dirname,'../wef-schedules/runtime/focus.json')));
 assert.ok(focus.checked_at>cutover.at);assert.equal(focus.last_collection.ok,true);assert.equal(focus.show_source.base,settings.BASE_ID);assert.equal(focus.show_source.record,p.show.id);assert.equal(focus.fields.show_id,p.show.fields.show_id);
 const b='appUq5Qz9kARqFija', [shows,schedules,rings,logs]=await Promise.all(['shows','schedules','rings','run-logs'].map(t=>list(b,t)));
 const baseline=JSON.parse(fs.readFileSync(path.join(__dirname,'evidence/links-before.json'))),current=shows.find(r=>r.fields.show_id===p.show.fields.show_id);assert.equal(current.id,baseline.show.id);
 for(const key of ['show_id','show_name','customer_id','facility_id','start_date','end_date','selected_date','available_dates'])assert.equal(current.fields[key],p.show.fields[key],'Mirror mismatch '+key);
 assert.equal(current.fields.last_checked,focus.show_source.checked_at);
 const old=new Map(baseline.schedules.map(r=>[r.key,r]));let preserved=0;
 for(const r of schedules){const prior=old.get(r.fields['stage-key']);if(!prior)continue;assert.equal(r.id,prior.id);assert.deepEqual(r.fields['show-details'],prior.show);assert.deepEqual(r.fields.rings??[],prior.rings??[]);preserved++;}assert.equal(preserved,baseline.schedules.length);
 const priorRings=new Map(baseline.rings.map(r=>[r.key,r.id]));for(const r of rings)if(priorRings.has(r.fields['ring-key']))assert.equal(r.id,priorRings.get(r.fields['ring-key']));
 const completed=logs.filter(r=>r.fields.started>=cutover.at&&['Success','Warning'].includes(r.fields.result)).sort((a,b)=>a.fields.started.localeCompare(b.fields.started));assert.ok(completed.length>=1);const log=completed.at(-1);assert.equal(log.fields['show-id'],p.show.fields.show_id);
 assert.ok(focus.last_collection.days.every(d=>d.ok));assert.deepEqual(focus.last_collection.days.map(d=>d.date).sort(),p.show.fields.available_dates.split(', '));
 assert.equal(log.fields['days-failed']??0,0);assert.equal(log.fields['days-complete'],focus.last_collection.days.length);
 // Schedules has always retained a Warning for the source-confirmed empty day.
 for(const d of focus.last_collection.days)for(const warning of d.warnings||[])assert.equal(warning.code,'empty_show_day','Unexpected schedule warning');
 assert.deepEqual(log.fields.shows,[baseline.show.id]);
 const archived=[...String(log.fields.archives||'').matchAll(/; ([^;\r\n]+\.json\.gz); SHA256 ([a-f0-9]{64})/g)];assert.equal(archived.length,log.fields['archive-count']);
 for(const [,file,hash]of archived){const body=zlib.gunzipSync(fs.readFileSync(file));assert.equal(crypto.createHash('sha256').update(body).digest('hex'),hash);}
 const result={at:new Date().toISOString(),result:'PASS',run:log,producerRun:p.receipt.run_id,canonicalShowId:p.show.id,scheduleShowId:current.id,source:focus.show_source,days:focus.last_collection.days.map(d=>({date:d.date,ok:d.ok,rows:d.row_count})),preservedScheduleIds:preserved,preservedRingIds:priorRings.size,shows,scheduleIds:schedules.map(r=>({id:r.id,key:r.fields['stage-key']}))};
 fs.writeFileSync(path.join(__dirname,'evidence','consumer-'+log.fields['run-id']+'.json'),JSON.stringify(result,null,2));return result;
}
if(require.main===module)(process.argv.includes('--consumer')?consumer():producer()).then(r=>console.log(JSON.stringify({result:r.result,run:r.receipt?.run_id||r.run?.fields['run-id'],show:r.show?.fields.show_id,preservedScheduleIds:r.preservedScheduleIds,days:r.days},null,2))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={producer,consumer,list};
