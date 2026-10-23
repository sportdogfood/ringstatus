"use strict";
// Local evidence comparison only. No network, collector imports or business writes.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
function canonical(v){if(Array.isArray(v))return v.map(canonical);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])]));return v;}
function compare(a,b){
 assert.notEqual(a.receipt.run_id,b.receipt.run_id);assert.equal(a.receipt.show_id,b.receipt.show_id);assert.equal(a.receipt.selected_date,b.receipt.selected_date);
 const ignored={trainers:['last-checked'],horses:['helper-refreshed','last-seen'],riders:['helper-refreshed','last-seen']};
 const selected={heartbeat:['heartbeat-key','selected-date','show-id','result'],collections:['collection-id','show-id','trainer-id','date','status','row-count','shows','trainers'],entries:['entry-key','entry-id','class-id','horse-id','rider-id','date','ring-number','status','trainer-ids','schedule-key','schedule-link','shows','synced-rings','schedules','horses','riders','trainers'],'synced-shows':['show-key','show_id','customer_id','facility_id','show_name','start_date','end_date','selected_date','is_default','rings','schedules'],'synced-rings':['ring-key','show_id','ring_date','ring_id','ring_number','ring_name','show-details','schedules'],'synced-schedules':['stage-key','show-id','date','ring','class-id','show-details','rings']};
 const stable=[];for(const table of ['tenant-details','trainers','heartbeat','synced-shows','collections','synced-rings','synced-schedules','rings-helper','ring-walks','horses','riders','entries','code-reviews']){
  const normalize=rows=>rows.map(r=>({id:r.id,fields:Object.fromEntries(Object.entries(r.fields).filter(([k])=>!(ignored[table]||[]).includes(k)&&(!selected[table]||selected[table].includes(k))))})).sort((a,b)=>a.id.localeCompare(b.id));
  assert.deepEqual(canonical(normalize(a.records[table])),canonical(normalize(b.records[table])),table+' changed beyond expected timestamps');stable.push(table);
 }
 assert.equal(b.records['run-logs'].length,a.records['run-logs'].length+1);
 for(const old of a.records['run-logs'])assert.deepEqual(canonical(b.records['run-logs'].find(r=>r.id===old.id)),canonical(old),'Earlier run-log was overwritten');
 return{ok:true,first:a.receipt.run_id,second:b.receipt.run_id,first_completed:a.receipt.completed,second_completed:b.receipt.completed,stable_tables:stable,new_run_logs:1,verified_archive_events:a.verified_archives+b.verified_archives};
}
if(require.main===module){const files=process.argv.slice(2);if(files.length!==2)throw Error('Two saved verification files required');const r=compare(...files.map(file=>JSON.parse(fs.readFileSync(file))));fs.writeFileSync(path.join(__dirname,'evidence','repeat-comparison.json'),JSON.stringify(r,null,2));console.log(JSON.stringify(r,null,2));}
module.exports={compare};
