"use strict";
// Passive Airtable/local-file readback only. Never executes collection.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),zlib=require('node:zlib');
const api=require('./collector-core'),settings=require('./settings');
const dir=path.join(__dirname,'evidence','heartbeat-20261020');
const cell=v=>v==null||v===''?null:v;
async function verify(){
 const tables={};for(const name of ['heartbeat','run-logs','show-collection','schedules','shows','rings','rings-helper','wef-tenant-index'])tables[name]=await api.airtableList(name,{},settings.SCHEDULE_STAGING_BASE_ID);
 assert.equal(tables.heartbeat.length,1,'One status row');const heartbeat=tables.heartbeat[0],f=heartbeat.fields;assert.equal(f['heartbeat-key'],'schedules');
 const matching=tables['run-logs'].filter(r=>r.fields['run-id']===f['run-id']);assert.equal(matching.length,1,'Unique matching run-log');const log=matching[0].fields;
 assert.ok(['Success','Warning'].includes(log.result),'Scheduled run must succeed');assert.ok(log.completed);
 for(const key of ['run-id','started','show-id','result','rows','days-complete','days-failed','archive-count','summary','errors'])assert.deepEqual(cell(f[key]),cell(log[key]),'Matching heartbeat '+key);
 assert.equal(Date.parse(f['checked-at']),Date.parse(log.completed));assert.equal(f['days-failed'],0);
 const focus=JSON.parse(fs.readFileSync(settings.SCHEDULE_FOCUS_FILE));assert.equal(focus.last_collection.ok,true);assert.equal(f['show-id'],focus.show_id);assert.equal(f['selected-date'],focus.fields.sgl_default_day);
 const show=tables['shows'].filter(r=>r.fields.is_default);assert.equal(show.length,1);assert.equal(show[0].fields.show_id,f['show-id']);assert.equal(show[0].fields.selected_date,f['selected-date']);
 const collections=tables['show-collection'].filter(r=>Number(r.fields['show-id'])===f['show-id']);assert.equal(collections.length,f['days-complete']);assert.ok(collections.every(r=>r.fields.status==='complete'));assert.equal(collections.reduce((n,r)=>n+(r.fields['row-count']||0),0),f.rows);
 for(const[table,key]of [['schedules','stage-key'],['show-collection','collection-id'],['rings','ring-key'],['shows','show-key']]){const keys=tables[table].map(r=>r.fields[key]);assert.equal(new Set(keys).size,keys.length,'No duplicate '+table);assert.ok(keys.every(Boolean));}
 const archives=[];for(const line of (log.archives||'').split('\n').filter(Boolean)){
  const match=line.match(/; ([^;]+); SHA256 ([a-f0-9]{64})/);assert.ok(match,'Readable archive path/hash');
  const body=zlib.gunzipSync(fs.readFileSync(match[1]));assert.equal(crypto.createHash('sha256').update(body).digest('hex'),match[2]);archives.push({file:match[1],sha256:match[2]});
 }
 assert.equal(archives.length,log['archive-count']||0);
 const evidence={checked_at:new Date().toISOString(),ok:true,heartbeat,run_log:matching[0],verified_archives:archives,focus,tables};
 const file=path.join(dir,'scheduled-'+f['run-id']+'.json');fs.writeFileSync(file,JSON.stringify(evidence,null,2));
 console.log(JSON.stringify({ok:true,run:f['run-id'],result:f.result,completed:log.completed,rows:f.rows,days:f['days-complete'],selected_date:f['selected-date'],heartbeat_id:heartbeat.id,archives:archives.length,file},null,2));return evidence;
}
if(require.main===module)verify().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verify};
