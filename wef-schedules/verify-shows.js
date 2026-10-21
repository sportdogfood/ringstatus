"use strict";
// Passive readback of normal runs only. No collection, source requests or repairs.
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const settings=require('./settings'),{verify}=require('./verify-heartbeat');
const dir=path.join(__dirname,'evidence','show-index-20261020');
async function verifyShows(){
 const evidence=await verify(),tables=evidence.tables,heartbeat=evidence.heartbeat.fields;
 const response=await fetch('https://api.airtable.com/v0/meta/bases/'+settings.SCHEDULE_STAGING_BASE_ID+'/tables',{headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});assert.equal(response.ok,true);const schema=await response.json();
 assert.equal(schema.tables.find(t=>t.id===settings.SHOWS_TABLE).name,'shows');assert.equal(schema.tables.find(t=>t.id===settings.SCHEDULE_COLLECTIONS_TABLE).name,'show-collection');assert.ok(!schema.tables.some(t=>['meta','show-details','collections'].includes(t.name)));
 for(const name of ['tenants','wef-tenant-index'])assert.ok(schema.tables.some(t=>t.name===name));
 const shows=tables.shows,current=shows.find(r=>r.fields.show_id===heartbeat['show-id']),archived=shows.find(r=>r.fields.show_id===200000074);
 assert.ok(current&&archived);assert.equal(current.fields['show-status'],'current');assert.equal(archived.fields['show-status'],'archived');assert.equal(archived.fields.show_name,'ESP Fall II');assert.equal(archived.fields.start_date,'2026-10-15');assert.equal(archived.fields.end_date,'2026-10-18');assert.equal(archived.fields['archive-status'],'Verified');
 assert.equal(archived.fields['archived-schedule-count'],499);assert.equal(archived.fields['archived-collection-count'],4);
 const ids={schedules:new Set(),'show-collection':new Set()},files=[];
 for(const line of archived.fields['archive-files'].split('\n')){
  const m=line.match(/^(schedules|show-collection): (\d+) records; (.+); SHA256 ([a-f0-9]{64})$/);assert.ok(m);const body=zlib.gunzipSync(fs.readFileSync(m[3]));assert.equal(crypto.createHash('sha256').update(body).digest('hex'),m[4]);const rows=JSON.parse(body);assert.equal(rows.length,Number(m[2]));for(const r of rows){assert.equal(Number(r.fields['show-id']),200000074);ids[m[1]].add(r.id);}files.push({kind:m[1],file:m[3],records:rows.length,sha256:m[4]});
 }
 assert.equal(ids.schedules.size,499);assert.equal(ids['show-collection'].size,4);
 for(const table of ['schedules','show-collection'])assert.ok(tables[table].every(r=>Number(r.fields['show-id'])===heartbeat['show-id']),'Archived rows must not be restored');
 assert.deepEqual(heartbeat.shows,[current.id]);assert.deepEqual(evidence.run_log.fields.shows,[current.id]);
 for(const r of tables['show-collection'])assert.deepEqual(r.fields.shows,[current.id]);
 for(const r of tables.schedules)assert.ok(r.fields['show-details'].includes(current.id));
 for(const r of tables.rings)assert.ok(r.fields['show-details'].includes(current.id));
 for(const r of tables['wef-tenant-index'].filter(r=>r.fields.active))assert.ok(r.fields['show-details'].includes(current.id));
 const historical=tables['run-logs'].find(r=>r.fields['run-id']==='historical-archive-ESP-Fall-II-200000074');assert.ok(historical.fields.shows.includes(archived.id));assert.ok(archived.fields['run-logs'].includes(historical.id));
 const baseline=JSON.parse(fs.readFileSync(path.join(dir,'records-before.json'))),sort=rows=>rows.map(r=>({id:r.id,fields:r.fields})).sort((a,b)=>a.id.localeCompare(b.id));
 for(const name of ['schedules','rings','rings-helper'])assert.deepEqual(sort(tables[name]),sort(baseline.tables[name]),name+' baseline changed');
 assert.deepEqual(tables['show-collection'].map(r=>r.id).sort(),baseline.tables.collections.map(r=>r.id).sort());
 const preserved=JSON.parse(fs.readFileSync(path.join(dir,'meta-preserved.json'))),notes=fs.readFileSync(path.join(__dirname,'..','docs','coordination','wef-schedules-meta-notes-preserved-20261020.md'),'utf8');for(const r of preserved.records)assert.ok(notes.includes(r.fields.notes));
 evidence.show_index={ok:true,show_ids:shows.map(r=>r.fields.show_id),current_id:current.id,archived_id:archived.id,archived_files:files,meta_notes_preserved:true,original_table_ids_preserved:true};evidence.schema=schema;
 const file=path.join(dir,'scheduled-'+heartbeat['run-id']+'.json');fs.writeFileSync(file,JSON.stringify(evidence,null,2));console.log(JSON.stringify({ok:true,run:heartbeat['run-id'],completed:heartbeat['checked-at'],current_show:current.fields.show_id,archived_show:archived.fields.show_name,archived_schedules:ids.schedules.size,archived_collections:ids['show-collection'].size,archived_files:files.length,schedules:tables.schedules.length,file},null,2));return evidence;
}
if(require.main===module)verifyShows().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verifyShows};
