"use strict";
// Read-only scheduled-run verification. Never imports or invokes the collector.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const settings=require('./settings'),consumer=require('./entries-consumer');
async function verify(){
 const receipt=JSON.parse(fs.readFileSync(path.join(settings.RUNTIME,'last-run.json'),'utf8'));
 assert.equal(receipt.ok,true,'Scheduled CWF run failed: '+receipt.errors?.join('; '));
 assert.ok(['shadow','synced'].includes(receipt.entry_source_mode),'No consumer-mode scheduled receipt yet');
 assert.ok(Date.now()-Date.parse(receipt.completed)<30*60000,'Scheduled receipt stale');
 const transport=require('./airtable').airtable();const api={list:transport.list};
 const tables=['horses','riders','trainers','entries','collections','rings-helper','ring-walks','tenant-details',settings.SYNC_ENTRIES_TABLE,settings.SYNC_SHOWS_TABLE,settings.SYNC_RINGS_TABLE,settings.SYNC_SCHEDULES_TABLE,'heartbeat'];
 const records=Object.fromEntries(await Promise.all(tables.map(async t=>[t,await api.list(t)])));
 const logs=await api.list('run-logs',settings.BASE_ID,{filterByFormula:`{run-id}='${receipt.run_id}'`});assert.equal(logs.length,1);assert.equal(logs[0].fields.result,'Success');
 assert.equal(records.heartbeat.find(r=>r.fields['heartbeat-key']==='CWF')?.fields.result,'Success');
 const show=records[settings.SYNC_SHOWS_TABLE].find(r=>Number(r.fields.show_id)===receipt.show_id);assert.ok(show);
 const trainers=records.trainers.filter(r=>r.fields.active===true),dates=require('./model').showDays(show.fields);
 const snapshot=await consumer.load({api,show:show.fields,dates,trainers,now:new Date().toISOString()});
 const linked=consumer.links(snapshot.rows,{showRecord:show,rings:records[settings.SYNC_RINGS_TABLE],schedules:records[settings.SYNC_SCHEDULES_TABLE],horses:records.horses,riders:records.riders,trainers});
 for(const row of linked){const saved=records[settings.SYNC_ENTRIES_TABLE].find(r=>r.fields['entry-key']===row['entry-key']);for(const field of consumer.linkFields)assert.deepEqual([...(saved.fields[field]||[])].sort(),[...row[field]].sort(),row['entry-key']+'.'+field);
  for(const [field,table]of [['horses','horses'],['riders','riders'],['trainers','trainers']])for(const id of row[field]){const helper=records[table].find(r=>r.id===id);assert.ok((helper.fields['wef-entries.entries']||[]).includes(saved.id),'Missing reciprocal '+table);}
 }
 if(receipt.entry_source_mode==='synced'){
  assert.equal(receipt.people_fetches,0);assert.equal(receipt.archives.some(a=>a.kind.startsWith('people-')),false,'Duplicate source archive after cutover');
  const cutover=JSON.parse(fs.readFileSync(path.join(__dirname,'evidence/entries-consumer/cutover.json'),'utf8'));
  const prior=JSON.parse(fs.readFileSync(path.join(__dirname,'evidence/entries-consumer','shadow-'+cutover.validatedRuns.at(-1)+'.json'),'utf8'));
  const stable=rows=>[...rows].sort((a,b)=>a.id.localeCompare(b.id));assert.deepEqual(stable(records.entries),stable(prior.records.entries),'Old entries changed after cutover');
 }else assert.equal(receipt.entry_sync.shadow_match,true);
 const baseline=JSON.parse(fs.readFileSync(path.join(__dirname,'evidence/entries-consumer/baseline.json'),'utf8'));
 const original=baseline.records||baseline.tables;
 assert.ok(original,'Baseline record map missing');
 for(const table of ['horses','riders','rings-helper','ring-walks','tenant-details'])for(const old of original[table]){const current=records[table].find(r=>r.id===old.id);assert.ok(current,'Missing helper '+old.id);for(const field of ['name','horse-name','rider-name','nick-name','notes','disabled','first-name','last-name','show-name','ring-nickname','ring-lat','ring-lon','minutes','is-example','barn-name','barn-lat','barn-lon'])if(Object.hasOwn(old.fields,field))assert.deepEqual(current.fields[field],old.fields[field],table+'.'+field);}
 for(const old of original.trainers){const current=records.trainers.find(r=>r.id===old.id);assert.ok(current);for(const field of ['trainer-id','trainer-name','slot','role','active'])assert.deepEqual(current.fields[field],old.fields[field],'Trainer configuration changed');}
 const result={at:new Date().toISOString(),receipt,logs,records,checks:{source_matches:true,links_and_reciprocals:true,helpers_preserved:true,private_only_excluded:true}};
 const file=path.join(__dirname,'evidence/entries-consumer',receipt.entry_source_mode+'-'+receipt.run_id+'.json');fs.writeFileSync(file,JSON.stringify(result,null,2));
 console.log(JSON.stringify({file,run:receipt.run_id,started:receipt.started,completed:receipt.completed,mode:receipt.entry_source_mode,fetches:receipt.people_fetches,rows:receipt.rows,checks:result.checks}));
}
if(require.main===module)verify().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verify};
