"use strict";
// Read-only acceptance of existing normal scheduled receipts; never invokes collection.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const root=path.resolve(__dirname,'../../..'),read=n=>JSON.parse(fs.readFileSync(path.join(__dirname,n)));
async function main(){
 const deployment=read('deployment.json'),before=read('before.json'),receipt=JSON.parse(fs.readFileSync(path.join(root,'wef-tenants/runtime/last-run.json')));
 if(receipt.started<=deployment.at){console.log(JSON.stringify({pending:true,lastCompleted:receipt.completed}));return;}
 assert.equal(receipt.ok,true,'Normal scheduled run failed; stop this verification path');
 const result=await require(path.join(root,'wef-tenants/verify')).verify();assert.equal(result.receipt.run_id,receipt.run_id);
 const current=result.records;
 for(const table of ['horses','riders','trainers','rings-helper','ring-walks']){
  assert.equal(current[table].length,before[table].length);
  const fields=table==='trainers'?['trainer-id','trainer-key','trainer-name','slot','role','active']:table==='rings-helper'?['ring-key','ring_id','ring_number','ring_name','ring-nickname','ring-lat','ring-lon','notes']:table==='ring-walks'?Object.keys(before[table][0].fields):['horse-key','horse-id','horse-name','rider-key','rider-id','rider-name','name','nick-name','first-name','last-name','show-name','aliases','notes','disabled','horses','riders','entries','trainers'];
  for(const old of before[table]){const row=current[table].find(r=>r.id===old.id);assert.ok(row);for(const k of fields)assert.deepEqual(row.fields[k],old.fields[k],table+'.'+k+' '+old.id);}
 }
 for(const table of ['entries','collections']){
  assert.equal(current[table].length,before[table].length);
  for(const old of before[table]){const row=current[table].find(r=>r.id===old.id);assert.ok(row);const fields=table==='entries'?['entry-key','entry-id','class-id','horse-id','rider-id','date','ring-number','status','trainer-ids','schedule-key','horses','riders','trainers']:['collection-id','show-id','trainer-id','date','status','row-count','trainers'];for(const key of fields)assert.deepEqual(row.fields[key],old.fields[key],table+'.'+key);
   for(const legacy of table==='entries'?['show-details','rings']:['show-details'])assert.deepEqual(row.fields[legacy]??row.fields['retired-'+legacy],old.fields[legacy],'Preserved legacy '+legacy);
  }
 }
 const {airtable}=require(path.join(root,'wef-tenants/airtable')),api=airtable(),freeze=read('legacy-freeze.json');for(const [table,key]of [['tblxutSCBQzmkJLLO','shows'],['tblsVT0Poao9wKhs4','rings']])assert.deepEqual(await api.list(table),freeze[key],'Retired reference contents changed');
 for(const {name,sha256}of deployment.files)assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,'wef-tenants',name))).digest('hex'),sha256,name);
 for(const [file,hash]of Object.entries(read('protected-hashes.json')))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,file))).digest('hex'),hash,file);
 const earlier=fs.readdirSync(__dirname).filter(n=>/^scheduled-.*\.json$/.test(n)).map(read).find(r=>r.receipt.run_id!==receipt.run_id);
 let comparison;if(earlier)comparison=require(path.join(root,'wef-tenants/compare')).compare(earlier,result);
 const file='scheduled-'+receipt.run_id+'.json';fs.writeFileSync(path.join(__dirname,file),JSON.stringify(result,null,2));
 const summary={verifiedAt:new Date().toISOString(),runId:receipt.run_id,started:receipt.started,completed:receipt.completed,result:'Success',entries:current.entries.length,collections:current.collections.length,riders:current.riders.length,horses:current.horses.length,syncedShows:current['synced-shows'].length,syncedRings:current['synced-rings'].length,syncedSchedules:current['synced-schedules'].length,existingHelperValuesAndLinksPreserved:true,legacyReferencesFrozen:true,archiveHashesVerified:result.verified_archives,protectedFilesUnchanged:true,repeatStable:!!comparison,comparison};
 fs.writeFileSync(path.join(__dirname,'verified-'+receipt.run_id+'.json'),JSON.stringify(summary,null,2));console.log(JSON.stringify(summary));
}
main().catch(e=>{console.error(e.message);process.exitCode=1;});
