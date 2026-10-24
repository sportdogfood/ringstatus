"use strict";
// Passive readback of an ordinary scheduled run. Never executes a collector.
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib'),assert=require('node:assert/strict');
const {verify}=require('./verify'),{lookupHelpers,planStandingRoster}=require('./standing-roster');
async function verifyStanding(){
 const evidence=await verify(),r=evidence.receipt;
 if(!r.roster)throw Error('Receipt predates standing roster release');
 const horses=evidence.records.horses,riders=evidence.records.riders;
 assert.equal(horses.length,r.roster.horses);assert.equal(riders.length,r.roster.riders);
 const sourceEvent=r.archives.find(a=>a.kind==='standing-roster');assert.ok(sourceEvent);
 const source=JSON.parse(zlib.gunzipSync(fs.readFileSync(sourceEvent.file)));
 const expected=planStandingRoster({source,tenant:evidence.records['tenant-details'][0],trainers:evidence.records.trainers,now:r.started});
 for(const[type,rows]of [['horses',horses],['riders',riders]])for(const sourceRow of expected[type]){const matches=rows.filter(row=>row.fields['source-record']===sourceRow['source-record']);assert.equal(matches.length,1,'Source standing identity coverage');for(const key of ['source-id-missing','source-inactive'])assert.equal(Boolean(matches[0].fields[key]),Boolean(sourceRow[key]));}
 for(const [type,rows]of [['horses',horses],['riders',riders]]){
  for(const row of rows){const f=row.fields;assert.equal(f['tenant-id'],8778);assert.equal(f['source-base'],'apptdhhNzduxm5gjn');assert.ok(f['source-record']);assert.ok(f['source-link'].includes(f['source-record']));assert.equal(f['show-details'],undefined);assert.ok(f['lookup-text']);if(type==='horses')assert.ok(f['nick-name']);}
 }
 const searches=[['horse name',lookupHelpers(horses,{name:'CHOCO DU REVERDY'})],['horse nickname',lookupHelpers(horses,{'nick-name':'Choco'})],['horse show name',lookupHelpers(horses,{'show-name':'Choco Du Reverdy'})],['horse numeric ID',lookupHelpers(horses,{'horse-id':139117})],['rider first/last',lookupHelpers(riders,{'first-name':'Grace','last-name':'Lyons'})],['rider last-first alias',lookupHelpers(riders,{name:'LYONS, GRACE'})]];
 for(const [name,matches]of searches)assert.equal(matches.length,1,name+' lookup mismatch');
 for(const[type,rows]of [['horses',horses],['riders',riders]])assert.equal(lookupHelpers(rows,{'trainer-id':8778,'tenant-id':8778}).length,lookupHelpers(expected[type].map(fields=>({fields})),{'trainer-id':8778,'tenant-id':8778}).length);
 evidence.standing_lookups=searches.map(([name,rows])=>({name,records:rows.map(r=>r.id)}));
 const file=path.join(__dirname,'evidence','standing-roster-20261020','scheduled-'+r.run_id+'.json');fs.writeFileSync(file,JSON.stringify(evidence,null,2));
 return{ok:true,run:r.run_id,completed:r.completed,horses:horses.length,riders:riders.length,missing_source_horse_ids:expected.missing_ids.filter(x=>x.type==='horses').length,source_inactive_horses:expected.horses.filter(x=>x['source-inactive']).length,verified_archive_events:evidence.verified_archives,lookup_checks:searches.length+2,file};
}
if(require.main===module)verifyStanding().then(r=>console.log(JSON.stringify(r,null,2))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verifyStanding};
