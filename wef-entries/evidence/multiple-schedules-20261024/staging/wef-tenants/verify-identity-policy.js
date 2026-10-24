"use strict";
// Passive verification only: no collector, source endpoint or mutation.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),settings=require('./settings');
const {verify}=require('./verify'),{assertUniqueRoster}=require('./identity-policy');
async function verifyIdentity(){
 const result=await verify();
 for(const table of ['horses','riders'])assertUniqueRoster(table,result.records[table].map(r=>r.fields));
 const response=await fetch('https://api.airtable.com/v0/meta/bases/'+settings.BASE_ID+'/tables',{headers:{Authorization:'Bearer '+settings.AIRTABLE_TOKEN},signal:AbortSignal.timeout(30000)});if(!response.ok)throw Error('Schema readback HTTP '+response.status);
 const schema=await response.json(),rider=schema.tables.find(t=>t.name==='riders');
 assert.ok(!rider.fields.some(f=>f.name==='show-name'));assert.ok(rider.fields.some(f=>f.name==='legacy-display-name'));
 const formula=rider.fields.find(f=>f.name==='lookup-text').options;assert.equal(formula.isValid,true);const references=formula.referencedFieldIds;assert.ok(!references.includes(rider.fields.find(f=>f.name==='legacy-display-name').id));for(const name of ['first-name','last-name'])assert.ok(references.includes(rider.fields.find(f=>f.name===name).id));
 // Verify owner/static values against the final accepted standing-roster release.
 const baseline=JSON.parse(fs.readFileSync(path.join(__dirname,'evidence','standing-roster-20261020','scheduled-2c41252f-12c2-4fa7-a7cf-2974b10d729c.json')));
 const fields=['name','nick-name','show-name','horse-name','rider-name','horse-key','rider-key','horse-id','rider-id','first-name','last-name','aliases','notes','disabled'];
 for(const table of ['horses','riders'])for(const prior of baseline.records[table]){
  const saved=result.records[table].find(r=>r.id===prior.id);assert.ok(saved,'Retained roster record ID');
  for(const field of fields){const target=table==='riders'&&field==='show-name'?'legacy-display-name':field;assert.deepEqual(saved.fields[target],prior.fields[field],table+'.'+target+' static value changed');}
 }
 result.identity_policy={ok:true,horses:result.records.horses.length,riders:result.records.riders.length,unknown_horse_ids:result.records.horses.filter(r=>!r.fields['horse-id']).length,blank_rider_nicknames:result.records.riders.filter(r=>!r.fields['nick-name']).length,rider_show_name:'retired; original values preserved',static_values_preserved:true};
 const file=path.join(__dirname,'evidence','identity-policy-20261020','scheduled-'+result.receipt.run_id+'.json');fs.writeFileSync(file,JSON.stringify(result,null,2));
 return{ok:true,run:result.receipt.run_id,completed:result.receipt.completed,...result.identity_policy,archives:result.verified_archives,file};
}
if(require.main===module)verifyIdentity().then(r=>console.log(JSON.stringify(r,null,2))).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verifyIdentity};
