"use strict";
// Read-only acceptance check. Never invokes a collector or writes Airtable records.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
const settings=require('./settings'),{airtable}=require('./airtable');
const views={entries:'viwAqV0OQzzlI9qft',horses:'viwveStzE4tXrLGIT',riders:'viwDXz2D9KM04P8hK'};
const inactive=r=>r.fields.disabled===true||r.fields['source-inactive']===true;
const ids=rows=>rows.map(r=>r.id).sort();
async function verify({scheduled=true}={}){
 const api=airtable(),baseline=JSON.parse(fs.readFileSync(path.join(__dirname,'evidence/tenant-slices-20261024/baseline.json'),'utf8'));
 const data={};for(const table of ['horses','riders','entries',settings.SYNC_ENTRIES_TABLE,'heartbeat'])data[table]=await api.list(table);
 const local=data[settings.SYNC_ENTRIES_TABLE],source=await api.list('entries',settings.ENTRIES_BASE_ID),collections=await api.list('collections',settings.ENTRIES_BASE_ID);
 const viewData={};for(const [name,view]of Object.entries(views))viewData[name]=(await api.call(settings.BASE_ID,name==='entries'?settings.SYNC_ENTRIES_TABLE:name,'GET',undefined,'?'+new URLSearchParams({view,pageSize:'100'}))).records;
 for(const table of ['horses','riders']){
  assert.deepEqual(ids(viewData[table]),ids(data[table].filter(r=>!inactive(r))),table+' view must exclude both inactive flags');
  for(const row of baseline.records[table]){const current=data[table].find(r=>r.id===row.id);assert.ok(current,'Existing helper retained');for(const field of ['disabled','nick-name','notes','aliases'])assert.deepEqual(current.fields[field],row.fields[field],'Owner helper preserved: '+field);}
 }
 assert.deepEqual(data.entries,baseline.records.entries,'Retired entry records untouched');
 const privateOnly=source.filter(r=>!String(r.fields['trainer-ids']||'').trim());
 assert.ok(!local.some(r=>privateOnly.some(p=>p.fields['entry-key']===r.fields['entry-key'])),'No private-only entries in shared CWF sync');
 assert.ok(local.every(r=>!['source-json','collections','owner-key','source-id'].some(k=>k in r.fields)),'No follow ownership exported');
 const result={checked_at:new Date().toISOString(),horses:{all:data.horses.length,active:viewData.horses.length,inactive:data.horses.filter(inactive).length},riders:{all:data.riders.length,active:viewData.riders.length,inactive:data.riders.filter(inactive).length},synced_entries:local.length,active_team:viewData.entries.length,private_only_source_entries:privateOnly.length,private_only_exported:0,views};
 if(scheduled){
  const receipt=JSON.parse(fs.readFileSync(path.join(settings.RUNTIME,'last-run.json'),'utf8'));
  assert.equal(receipt.ok,true,'Normal run succeeded');assert.ok(receipt.tenant_slices,'Normal run used tenant slices');assert.equal(receipt.people_fetches,0);assert.equal(receipt.entry_source_mode,'synced');
  const selectedIds=new Set(collections.filter(c=>c.fields['source-kind']==='trainer-feed'&&c.fields.status==='complete'&&Number(c.fields['show-id'])===receipt.show_id).flatMap(c=>c.fields.entries||[]));
  const selected=source.filter(r=>selectedIds.has(r.id));
  const expected=local.filter(r=>selected.some(s=>s.fields['entry-key']===r.fields['entry-key'])&&r.fields.status==='active'&&String(r.fields['trainer-ids']||'').trim()&&!inactive(data.horses.find(h=>Number(h.fields['horse-id'])===Number(r.fields['horse-id'])))&&!inactive(data.riders.find(h=>Number(h.fields['rider-id'])===Number(r.fields['rider-id']))));
  assert.deepEqual(ids(viewData.entries),ids(expected),'Active team view equals independent current collection/roster intersection');
  for(const row of local){assert.equal(Boolean(row.fields['active-team']),expected.some(e=>e.id===row.id));assert.equal(Date.parse(row.fields['team-checked-at']),Date.parse(receipt.tenant_slices.checked_at));}
  for(const table of ['horses','riders'])for(const row of baseline.records[table].filter(inactive)){const current=data[table].find(r=>r.id===row.id);assert.ok(Date.parse(current.fields['helper-refreshed'])>=Date.parse(receipt.started),'Inactive helper still refreshed by normal run');}
  const beat=data.heartbeat.find(r=>r.fields['heartbeat-key']==='CWF');assert.equal(beat.fields.result,'Success');assert.ok(beat.fields.summary.includes('Tenant slice:'));
  const logs=await api.list('run-logs',settings.BASE_ID,{filterByFormula:"{run-id}='"+receipt.run_id+"'"});assert.ok(logs.some(r=>r.fields.result==='Success'&&r.fields.summary.includes('Tenant slice:')),'Successful Airtable run log contains slice summary');
  Object.assign(result,{run_id:receipt.run_id,started:receipt.started,completed:receipt.completed,tenant_slices:receipt.tenant_slices,people_fetches:receipt.people_fetches,heartbeat:beat.fields,run_logs:logs,receipt});
 }
 return result;
}
if(require.main===module)verify({scheduled:!process.argv.includes('--preflight')}).then(result=>{const out=process.argv.find(x=>x.startsWith('--output='));if(out)fs.writeFileSync(out.slice(9),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({...result,receipt:undefined,heartbeat:undefined,run_logs:undefined}));}).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verify};
