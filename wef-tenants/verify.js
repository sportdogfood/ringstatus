"use strict";
// Read-only acceptance. Does not import the collector or fetch any source endpoint.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib');
const {airtable,unique}=require('./airtable'),settings=require('./settings');
async function verify({api=airtable(),receipt:providedReceipt,now=()=>new Date().toISOString()}={}){
 const receiptFile=path.join(settings.RUNTIME,'last-run.json');if(!providedReceipt&&!fs.existsSync(receiptFile))throw Error('No scheduled run receipt yet');
 const receipt=providedReceipt||JSON.parse(fs.readFileSync(receiptFile)),tables={};
 for(const table of ['tenant-details','trainers','heartbeat','collections','rings-helper','ring-walks','horses','riders','entries','run-logs','code-reviews'])tables[table]=await api.list(table);
 for(const [name,id]of [['synced-shows',settings.SYNC_SHOWS_TABLE],['synced-rings',settings.SYNC_RINGS_TABLE],['synced-schedules',settings.SYNC_SCHEDULES_TABLE]])tables[name]=await api.list(id);
 const keys={'tenant-details':'tenant-key',trainers:'trainer-key',heartbeat:'heartbeat-key','synced-shows':'show-key',collections:'collection-id','synced-rings':'ring-key','synced-schedules':'stage-key','rings-helper':'ring-key','ring-walks':'walk-key',horses:'horse-key',riders:'rider-key',entries:'entry-key','run-logs':'run-id','code-reviews':'routine'};
 for(const[t,k]of Object.entries(keys))unique(tables[t],k);
 const active=tables['synced-shows'].filter(r=>r.fields.is_default),index=(await api.list('wef-tenant-index',settings.SHARED_BASE_ID)).filter(r=>r.fields['tenant-key']==='CWF');
 if(active.length!==1||active[0].fields.show_id!==receipt.show_id||active[0].fields.selected_date!==receipt.selected_date)throw Error('Scheduled receipt does not match shared source-selected show');
 if(!receipt.ok)throw Error('Scheduled collector failed: '+receipt.errors.join('; '));
 const log=tables['run-logs'].find(r=>r.fields['run-id']===receipt.run_id);if(!log||!log.fields.completed||log.fields.result==='Running'||log.fields.result==='Failed'||log.fields.errors)throw Error('Scheduled run log not complete and successful');
 for(const event of receipt.archives){if(event.outcome==='Failed')throw Error('Failed archive');const body=zlib.gunzipSync(fs.readFileSync(event.file));if(crypto.createHash('sha256').update(body).digest('hex')!==event.sha256)throw Error('Archive hash mismatch');}
 const refs=require('./synced-references').validateReferences({shows:tables['synced-shows'],rings:tables['synced-rings'],schedules:tables['synced-schedules'],now:now()}),show=refs.showRecord,helpers=tables['rings-helper'],rings=refs.rings,sourceSchedules=refs.schedules;
 for(const ring of rings)if(!helpers.some(h=>(h.fields['synced-rings']||[]).includes(ring.id)))throw Error('Broken CWF helper to synced ring link');
 for(const walk of tables['ring-walks'])if(!(walk.fields['rings-helper']||[]).some(id=>helpers.some(h=>h.id===id))||!Number.isFinite(walk.fields.minutes)||walk.fields.minutes<0)throw Error('Invalid tenant walking helper');
 const activeIndex=index.filter(r=>r.fields.active&&r.fields['show-id']===receipt.show_id&&r.fields['tenant-base-id']===settings.BASE_ID);if(activeIndex.length!==1||activeIndex[0].fields['last-result']!==(receipt.rows?'Success':'No entries'))throw Error('Tenant directory not current or terminal');
 const expectedCollections=receipt.days.flatMap(d=>receipt.trainers.map(t=>`${receipt.show_id}|${t.trainer_id}|${d.date}`)).sort();
 if(tables.collections.some(r=>r.fields.status!=='complete'||!(r.fields.shows||[]).includes(show.id))||JSON.stringify(tables.collections.map(r=>r.fields['collection-id']).sort())!==JSON.stringify(expectedCollections))throw Error('Incomplete per-trainer daily collection key coverage');
 const activeEntries=tables.entries.filter(r=>r.fields.status==='active');if(activeEntries.length!==receipt.rows||log.fields.rows!==receipt.rows)throw Error('Active entry count differs from receipt/run-log');
 for(const entry of activeEntries){const f=entry.fields,ring=rings.find(r=>r.id===(f['synced-rings']||[])[0]),schedule=sourceSchedules.find(r=>r.fields['stage-key']===f['schedule-key']);if(!(f.shows||[]).includes(show.id)||!ring||ring.fields.ring_date!==f.date||Number(ring.fields.ring_number)!==Number(f['ring-number'])||!schedule||Number(schedule.fields['class-id'])!==Number(f['class-id'])||schedule.fields.date!==f.date||!(f.schedules||[]).includes(schedule.id)||f['schedule-link']!==`https://airtable.com/${settings.BASE_ID}/${settings.SYNC_SCHEDULES_TABLE}/${schedule.id}`)throw Error('Broken active entry identities/links');}
 return{checked_at:new Date().toISOString(),ok:true,receipt,counts:Object.fromEntries(Object.entries(tables).map(([k,v])=>[k,v.length])),records:tables,index,verified_archives:receipt.archives.length};
}
if(require.main===module)verify().then(r=>{fs.mkdirSync(path.join(__dirname,'evidence'),{recursive:true});const file=path.join(__dirname,'evidence','scheduled-'+r.receipt.run_id+'.json');fs.writeFileSync(file,JSON.stringify(r,null,2));console.log(JSON.stringify({ok:r.ok,run:r.receipt.run_id,completed:r.receipt.completed,show:r.receipt.show_id,selected:r.receipt.selected_date,counts:r.counts,archives:r.verified_archives,file},null,2));}).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verify};
