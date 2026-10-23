"use strict";
// Read-only acceptance. Does not import the collector or fetch any source endpoint.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),zlib=require('node:zlib');
const {airtable,unique}=require('./airtable'),settings=require('./settings');
async function verify(){
 const api=airtable(),receiptFile=path.join(settings.RUNTIME,'last-run.json');if(!fs.existsSync(receiptFile))throw Error('No scheduled run receipt yet');
 const receipt=JSON.parse(fs.readFileSync(receiptFile)),tables={};
 for(const table of ['tenant-details','trainers','heartbeat','show-details','collections','rings','rings-helper','ring-walks','horses','riders','entries','run-logs','code-reviews'])tables[table]=await api.list(table);
 const keys={'tenant-details':'tenant-key',trainers:'trainer-key',heartbeat:'heartbeat-key','show-details':'show-key',collections:'collection-id',rings:'ring-key','rings-helper':'ring-key','ring-walks':'walk-key',horses:'horse-key',riders:'rider-key',entries:'entry-key','run-logs':'run-id','code-reviews':'routine'};
 for(const[t,k]of Object.entries(keys))unique(tables[t],k);
 const sharedShows=await api.list(settings.SHARED_SHOWS_TABLE,settings.SHARED_BASE_ID),active=sharedShows.filter(r=>r.fields.is_default),index=(await api.list('wef-tenant-index',settings.SHARED_BASE_ID)).filter(r=>r.fields['tenant-key']==='CWF');
 if(active.length!==1||active[0].fields.show_id!==receipt.show_id||active[0].fields.selected_date!==receipt.selected_date)throw Error('Scheduled receipt does not match shared source-selected show');
 if(!receipt.ok)throw Error('Scheduled collector failed: '+receipt.errors.join('; '));
 const log=tables['run-logs'].find(r=>r.fields['run-id']===receipt.run_id);if(!log||!log.fields.completed||log.fields.result==='Running'||log.fields.result==='Failed'||log.fields.errors)throw Error('Scheduled run log not complete and successful');
 for(const event of receipt.archives){if(event.outcome==='Failed')throw Error('Failed archive');const body=zlib.gunzipSync(fs.readFileSync(event.file));if(crypto.createHash('sha256').update(body).digest('hex')!==event.sha256)throw Error('Archive hash mismatch');}
 const show=tables['show-details'].find(r=>r.fields.show_id===receipt.show_id),helpers=tables['rings-helper'],rings=tables.rings.filter(r=>r.fields.show_id===receipt.show_id);
 const sourceRings=(await api.list('rings',settings.SHARED_BASE_ID)).filter(r=>r.fields.show_id===receipt.show_id),sourceSchedules=await api.list('schedules',settings.SHARED_BASE_ID);
 if(!show||rings.length!==sourceRings.length||rings.length===0)throw Error('Daily ring projection count mismatch');
 for(const source of sourceRings){const ring=rings.find(r=>r.fields['ring-key']===source.fields['ring-key']);if(!ring||ring.fields['source-record']!==source.id||['show_id','ring_date','ring_id','ring_number','ring_name'].some(k=>ring.fields[k]!==source.fields[k]))throw Error('Daily ring projection identity mismatch');}
 for(const ring of rings){if(!(ring.fields['show-details']||[]).includes(show.id)||(ring.fields['rings-helper']||[]).length!==1||!helpers.some(h=>h.id===ring.fields['rings-helper'][0]))throw Error('Broken daily ring links');}
 const expected=new Map([[1,8],[2,10],[3,6],[4,12],[5,14],[6,16]]);for(const[number,minutes]of expected){const h=helpers.find(h=>Number(h.fields.ring_number)===number),walk=tables['ring-walks'].find(w=>w.fields['ring-key']===h?.fields['ring-key']);if(!walk||walk.fields.minutes!==minutes||walk.fields['is-example']!==true)throw Error('CWF example walking times changed');}
 const activeIndex=index.filter(r=>r.fields.active&&r.fields['show-id']===receipt.show_id&&r.fields['tenant-base-id']===settings.BASE_ID);if(activeIndex.length!==1||activeIndex[0].fields['last-result']!==(receipt.rows?'Success':'No entries'))throw Error('Tenant directory not current or terminal');
 const expectedCollections=receipt.days.flatMap(d=>receipt.trainers.map(t=>`${receipt.show_id}|${t.trainer_id}|${d.date}`)).sort();
 if(tables.collections.some(r=>r.fields.status!=='complete')||JSON.stringify(tables.collections.map(r=>r.fields['collection-id']).sort())!==JSON.stringify(expectedCollections))throw Error('Incomplete per-trainer daily collection key coverage');
 const activeEntries=tables.entries.filter(r=>r.fields.status==='active');if(activeEntries.length!==receipt.rows||log.fields.rows!==receipt.rows)throw Error('Active entry count differs from receipt/run-log');
 for(const entry of activeEntries){const f=entry.fields,ring=rings.find(r=>r.id===(f.rings||[])[0]),schedule=sourceSchedules.find(r=>r.fields['stage-key']===f['schedule-key']);if(!(f['show-details']||[]).includes(show.id)||!ring||ring.fields.ring_date!==f.date||Number(ring.fields.ring_number)!==Number(f['ring-number'])||!schedule||Number(schedule.fields['class-id'])!==Number(f['class-id'])||schedule.fields.date!==f.date||!f['schedule-link'].endsWith(schedule.id))throw Error('Broken active entry identities/links');}
 return{checked_at:new Date().toISOString(),ok:true,receipt,counts:Object.fromEntries(Object.entries(tables).map(([k,v])=>[k,v.length])),records:tables,index,verified_archives:receipt.archives.length};
}
if(require.main===module)verify().then(r=>{fs.mkdirSync(path.join(__dirname,'evidence'),{recursive:true});const file=path.join(__dirname,'evidence','scheduled-'+r.receipt.run_id+'.json');fs.writeFileSync(file,JSON.stringify(r,null,2));console.log(JSON.stringify({ok:r.ok,run:r.receipt.run_id,completed:r.receipt.completed,show:r.receipt.show_id,selected:r.receipt.selected_date,counts:r.counts,archives:r.verified_archives,file},null,2));}).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verify};
