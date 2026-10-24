"use strict";
const settings=require('./settings');
const sourceFields=['entry-key','show-id','date','entry-id','class-id','horse-id','horse-name','rider-id','rider-name','trainer-ids','class-name','ring-number','status','schedule-key'];
const linkFields=['horses','riders','trainers','shows','synced-rings','schedules'];
const ids=value=>String(value||'').split(',').map(x=>x.trim()).filter(Boolean);
const sorted=value=>JSON.stringify([...(value||[])].sort());
function fresh(value,now){const age=Date.parse(now)-Date.parse(value);return Number.isFinite(age)&&age>=-60000&&age<=30*60000;}
function unique(rows,key){const map=new Map();for(const row of rows){const value=row.fields[key];if(!value||map.has(value))throw Error('Synced entries missing or duplicate '+key);map.set(value,row);}return map;}
async function load({api,show,dates,trainers,now}){
 const base=settings.ENTRIES_BASE_ID,sid=Number(show.show_id);
 const [source,local,collections,heartbeats]=await Promise.all([api.list('entries',base),api.list(settings.SYNC_ENTRIES_TABLE),api.list('collections',base),api.list('heartbeat',base)]);
 const beats=heartbeats.filter(r=>r.fields['heartbeat-key']==='CWF');
 if(beats.length!==1||beats[0].fields.result!=='Success'||Number(beats[0].fields['show-id'])!==sid||!fresh(beats[0].fields['checked-at'],now))throw Error('wef-entries successful current-show heartbeat missing or stale');
 const team=source.filter(r=>Number(r.fields['show-id'])===sid&&ids(r.fields['trainer-ids']).length);
 const localTeam=local.filter(r=>Number(r.fields['show-id'])===sid),byKey=unique(localTeam,'entry-key');unique(team,'entry-key');
 if(team.length!==localTeam.length)throw Error('CWF entry sync incomplete or contains private-only entries');
 for(const row of team){const other=byKey.get(row.fields['entry-key']);if(!other)throw Error('CWF entry sync missing '+row.fields['entry-key']);for(const field of sourceFields)if(String(row.fields[field]??'')!==String(other.fields[field]??''))throw Error('CWF entry sync differs: '+row.fields['entry-key']+'.'+field);if(row.fields.status==='active'&&!fresh(other.fields['last-seen'],now))throw Error('CWF entry sync stale: '+row.fields['entry-key']);}
 const byTrainer=new Map();
 for(const trainer of trainers){const pid=String(trainer.fields['trainer-id']),rows=[];
  for(const date of dates){const key=['CWF',sid,date,'trainer-feed',pid].join('|'),matches=collections.filter(c=>c.fields['collection-id']===key);if(matches.length!==1||matches[0].fields.status!=='complete'||!fresh(matches[0].fields.completed,now))throw Error('wef-entries collection incomplete or stale: '+key);
   const collection=matches[0].fields,refs=collection.entries||[];
   if(refs.length!==Number(collection['row-count']))throw Error('wef-entries collection count mismatch: '+key);
   for(const id of refs){const sourceRow=team.find(r=>r.id===id),f=sourceRow?.fields;if(!f||f.date!==date||f.status!=='active'||!ids(f['trainer-ids']).includes(pid))throw Error('wef-entries collection identity mismatch: '+key);rows.push({...byKey.get(f['entry-key']).fields});}
  }byTrainer.set(pid,rows);
 }
 const selected=new Set([...byTrainer.values()].flat().map(f=>f['entry-key']));
 return{local:localTeam,rows:localTeam.filter(r=>selected.has(r.fields['entry-key'])).map(r=>({...r.fields})),byTrainer,proof:{base,table:settings.SYNC_ENTRIES_TABLE,checked_at:beats[0].fields['checked-at'],synced_rows:localTeam.length,active_rows:selected.size}};
}
function links(rows,{showRecord,rings,schedules,horses,riders,trainers}){
 const one=(matches,label)=>{if(matches.length!==1)throw Error('Synced entry '+label+' link missing or ambiguous');return[matches[0].id];};
 return rows.map(row=>({...row,
  shows:[showRecord.id],
  schedules:one(schedules.filter(r=>r.fields['stage-key']===row['schedule-key']),'schedule'),
  'synced-rings':one(rings.filter(r=>r.fields.ring_date===row.date&&Number(r.fields.ring_number)===Number(row['ring-number'])),'ring'),
  horses:one(horses.filter(r=>Number(r.fields['horse-id'])===Number(row['horse-id'])),'horse'),
  riders:one(riders.filter(r=>Number(r.fields['rider-id'])===Number(row['rider-id'])),'rider'),
  trainers:trainers.filter(r=>ids(row['trainer-ids']).includes(String(r.fields['trainer-id']))).map(r=>r.id)
 }));
}
async function saveLinks(api,local,rows){
 const changes=[];
 for(const row of rows){const record=local.find(r=>r.fields['entry-key']===row['entry-key']);if(!record)throw Error('Synced entry disappeared');const fields=Object.fromEntries(linkFields.filter(k=>sorted(record.fields[k])!==sorted(row[k])).map(k=>[k,row[k]]));if(Object.keys(fields).length)changes.push({id:record.id,fields});}
 for(let i=0;i<changes.length;i+=10)await api.call(settings.BASE_ID,settings.SYNC_ENTRIES_TABLE,'PATCH',{records:changes.slice(i,i+10)});
 const saved=await api.list(settings.SYNC_ENTRIES_TABLE);
 for(const row of rows){const record=saved.find(r=>r.fields['entry-key']===row['entry-key']);if(!record||linkFields.some(k=>sorted(record.fields[k])!==sorted(row[k])))throw Error('Synced entry links readback mismatch');}
 return changes.length;
}
module.exports={load,links,saveLinks,sourceFields,linkFields};
