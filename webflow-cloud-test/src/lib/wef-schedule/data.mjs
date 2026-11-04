import {buildSnapshot} from './seed-projection.mjs';

export const BASE = 'appdl16HJ5ETm8N69';
const TABLES = {shows:'tblGc03icHzudV1v2',days:'tblpRrsNXQdr3KrC9',rings:'tblhKkfYXppW3TbTq',schedules:'tbl6KKlYuMIPHEJWe',groups:'tblQObOvktufkyq54',classes:'tbl0IFf067w4wUBj5',trips:'tblt0GvQ39bHIu930',entries:'tblcKPbT4DPbFPu5b',horses:'tblkGRRMJxnH55hi8',riders:'tblGEzjOJzpn3vxX5',trainers:'tbl9ROAViHtbKqpRd',ringHelpers:'tblx3RAYeM52N3zaD'};
// Read only the seed's fields and the verified nickname lookups. Never return raw Airtable records.
const FIELDS = {
 shows:['show-key','show_name','customer_id','facility_id','selected_date','rings','schedules'],
 days:['show','date','day-priority','rings','schedules'], rings:['w-shows','w-days','ring-id','ring-number','ring-name','w-rings'],
 schedules:['schedule-key','w-shows','w-days','ring','class','group','kind','source-position','name','start-time','source-json','record-status','last-success'],
 groups:['group-name'],classes:['ring','w-days','class-number','class-name','status','entry-count','source-json','trips'],
 trips:['source-json','trip-key','class','entry','horse','rider','trainer','record-status','order-of-go','estimated-go-time','scratch','gone-in'],
 entries:['w-shows','horse','entry-number'],
 horses:['horse-id','horse-name','horse-nickname (from w-horses)'],
 riders:['rider-id','rider-name','rider-nickname (from w-riders)','rider-fallback (from w-riders)'],
 trainers:['trainer-id','trainer-name','trainer-nickname (from w-trainers)','trainer-fallback (from w-trainers)','active-team'],
 ringHelpers:['ring-key','ring-number','ring-id','ring-nickname','active']
};
export function displayName(nickname,fallback,name){
 for(const value of [nickname,fallback,name]){
  const names=[...new Set((Array.isArray(value)?value:[value]).filter(v=>typeof v==='string'&&v.trim()))];
  if(names.length>1)throw Error('ambiguous_display_name');
  if(names.length)return names[0];
 }
 return '';
}
export function projectData(input){
 const copy=structuredClone(input);
 for(const [table,kind] of [['horses','horse'],['riders','rider'],['trainers','trainer']])for(const row of copy[table]){
  row[`${kind}-name`]=displayName(row[`${kind}-nickname (from w-${table})`],row[`${kind}-fallback (from w-${table})`],row[`${kind}-name`]);
 }
 const data=buildSnapshot(copy);
 data.metadata.source='wef-data';
 data.metadata.liveIntegration='undecided';
 data.metadata.refreshedAt=input.capturedAt;
 data.metadata.sourceUpdatedAt=input.schedules.map(r=>r['last-success']).filter(Boolean).sort().at(-1)||null;
 return data;
}
const links=(rows,field)=>[...new Set(rows.flatMap(r=>Array.isArray(r[field])?r[field]:[]))];
export async function loadSchedule({showId,token,lane='full',fetchImpl=fetch,now=()=>new Date().toISOString(),pause=ms=>new Promise(r=>setTimeout(r,ms))}){
 if(!['day0','full'].includes(lane))throw Error('invalid_lane');
 const startedAt=now();
 if(!/^rec[A-Za-z0-9]{14}$/.test(showId))throw Error('invalid_show');
 if(!token)throw Error('missing_credentials');
 let previous=0;
 async function read(kind,ids){
  const rows=[];
  for(let i=0;i<ids.length;i+=40){
   const batch=ids.slice(i,i+40);
   if(batch.some(id=>!/^rec[A-Za-z0-9]{14}$/.test(id)))throw Error('invalid_link');
   const url=new URL(`https://api.airtable.com/v0/${BASE}/${TABLES[kind]}`);
   url.searchParams.set('filterByFormula','OR('+batch.map(id=>`RECORD_ID()='${id}'`).join(',')+')');
   for(const field of FIELDS[kind])url.searchParams.append('fields[]',field);
   const offsets=new Set();
   do{
    await pause(Math.max(0,225-(Date.now()-previous))); previous=Date.now();
    const response=await fetchImpl(url,{headers:{Authorization:`Bearer ${token}`},redirect:'manual',cache:'no-store',signal:AbortSignal.timeout(15000)});
    if(!response.ok)throw Error('airtable_unavailable');
    const body=await response.json();
    if(!Array.isArray(body.records))throw Error('invalid_airtable_response');
    for(const record of body.records){
     if(!batch.includes(record.id))throw Error('unexpected_record');
     const row={...record.fields,id:record.id};
     if(['entries','rings','schedules'].includes(kind))row.show=row['w-shows'];
     if(['rings','schedules','classes'].includes(kind))row.day=row['w-days'];
     if(kind==='shows')Object.assign(row,{'show-name':row.show_name,'customer-id':row.customer_id,'facility-id':row.facility_id,'selected-date':row.selected_date});
     if(typeof row['source-json']==='string')row['source-json']=JSON.parse(row['source-json']);
     rows.push(row);
    }
    if(!body.offset)break;
    if(offsets.has(body.offset))throw Error('repeated_page');
    offsets.add(body.offset);url.searchParams.set('offset',body.offset);
   }while(true);
  }
  if(rows.length!==ids.length||new Set(rows.map(r=>r.id)).size!==ids.length)throw Error('missing_linked_records');
  return rows;
 }
 const shows=await read('shows',[showId]);
 const showKey=shows[0]['show-key'];
 if(!/^\d+\|\d+$/.test(showKey||''))throw Error('invalid_show_key');
 const allDays=await readQuery('days',`{show}='${showKey}'`,FIELDS.days,token,fetchImpl,pause);
 const priorityDays=allDays.filter(d=>d['day-priority']===0);
 if(priorityDays.length!==1)throw Error('invalid_day0');
 shows[0]['selected-date']=priorityDays[0].date;
 for(const day of allDays)day.show=[showId];
 const days=lane==='day0'?priorityDays:allDays;
 if(!days.length)throw Error('missing_day_scope');
 const rings=await read('rings',lane==='day0'?links(days,'rings'):links(shows,'rings'));
 const schedules=(await read('schedules',lane==='day0'?links(days,'schedules'):links(shows,'schedules'))).filter(r=>!['absent','dropped'].includes(String(r['record-status']).toLowerCase()));
 const classes=await read('classes',links(schedules,'class'));
 const groups=await read('groups',links(schedules,'group'));
 const trips=await read('trips',links(classes,'trips'));
 const entries=await read('entries',links(trips,'entry'));
 const horses=await read('horses',links(trips,'horse'));
 const riders=await read('riders',links(trips,'rider'));
 const trainers=await read('trainers',links(trips,'trainer'));
 const helpers=await read('ringHelpers',links(rings,'w-rings'));
 const ringHelper=helpers.map(r=>({id:r.id,key:r['ring-key'],number:r['ring-number'],ringId:r['ring-id'],nickname:r['ring-nickname'],active:r.active===true}));
 const data=projectData({baseId:BASE,capturedAt:now(),shows,days,rings,schedules,classes,groups,trips,entries,horses,riders,trainers,ringHelper});
 // Empty linked collections are not evidence of a confirmed empty schedule.
 if(!schedules.length||!data.records.length)throw Error('unconfirmed_empty_schedule');
 data.metadata.package={lane,complete:true,startedAt,dates:days.map(d=>d.date).sort()};
 return data;
}

async function readQuery(kind,formula,fields,token,fetchImpl=fetch,pause=ms=>new Promise(r=>setTimeout(r,ms))){
 if(!token)throw Error('missing_credentials');
 const url=new URL(`https://api.airtable.com/v0/${BASE}/${TABLES[kind]}`);
 url.searchParams.set('filterByFormula',formula);
 for(const field of fields)url.searchParams.append('fields[]',field);
 const rows=[],offsets=new Set();
 do{
  await pause(225);
  const response=await fetchImpl(url,{headers:{Authorization:`Bearer ${token}`},redirect:'manual',cache:'no-store',signal:AbortSignal.timeout(15000)});
  if(!response.ok)throw Error('airtable_unavailable');
  const body=await response.json();if(!Array.isArray(body.records))throw Error('invalid_airtable_response');
  rows.push(...body.records.map(r=>({...r.fields,id:r.id})));
  if(!body.offset)break;
  if(offsets.has(body.offset))throw Error('repeated_page');offsets.add(body.offset);url.searchParams.set('offset',body.offset);
 }while(true);
 return rows;
}
export async function loadScope({token,fetchImpl=fetch,pause}){
 const shows=await readQuery('shows','{is_default}=1',['show-key','show_name','selected_date'],token,fetchImpl,pause);
 if(shows.length!==1)throw Error('ambiguous_default_show');
 return {showId:shows[0].id,show:shows[0].show_name,defaultDate:shows[0].selected_date,timezone:'America/New_York'};
}
