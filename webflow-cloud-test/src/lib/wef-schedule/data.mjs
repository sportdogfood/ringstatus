import {buildSnapshot} from './seed-projection.mjs';

export const BASE = 'appdl16HJ5ETm8N69';
const TABLES = {shows:'tbl7HtrFIRUlJpVsc',days:'tbl7WyLyshiM4KNjh',rings:'tblhKkfYXppW3TbTq',schedules:'tbl6KKlYuMIPHEJWe',groups:'tblQObOvktufkyq54',classes:'tbl0IFf067w4wUBj5',trips:'tblt0GvQ39bHIu930',entries:'tblcKPbT4DPbFPu5b',horses:'tblkGRRMJxnH55hi8',riders:'tblGEzjOJzpn3vxX5',trainers:'tbl9ROAViHtbKqpRd',ringHelpers:'tblx3RAYeM52N3zaD'};
// Read only the seed's fields and the verified nickname lookups. Never return raw Airtable records.
const FIELDS = {
 shows:['show-key','show-name','customer-id','selected-date','days','rings','schedules'],
 days:['show','date','rings','schedules'], rings:['show','day','ring-id','ring-number','w-rings'],
 schedules:['schedule-key','show','day','ring','class','group','kind','source-position','name','start-time','source-json','record-status','last-success'],
 groups:['group-name'],classes:['ring','day','class-number','class-name','status','entry-count','source-json','trips'],
 trips:['trip-key','class','entry','horse','rider','trainer','record-status','order-of-go','estimated-go-time','scratch','gone-in'],
 entries:['___show','horse','entry-number'],
 horses:['horse-name','horse-nickname (from w-horses)'],
 riders:['rider-name','rider-nickname (from w-riders)','rider-fallback (from w-riders)'],
 trainers:['trainer-name','trainer-nickname (from w-trainers)','trainer-fallback (from w-trainers)','active-team'],
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
    const response=await fetchImpl(url,{headers:{Authorization:`Bearer ${token}`},redirect:'error',cache:'no-store',signal:AbortSignal.timeout(15000)});
    if(!response.ok)throw Error('airtable_unavailable');
    const body=await response.json();
    if(!Array.isArray(body.records))throw Error('invalid_airtable_response');
    for(const record of body.records){
     if(!batch.includes(record.id))throw Error('unexpected_record');
     const row={...record.fields,id:record.id};
     if(kind==='entries')row.show=row['___show'];
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
 if(lane==='day0'){
  // Planning day-0 comes from the existing w-days contract, never calendar TODAY.
  const showKey=shows[0]['show-key'];
  if(!/^\d+\|\d+$/.test(showKey||''))throw Error('invalid_show_key');
  const url=new URL(`https://api.airtable.com/v0/${BASE}/tblpRrsNXQdr3KrC9`);
  url.searchParams.set('filterByFormula',`AND({show}='${showKey}',{day-priority}=0)`);
  for(const field of ['show','date','day-priority'])url.searchParams.append('fields[]',field);
  await pause(225);
  const response=await fetchImpl(url,{headers:{Authorization:`Bearer ${token}`},redirect:'error',cache:'no-store',signal:AbortSignal.timeout(15000)});
  if(!response.ok)throw Error('day0_unavailable');
  const body=await response.json(),day=body.records?.[0]?.fields;
  if(body.offset||body.records?.length!==1||day.show!==showKey||day['day-priority']!==0||!/^\d{4}-\d{2}-\d{2}$/.test(day.date||''))throw Error('invalid_day0');
  shows[0]['selected-date']=day.date;
 }
 const allDays=await read('days',links(shows,'days'));
 const days=lane==='day0'?allDays.filter(d=>d.date===shows[0]['selected-date']):allDays;
 if(!days.length||(lane==='day0'&&days.length!==1))throw Error('missing_day_scope');
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
