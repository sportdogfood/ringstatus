// Current prototype uses a same-day illustrative snapshot. Live mapping must supply timestamps.
export function timeMinutes(value){
 const m=/^(\d{1,2}):(\d{2})([AP])$/.exec(value??'');
 return m?(Number(m[1])%12)*60+Number(m[2])+(m[3]==='P'?720:0):Infinity;
}
export function dayRows(records,{status='',sort='time',direction='asc'}={}){
 const statuses={done:['done'],upc:['upc','next'],live:['now','live']};
 return records.filter(r=>!status||(r.kind==='class'&&statuses[status]?.includes(r.status))).slice().sort((a,b)=>{
  const time=timeMinutes(a.time)-timeMinutes(b.time);
  const ring=String(a.ring).localeCompare(String(b.ring),undefined,{numeric:true});
  return (sort==='ring'?(direction==='desc'?-ring:ring)||time:(direction==='desc'?-time:time)||ring)||(a.position-b.position);
 });
}
