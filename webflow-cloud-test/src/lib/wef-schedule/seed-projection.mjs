// Preserved October 30 projection, from schedule-day-dropdown/scripts/build-snapshot.mjs.
export function buildSnapshot(input) {
  const index = name => new Map(input[name].map(row => [row.id, row]));
  const tables = Object.fromEntries(['shows','days','rings','groups','classes','entries','horses','riders','trainers'].map(name => [name,index(name)]));
  const one = (row, field, table, optional=false) => {
    const ids=row[field]||[];
    if(ids.length!==1) {if(optional&&ids.length===0)return null;throw Error(`${row.id}: expected one ${field}`);}
    const target=tables[table].get(ids[0]);
    if(!target)throw Error(`${row.id}: unresolved ${field} ${ids[0]}`);
    return target;
  };
  if(input.shows.length!==1)throw Error('Select one show before building this prototype.');
  const show=input.shows[0];
  const days=input.days.filter(d=>(d.show||[]).includes(show.id)).map(d=>({id:d.id,date:d.date})).sort((a,b)=>a.date.localeCompare(b.date));
  const ringDetails={};
  const matched=new Map();
  for(const ring of input.rings) {
    if(!(ring.show||[]).includes(show.id))continue;
    const helpers=input.ringHelper.filter(h=>h.active&&h.ringId===ring['ring-id']&&Number(h.key.split('|')[0])===show['customer-id']);
    if(helpers.length>1)throw Error(`Ambiguous active ring identity: ${ring.id}`);
    if(!helpers.length)continue;
    const h=helpers[0];
    if(h.number!==ring['ring-number'])throw Error(`Ring number mismatch: ${ring.id}`);
    matched.set(ring.id,h);
    ringDetails[h.ringId]={name:h.nickname,number:h.number,sourceId:h.id};
  }
  for(const day of days)day.rings=input.rings.filter(r=>(r.day||[]).includes(day.id)&&matched.has(r.id)).map(r=>matched.get(r.id).ringId).sort((a,b)=>ringDetails[a].number-ringDetails[b].number);
  const tripsByClass=new Map();
  for(const trip of input.trips) {
    if(trip['record-status']!=='active')continue;
    const c=one(trip,'class','classes');
    if(!tripsByClass.has(c.id))tripsByClass.set(c.id,[]);
    tripsByClass.get(c.id).push(trip);
  }
  const clock=value=>{
    if(!value||value==='00:00:00')return '';
    const m=/^(\d{2}):(\d{2})(?::\d{2})?$/.exec(value);
    if(!m)return '';
    const hour=Number(m[1]);return `${hour%12||12}:${m[2]}${hour>=12?'P':'A'}`;
  };
  const statusMap={'Not Started':'upc','Completed':'done'};
  const seqMap={'Over Fences':'OVF','Under Saddle/Flat':'US/F','Model':'Model'};
  const typeMap={Hunters:'HNT',Jumpers:'JMP',Equitation:'EQ'};
  const records=[];
  for(const s of input.schedules) {
    if(!(s.show||[]).includes(show.id))continue;
    const ring=one(s,'ring','rings'),h=matched.get(ring.id);
    if(!h)continue;
    const day=one(s,'day','days');
    if(!(ring.day||[]).includes(day.id))throw Error(`Schedule/ring day mismatch: ${s.id}`);
    const c=one(s,'class','classes',s.kind!=='class');
    const g=one(s,'group','groups',true);
    if(c&&(!(c.ring||[]).includes(ring.id)||!(c.day||[]).includes(day.id)))throw Error(`Class scope mismatch: ${s.id}`);
    const source=s['source-json']||{};
    const orderSet=Number(c?.['source-json']?.class?.order_of_go_set)===1;
    const entries=(tripsByClass.get(c?.id)||[]).map(t=>{
      const entry=one(t,'entry','entries'),horse=one(t,'horse','horses');
      const rider=one(t,'rider','riders',true),trainer=one(t,'trainer','trainers',true);
      if(!(entry.show||[]).includes(show.id)||!(entry.horse||[]).includes(horse.id))throw Error(`Trip/entry identity mismatch: ${t.id}`);
      const rawOrder=t['order-of-go'];
      return {key:t['trip-key'],tripId:t.id,entryId:entry.id,horseId:horse.id,name:horse['horse-name'],entryNo:entry['entry-number'],
        rider:rider?.['rider-name']||'',riderId:rider?.id||null,trainer:trainer?.['trainer-name']||'',trainerId:trainer?.id||null,
        order:orderSet&&Number.isInteger(rawOrder)&&rawOrder>0&&rawOrder<100000?rawOrder:null,
        go:clock(t['estimated-go-time']),status:Number(t.scratch)===1?'scratch':'',goneIn:Number(t['gone-in'])===1?1:0};
    }).sort((a,b)=>(a.order??Infinity)-(b.order??Infinity)||String(a.entryNo).localeCompare(String(b.entryNo),undefined,{numeric:true})||a.key.localeCompare(b.key));
    records.push({key:s['schedule-key'],scheduleId:s.id,classId:c?.id||null,kind:s.kind,date:day.date,dayId:day.id,ring:h.ringId,ringRecordId:ring.id,
      entryCount:Number.isFinite(c?.['entry-count'])?c['entry-count']:null,position:s['source-position'],id:c?.['class-number']||'',name:s.name||c?.['class-name']||'',group:g?.['group-name']||'',groupId:g?.id||null,
      sequence:seqMap[source.schedule_sequencetype]||'',type:typeMap[source.class_type]||'',time:clock(s['start-time']),
      status:c?(Number(source.cancelled)===1?'cancelled':statusMap[c.status]||''):'',
      trips:s.kind==='class'&&Number.isFinite(source.total_trips)?source.total_trips:null,entries});
  }
  records.sort((a,b)=>a.date.localeCompare(b.date)||ringDetails[a.ring].number-ringDetails[b.ring].number||a.position-b.position||a.key.localeCompare(b.key));
  if(new Set(records.map(r=>r.key)).size!==records.length)throw Error('Duplicate schedule keys.');
  return {metadata:{show:show['show-name'],showId:show.id,baseId:input.baseId,capturedAt:input.capturedAt,defaultDate:show['selected-date'],timezone:'America/New_York'},days,ringDetails,records};
}
