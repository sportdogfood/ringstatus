'use strict';
// Saved-input snapshot calculation only. No network, clock, task or production writes.
const {localInstant}=require('./class-adapter');
const {instant}=require('./class-timing');
const BASE='appeqxxJyPGS1iqdm',TABLE='tblxhS1xWpfHKTd6l',PACE=2.5746;
const tables={classes:'tblUgcgOMvfjbEthx',live:'tblXwDb5zxYbwFcLe',groups:'tblqj4kMTYxSM9KMg',rings:'tblxxij3WQdPVjelc',schedules:'tbluS3qhA6LNEmH1H',pace:'tbl1DRKWEQmkiUBsj'};
const url=(table,row)=>row?`https://airtable.com/${BASE}/${table}/${row.id}`:null;
const links=x=>(x||[]).map(v=>typeof v==='string'?v:v.id);
const count=x=>Number.isSafeInteger(x)&&x>=0;
const json=x=>{try{return JSON.parse(x);}catch{return {};}};
const add=(at,minutes)=>instant(at)!==null&&Number.isFinite(minutes)?new Date(instant(at)+minutes*60000).toISOString():null;
const maximum=(a,b)=>a&&b?(instant(a)>instant(b)?a:b):a||b;
const stable=x=>JSON.stringify(x);
const material=f=>Object.fromEntries(Object.entries(f).filter(([k])=>!['snapshot-key','snapshot-at','observed-at','previous-snapshot','changed-fields','changes'].includes(k)));

function snapshots({tables:input,customer,show,day,at,previous=[]}){
 if(!Number.isSafeInteger(customer)||!Number.isSafeInteger(show)||instant(at)===null||!/^\d{4}-\d{2}-\d{2}$/.test(day))throw Error('Explicit show/day/evaluation time required');
 const rows=name=>input[name]||[],prefix=`${customer}|${show}|${day}|`;
 const schedules=rows('wef-data.schedules').filter(r=>r.fields['schedule-key']?.startsWith(prefix)&&r.fields['record-status']==='Active');
 const classes=rows('wef-data.classes').filter(r=>r.fields['class-key']?.startsWith(prefix)&&r.fields['record-status']==='Active');
 const groups=rows('wef-live.groups').filter(r=>r.fields.date===day&&r.fields['group-key']?.startsWith(`${show}|${day}|`)&&r.fields['record-status']!=='Dropped');
 const rings=rows('wef-live.rings').filter(r=>r.fields.date===day&&r.fields['show-id']===show&&r.fields['customer-id']===customer);
 const live=rows('wef-live.classes').filter(r=>r.fields.date===day&&r.fields['show-id']===show&&r.fields['customer-id']===customer);
 const unique=(list,reason,issues)=>{if(list.length!==1){issues.push(reason);return null;}return list[0];};
 const out=[];
 for(const c of classes){
  const f=c.fields,issues=[],notes=[],key=f['class-key'];
  if(classes.filter(x=>x.fields['class-key']===key).length!==1)throw Error('Duplicate Data class key');
  const ss=schedules.filter(s=>s.fields.class===key),sf=ss.length===1?ss[0].fields:null;
  // Multiple appearances must not be silently merged into a single timing estimate.
  if(ss.length!==1)issues.push('schedule_missing_or_multiple_appearances');
  const ring=unique(rings.filter(r=>r.fields['ring-number']===Number(f.ring?.split('|')[3])),'ring_missing_or_ambiguous',issues);
  const group=unique(groups.filter(g=>g.fields['class-group-id']===Number(f.group?.split('|')[4])&&(!ring||links(g.fields.ring).includes(ring.id))),'group_missing_or_ambiguous',issues);
  const l=unique(live.filter(r=>r.fields['class-id']===f['class-id']&&(!ring||links(r.fields.ring).includes(ring.id))&&(!group||links(r.fields.groups).includes(group.id))),'live_class_missing_or_ambiguous',issues);
  const gf=group?.fields||{},lf=l?.fields||{},raw=json(gf['source-json']);
  const multi=raw.JOUnderway===true||raw.R2Underway===true;
  const phase=raw.JOUnderway===true?'jump_off':raw.R2Underway===true?'round_two':'base';
  let total=count(lf.total)?lf.total:count(sf?.total_trips)?sf.total_trips:null;
  let gone=count(lf.gone)?lf.gone:count(sf?.completed_trips)?sf.completed_trips:null;
  const basis=count(lf.total)&&count(lf.gone)?'live-class':sf?'schedule-class':'unknown';
  if(multi){total=null;gone=phase==='jump_off'?lf['jo-gone']??null:lf['r2-gone']??null;issues.push('active_phase_total_not_available');}
  if(total!==null&&gone!==null&&gone>total){total=null;gone=null;issues.push('class_counts_conflict');}
  const left=total!==null&&gone!==null?total-gone:null;
  const observed=lf['last-seen']||gf['last-seen']||f['last-success']||null;
  if(instant(observed)===null||instant(observed)>instant(at))throw Error('Missing or future original observation');
  const priors=previous.filter(p=>p['class-key']===key&&p.phase===phase&&instant(p['snapshot-at'])<instant(at)).sort((a,b)=>instant(b['snapshot-at'])-instant(a['snapshot-at']));
  const prev=priors[0]||null;
  let status=f.status==='Not Started'?'Upcoming':f.status||'Unknown',statusBasis='data-class';
  if(gone>0&&left>0&&status!=='Completed'){status='Underway';statusBasis='live-class-progress';}
  if(status==='Completed'&&left>0)issues.push('class_status_count_conflict');
  else if(!f.status){status=gf.status?`Group ${gf.status}`:'Unknown';statusBasis='group-only';}
  const isLive=typeof gf['is-live']==='boolean'?String(gf['is-live']):raw.is_live===true?'true':raw.is_live===false?'false':'unknown';
  if(group)notes.push('is-live describes the linked group; it does not prove this class is currently on course.');
  notes.push(`source clocks: ${stable({class_seen:lf['last-seen'],class_updated:lf['source-updated-at'],group_seen:gf['last-seen'],group_updated:gf['source-updated-at'],data_seen:f['last-success']})}`);
  const start=sf?localInstant(day,sf['start-time']):null;
  const first=prev?.['first-seen-underway']||(status==='Underway'?observed:null);
  let average=null;
  // Average class throughput uses only compatible saved count observations, not refresh times.
  const comparable=priors.find(p=>p.total===total&&count(p.gone)&&p.gone<gone&&instant(p['observed-at'])<instant(observed));
  if(comparable)average=(instant(observed)-instant(comparable['observed-at']))/60000/(gone-comparable.gone);
  else if(count(gone)&&count(prev?.gone)&&prev?.['average-pace']>0&&prev.total===total&&gone>=prev.gone)average=prev['average-pace'];
  const paceRows=rows('ring-status').filter(r=>r.fields['show-id']===show&&r.fields.date===day&&r.fields['ring-number']===ring?.fields['ring-number']&&instant(r.fields['observed-at'])!==null&&instant(r.fields['observed-at'])<=instant(at)).sort((a,b)=>instant(b.fields['observed-at'])-instant(a.fields['observed-at']));
  const rp=paceRows[0]||null;
  if(rp)notes.push(`Ring pace (informational): ${rp.fields['pace-minutes-per-trip']} min/trip; observed ${rp.fields['observed-at']}; ${json(rp.fields['payload-json']).diagnostic?'diagnostic':'shared snapshot'}. Predictions use fixed 2.5746.`);
  if(!first)issues.push('actual_start_not_supplied');
  else issues.push('first_seen_underway_is_not_actual_start');
  if(prev&&gone<prev.gone)notes.push('Gone count decreased; count correction or reset.');
  const result={'snapshot-key':`${key}|${phase}|${at}`,'class-key':key,'show-id':show,date:day,phase,'snapshot-at':at,'observed-at':observed,
   class:url(tables.classes,c),ring:url(tables.rings,ring),group:url(tables.groups,group),schedule:ss.map(s=>url(tables.schedules,s)).join('\n'),'ring-snapshot':url(tables.pace,rp),
   status,'is-live':isLive,'status-basis':statusBasis,total,gone,left,'count-basis':basis,'scheduled-start':start,'actual-start':null,'first-seen-underway':first,
   'expected-start':status==='Upcoming'?start:null,'expected-end':null,'average-pace':average,'pace-used':PACE,'pace-method':'fixed-default',
   'minutes-left':null,'schedule-offset-minutes':null,'next-classes':'[]','previous-snapshot':prev?.['snapshot-key']||null,
   observations:notes.join('\n'),'unavailable-reasons':issues.join('\n')};
  out.push({result,c,ss,group,ring,prev,issues,notes});
 }
 // Sequence each ring once. Classes sharing a group share its workload, never summed twice.
 for(const ring of rings){
  const ordered=schedules.filter(s=>s.fields.ring===`${prefix}${ring.fields['ring-number']}`).sort((a,b)=>a.fields['source-position']-b.fields['source-position']);
  const blocks=[];
  for(const s of ordered){const id=s.fields.kind==='class'?s.fields.group:s.fields['schedule-key'];let b=blocks.at(-1);if(!b||b.id!==id){b={id,ss:[],items:[]};blocks.push(b);}b.ss.push(s);b.items.push(...out.filter(x=>x.ring?.id===ring.id&&x.ss.some(q=>q.id===s.id)));}
  let cursor=null;
  for(let i=0;i<blocks.length;i++){
   const b=blocks[i],gf=b.items[0]?.group?.fields,raw=json(gf?.['source-json']);
   if(b.ss[0].fields.kind!=='class'){cursor=maximum(cursor,localInstant(day,b.ss[0].fields['end-time']));continue;}
   const shared=b.items.length>1||(Array.isArray(raw.classes)&&raw.classes.length>1);
   const underway=gf?.status==='Underway';
   let finish=null;
   if(underway&&count(gf.total)&&count(gf.gone)&&gf.gone<=gf.total&&!raw.JOUnderway&&!raw.R2Underway){finish=add(gf['last-seen'],(gf.total-gf.gone)*PACE);}
   const scheduled=b.items.map(x=>x.result['scheduled-start']).filter(Boolean).sort()[0]||null;
   const projectedStart=maximum(cursor,scheduled);
   if(!underway&&gf?.status!=='Completed'){
    const workload=shared?gf?.total:b.items[0]?.result.total;
    if(count(workload))finish=add(projectedStart,workload*PACE);
   }
   for(const x of b.items){const r=x.result;
    if(x.ss.length!==1)continue;
    if(r.status==='Underway'&&r.left!==null&&!shared&&!raw.JOUnderway&&!raw.R2Underway)r['expected-end']=add(r['observed-at'],r.left*PACE);
    else if(r.status==='Upcoming'){r['expected-start']=projectedStart;r['expected-end']=finish;}
    if(shared){x.notes.push('Classes share a group; individual finish depends on their interleaved order.');x.issues.push('individual_finish_unavailable_shared_group');r['expected-end']=null;}
    if(r.status==='Completed'){r['expected-start']=null;r['expected-end']=null;}
    r['minutes-left']=r['expected-end']?Math.max(0,(instant(r['expected-end'])-instant(at))/60000):null;
    const scheduledEnd=localInstant(day,x.ss[0].fields['end-time']);
    r['schedule-offset-minutes']=scheduledEnd&&r['expected-end']?(instant(r['expected-end'])-instant(scheduledEnd))/60000:null;
    r['next-classes']=stable(blocks.slice(i+1).find(q=>q.items.length)?.items.map(y=>({key:y.result['class-key'],link:y.result.class}))||[]);
   }
   if(finish)cursor=maximum(cursor,finish);
  }
 }
 return out.map(x=>{const r=x.result;r.observations=x.notes.join('\n');r['unavailable-reasons']=[...new Set(x.issues)].join('\n');const before=x.prev?material(x.prev):{},after=material(r);const changes=Object.keys(after).filter(k=>stable(before[k]??null)!==stable(after[k]??null)).map(field=>({field,previous:before[field]??null,current:after[field]??null}));r['changed-fields']=changes.length?changes.map(x=>x.field).join('\n'):'unchanged';r.changes=stable(changes);return r;});
}
module.exports={BASE,TABLE,PACE,snapshots};
