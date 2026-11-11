'use strict';
// Class only: read -> observe/calculate -> compare -> save. No scheduler is installed here.
const fs=require('node:fs'),path=require('node:path'),zlib=require('node:zlib');
const {localInstant}=require('./class-adapter');
const BASE='appeqxxJyPGS1iqdm',DEFAULT_PACE=2.5746;
const T={shows:'tblQPScqNeK0sF8f8',days:'tblKpk4SNscvxAvdu',classes:'tblUgcgOMvfjbEthx',schedules:'tbluS3qhA6LNEmH1H',rings:'tblxxij3WQdPVjelc',groups:'tblqj4kMTYxSM9KMg',live:'tblXwDb5zxYbwFcLe',trips:'tblIJ8YtqPNAmQY1k',ringStatus:'tbl1DRKWEQmkiUBsj',snapshots:'tblxhS1xWpfHKTd6l',runs:'tblyI9N4ehBLoRI2S',errors:'tblQXKCLO4rxw9WK8',heartbeat:'tblPkpcdX1gYWYgMA',answers:'tblmoBFySYW2TkDHf',checkpoints:'tblJzYflUc6STJRI2',cleanup:'tblMovHLBW8GAHJ8P',catalog:'tblLepghPma7hV2Hw'};
const SUMMARY='class-status-as-of';
const json=(s,fallback={})=>{try{return JSON.parse(s);}catch{return fallback;}};
const ids=v=>(Array.isArray(v)?v:[]).map(x=>typeof x==='string'?x:x.id);
const number=v=>v===null||v===undefined||v===''?null:Number.isFinite(Number(v))?Number(v):null;
const count=v=>Number.isSafeInteger(number(v))&&number(v)>=0?number(v):null;
const time=v=>typeof v==='string'&&Number.isFinite(Date.parse(v))?Date.parse(v):null;
const iso=v=>Number.isFinite(v)?new Date(v).toISOString():null;
const round=v=>v===null?null:Math.round(v*10000)/10000;
const dayAt=at=>new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(at));
const clock=at=>at?new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',hour:'numeric',minute:'2-digit'}).format(new Date(at)):'unknown';
const url=(table,row)=>row?.id?`https://airtable.com/${BASE}/${table}/${row.id}`:null;
const same=(a,b)=>JSON.stringify(a===''?null:a??null)===JSON.stringify(b===''?null:b??null);
const minutes=(end,at)=>end===null?null:Math.max(0,(end-at)/60000);

// Read existing saved Class receipts only. Reused payloads keep their first observation.
function savedObservations({day,show,root=path.join(__dirname,'../wef-live/runtime')}){
 const files=new Map(),out=[];
 for(const name of fs.readdirSync(root).filter(n=>/^lane-classes-.*\.json$/.test(n)&&n!=='lane-classes-last-run.json')){
  const r=json(fs.readFileSync(path.join(root,name),'utf8'));
  if(r.ok!==true||r.scope?.show!==show||r.scope?.date!==day)continue;
  for(const a of r.archives||[]){if(!/^class-\d+$/.test(a.kind)||time(a.at)===null)continue;const prior=files.get(a.file);if(!prior||a.at<prior.at)files.set(a.file,{at:a.at,receipt:name});}
 }
 for(const [file,ref]of files){const d=json(zlib.gunzipSync(fs.readFileSync(file)).toString()).detail;if(!d)continue;
  const starts=(d.rows||[]).filter(r=>Number(r.Actual_OOG)===1).map(r=>time(r.r1_start)).filter(n=>n!==null&&dayAt(n)===day);
  out.push({classId:Number(d.ID),ring:Number(d.ring_number),day,at:ref.at,total:count(d.recs),gone:typeof d.gone_trips==='string'?d.gone_trips.split(',').filter(Boolean).length:null,actualStart:starts.length?iso(Math.min(...starts)):null,ref:file});
 }
 return out.sort((a,b)=>time(a.at)-time(b.at));
}

function build({input,history=[],previous=[],show,customer,day,now,runId}){
 const at=time(now);if(at===null)throw Error('Invalid snapshot time');
 const scoped=(name)=>input[name]||[],prefix=`${customer}|${show}|${day}|`;
 const data=scoped('classes').filter(r=>r.fields['class-key']?.startsWith(prefix)&&!String(r.fields['record-status']||'').startsWith('Dropped'));
 const schedules=scoped('schedules').filter(r=>r.fields['schedule-key']?.startsWith(prefix)&&!String(r.fields['record-status']||'').startsWith('Dropped'));
 const outputs=[];
 for(const ring of scoped('rings').filter(r=>r.fields.date===day&&r.fields['show-id']===show&&r.fields['customer-id']===customer)){
  const rn=Number(ring.fields['ring-number']);
  const groups=scoped('groups').filter(g=>g.fields.date===day&&g.fields['group-key']?.startsWith(`${show}|${day}|`)&&ids(g.fields.ring).includes(ring.id)&&g.fields['record-status']!=='Dropped');
  const pointed=(role,status)=>{const v=ring.fields[role+'-group'];const keys=typeof v==='string'?v.split(',').map(x=>x.trim()):[];const links=ids(v);return groups.filter(g=>g.fields.status===status&&(links.includes(g.id)||keys.includes(g.fields['group-key'])));};
  const current=pointed('current','Underway'),next=pointed('next','Upcoming');
  const liveFor=g=>data.filter(c=>String(c.fields.group||'').split(',').map(x=>x.trim()).includes(`${prefix}${rn}|${g.fields['class-group-id']}`)).map(c=>scoped('live').find(l=>l.fields.date===day&&l.fields['show-id']===show&&l.fields['customer-id']===customer&&l.fields['class-id']===c.fields['class-id']&&ids(l.fields.ring).includes(ring.id)&&(ids(l.fields.groups).includes(g.id)||ids(l.fields['live-groups']).includes(g.id)))||{id:null,fields:{'class-id':c.fields['class-id'],'class-name':c.fields['class-name']}});
  const ringRows=scoped('ringStatus').filter(r=>r.fields['show-id']===show&&r.fields.date===day&&r.fields['ring-number']===rn&&time(r.fields['observed-at'])!==null&&time(r.fields['observed-at'])<=at).sort((a,b)=>time(b.fields['evaluated-at'])-time(a.fields['evaluated-at']));
  const rp=ringRows[0];const currentResults=[];
  for(const [role,selected]of [['current',current],['next',next]])for(const g of selected)for(const l of liveFor(g)){
   const lf=l.fields,gf=g.fields,notes=[];
   const c=data.find(c=>c.fields['class-id']===lf['class-id']&&Number(c.fields.ring?.split('|').at(-1))===rn);if(!c)continue;
   const key=c.fields['class-key'],ss=schedules.filter(s=>ids(c.fields.schedules).includes(s.id)&&String(s.fields.group||'').split(',').map(x=>x.trim()).includes(`${prefix}${rn}|${gf['class-group-id']}`));
   if(outputs.some(x=>x.fields['class-key']===key))continue; // One Class snapshot; current takes precedence over next.
   const planned=ss.map(s=>localInstant(day,s.fields['start-time'])).filter(Boolean).sort()[0]||null;
   const sourceAt=l.id?lf['last-seen']:c.fields['last-success'];
   const observed=time(sourceAt)!==null&&time(sourceAt)<=at?sourceAt:null;
   const total=count(lf.total)??(ss.length===1?count(ss[0].fields.total_trips):null),gone=count(lf.gone)??(ss.length===1?count(ss[0].fields.completed_trips):null),left=total!==null&&gone!==null&&gone<=total?total-gone:null;
   const samples=history.filter(h=>h.classId===lf['class-id']&&h.ring===rn&&h.day===day&&time(h.at)!==null&&time(h.at)<=time(observed));
   const starts=samples.map(h=>time(h.actualStart)).filter(n=>n!==null&&n<=time(observed));const actualStart=starts.length?Math.min(...starts):null;
   let pace=null,method='default';
   if(actualStart!==null&&gone>0&&time(observed)>actualStart){pace=(time(observed)-actualStart)/60000/gone;method='elapsed-from-actual-start';}
   else {const first=samples.find(h=>h.total===total&&h.gone>0&&h.gone<gone&&time(h.at)<time(observed));if(first){pace=(time(observed)-time(first.at))/60000/(gone-first.gone);method='saved-class-observations';}}
   const measured=pace;
   if(pace===null){const ringPace=number(rp?.fields['pace-minutes-per-trip']);const payload=json(rp?.fields['payload-json']);if(ringPace>0&&!payload.diagnostic){pace=ringPace;method='shared-ring-pace';}else{pace=DEFAULT_PACE;method='default';}}
   let start=role==='next'?time(planned):actualStart,end=role==='current'&&observed&&left!==null?time(observed)+left*pace*60000:null;
   if(role==='next'){
    const unknown=currentResults.some(x=>x.fields['estimated-end']===null);
    const predecessor=currentResults.map(x=>time(x.fields['estimated-end'])).filter(n=>n!==null);
    if(unknown){start=null;notes.push('Current class finish is unknown; next start cannot yet be projected.');}
    else if(predecessor.length){const finish=Math.max(...predecessor);start=start===null?finish:Math.max(start,finish);if(time(planned)!==null&&finish>time(planned))notes.push(`Running ${Math.ceil((finish-time(planned))/60000)} minutes late against the scheduled start.`);}
    end=start!==null&&left!==null?start+left*pace*60000:null;
   }
   const multi=json(gf['source-json']);if(multi.JOUnderway||multi.R2Underway){end=null;notes.push('Another round is running; base-round counts do not give its remaining workload.');}
   if(role==='current'&&liveFor(g).length>1&&['default','shared-ring-pace'].includes(method)&&!multi.JOUnderway&&!multi.R2Underway){
    const gt=count(gf.total??multi.total),gg=count(gf.gone??multi.gone),ga=time(gf['last-seen'])??time(observed);
    end=gt!==null&&gg!==null&&gg<=gt&&ga!==null&&ga<=at?ga+(gt-gg)*pace*60000:null;
    notes.push('Shared group: this finish uses the whole group remaining workload; individual trip order is not available.');
   }
   const isLive=typeof gf['is-live']==='boolean'?String(gf['is-live']):typeof multi.is_live==='boolean'?String(multi.is_live):'unknown';
   if(!observed)notes.push('Original Class observation time unavailable.');
   if(role==='next'&&start!==null&&start<at)notes.push('Scheduled start has passed; Live has not confirmed a start.');
   const tripsFor=live=>{const linked=new Set(live.map(x=>x.id));return scoped('trips').filter(t=>t.fields.date===day&&t.fields['show-id']===show&&t.fields['customer-id']===customer&&ids(t.fields.class).some(id=>linked.has(id))).sort((a,b)=>(number(a.fields['actual-order'])||number(a.fields['order-of-go'])||Infinity)-(number(b.fields['actual-order'])||number(b.fields['order-of-go'])||Infinity)).map(t=>({id:t.id,link:url(T.trips,t),trip:t.fields['trip-id'],horse:t.fields['horse-name']||null,rider:t.fields['rider-name']||null,order:t.fields['actual-order']||t.fields['order-of-go']||null,gone:t.fields.gone??null,scratch:t.fields.scratch===true,reportedCurrent:live.some(l=>l.fields['current-trip-id']===t.fields['trip-id'])}));};
   const currentTrips=tripsFor(current.flatMap(liveFor)).filter(t=>t.reportedCurrent),nextTrips=tripsFor(next.flatMap(liveFor)).filter(t=>!t.scratch&&t.gone!==1);
   if(!l.id)notes.push('Live class detail not yet available; using saved schedule counts.');
   const prev=previous.filter(p=>p.fields['class-key']===key&&time(p.fields['snapshot-at'])<at).sort((a,b)=>time(b.fields['snapshot-at'])-time(a.fields['snapshot-at']))[0];
   const fields={'snapshot-key':`${key}|${runId}`,'class-key':key,'show-id':show,date:day,phase:'base','snapshot-at':now,'observed-at':observed,class:url(T.classes,c),ring:url(T.rings,ring),group:url(T.groups,g),schedule:ss.map(s=>url(T.schedules,s)).join('\n'),'ring-snapshot':url(T.ringStatus,rp),status:role==='current'?'Underway':'Upcoming','status-basis':'Live current/next Ring links','is-live':isLive,total,gone,left,'count-basis':l.id?'Live class':'Saved schedule class','scheduled-start':planned,'actual-start':iso(actualStart),'first-seen-underway':prev?.fields['first-seen-underway']||(role==='current'?observed:null),'estimated-start':iso(start),'estimated-starts-in':round(minutes(start,at)),'estimated-end':iso(end),'estimated-ends-in':round(minutes(end,at)),'average-pace':round(measured),'pace-used':round(pace),'pace-method':method,'expected-start':iso(start),'expected-end':iso(end),'minutes-left':round(minutes(end,at)),'schedule-offset-minutes':start!==null&&planned?round((start-time(planned))/60000):null,'current-trips':JSON.stringify(currentTrips),'next-trips':JSON.stringify(nextTrips),'next-classes':JSON.stringify(next.flatMap(liveFor).map(n=>({id:n.id,name:n.fields['class-name'],link:url(T.live,n)}))),'previous-snapshot':prev?.fields['snapshot-key']||null,observations:notes.join('\n'),'unavailable-reasons':[!observed?'observation_time_missing':null,actualStart===null?'actual_start_unknown':null,left===null?'class_counts_unknown':null].filter(Boolean).join('\n')};
   const ignored=new Set(['snapshot-key','snapshot-at','previous-snapshot','changed-fields','changes']);const changes=Object.keys(fields).filter(k=>!ignored.has(k)&&!same(fields[k],prev?.fields[k])).map(field=>({field,previous:prev?.fields[field]??null,current:fields[field]??null}));fields['changed-fields']=changes.length?changes.map(x=>x.field).join('\n'):'unchanged';fields.changes=JSON.stringify(changes);
   const summary=`As of ${clock(now)} — ${c.fields['class-name']} — ${fields.status}. ${total??'?'} total, ${gone??'?'} gone, ${left??'?'} left. Start ${clock(fields['estimated-start'])}${fields['estimated-starts-in']===null?'':`, in ${Math.ceil(fields['estimated-starts-in'])} minutes`}. Estimated end ${clock(fields['estimated-end'])}${fields['estimated-ends-in']===null?'':`, in ${Math.ceil(fields['estimated-ends-in'])} minutes`}. Pace ${fields['pace-used']} min/trip (${method}). ${notes.join(' ')}`;
   const result={fields,classId:c.id,summary};outputs.push(result);if(role==='current')currentResults.push(result);
  }
 }
 return outputs;
}

// Same Airtable request/pagination pattern as the existing adapters; writes are Class-owned only.
function airtable({token=process.env.WEF_TIMING_AIRTABLE_TOKEN,request=fetch,pause=ms=>new Promise(r=>setTimeout(r,ms)),deadline=Date.now()+160000}={}){
 async function call(table,method='GET',body,query=''){
  if(!Object.values(T).includes(table))throw Error('Unknown Class table');
  const deletable=[T.runs,T.errors,T.heartbeat,T.answers,T.checkpoints];
  if(method!=='GET'&&!(method==='DELETE'?deletable.includes(table):[T.snapshots,T.classes,T.runs,T.errors,T.heartbeat,T.cleanup,T.catalog].includes(table)))throw Error('Read-only Class input');
  if(table===T.classes&&method!=='GET'&&(method!=='PATCH'||body.records.some(r=>Object.keys(r.fields).some(k=>k!==SUMMARY))))throw Error('Only the local Class timing summary may be changed');
  await pause(210);const remaining=deadline-Date.now();if(remaining<=0)throw Error('Class deadline reached');const r=await request(`https://api.airtable.com/v0/${BASE}/${table}${query}`,{method,headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(Math.min(30000,remaining))});if(!r.ok)throw Error(`Class Airtable ${method} HTTP ${r.status}`);return r.json();
 }
 return {list:async(table,formula)=>{const rows=[];let offset;do{const q=new URLSearchParams({pageSize:'100',...(formula?{filterByFormula:formula}:{}),...(offset?{offset}:{})});const r=await call(table,'GET',null,'?'+q);rows.push(...r.records);offset=r.offset;}while(offset);return rows;},create:async(table,fields)=>(await call(table,'POST',{records:[{fields}]})).records[0],update:async(table,id,fields)=>(await call(table,'PATCH',{records:[{id,fields}]})).records[0],remove:async(table,recordIds)=>{const q=new URLSearchParams();for(const id of recordIds)q.append('records[]',id);return call(table,'DELETE',null,'?'+q);}};
}
async function run({deadline=Date.now()+160000,api=airtable({deadline}),now=new Date().toISOString(),runId=require('node:crypto').randomUUID(),readHistory=savedObservations}={}){
 const day=dayAt(now),report={run_id:runId,started:now,day,ok:false,written:0,summaries:0,issues:[]};let log;
 try{
  const logs=await api.list(T.runs,`{run-id}=${JSON.stringify(runId)}`);log=logs[0]||await api.create(T.runs,{'run-id':runId,started:now,result:'Running',date:day});
  const shows=await api.list(T.shows),defaults=shows.filter(s=>s.fields.is_default===true);if(defaults.length!==1)throw Error('Current default show is missing or ambiguous');
  const show=Number(defaults[0].fields.show_id),customer=Number(defaults[0].fields.customer_id),prefix=`${customer}|${show}|${day}|`;
  if(!Number.isSafeInteger(show)||show<=0||!Number.isSafeInteger(customer)||customer<=0)throw Error('Current show/customer identity is invalid');
  report.show=show;await api.update(T.runs,log.id,{'show-id':show});
  const input={days:await api.list(T.days,`DATETIME_FORMAT({date},'YYYY-MM-DD')=${JSON.stringify(day)}`)};for(const name of ['rings','groups','live','trips'])input[name]=await api.list(T[name],`{date}=${JSON.stringify(day)}`);
  for(const name of ['classes','schedules']){const key=name==='classes'?'class-key':'schedule-key';input[name]=await api.list(T[name],`LEFT({${key}},${prefix.length})=${JSON.stringify(prefix)}`);}
  input.ringStatus=await api.list(T.ringStatus,`AND({show-id}=${show},DATETIME_FORMAT({date},'YYYY-MM-DD')=${JSON.stringify(day)})`);
  const previous=await api.list(T.snapshots,`DATETIME_FORMAT({date},'YYYY-MM-DD')=${JSON.stringify(day)}`);
  let history=[];try{history=readHistory({day,show});}catch(e){report.issues.push('Saved history unavailable: '+e.message);}
  const output=build({input,history,previous,show,customer,day,now,runId});report.evaluated=output.length;
  for(const item of output){const existing=previous.filter(p=>p.fields['snapshot-key']===item.fields['snapshot-key']);if(!existing.length)await api.create(T.snapshots,item.fields);else if(existing.length>1)throw Error('Duplicate snapshot key');
   const check=await api.list(T.snapshots,`{snapshot-key}=${JSON.stringify(item.fields['snapshot-key'])}`);if(check.length!==1||Object.entries(item.fields).some(([k,v])=>!same(v,check[0].fields[k])))throw Error('Class snapshot readback mismatch');report.written++;
   await api.update(T.classes,item.classId,{[SUMMARY]:item.summary});const readback=await api.list(T.classes,`RECORD_ID()=${JSON.stringify(item.classId)}`);if(readback.length!==1||readback[0].fields[SUMMARY]!==item.summary)throw Error('Class summary readback mismatch');report.summaries++;
  }
  report.ok=true;
 }catch(e){report.issues.push(e.message);try{await api.create(T.errors,{'error-key':runId,'run-id':runId,...(report.show?{'show-id':report.show}:{}),'occurred-at':now,date:day,message:e.message});}catch{}}
 report.completed=new Date().toISOString();if(log)await api.update(T.runs,log.id,{completed:report.completed,result:report.ok?'Success':'Failed','class-count':report.evaluated||0,summary:JSON.stringify(report)});
 const heartbeats=await api.list(T.heartbeat,`{heartbeat-key}='class-snapshot'`);const beat={'heartbeat-key':'class-snapshot','checked-at':report.completed,'run-id':runId,...(report.show?{'show-id':report.show}:{}),date:day,result:report.ok?'Success':'Failed',summary:JSON.stringify(report),started:now,completed:report.completed};if(heartbeats.length)await api.update(T.heartbeat,heartbeats[0].id,beat);else await api.create(T.heartbeat,beat);
 return report;
}
module.exports={BASE,T,SUMMARY,DEFAULT_PACE,savedObservations,build,airtable,run};
