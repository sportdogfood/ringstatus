import fs from 'node:fs/promises';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

export const BASE = 'appCaSmL819KK7UFJ';
export const TABLES = Object.freeze({shows:'tblJA5CUon5s1sWib', days:'tbl14MaCCDw6dAywH', times:'tblBJ8DwRklrmDNb5', health:'tblcYgKFwKtILSdU8'});
export const CLOCK_URL = 'https://sglapp.showgroundslive.com/version.json';
export const HEALTH_URL = 'https://wef-schedules-decoded.gombcg.workers.dev/health';
const timezone = 'America/New_York';
const formatter = new Intl.DateTimeFormat('en-CA', {timeZone:timezone, year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit', hourCycle:'h23'});

export function localTime(now) {
  const date = new Date(now);
  if (!Number.isFinite(date.getTime())) throw Error('Invalid clock');
  const p = Object.fromEntries(formatter.formatToParts(date).map(x=>[x.type,x.value]));
  return {date:`${p.year}-${p.month}-${p.day}`, hour:Number(p.hour), minute:Number(p.minute), second:Number(p.second)};
}

// Slots: 07:00..18:57 every 3 minutes; overnight 19:00,22:00,01:00,04:00.
// Called by a one-minute wake, with no catch-up burst or independent resident loop.
export function cadenceSlot(now) {
  const p = localTime(now), minute = p.hour*60+p.minute;
  const due = minute>=420 && minute<1140 ? (minute-420)%3===0 : (minute-1140+1440)%180===0;
  return due ? `${p.date}|${String(p.hour).padStart(2,'0')}:${String(p.minute).padStart(2,'0')}` : null;
}

export function defaultShow(rows) {
  const matches = rows.filter(r=>r.fields.is_default===true && r.fields.customer_id===15);
  if (matches.length!==1) throw Error('Exactly one default show required');
  const show = matches[0];
  if (!Number.isSafeInteger(show.fields.show_id) || show.fields.show_id<=0 || show.fields['show-key']!==`15|${show.fields.show_id}`) throw Error('Invalid default show identity');
  return show;
}

export function showDay(rows, showId, date) {
  const matches = rows.filter(r=>r.fields.date===date && r.fields.show?.includes(showId));
  if (matches.length>1) throw Error('Duplicate linked show-day');
  return matches[0];
}

export function healthFields(key, endpoint, result, prior, links, now) {
  const ok = result.ok;
  return {'health-key':key, service:key, endpoint, ...links, status:ok?'Healthy':'Outage', 'checked-at':now,
    'http-status':result.status??null,
    'last-success-at':ok?now:(prior?.['last-success-at']??null),
    'outage-started-at':ok?null:(prior?.status==='Outage'?prior['outage-started-at']||now:now),
    'recovered-at':ok && prior?.status==='Outage'?now:(prior?.['recovered-at']??null),
    details:result.detail};
}

export function airtable({token, request=fetch, pause=ms=>new Promise(r=>setTimeout(r,ms))}) {
  if (!token) throw Error('Missing existing WEF_SHOWS_AIRTABLE_TOKEN');
  async function call(table, method='GET', body, query='') {
    if (!Object.values(TABLES).includes(table) || (method!=='GET' && ![TABLES.times,TABLES.health].includes(table))) throw Error('Forbidden table mutation');
    await pause(250);
    const r = await request(`https://api.airtable.com/v0/${BASE}/${table}${query}`, {method, headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'}, ...(body?{body:JSON.stringify(body)}:{}), signal:AbortSignal.timeout(30000)});
    if (!r.ok) throw Error(`Airtable ${method} ${table} HTTP ${r.status}`);
    return r.json();
  }
  async function list(table, filter) {
    const rows=[]; let offset;
    do {const q=new URLSearchParams({pageSize:'100',...(offset?{offset}:{}),...(filter?{filterByFormula:filter}:{})});const p=await call(table,'GET',undefined,'?'+q);rows.push(...p.records);offset=p.offset;}while(offset);
    return rows;
  }
  async function upsert(table, key, fields) {
    const literal=String(fields[key]).replace(/\\/g,'\\\\').replace(/'/g,"\\'");
    const rows=await list(table,`{${key}}='${literal}'`);
    if(rows.length>1)throw Error(`Duplicate ${key}`);
    const r=await call(table,rows.length?'PATCH':'POST',{records:[{...(rows.length?{id:rows[0].id}:{}),fields}]});
    const saved=r.records?.[0];
    if(!saved?.id)throw Error('Airtable save not confirmed');
    const verified=await list(table,`{${key}}='${literal}'`);
    if(verified.length!==1)throw Error('Airtable readback not confirmed');
    const comparable=v=>v===null||v===undefined||v===''||(Array.isArray(v)&&v.length===0)?null:
      Array.isArray(v)?JSON.stringify([...v].sort()):
      typeof v==='string'&&/^\d{4}-\d{2}-\d{2}T/.test(v)&&Number.isFinite(Date.parse(v))?Date.parse(v):v;
    for(const [k,v] of Object.entries(fields))if(comparable(verified[0].fields[k])!==comparable(v))throw Error(`Airtable readback mismatch: ${k}`);
    return verified[0];
  }
  return {list,upsert};
}

export async function observe({now, request=fetch}) {
  const results={};
  // Independent reads: a failed proxy must not suppress a valid clock observation.
  for(const [key,base] of [['clock',CLOCK_URL],['decoder',HEALTH_URL]]) {
    try {
      const url=new URL(base); if(key==='clock')url.searchParams.set('_',String(Date.parse(now)));
      const headers={Accept:'application/json'};
      if(key==='clock')headers['User-Agent']='Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1';
      const r=await request(url.toString(), {method:'GET',cache:'no-store',redirect:'error',headers,signal:AbortSignal.timeout(30000)});
      if(key==='clock') {
        const header=r.headers.get('date'), parsed=Date.parse(header||'');
        const age=r.headers.get('age');
        results.clock={ok:r.ok&&Number.isFinite(parsed),status:r.status,header,sourceTime:Number.isFinite(parsed)?new Date(parsed).toISOString():null,
          freshness:age!==null&&Number(age)>0?'Stale':'Unverified',
          detail:`HTTP ${r.status}; Date ${header||'missing'}; Age ${age??'absent'}. Response-header clock only; cache freshness is not yet independently verified.`};
      } else {
        const p=await r.json().catch(()=>null);
        results.decoder={ok:r.status===200&&p?.ok===true,status:r.status,detail:`HTTP ${r.status}; ok=${String(p?.ok)}; rings=${String(p?.rings)}; schedule_status=${String(p?.schedule_status)}`};
      }
    } catch {results[key]={ok:false,status:null,detail:`${key} request failed or timed out`};}
  }
  return results;
}

export async function run({now=new Date().toISOString(), api, request=fetch, statePath, filesystem=fs}={}) {
  const slot=cadenceSlot(now);
  if(!slot)return {status:'Skipped',reason:'Outside cadence slot'};
  if(!statePath)throw Error('State path required');
  await filesystem.mkdir(path.dirname(statePath),{recursive:true});
  const lock=await filesystem.open(statePath+'.lock','wx');
  try {
    let state;
    try {state=JSON.parse(await filesystem.readFile(statePath,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}
    if(state?.slot===slot)return {status:'Skipped',reason:'Slot already attempted'};
    // Reserve before network access. Do not retry an uncertain write within this slot.
    await filesystem.writeFile(statePath,JSON.stringify({slot,attemptedAt:now})+'\n');
    const [shows,days,health]=await Promise.all([api.list(TABLES.shows),api.list(TABLES.days),api.list(TABLES.health)]);
    const show=defaultShow(shows), day=showDay(days,show.id,localTime(now).date);
    const links={show:[show.id],'show-day':day?[day.id]:[]};
    const results=await observe({now,request}), failures=[];
    if(results.clock.ok) {
      const sourceDay=showDay(days,show.id,localTime(results.clock.sourceTime).date);
      try {await api.upsert(TABLES.times,'show-time-key',{'show-time-key':`15|${show.fields.show_id}|${localTime(results.clock.sourceTime).date}`,show:[show.id],'show-day':sourceDay?[sourceDay.id]:[], 'source-url':CLOCK_URL,'source-date-header':results.clock.header,'source-time':results.clock.sourceTime,'checked-at':now,freshness:results.clock.freshness,notes:results.clock.detail});}catch{failures.push('Clock observation save failed');}
    }
    for(const [key,endpoint] of [['clock',CLOCK_URL],['decoder',HEALTH_URL]]) {
      const prior=health.filter(r=>r.fields['health-key']===key);
      if(prior.length>1){failures.push(`Duplicate ${key} health record`);continue;}
      try{await api.upsert(TABLES.health,'health-key',healthFields(key,endpoint,results[key],prior[0]?.fields,links,now));}catch{failures.push(`${key} health save failed`);}
    }
    // Recording an upstream outage is a completed monitoring run, not healthy data.
    return {status:failures.length?'Failed':'Recorded',showId:show.fields.show_id,slot,clock:results.clock.ok,decoder:results.decoder.ok,errors:failures};
  } finally {await lock.close();await filesystem.unlink(statePath+'.lock');}
}

if(process.argv[1] && path.resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
  if(process.argv.length!==3 || process.argv[2]!=='--scheduled') {
    console.error('Use the approved scheduled entry: --scheduled');process.exitCode=1;
  } else {
    const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'../..');
    run({api:airtable({token:process.env.WEF_SHOWS_AIRTABLE_TOKEN}),statePath:path.join(root,'wef-data/runtime/show-time-health/state.json')})
      .then(r=>{console.log(JSON.stringify(r));if(r.status==='Failed')process.exitCode=1;})
      .catch(()=>{console.error(JSON.stringify({status:'Failed',error:'Show-time/health check could not complete; check configuration, state and Airtable access.'}));process.exitCode=1;});
  }
}
