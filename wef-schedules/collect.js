const settings = require("./settings");
"use strict";

// Independent schedules-only entry point using the existing per-day collector.

// No scheduler registration, process loop, or business work occurs on import.

const fs = require("node:fs");

const path = require("node:path");

const legacy = require("./collector-core");



function isoDate(value) {

  const day = String(value || "").slice(0, 10);

  if (!/^\d{4}-\d{2}-\d{2}$/.test(day) || new Date(day + "T00:00:00Z").toISOString().slice(0,10) !== day)

    throw new Error("Invalid show focus date");

  return day;

}

function validDefaultDay(value,start,end) {
  try {
    const day=isoDate(value);
    return day>=start&&day<=end?day:null;
  } catch { return null; }
}

function selectedDays(fields, now) {

  const start=isoDate(fields.start_date),end=isoDate(fields.end_date);

  if(start>end) throw new Error('Invalid show date range');

  const days=[];

  for(let day=start;day<=end;day=new Date(Date.parse(day+'T00:00:00Z')+86400000).toISOString().slice(0,10)) days.push(day);

  const sourceDay=validDefaultDay(fields.sgl_default_day,start,end);
  if (sourceDay || now) {
    const today=sourceDay || localDay(now);
    const tomorrow=new Date(Date.parse(today+'T00:00:00Z')+86400000).toISOString().slice(0,10);
    const priority=day=>day===today?0:day===tomorrow?1:day>tomorrow?2:3;
    days.sort((a,b)=>priority(a)-priority(b)||a.localeCompare(b));
  }
  return days;

}

function explicitScope(show, date, now) {

  const f=show.fields;

  if (!Number.isFinite(Number(f.show_id)) || !Number.isFinite(Number(f.customer_id)))

    throw new Error("Show/customer ID required");

  const dow=["Sun","Mon","Tue","Wed","Thu","Fri","Sat"][new Date(date+"T00:00:00Z").getUTCDay()];

  return {

    app_show_idv2:Number(f.show_id), customer_id:Number(f.customer_id),

    app_sql_datev2:date, app_dow_rawv2:dow, raw_sql_date:date,

    focus_day:date, show_record_id:show.id,

    show_scope_key:`${f.customer_id}|${f.show_id}|${date}`,

    scope_run_id:`schedule|${f.show_id}|${date}|${now}`,

    // Preparation guard retains existing NIGHT/shift semantics; it never moves focus_day.

    mode:date===isoDate(f.focus_day) ? "DAY" : "NIGHT",

    shifted_to_next_dayv2:date!==isoDate(f.focus_day),

    default_app_sql_date_is:isoDate(f.focus_day), app_sql_date_source:"standalone_schedule_focus",

    set_to_default_app_sql_date:false,

    show_app_sql_start_date:f.start_date, show_app_sql_end_date:f.end_date,

    show_app_name:f.show_name, ring_collection:f.ring_collection,

    has_full_schedule_payload:f.has_full_schedule_payload===true,

    full_schedule_payload_file:f.full_schedule_payload_file || [],

  };

}

async function atomicReport(filename, report) {

  fs.mkdirSync(path.dirname(filename),{recursive:true});

  const tmp=filename+"."+process.pid+".tmp";

  try {

    fs.writeFileSync(tmp,JSON.stringify(report,null,2));

    // Windows can briefly hold the destination open. Retry replacement only;

    // never delete the last complete receipt or repeat business writes.

    for(let attempt=0;;attempt++) {

      try { fs.renameSync(tmp,filename);break; }

      catch(error) {

        if(!["EPERM","EACCES","EBUSY"].includes(error.code)||attempt===4) throw error;

        await new Promise(resolve=>setTimeout(resolve,25*(attempt+1)));

      }

    }

  }

  finally { if(fs.existsSync(tmp)) fs.unlinkSync(tmp); }

}

function acquireLock(filename) {

  fs.mkdirSync(path.dirname(filename),{recursive:true});

  try { const fd=fs.openSync(filename,"wx");fs.writeFileSync(fd,String(process.pid));fs.closeSync(fd); }

  catch(error) {

    if(error.code!=="EEXIST") throw error;

    const pid=Number(fs.readFileSync(filename,"utf8"));

    if(!Number.isInteger(pid)||pid<=0) throw new Error("Invalid collector lock; inspect before clearing");

    try { process.kill(pid,0);throw new Error("Schedule collector already running"); }

    catch(probe) {

      if(probe.code!=="ESRCH") throw probe;

      // Only reclaim a proven dead process, never a time-based guessed expiry.

      fs.unlinkSync(filename);return acquireLock(filename);

    }

  }

  return ()=>fs.unlinkSync(filename);

}

// The proven tagger uses /ring to discover show.show_id, then the default

// schedule response for show metadata. Do not import tagger: it executes work.

function localDay(now) {

  const parts=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(new Date(now));

  const get=t=>parts.find(p=>p.type===t).value;

  return `${get('year')}-${get('month')}-${get('day')}`;

}

function collectorIO(overrides={}) {

  return {...legacy.scheduleIO,

    fetchJson:(url,audit={})=>legacy.scheduleIO.fetchJson(url,{...audit,runtimeAudit:false}),

    fetchClassListJson:(url,audit={})=>legacy.scheduleIO.fetchClassListJson(url,{...audit,runtimeAudit:false}),

    ...overrides};

}

async function discoverFocus({statePath=settings.SCHEDULE_FOCUS_FILE,io={},now=new Date().toISOString()}={}) {

  if(!statePath) throw new Error('SCHEDULE_FOCUS_FILE path required');

  const previous=fs.existsSync(statePath)?JSON.parse(fs.readFileSync(statePath,'utf8')):{};

  const adapters=collectorIO(io);

  const ring=await adapters.fetchJson(`${legacy.SGL_BASE_URL}/ring?customer_id=${encodeURIComponent(legacy.CUSTOMER_ID)}`);

  const showId=Number(ring?.show?.show_id);

  if(!Number.isSafeInteger(showId)||showId<=0) throw new Error('SGL default show ID missing or invalid');

  const endpoint=legacy.buildScheduleEmptyEndpoint(showId,legacy.CUSTOMER_ID);

  const payload=await adapters.fetchJson(endpoint);

  const responseId=payload?.show?.show_id ?? payload?.show_id;

  if(responseId!=null && Number(responseId)!==showId) throw new Error('SGL default schedule show mismatch');

  const meta=payload.show||{};

  const date=value=>value?isoDate(value):null;

  const start=date(meta.start_date||payload.start_date),end=date(meta.end_date||payload.end_date);

  if(!start || !end) throw new Error('SGL show start/end required to collect all days');

  if(start && end && start>end) throw new Error('SGL show date range invalid');

  // Optional manual settings stay scoped to their show, including on rollover.

  // They are not copied to a newly discovered show or written into legacy records.

  const manual=previous.overrides?.[String(showId)]||{};
  const rawDefaultDay=payload.show_date ?? payload.showDate;
  const sourceDay=validDefaultDay(rawDefaultDay,start,end);

  const fields={show_id:showId,customer_id:legacy.CUSTOMER_ID,

    show_name:meta.show_name||payload.show_name||'',start_date:start,end_date:end,

    focus_day:manual.focus_day?isoDate(manual.focus_day):localDay(now),
    sgl_default_day:sourceDay,

    ring_collection:manual.ring_collection,

    has_full_schedule_payload:manual.has_full_schedule_payload===true,

    full_schedule_payload_file:manual.full_schedule_payload_file||[]};

  const today=localDay(now);
  const defaultShow={
    level:'INFO',code:previous.show_id===showId?'default_show_current':'default_show_changed',
    previous:previous.show_id?{show_id:previous.show_id,start_date:previous.fields?.start_date||null,end_date:previous.fields?.end_date||null}:null,
    current:{show_id:showId,start_date:start,end_date:end},
    schedule_day:{previous:previous.show_id===showId?previous.fields?.sgl_default_day||null:null,
      current:sourceDay,priority_day:sourceDay||today,
      fallback_reason:sourceDay?null:rawDefaultDay==null?'SGL default day missing':'SGL default day invalid or outside show range'},
    today,phase:today<start?'upcoming':today>end?'ended':'current',
    message:today<start?'Upcoming default show; dates before its start are not missing show days':today>end?'Default show has ended; continuing default discovery':'Collecting all days of current default show'
  };
  const state={...previous,version:1,show_id:showId,fields,checked_at:now,default_show:defaultShow,

    discovery:previous.show_id===showId?'current':'new',

    overrides:previous.overrides||{}};

  await atomicReport(statePath,state);

  return {show:{fields},state,endpoint,payload};

}

module.exports={isoDate,selectedDays,explicitScope,acquireLock,atomicReport,localDay,collectorIO,discoverFocus};

if(require.main===module) {
  if(!settings.AIRTABLE_TOKEN) throw new Error('WEF_SCHEDULES_AIRTABLE_TOKEN required');

  require('./store').runStagedCollector()

    .then(r=>{console.log(JSON.stringify(r,null,2));process.exitCode=r.ok?0:1;})

    .catch(error=>{console.error(error.message);process.exitCode=1;});

}

