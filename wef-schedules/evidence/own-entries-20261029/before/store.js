const settings = require("./settings");
"use strict";

// Staging is an explicit destination. Runtime adapters retain their original base.

const crypto = require("node:crypto");

const legacy = require("./collector-core");

const shared = require("./collect");

const quote = value => '"' + String(value).replace(/\\/g,"\\\\").replace(/"/g,'\\"') + '"';

function checked(result) {

  if(result.failedRows.length) throw new Error("Required staging write failed: "+JSON.stringify(result.failedRows));

}

function canonical(value) {

  if(Array.isArray(value)) return value.map(canonical);

  if(value && typeof value === "object") return Object.fromEntries(Object.keys(value).sort().map(k=>[k,canonical(value[k])]));

  return value;

}

function sourceRows(rows) {

  return rows.map(row=>{

    const fields=legacy.collectorSourceFields(row.fields);

    delete fields.shows;delete fields.live_groups;
    for(const name of ['manual_time_override','manual_time_overide'])
      if(row.fields[name]===true) fields[name]=true;

    return {key:row.key,fields,...(row.inactive===true?{inactive:true,dropped_at:row.dropped_at}:{})};

  }).sort((a,b)=>String(a.key).localeCompare(String(b.key)));

}

// Transport tokens rotate independently of schedule content. Keep full stored
// fingerprints for integrity, but compare only schedule content for reuse.
const TOKEN_FIELDS=['sgl_token_raw','sgl_token_prefix','sgl_token_length','sgl_token_hash'];
function semanticRows(rows) {
  return sourceRows(rows).map(row=>{
    const fields={...row.fields};
    for(const key of TOKEN_FIELDS) delete fields[key];
    return {...row,fields};
  });
}
function sameSchedule(a,b) {
  return JSON.stringify(canonical(semanticRows(a)))===JSON.stringify(canonical(semanticRows(b)));
}
function fingerprint(rows) {

  return crypto.createHash("sha256").update(JSON.stringify(canonical(sourceRows(rows)))).digest("hex");

}

// Existing schedule rule: compare keys within one show/day, retain absent

// records as inactive, and reactivate them when returned by the source.

function reconcileRows(previous,current,date) {

  const index=rows=>{

    const result=new Map();

    for(const row of sourceRows(rows)) {

      if(!row.key || result.has(row.key)) throw new Error('Missing or duplicate schedule key');

      result.set(row.key,row);

    }

    return result;

  };

  const old=index(previous),next=index(current);

  const changes={added:[],changed:[],unchanged:[],dropped:[]};

  for(const [key,row] of next) {

    if(!old.has(key)||old.get(key).inactive) changes.added.push(key);

    else if(!sameSchedule([row],[old.get(key)])) changes.changed.push(key);

    else changes.unchanged.push(key);

  }

  for(const [key,row] of old) if(!next.has(key)) {

    if(row.inactive) next.set(key,row);

    else {

      changes.dropped.push(key);

      next.set(key,{...row,inactive:true,dropped_at:shared.isoDate(date)});

    }

  }

  return {rows:[...next.values()].sort((a,b)=>String(a.key).localeCompare(String(b.key))),changes};

}

function stageStore(options={}) { return require('./current-store').currentStore(options); }

function verifyDataset(record,rows,showId,date) {

  const f=record.fields;

  if(f.status!=="complete" || Number(f['show-id'])!==Number(showId) || f.date!==date) throw new Error("Completed collection scope mismatch");

  if(rows.length!==f['row-count'] || fingerprint(rows)!==f.fingerprint) throw new Error("Staged dataset readback mismatch");

  for(const row of rows) {

    if(Number(row.fields.show_id ?? row.fields.app_show_idv2)!==Number(showId) ||

       legacy.resolveExistingScheduleShowDate({fields:row.fields})!==date) throw new Error("Staged row scope mismatch");

  }

}

async function collectStagedSchedules({store=stageStore(),io={},shows,now=new Date().toISOString(),clock=()=>new Date().toISOString()}={}) {

  if(legacy.DRY_RUN) return {ok:false,dry_run:true,source:"staging_not_written"};

  const adapters=shared.collectorIO(io);

  if(!Array.isArray(shows)) throw new Error("Explicit discovered show required");

  const days=[];

  for(const show of shows) for(const date of shared.selectedDays(show.fields,now)) {

    const id=crypto.randomUUID(),showId=Number(show.fields.show_id);

    let record;

    try {

      record=await store.begin({'collection-id':id,'show-id':showId,date,status:'running',started:now});

      const scope=shared.explicitScope(show,date,now);

      const prepared=await legacy.prepareScheduleDay({heartbeatRecord:null,baseHeartbeatContext:scope,

        explicitScope:scope,nowIso:now,dateOnly:now.slice(0,10),runId:id,heartbeatFieldSet:new Set(),

        collectionOnly:true,patchHeartbeat:false,io:adapters});

      let observed=sourceRows(prepared.scheduleHtmlTimeOverlay.rows);

      const previous=await store.latest(showId,date);

      const priorRows=previous?await store.loadRows(previous.fields['dataset-id']):[];

      if(previous) verifyDataset(previous,priorRows,showId,date);

      // Preserve the legacy destructive-empty guard, scoped to this day.

      // A fallback after a failed fetch must not generate drop decisions.

      if(prepared.datedResult.fetch_error) throw new Error('Schedule fetch failed: '+prepared.datedResult.fetch_error);

      if(!observed.length && priorRows.some(row=>!row.inactive))

        throw new Error('Refusing destructive zero-row schedule sync for established show/day');

      const times=await require('./schedule-times').enrichTimes({rows:prepared.scheduleHtmlTimeOverlay.rows,
        previous:priorRows,showId,date,capture:io.captureScheduleHtml});
      observed=sourceRows(times.rows);
      const {rows,changes}=reconcileRows(priorRows,observed,now.slice(0,10));

      const observedHash=fingerprint(rows);
      const counts={classes:observed.filter(row=>!row.fields.schedule_break).length,schedule_blocks:observed.filter(row=>row.fields.schedule_break).length};
      const warnings=observed.length?[]:[{level:'WARNING',code:'empty_show_day',show_id:showId,date,message:'Successful response contains no classes, schooling sessions or breaks for this show/date'}];
      if(times.summary.error||times.summary.unmatched) warnings.push({level:'WARNING',code:'estimated_times_incomplete',...times.summary});
      const oldByKey=new Map(priorRows.map(row=>[row.key,row]));
      const rotatedRows=observed.filter(row=>{
        const old=oldByKey.get(row.key);
        return old && TOKEN_FIELDS.some(key=>row.fields[key]!==old.fields[key]);
      }).length;
      let hash=observedHash;

      let datasetId=id;

      if(previous && sameSchedule(priorRows,rows)) {datasetId=previous.fields['dataset-id'];hash=previous.fields.fingerprint;}

      else await store.writeRows(datasetId,showId,date,rows);

      const savedRows=await store.loadRows(datasetId);

      const completed={status:'complete',completed:clock(),'dataset-id':datasetId,'row-count':rows.length,fingerprint:hash};

      verifyDataset({fields:{...record.fields,...completed}},savedRows,showId,date);

      // Store metadata only; normalized rows are in schedules. Never persist runtime links.

      const context={...prepared,schedule_counts:counts,estimated_times:times.summary,warnings,observed_fingerprint:observedHash,token_evidence:{rotated_rows:rotatedRows},changes,observed_row_count:observed.length,previous_dataset_id:previous?.fields['dataset-id']||null,chosen:{...prepared.chosen,rows:[]},datedResult:{...prepared.datedResult,rows:[]},

        emptyResult:{...prepared.emptyResult,rows:[]},scheduleHtmlTimeOverlay:{...prepared.scheduleHtmlTimeOverlay,rows:[]}};

      completed['context-json']=JSON.stringify(context);

      completed.message=JSON.stringify({...counts,added:changes.added.length,changed:changes.changed.length,dropped:changes.dropped.length,warnings});

      await store.finish(record,completed);

      days.push({show_id:showId,date,ok:true,dataset_id:datasetId,unchanged:datasetId!==id,changes,warnings,row_count:observed.length,classes_count:counts.classes,schedule_blocks_count:counts.schedule_blocks,token_rotated_rows:rotatedRows});

    } catch(error) {

      const message=String(error.message||error);

      if(record) {try {await store.finish(record,{status:'failed',message});}catch(logError){days.push({show_id:showId,date,ok:false,error:message,log_error:logError.message});continue;}}

      days.push({show_id:showId,date,ok:false,error:message});

    }

  }

  return {ok:days.every(d=>d.ok),idle:days.length===0,source:'staged_schedule_collection',days};

}

async function runStagedCollector({lockPath=settings.SCHEDULE_COLLECTION_LOCK,statePath=settings.SCHEDULE_FOCUS_FILE,runLog,references,resolveShow=require('./show-source').readShowFocus,...options}={}) {

  if(!lockPath) throw new Error("Explicit collector-only lock path required");

  const release=shared.acquireLock(lockPath);
  let runRecord,report,runError;

  try {

    // Validate destination before contacting SGL. No legacy base fallback.

    const store=options.store||stageStore({onArchive:runLog?event=>runLog.archive(runRecord,event):undefined});
    if(runLog) runRecord=await runLog.begin();

    const found=await resolveShow({statePath,io:options.io,now:options.now});

    const adapters=shared.collectorIO(options.io);
    const payloads=new Map();

    report=await collectStagedSchedules({...options,store,shows:[found.show],io:{...adapters,

      fetchJson:async(url,audit)=>{
        const payload=url===found.endpoint?found.payload:await adapters.fetchJson(url,audit);
        if(references){const endpoint=new URL(url),date=endpoint.searchParams.get('date');
          if(endpoint.pathname==='/schedule'&&/^\d{4}-\d{2}-\d{2}$/.test(date||''))payloads.set(date,payload);
        }
        return payload;
      }}});

    if(report.ok && store.finalize) await store.finalize(found.show.fields.show_id);
    if(report.ok && references) report.references=await references.sync({show:found.show,payload:found.payload,payloads,days:report.days,now:found.state.show_source?.checked_at||options.now||new Date().toISOString()});
    await shared.atomicReport(statePath,{...found.state,last_collection:report});

    report={...report,discovery:found.state.discovery,default_show:found.state.default_show,focus_day:found.show.fields.focus_day};
    return report;

  } catch(error) {runError=error;throw error;}
  finally {
    try {if(runRecord) await runLog.finish(runRecord,report,runError);}
    catch(logError) {throw Error((runError?runError.message+"; ":"")+"Run log completion failed: "+logError.message);}
    finally {release();}
  }

}

module.exports={stageStore,collectStagedSchedules,runStagedCollector,fingerprint,verifyDataset,reconcileRows};

