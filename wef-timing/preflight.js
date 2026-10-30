"use strict";
// Read-only deployment diagnostic. Does not invoke the timing runner or any collector.
const fs=require('node:fs'),path=require('node:path');
const {api,SCHEDULES,LIVE,ENGINE,TABLES}=require('./api');
const {scheduleFingerprint,archive}=require('./runner');
async function preflight(){
  const a=api({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline:Date.now()+45000}),shows=await a.list(SCHEDULES,'shows');
  const active=shows.filter(r=>r.fields.is_default===true);if(active.length!==1)throw Error('Default show invalid');
  const sid=active[0].fields.show_id,collections=await a.list(SCHEDULES,'show-collection',`{show-id}=${sid}`);
  const checks=[];
  for(const r of collections){const f=r.fields;if(f.status!=='complete')continue;
    const root=path.resolve(__dirname,'../wef-schedules/runtime/history/datasets'),rows=archive(path.join(root,f['dataset-id']+'.json.gz'),root);
    if(scheduleFingerprint(rows)!==f.fingerprint||rows.length!==f['row-count'])throw Error('Schedule archive preflight mismatch');
    checks.push({date:f.date,rows:rows.length,completed:f.completed,fingerprint_verified:true});}
  const live=await a.list(LIVE,'collections',`AND({show-id}=${sid},{source-kind}='groups')`);
  for(const r of live){if(r.fields.status!=='complete')continue;archive(r.fields['archive-file'],path.resolve(__dirname,'../wef-live/runtime/history'),r.fields.fingerprint);}
  const checkpoint=await a.list(ENGINE,TABLES.checkpoint);const answer=await a.list(ENGINE,TABLES.answers);
  console.log(JSON.stringify({read_only:true,show:sid,show_checked:active[0].fields.last_checked,schedule_scopes:checks,live_group_collections:live.length,clock_verified:a.clockVerified,engine_answers:answer.length,engine_checkpoints:checkpoint.length}));
}
preflight().catch(e=>{console.error(e.message);process.exitCode=1;});
