'use strict';
// Prepared integration only: this file is NOT the deployed data-runner.js.
async function run({ringApi,runId=require('node:crypto').randomUUID(),clock=()=>new Date().toISOString(),deadline=Date.now()+160000,readRingHistory,runRing,runClass,classApi,readClassHistory}={}){
 const started=clock();
 const warnings=[];
 const transport=ringApi||require('./api').api({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,lane:'ring_state',deadline,onClockIssue:message=>warnings.push(message)});
 const ring=await(runRing||require('./ring-workflow').runRingSnapshots)({api:transport,now:started,runId,...(readRingHistory?{readHistory:readRingHistory}:{})});
 const classes=await(runClass||require('./class-snapshot').run)({...(classApi?{api:classApi}:{}),now:clock(),runId,deadline,...(readClassHistory?{readHistory:readClassHistory}:{})});
 return {run_id:runId,started,completed:clock(),ok:ring.ok&&classes.ok,ring,classes,issues:[...warnings,...classes.issues],counts:{evaluated:ring.rings?.length||0,written:ring.written||0,classes_evaluated:classes.evaluated||0,classes_written:classes.written||0}};
}
module.exports={run};
