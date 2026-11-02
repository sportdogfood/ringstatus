"use strict";
const fs=require('node:fs'),path=require('node:path'),settings=require('./settings');
async function scheduled(){if(!settings.AIRTABLE_TOKEN)throw Error('WEF_SHOWS_AIRTABLE_TOKEN required');const {acquireLock,atomicReport}=require('../wef-schedules/collect');const release=acquireLock(path.join(settings.RUNTIME,'collector.lock'));try{const report=await require('./collector').runShows();await atomicReport(path.join(settings.RUNTIME,'last-run.json'),report);console.log(JSON.stringify(report,null,2));process.exitCode=report.ok?0:1;}finally{release();}}
if(require.main===module)scheduled().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={scheduled};
