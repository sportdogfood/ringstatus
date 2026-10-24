"use strict";
const fs=require('node:fs'),path=require('node:path'),settings=require('./settings');
// Copied pure lock behavior from wef-schedules/collect.js. No legacy imports.
function acquireLock(file){fs.mkdirSync(path.dirname(file),{recursive:true});try{const fd=fs.openSync(file,'wx');fs.writeFileSync(fd,String(process.pid));fs.closeSync(fd);}catch(e){if(e.code!=='EEXIST')throw e;const pid=Number(fs.readFileSync(file,'utf8'));if(!Number.isInteger(pid)||pid<=0)throw Error('Invalid tenant collector lock');try{process.kill(pid,0);throw Error('Tenant collector already running');}catch(probe){if(probe.code!=='ESRCH')throw probe;fs.unlinkSync(file);return acquireLock(file);}}return()=>fs.unlinkSync(file);}
async function scheduled(){
 if(!settings.AIRTABLE_TOKEN)throw Error('WEF_TENANTS_AIRTABLE_TOKEN required');const release=acquireLock(path.join(settings.RUNTIME,'collector.lock'));
 try{const report=await require('./collector').runTenant();const file=path.join(settings.RUNTIME,'last-run.json'),tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify(report,null,2));fs.renameSync(tmp,file);console.log(JSON.stringify(report,null,2));process.exitCode=report.ok?0:1;return report;}finally{release();}
}
if(require.main===module)scheduled().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={acquireLock,scheduled};
