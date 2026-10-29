"use strict";
const fs=require('node:fs'),path=require('node:path'),settings=require('./settings');
const {acquireLock}=require('./runtime-utils');
async function scheduled(){if(!require('./freshness').activationPolicyConfirmed)throw Error('Entries activation held: Entries release review pending');if(!settings.AIRTABLE_TOKEN)throw Error('WEF_ENTRIES_AIRTABLE_TOKEN required');const release=acquireLock(path.join(settings.RUNTIME,'collector.lock'));try{const report=await require('./collector').runEntries();const file=path.join(settings.RUNTIME,'last-run.json'),tmp=file+'.'+process.pid+'.tmp';fs.writeFileSync(tmp,JSON.stringify(report,null,2));fs.renameSync(tmp,file);console.log(JSON.stringify(report,null,2));process.exitCode=report.ok?0:1;return report;}finally{release();}}
if(require.main===module)scheduled().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={scheduled};
