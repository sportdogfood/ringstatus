"use strict";
const {scheduled}=require('./lanes');
const alerts=require('./records-alerts');
async function notify(reports){try{return await alerts.notify(reports);}catch(e){return{result:'Failed',error:e.message};}}
if(require.main===module)scheduled().then(async reports=>{
 const delivery=await notify(reports);
 console.log(JSON.stringify(reports));
 if(delivery.result!=='Not needed')console.log(JSON.stringify({records_alert:delivery}));
 process.exitCode=Array.isArray(reports)&&reports.some(r=>!r.ok)?1:0;
}).catch(async e=>{await notify([{lane:'scheduled',ok:false,stage_errors:[{message:e.message}]}]);console.error(e.message);process.exitCode=1;});
module.exports={scheduled};
