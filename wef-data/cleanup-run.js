'use strict';
// Independent task entry: consumes Records' cleanup alert; no scheduled trigger.
const fs=require('node:fs'),path=require('node:path');
function alertShow(file){
 const a=JSON.parse(fs.readFileSync(file,'utf8')),event=a.cleanup_alert;
 if(a.automation_id!=='wef-shows-changes-to-records'||!event||
  !['new-show','calendar-day','log-check'].includes(event.trigger)||
  !Number.isSafeInteger(event.show_id)||event.show_id<=0||
  !/^\d{4}-\d{2}-\d{2}$/.test(event.calendar_day||'')||
  typeof event.event_id!=='string'||!event.event_id||(!Number.isFinite(Date.parse(event.issued_at))||new Intl.DateTimeFormat('en-CA',{timeZone:'America/New_York',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(event.issued_at))!==event.calendar_day))
  throw Error('Records cleanup alert required');
 return {alertShow:event.show_id,alertDay:event.calendar_day,trigger:event.trigger,eventId:event.event_id,alertAt:event.issued_at};
}

async function main(){
 const file=path.join(__dirname,'..','.git','ringstatus-control','wef-records-shows-transition-alert-state.json');
 const result=await require('./cleanup').cleanup(alertShow(file));
 console.log(JSON.stringify({result:result.result,run_id:result.run_id,show:result.show,actions:result.actions.length,error:result.error}));
 if(!['Success'].includes(result.result))process.exitCode=1;
}
if(require.main===module)main().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={alertShow};

