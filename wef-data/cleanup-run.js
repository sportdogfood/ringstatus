'use strict';
// Independent task entry: consumes Records' existing verified new-show notification receipt.
const fs=require('node:fs'),path=require('node:path');
function alertShow(file){
 const a=JSON.parse(fs.readFileSync(file,'utf8')),t=a.last_reported_transition;
 if(a.automation_id!=='wef-shows-changes-to-records'||!t||
  t.new?.show_id!==a.last_reported_pair?.show_id||t.new?.day0!==a.last_reported_pair?.day0||
  !Number.isSafeInteger(t.new?.show_id)||t.new.show_id<=0||!/^\d{4}-\d{2}-\d{2}$/.test(t.new?.day0)||!t.normal_run_id)
  throw Error('Existing verified show/day alert receipt required');
 const trigger=t.old?.show_id!==t.new.show_id?'new-show':t.old?.day0!==t.new.day0?'day-change':null;
 if(!trigger)throw Error('No show/day transition in alert receipt');
 return {alertShow:t.new.show_id,alertDay:t.new.day0,trigger};
}
async function main(){
 const file=path.join(__dirname,'..','.git','ringstatus-control','wef-records-shows-transition-alert-state.json');
 const result=await require('./cleanup').cleanup(alertShow(file));
 console.log(JSON.stringify({result:result.result,run_id:result.run_id,show:result.show,actions:result.actions.length,error:result.error}));
 if(!['Success'].includes(result.result))process.exitCode=1;
}
if(require.main===module)main().catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={alertShow};

