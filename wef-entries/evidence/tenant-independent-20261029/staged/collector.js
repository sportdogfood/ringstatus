"use strict";
// Candidate core for collector.js. Production readback/storage adapters remain gated.
const model=require('./model');
async function linkRetained({shows,sourceSchedules,localSchedules,classes,now}){
 const {show}=model.scope(shows,now);
 const rows=[],unresolved=[],completedScopes=[],linkIssues=[];
 let schedules=[];
 try{schedules=model.schedulesFor(show,sourceSchedules,localSchedules);}
 catch(error){linkIssues.push({kind:'schedule-reference',reason:error.message});}
 for(const item of classes){
  const scope={show_id:show.show_id,class_id:item.payload?.class?.class_id,date:item.payload?.class_related_data?.date};
  try{const linked=model.linkClass(item.payload,show,schedules,item.observedAt,{inventory:item.inventory,sourceEvidence:item.sourceEvidence});rows.push(...linked);completedScopes.push({...scope,count:linked.length});
   for(const row of linked)for(const kind of ['group','schedule'])if(row[kind+'-link-status']==='unresolved')linkIssues.push({kind,'entry-key':row['entry-key'],reason:row[kind+'-link-reason']});
  }
  catch(error){unresolved.push({...scope,error:error.message});}
 }
 const seen=new Set();for(const row of rows){if(seen.has(row['entry-key']))throw Error('Repeated class scope/participation');seen.add(row['entry-key']);}
 // Optional downstream systems are not called and cannot gate the core links.
 return{rows,completedScopes,unresolved,linkIssues,optionalConnections:{messages:'developing',results:'developing',waze:'developing'}};
}
module.exports={linkRetained};
