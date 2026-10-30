'use strict';
const {adapt}=require('./class-adapter');
function prepare({data,liveClasses,answers,now,runId,onIssue}){
 if(!runId||!Array.isArray(answers))throw Error('Class batch inputs missing');
 const latest=new Map();
 for(const r of answers){const f=r.fields;if(f['answer-kind']!=='class-timing'||f['source-lane']!=='data_live')continue;
  const prior=latest.get(f['scope-key']);
  if(prior&&prior.fields['evaluated-at']===f['evaluated-at']&&prior.fields['content-hash']!==f['content-hash'])throw Error('Ambiguous latest class snapshot');
  if(!prior||String(f['evaluated-at'])>String(prior.fields['evaluated-at']))latest.set(f['scope-key'],r);
 }
 const outputs=adapt({lane:'data_live',now,data,paceRows:answers.filter(r=>r.fields['answer-kind']==='ring-pace'),liveClasses,onIssue});
 const scopes=new Set(),writes=[];let unchanged=0;
 for(const answer of outputs){const f=answer.fields;
  if(scopes.has(f['scope-key']))throw Error('Duplicate class occurrence scope');scopes.add(f['scope-key']);
  if(latest.get(f['scope-key'])?.fields['content-hash']===f['content-hash']){unchanged++;continue;}
  f['answer-key']=f['scope-key']+'|'+runId+'|'+f['content-hash'].slice(0,16);writes.push(f);
 }
 return{outputs,writes,unchanged};
}
module.exports={prepare};
