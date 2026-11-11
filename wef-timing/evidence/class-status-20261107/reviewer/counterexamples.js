'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {snapshots}=require('../../../class-status'),cleanup=require('../../../class-status-cleanup');
const at='2026-11-06T15:00:00.000Z',day='2026-11-06',prefix='15|200000077|2026-11-06|4';
const ring={id:'rec00000000000001',fields:{date:day,'show-id':200000077,'customer-id':15,'ring-number':4}};
function corpus(){
 const tables={'wef-live.rings':[ring],'wef-data.classes':[],'wef-data.schedules':[],'wef-live.groups':[],'wef-live.classes':[],'ring-status':[]};
 for(let i=1;i<=2;i++){
  const classId=100+i,gid=200+i,groupId='rec'+String(20+i).padStart(14,'0'),key=prefix+'|'+classId;
  tables['wef-data.classes'].push({id:'rec'+String(100+i).padStart(14,'0'),fields:{'class-key':key,'class-id':classId,ring:prefix,group:prefix+'|'+gid,status:i===1?'Underway':'Not Started','record-status':'Active','last-success':at}});
  tables['wef-data.schedules'].push({id:'rec'+String(300+i).padStart(14,'0'),fields:{'schedule-key':prefix+'|s'+i,ring:prefix,group:prefix+'|'+gid,class:key,kind:'class','source-position':i,'start-time':i===1?'09:00:00':'09:30:00','end-time':i===1?'09:30:00':'10:00:00',total_trips:10,completed_trips:0,'record-status':'Active'}});
  tables['wef-live.groups'].push({id:groupId,fields:{'group-key':'200000077|'+day+'|'+gid,date:day,'class-group-id':gid,ring:[ring.id],status:i===1?'Underway':'Upcoming',total:10,gone:i===1?5:0,'last-seen':at,'source-json':JSON.stringify({classes:[classId],JOUnderway:i===1,is_live:i===1})}});
  tables['wef-live.classes'].push({id:'rec'+String(400+i).padStart(14,'0'),fields:{'class-id':classId,'customer-id':15,'show-id':200000077,date:day,ring:[ring.id],groups:[groupId],total:10,gone:i===1?5:0,'jo-gone':1,'last-seen':at}});
 }
 return tables;
}
const results=[];function test(name,fn){try{fn();results.push({name,result:'PASS'});}catch(e){results.push({name,result:'FAIL',message:e.message});}}
test('unknown finish of active jump-off prevents a guessed downstream expected start',()=>{
 const output=snapshots({tables:corpus(),customer:15,show:200000077,day,at});
 const next=output.find(r=>r['class-key'].endsWith('|102'));
 assert.equal(next['expected-start'],null,'Active preceding jump-off has unknown remaining total, but next expected start is '+next['expected-start']);
});
test('invalid source clock is local to its class, preserving other class snapshots',()=>{
 const tables=corpus();tables['wef-live.classes'][0].fields['last-seen']='invalid';
 let result;assert.doesNotThrow(()=>{result=snapshots({tables,customer:15,show:200000077,day,at});},'One bad class clock aborts all class evaluation');
 assert(result.some(r=>r['class-key'].endsWith('|102')));
});
const protection={complete:true,pending:false,recordIds:[],runIds:[],resolvedErrorIds:[],retiredCheckpointIds:[],unreferencedAnswerIds:['rec00000000000001']};
const old={id:'rec00000000000001',createdTime:'2026-11-05T15:00:00Z',fields:{'show-id':200000076,'answer-key':'old-answer'}};
test('missing answer date/identity is protected during new-show cleanup',()=>{
 const picked=cleanup.plan({table:'timing-answers',rows:[old],event:{trigger:'new-show',oldShow:200000076,show:200000077,day},now:at,checked:true,protection});
 assert.equal(picked.length,0,'Answer without date/validated identity is selected for deletion');
});
// Keep the archive gate diagnostic synchronous: the executor's only prerequisites
// are unreferenced ID, base, unchanged readback and intent, with no archive proof.
const dated={...old,createdTime:'2026-11-06T02:00:00Z',fields:{...old.fields,date:'2026-11-05'}};
const selected=cleanup.plan({table:'timing-answers',rows:[dated],event:{trigger:'new-show',oldShow:200000076,show:200000077,day},now:at,checked:true,protection});
test('13-hour-old answer lacking archive/catalog verification is excluded',()=>assert.equal(selected.length,0,'13-hour-old answer is selected with no archive/catalog evidence'));
fs.writeFileSync(path.join(__dirname,'counterexample-results.json'),JSON.stringify(results,null,2));
for(const result of results)console.log(JSON.stringify(result));
process.exitCode=results.some(r=>r.result==='FAIL')?1:0;
