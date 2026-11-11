'use strict';
// Independent cleanup-only review. Does not invoke the stopped Trip calculator.
const assert=require('node:assert/strict');
const c=require('../../../cleanup');
const BASE='appyoLTcNmLrgpHcg';
const rid=n=>'rec'+String(n).padStart(14,'0');
let passed=0;function check(name,fn){fn();passed++;console.log('PASS '+name);}
const now='2026-11-07T00:00:00.000Z'; // November 6 at 19:00 Eastern.
const protection={complete:true,pending:false,recordIds:[],runIds:[],resolvedErrorIds:[],retiredCheckpointIds:[],unreferencedAnswerIds:[]};
const rows=Array.from({length:1000},(_,n)=>({id:rid(n),createdTime:new Date(Date.parse(now)-3600000+n).toISOString(),fields:{'run-id':'run-'+n,'show-id':77,date:'2026-11-06',result:'Success',completed:now}}));
const event={trigger:'three-hour',show:77,day:'2026-11-06'};
const plan=(extra={})=>c.plan({table:'run-logs',rows,event,now,checked:true,protection,...extra});
check('daily transition is Eastern19:00, winter and summer',()=>{
 for(const [before,after,day] of [['2026-11-06T23:59:59Z',now,'2026-11-06'],['2026-07-01T22:59:59Z','2026-07-01T23:00:00Z','2026-07-01']]){
  const state={show:77,quickAt:before,dailyDay:'old'};assert.equal(c.due(state,77,before),null);assert.equal(c.due(state,77,after).trigger,'daily-1900');assert.equal(c.due(state,77,after).day,day);
 }
});
check('three-hour check independent of completed daily event',()=>{assert.equal(c.due({show:77,quickAt:'2026-11-06T12:00:00Z',dailyDay:'2026-11-06'},77,'2026-11-06T14:59:59Z'),null);assert.equal(c.due({show:77,quickAt:'2026-11-06T12:00:00Z',dailyDay:'2026-11-06'},77,'2026-11-06T15:00:00Z').trigger,'three-hour');});
check('threshold999 skips, threshold1000 selects oldest500',()=>{assert.equal(plan({rows:rows.slice(0,999)}).length,0);assert.deepEqual(plan(),rows.slice(0,500));});
check('daily clears all eligible completed logs instead of500',()=>{assert.equal(plan({event:{...event,trigger:'daily-1900'}}).length,1000);});
check('current active, protected runs, pending and unchecked selections survive',()=>{
 const copy=structuredClone(rows);copy[0].fields.result='Running';copy[0].fields.completed=null;assert(!plan({rows:copy}).some(x=>x.id===copy[0].id));
 assert(!plan({protection:{...protection,runIds:['run-0']}}).some(x=>x.id===rows[0].id));assert.equal(plan({protection:{...protection,pending:true}}).length,0);assert.equal(plan({checked:false}).length,0);
});
check('synced and control tables never selected',()=>{for(const table of ['ring-status','class-status','wef-data.trips','wef-live.entries','w-days','w-shows','cleanup','code-reviews','___archive-catalog'])assert.equal(plan({table,event:{...event,trigger:'new-show',oldShow:76}}).length,0);});
check('new show retires only approved unreferenced old-show snapshots',()=>{const row={...rows[0],fields:{'show-id':76,date:'2026-11-05'}};const e={...event,trigger:'new-show',oldShow:76};assert.equal(plan({table:'trip-status',rows:[row],event:e,protection:{...protection,unreferencedAnswerIds:[row.id]}}).length,1);assert.equal(plan({table:'trip-status',rows:[row],event:e}).length,0);assert.equal(c.due({show:76},77,now).trigger,'new-show');});
check('same-day and future snapshots remain protected on new show',()=>{for(const day of ['2026-11-06','2026-11-07']){const row={...rows[0],fields:{'show-id':76,date:day}};assert.equal(plan({table:'trip-status',rows:[row],event:{...event,trigger:'new-show',oldShow:76},protection:{...protection,unreferencedAnswerIds:[row.id]}}).length,0);}});
check('unresolved errors survive while explicitly resolved errors are eligible',()=>{const row={...rows[0],fields:{'show-id':77,'occurred-at':now}};assert.equal(plan({table:'errs-log',rows:[row],event:{...event,trigger:'daily-1900'}}).length,0);assert.equal(plan({table:'errs-log',rows:[row],event:{...event,trigger:'daily-1900'},protection:{...protection,resolvedErrorIds:[row.id]}}).length,1);});
(async()=>{
 const selected=plan();let data=structuredClone(selected),intent=false,finished=false;
 const api={base:BASE,read:async()=>structuredClone(data),beginIntent:async()=>{intent=true;return 'intent-1';},remove:async()=>{assert(intent);data=[];},finishIntent:async()=>{finished=true;}};
 const args={api,table:'run-logs',rows,event,now,checked:true,protection};
 const result=await c.cleanup(args);check('delete requires intent and separate absence readback',()=>{assert.equal(result.deleted,500);assert(finished);});
 let deleteCalled=false;
 await assert.rejects(()=>c.cleanup({...args,api:{...api,read:async()=>[{...selected[0],fields:{result:'Running'}}],remove:async()=>{deleteCalled=true;}}}),/changed/);check('changed-target readback prevents deletion',()=>assert.equal(deleteCalled,false));
 let closeCalled=false;
 await assert.rejects(()=>c.cleanup({...args,api:{...api,read:async()=>structuredClone(selected),remove:async()=>{},finishIntent:async()=>{closeCalled=true;}}}),/absence/);check('uncertain delete leaves intent unfinished',()=>assert.equal(closeCalled,false));
 await assert.rejects(()=>c.cleanup({...args,api:{...api,base:'other-base'}}),/isolation/);passed++;console.log('PASS base isolation');
 console.log(JSON.stringify({checks:passed,scope:'Independent offline cleanup-only; fake transport; no calculator execution or production writes'}));
})().catch(e=>{console.error(e);process.exitCode=1;});
