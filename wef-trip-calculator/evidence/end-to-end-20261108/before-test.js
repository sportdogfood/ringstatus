'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const {snapshots,PACE,BASE}=require('./trip-status'),cleanup=require('./cleanup');
const {hash}=require('../wef-timing/pace');
const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'evidence/20261107/yesterday-inputs.json')));
const clocks=Object.values(fixture.tables).flat().flatMap(r=>[r.fields['last-success'],r.fields['last-seen']]).filter(x=>Number.isFinite(Date.parse(x)));
const args={tables:fixture.tables,customer:15,show:200000077,day:'2026-11-06',at:new Date(Math.max(...clocks.map(Date.parse))).toISOString()};
let passed=0;function check(name,fn){fn();passed++;console.log('PASS '+name);}
const results=snapshots(args);
check('all 656 saved November 6 trips evaluated, original identities retained',()=>{assert.equal(results.length,656);assert.equal(new Set(results.map(r=>r['trip-key'])).size,656);});
check('missing saved Class/Ring dependencies never produce guessed ETA',()=>{assert(results.every(r=>r['estimated-go-time']===null));assert(results.every(r=>r['pace-used']===2.5746));});
const repeat=snapshots({...args,at:new Date(Date.parse(args.at)+180000).toISOString(),previous:results});
check('repeat snapshots retain original source clocks and stable material',()=>{repeat.forEach((r,i)=>{assert.equal(r['observed-at'],results[i]['observed-at']);assert.notEqual(r['snapshot-key'],results[i]['snapshot-key']);assert.equal(r.changes,'[]');});});
// Explicit synthetic cases use the inspected existing payload contracts. No production rows.
const rid=n=>'rec'+String(n).padStart(14,'0'),now='2026-11-06T14:00:00.000Z',ck='15|200000077|2026-11-06|4|101';
function testInput(){
 const ts=[1,2,3].map(n=>({id:rid(n),fields:{'trip-key':ck+'|'+(100+n)+'|uuid'+n,class:ck,'trip-id':String(200+n),'source-uuid':'uuid'+n,'order-of-go':n,'actual-order':0,'gone-in':'0',scratch:'0','last-success':now,'record-status':'active','estimated-go-time':'00:00:00'}}));
 const c={id:rid(10),fields:{'class-key':ck,'class-id':101,group:'15|200000077|2026-11-06|4|301',ring:'15|200000077|2026-11-06|4',status:'Not Started','record-status':'Active',trips_json:JSON.stringify(ts.map((t,i)=>({trip_id:201+i,entry_id:101+i,entryxclasses_uuid:'uuid'+(i+1)})))}};
 const p={policy:'class-timing-v1',source_lane:'data_live',identity:{customer:15,show:200000077,date:'2026-11-06',ring:4,group:301,class:101,phase:'base'},evaluated_at:now,start_at:'2026-11-06T15:00:00.000Z',estimated_end_at:'2026-11-06T15:10:00.000Z',input_refs:[{source_key:'synthetic-contract-start',revision:'fixture',observed_at:now}]};
 const {evaluated_at,...material}=p;
 const cls={id:rid(11),fields:{'payload-json':JSON.stringify(p),'evaluated-at':now,'observed-at':now,'content-hash':hash(material)}};
 const rp={policy:'ring-state-v1',result:{identity:{customer:15,show:200000077,date:'2026-11-06',ring:4},current_class:{group_id:301,classes:[{id:101}]},as_of:now}};
 return {'wef-data.trips':ts,'wef-data.classes':[c],'wef-data.schedules':[{id:rid(12),fields:{'schedule-key':ck+'|occurrence',class:ck,group:c.fields.group,ring:c.fields.ring,kind:'class','source-position':1,'record-status':'Active'}}],
  'wef-live.groups':[{id:rid(13),fields:{'group-key':'200000077|2026-11-06|301','source-json':'{"JOUnderway":false,"R2Underway":false}'}}],
  'class-status':[cls],'ring-status':[{id:rid(14),fields:{'show-id':200000077,date:'2026-11-06','ring-number':4,'observed-at':now,'payload-json':JSON.stringify(rp),'content-hash':hash(rp)}}]};
}
const evaluate=tables=>snapshots({tables,customer:15,show:200000077,day:'2026-11-06',at:now});
check('legacy pending rank arithmetic uses fixed pace and verified class start',()=>{const r=evaluate(testInput());assert.equal(r[2]['trips-ahead'],2);assert.equal(r[2]['estimated-go-time'],new Date(Date.parse('2026-11-06T15:00:00Z')+2*PACE*60000).toISOString());});
check('100000 peer cannot manufacture exact queue or ETA',()=>{const t=testInput();t['wef-data.trips'][0].fields['order-of-go']=100000;assert(evaluate(t).every(r=>r['trips-ahead']===null&&r['estimated-go-time']===null));});
check('scratched and gone trips excluded from pending queue',()=>{const t=testInput();t['wef-data.trips'][0].fields.scratch='1';t['wef-data.trips'][1].fields['gone-in']='1';const r=evaluate(t);assert.equal(r[0].status,'Scratched');assert.equal(r[1].status,'Gone');assert.equal(r[2]['trips-ahead'],0);assert.equal(r[0]['estimated-go-time'],null);});
check('missing roster member withholds counts and ETA',()=>{const t=testInput();t['wef-data.trips'].shift();assert(evaluate(t).every(r=>r['trips-ahead']===null));});
check('wrong phase, shared group and future observations withhold ETA',()=>{for(const mutate of [t=>t['wef-live.groups'][0].fields['source-json']='{"JOUnderway":true}',t=>t['wef-data.schedules'].push({...t['wef-data.schedules'][0],id:rid(30),fields:{...t['wef-data.schedules'][0].fields,class:ck+'other'}}),t=>t['wef-data.trips'][0].fields['last-success']='2026-11-06T15:00:00Z']){const t=testInput();mutate(t);assert(evaluate(t).every(r=>r['estimated-go-time']===null));}});
check('invalid Class hash and wrong-day Ring cannot supply dependencies',()=>{const t=testInput();t['class-status'][0].fields['content-hash']='invalid';t['ring-status'][0].fields.date='2026-11-07';assert(evaluate(t).every(r=>r['estimated-go-time']===null));});
check('provider time separate; midnight absent; duplicate trip rejected',()=>{const t=testInput();t['wef-data.trips'][0].fields['estimated-go-time']='10:05:00';const r=evaluate(t);assert.equal(r[0]['provider-go-time'],'2026-11-06T15:05:00.000Z');assert.equal(r[1]['provider-go-time'],null);t['wef-data.trips'].push(t['wef-data.trips'][0]);assert.throws(()=>evaluate(t),/Duplicate/);});
check('preceding class estimated end delays next class trip',()=>{const t=testInput(),pc=ck.replace(/101$/,'100');const c=structuredClone(t['wef-data.classes'][0]);c.id=rid(40);Object.assign(c.fields,{'class-key':pc,'class-id':100,group:c.fields.group.replace(/301$/,'300')});t['wef-data.classes'].push(c);t['wef-data.schedules'].push({id:rid(41),fields:{...t['wef-data.schedules'][0].fields,class:pc,group:c.fields.group,'source-position':0}});const row=structuredClone(t['class-status'][0]),p=JSON.parse(row.fields['payload-json']);p.identity.class=100;p.identity.group=300;p.estimated_end_at='2026-11-06T15:30:00.000Z';row.id=rid(42);row.fields['payload-json']=JSON.stringify(p);const {evaluated_at,...m}=p;row.fields['content-hash']=hash(m);t['class-status'].push(row);assert.equal(evaluate(t)[0]['estimated-go-time'],p.estimated_end_at);});
const protection={complete:true,pending:false,recordIds:[],runIds:[],resolvedErrorIds:[],retiredCheckpointIds:[],unreferencedAnswerIds:[]};
const event={trigger:'three-hour',show:200000077,day:'2026-11-06'};
const logs=Array.from({length:1000},(_,i)=>({id:rid(1000+i),createdTime:new Date(Date.parse(now)-3600000+i).toISOString(),fields:{'show-id':200000077,date:'2026-11-06',result:'Success',completed:now}}));
const select=(rows,extra={})=>cleanup.plan({table:'run-logs',rows,event,now,checked:true,protection,...extra});
check('cleanup 999 none; 1000 oldest eligible 500',()=>{assert.equal(select(logs.slice(0,999)).length,0);assert.deepEqual(select(logs),logs.slice(0,500));});
check('19:00 Eastern and three-hour due; no daily/quick conflation',()=>{const state={show:200000077,quickAt:'2026-11-06T22:30:00Z',dailyDay:'2026-11-05'};assert.equal(cleanup.due(state,200000077,'2026-11-06T23:59:00Z'),null);assert.equal(cleanup.due(state,200000077,'2026-11-07T00:00:00Z').trigger,'daily-1900');assert.equal(cleanup.due({show:200000077,quickAt:now,dailyDay:'2026-11-06'},200000077,'2026-11-06T17:00:00Z').trigger,'three-hour');});
check('daily clears completed runs but protects active/pending/sync tables',()=>{const x=structuredClone(logs);x[0].fields.result='Running';x[0].fields.completed=null;const daily={...event,trigger:'daily-1900'};assert.equal(select(x,{event:daily}).length,999);assert.equal(select(logs,{protection:{...protection,pending:true}}).length,0);for(const table of ['ring-status','class-status','wef-data.trips','w-shows','cleanup','___archive-catalog'])assert.equal(select(logs,{table,event:daily}).length,0);});
check('new show only retires unreferenced eligible owned snapshots',()=>{const row={...logs[0],fields:{'show-id':200000076,date:'2026-11-05'}},e={...event,trigger:'new-show',oldShow:200000076};assert.equal(select([row],{table:'trip-status',event:e,protection:{...protection,unreferencedAnswerIds:[row.id]}}).length,1);assert.equal(select([row],{table:'trip-status',event:e}).length,0);});
(async()=>{
 let records=structuredClone(logs.slice(0,500)),intent=false,closed=false;
 const api={base:BASE,read:async()=>structuredClone(records),beginIntent:async()=>{intent=true;return 'intent';},remove:async()=>{assert(intent);records=[];},finishIntent:async()=>{closed=true;}};
 const r=await cleanup.cleanup({api,table:'run-logs',rows:logs,event,now,checked:true,protection});
 check('cleanup requires intent and independent absence readback',()=>{assert.equal(r.deleted,500);assert(closed);});
 await assert.rejects(()=>cleanup.cleanup({api:{...api,base:'other'},table:'run-logs',rows:logs,event,now,checked:true,protection}),/isolation/);passed++;console.log('PASS cleanup base isolation');
 const summary={passed,day:args.day,evaluation_at:args.at,trips:results.length,computed_go_times:results.filter(r=>r['estimated-go-time']).length,statuses:Object.fromEntries([...new Set(results.map(r=>r.status))].map(s=>[s,results.filter(r=>r.status===s).length])),note:'Real saved corpus plus explicitly synthetic contract/cleanup cases. No provider calls or production writes.'};
 fs.writeFileSync(path.join(__dirname,'evidence/20261107/test-summary.json'),JSON.stringify(summary,null,2));
 fs.writeFileSync(path.join(__dirname,'evidence/20261107/yesterday-results.json'),JSON.stringify(results,null,2));
 console.log(JSON.stringify(summary));
})().catch(e=>{console.error(e);process.exitCode=1;});
