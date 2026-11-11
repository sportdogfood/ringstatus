'use strict';
const assert=require('node:assert/strict'),test=require('node:test'),fs=require('node:fs'),path=require('node:path');
const {snapshots,PACE}=require('./trip-status'),{base,fillReferences}=require('./linker'),{run,airtable}=require('./runner');
const {hash}=require('../wef-timing/pace');
const day='2026-11-08',at='2026-11-08T19:28:00.000Z',show=200000077,customer=15;
const prefix=`${customer}|${show}|${day}|`,native=`${show}|${day}|`,scope={customer,show,day,at,runId:'test-cycle'};
const rec=n=>'rec'+String(n).padStart(14,'0'),row=(n,fields)=>({id:rec(n),fields});
const context=item=>JSON.parse(item.snapshot['trip-context']);

function example(){
 const ring=row(1,{'ring-key':native+'37','ring-id':37,'ring-number':4,date:day,'current-group':[rec(2)],'next-group':[rec(3)]});
 const group=(id,n,status,total,gone)=>row(id,{'group-key':native+n,date:day,status,total,gone,ring:[ring.id],'last-seen':at,'source-json':'{"JOUnderway":false,"R2Underway":false}'});
 const cls=(id,n,g,total,gone)=>row(id,{'class-key':native+n,'class-id':n,date:day,total,gone,ring:[ring.id],groups:[rec(g)],'last-seen':at});
 const classes=[cls(4,101,2,15,10),cls(5,102,3,6,0)],groups=[group(2,201,'Underway',15,10),group(3,202,'Upcoming',6,0)];
 const trip=(n,c,g,order,gone,scratch=false)=>row(100+n,{'trip-key':`${prefix}37|${c}||${n}`,date:day,'trip-id':n,'ring-id':37,'class-id':c,'class-key':native+c,ring:[ring.id],class:[rec(c===101?4:5)],groups:[rec(g)],'order-of-go':order,gone,scratch,'record-status':'Active','last-seen':at});
 const trips=[...Array.from({length:16},(_,i)=>trip(i+1,101,2,i+1,i<10?1:0,i===11)),...Array.from({length:6},(_,i)=>trip(30+i,102,3,i+1,0))];
 const schedules=[101,102].map((n,i)=>row(6+i,{'schedule-key':prefix+`4|schedule${i}`,kind:'class',class:prefix+`4|${n}`,group:prefix+`4|${201+i}`,ring:prefix+'4','start-time':i?'15:00:00':'13:59:00','record-status':'Active'}));
 const classStatus=[101,102].map((n,i)=>row(8+i,{'class-key':prefix+`4|${n}`,'show-id':show,date:day,phase:'base',status:i?'Upcoming':'Underway','snapshot-at':at,'observed-at':at,'estimated-start':i?'2026-11-08T20:26:00.000Z':'2026-11-08T18:59:00.000Z','average-pace':2.9,'schedule-offset-minutes':i?26:0}));
 const p={policy:'current-group-snapshot-v1',as_of:at,offset_minutes:26};
 const ringStatus=[row(10,{'scope-key':`ring-current-group|${prefix}37`,'show-id':show,date:day,'ring-number':4,'group-id':201,'evaluated-at':at,'observed-at':at,'payload-json':JSON.stringify(p),'content-hash':hash(p),'pace-minutes-per-trip':3.5})];
 return{trips,rings:[ring],groups,classes,schedules,classStatus,ringStatus};
}
const getTrip=(input,id)=>snapshots({...scope,input}).output.find(item=>item.trip.fields['trip-id']===id);

test('eligible queue skips gone and scratched trips and identifies next trip',()=>{
 const item=getTrip(example(),15);
 assert.equal(item.calculated['calculated-order-of-go'],4);
 assert.equal(item.snapshot['trips-ahead'],3);
 assert.equal(item.calculated['calculated-estimated-go-time'],'2026-11-08T19:36:42.000Z');
 assert.equal(item.calculated['go-time-start-in'],8.7);
 assert.equal(item.calculated['go-time-end-in'],11.6);
 assert.equal(item.snapshot['pace-method'],'measured-class');
 assert.match(context(item).nextTrip.key,/\|16$/);
});
test('current trip contributes its remaining estimated time when actual start is known',()=>{
 const input=example();input.classes[0].fields['current-trip-id']=11;
 input.trips.find(r=>r.fields['trip-id']===11).fields['source-json']='{"live":{"r1_start":"2026-11-08T19:26:06.000Z"}}';
 assert.equal(getTrip(input,15).calculated['calculated-estimated-go-time'],'2026-11-08T19:34:48.000Z');
 assert.equal(getTrip(input,11).snapshot.status,'Reported current');
});
test('upcoming trip consumes the inherited Class delay once',()=>{
 const item=getTrip(example(),35);
 assert.equal(item.calculated['calculated-estimated-go-time'],'2026-11-08T20:40:30.000Z');
 assert.equal(context(item).lateMinutes,26);
});
test('Ring pace and default are successive fallbacks',()=>{
 const input=example();input.classStatus[0].fields['average-pace']=null;
 assert.equal(getTrip(input,15).snapshot['pace-used'],3.5);
 input.ringStatus[0].fields['group-id']=999;
 assert.equal(getTrip(input,15).snapshot['pace-used'],PACE);
});
test('unknown peers and elapsed forecasts leave dependent timing unavailable',()=>{
 const input=example();input.trips.find(r=>r.fields['trip-id']===11).fields['order-of-go']=100000;
 assert.equal(getTrip(input,15).calculated['calculated-order-of-go'],null);
 assert.equal(getTrip(input,15).calculated['calculated-estimated-go-time'],null);
 assert(snapshots({...scope,input:example(),at:'2026-11-08T21:00:00.000Z'}).output.every(x=>x.calculated['calculated-estimated-go-time']===null));
});
test('linker preserves populated references and logs unmatched links',()=>{
 const issues=[];const result=fillReferences({trip:'https://airtable.com/existing'},{trips:row(1,{})},x=>issues.push(x));
 assert.equal(result.trip,'https://airtable.com/existing');assert.equal(result.schedule,null);
 assert(issues.includes('missing_or_ambiguous_schedule_reference'));
});

function memoryApi(input){
 const tables=structuredClone({...input,shows:[row(91,{'show-key':`${customer}|${show}`,is_default:true,show_id:show,customer_id:customer})],days:[row(92,{'show-day-key':prefix.slice(0,-1),date:day})],snapshots:[],runs:[],errors:[],heartbeat:[],cleanup:[row(93,{'\uFEFFName':'run-logs',cleanup:'checked'}),row(94,{'\uFEFFName':'live-rings',cleanup:'checked'})]});
 let id=10000;
 return{tables,base,
  async list(table,formula=''){
   let rows=tables[table];
   const ids=[...formula.matchAll(/RECORD_ID\(\)="([^"]+)"/g)].map(m=>m[1]);
   if(ids.length)rows=rows.filter(r=>ids.includes(r.id));
   const keys=[...formula.matchAll(/\{(run-id|error-key|heartbeat-key|snapshot-key)\}="([^"]+)"/g)];
   if(keys.length)rows=rows.filter(r=>keys.some(([,k,v])=>r.fields[k]===v));
   const suffixes=[...formula.matchAll(/RIGHT\(\{snapshot-key\},\d+\)="([^"]+)"/g)].map(m=>m[1]);
   if(suffixes.length)rows=rows.filter(r=>suffixes.some(s=>r.fields['snapshot-key'].endsWith(s)));
   return structuredClone(rows);
  },
  async upsert(table,key,fields){for(const f of fields){const r=tables[table].find(r=>r.fields[key]===f[key]);if(r)Object.assign(r.fields,f);else tables[table].push(row(id++,structuredClone(f)));}},
  async update(table,records){for(const r of records)Object.assign(tables[table].find(x=>x.id===r.id).fields,r.fields);},
  async remove(table,ids){tables[table]=tables[table].filter(r=>!ids.includes(r.id));},
 };
}
test('entry reads, saves, marks, verifies and appends a snapshot on the next cycle',async()=>{
 const api=memoryApi(example());let state={};const store={load:()=>structuredClone(state),save:s=>{state=structuredClone(s);}};
 const first=await run({api,store,now:at,clock:()=>at,runId:'first'});
 assert.equal(first.ok,true,JSON.stringify(first.issues));assert.equal(first.snapshots,22);assert.equal(first.marked,22);
 assert.equal(api.tables.runs[0].fields.result,'Success');assert.equal(api.tables.heartbeat[0].fields['run-id'],'first');
 assert.equal(api.tables.trips.find(r=>r.fields['trip-id']===15).fields['calculated-order-of-go'],4);
 const repeat=await run({api,store,now:at,clock:()=>at,runId:'first'});
 assert.equal(repeat.ok,true,JSON.stringify(repeat.issues));assert.equal(api.tables.snapshots.length,22);
 const next='2026-11-08T19:31:00.000Z',second=await run({api,store,now:next,clock:()=>next,runId:'second'});
 assert.equal(second.ok,true,JSON.stringify(second.issues));assert.equal(api.tables.snapshots.length,44);
 assert(api.tables.snapshots.filter(r=>r.fields['snapshot-key'].endsWith('|second')).every(r=>r.fields['previous-snapshot'].endsWith('|first')));
});
test('transport restricts mutations to Trip fields and owned tables',async()=>{
 const api=airtable({token:'offline',deadline:Date.now()+10000,pause:async()=>{},request:async()=>assert.fail('unexpected HTTP request')});
 await assert.rejects(()=>api.update('trips',[{id:rec(1),fields:{'order-of-go':10}}]),/Only Trip calculated fields/);
 await assert.rejects(()=>api.update('classStatus',[{id:rec(1),fields:{'estimated-start':at}}]),/forbidden/);
 await assert.rejects(()=>api.remove('trips',[rec(1)]),/forbidden/);
});

test('saved November 8 sync records preserve scoped trip identities and original clocks',()=>{
 const file=path.join(__dirname,'evidence/end-to-end-20261108/saved-inputs.jsonl');
 const names={'wef-live.trips':'trips','wef-live.rings':'rings','wef-live.groups':'groups','wef-live.classes':'classes','wef-data.schedules':'schedules','ring-status':'ringStatus','class-status':'classStatus'};
 const input=Object.fromEntries(Object.values(names).map(name=>[name,[]]));
 for(const line of fs.readFileSync(file,'utf8').trim().split(/\r?\n/)){const {table,...r}=JSON.parse(line);if(names[table])input[names[table]].push(r);}
 assert.equal(input.trips.length,307);assert(input.classStatus.length>0&&input.ringStatus.length>0);
 const clocks=Object.values(input).flat().flatMap(r=>['last-seen','evaluated-at','snapshot-at'].map(k=>Date.parse(r.fields[k]))).filter(Number.isFinite);
 const observedAt=new Date(Math.max(...clocks)).toISOString(),{output}=snapshots({...scope,input,at:observedAt});
 const active=input.trips.filter(r=>String(r.fields['record-status']).toLowerCase()==='active'&&r.fields['trip-key']?.startsWith(prefix));
 assert.equal(output.length,active.length);
 assert(output.every(r=>r.snapshot.date===day&&r.snapshot['observed-at']===(r.trip.fields['last-seen']??null)));
 console.log(JSON.stringify({savedTrips:input.trips.length,active:output.length,sourceAsOf:observedAt,ranked:output.filter(r=>r.calculated['calculated-order-of-go']!==null).length,estimated:output.filter(r=>r.snapshot['estimated-go-time']).length}));
});
