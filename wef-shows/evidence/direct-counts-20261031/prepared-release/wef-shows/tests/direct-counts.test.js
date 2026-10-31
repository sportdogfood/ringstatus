const test=require('node:test'),assert=require('node:assert/strict');
const {countDays,source}=require('../direct-counts'),{publishDays}=require('../show-days');
const clock=()=> '2026-10-31T12:00:00Z';
const show={id:'recShow',fields:{'show-key':'15|76',show_id:76,customer_id:15,is_default:true,start_date:'2026-10-30',end_date:'2026-10-31',selected_date:'2026-10-31',available_dates:'2026-10-30, 2026-10-31'}};
function fixture(){let active=0,peak=0;const calls=[];const inventories={};for(const [date,ids]of [['2026-10-30',[1,2]],['2026-10-31',[3]]])inventories[date]={show:{show_id:76,start_date:show.fields.start_date,end_date:show.fields.end_date},show_date:date,rings:[{ring_number:6,classes:ids.map(class_id=>({class_id}))}]};
 const classes={};for(const [id,date]of [[1,'2026-10-30'],[2,'2026-10-30'],[3,'2026-10-31']])classes[id]={show_id:76,class:{class_id:id},class_related_data:{date,ring:'6'},total_entry_trips:1,trips:[{entry_id:id===2?1:id,trip_id:100+id,horse:'DO_NOT_SAVE_HORSE',rider:'DO_NOT_SAVE_RIDER',scratch_trip:1}]};
 const read=async url=>{calls.push(url);active++;peak=Math.max(peak,active);await Promise.resolve();const u=new URL(url);assert.equal(u.origin,'https://wef-schedules-decoded.gombcg.workers.dev');const payload=u.pathname==='/schedule'?inventories[u.searchParams.get('date')]:classes[u.pathname.split('/').at(-1)];active--;return{payload:structuredClone(payload),sha256:'a'.repeat(64)};};return{inventories,classes,calls,read,peak:()=>peak};}
const options=f=>({show,runId:'fixture',clock,fetchCount:f.read});
test('all days including past are counted directly; unique entries, scratches and duplicate schedule classes; no entity detail survives',async()=>{
 const f=fixture();f.inventories['2026-10-30'].rings[0].classes.push({class_id:1});const r=await countDays(options(f));assert.equal(r.mode,'direct-proxy');assert.equal(r.request_count,7);assert.deepEqual(r.days.map(d=>[d.date,d['unique-entry-count'],d['proven-trip-count'],d.completeness]),[['2026-10-30',1,2,'complete'],['2026-10-31',1,1,'complete']]);assert(!JSON.stringify(r).includes('DO_NOT_SAVE'));assert(!JSON.stringify(r).includes('entry_id'));assert(f.peak()<=4);const again=await countDays(options(f));assert.deepEqual(again.days.map(d=>d['unique-entry-count']),[1,1]);
});
test('explicit empty inventory and class trips are verified zero, missing identities remain incomplete',async()=>{
 const f=fixture();f.inventories['2026-10-30'].rings=[];f.classes[3].trips=[];f.classes[3].total_entry_trips=0;let r=await countDays(options(f));assert(r.days.every(d=>d['unique-entry-count']===0&&d.completeness==='complete'));
 const other=fixture();delete other.classes[2].trips[0].trip_id;r=await countDays(options(other));assert.equal(r.days[0].completeness,'partial');assert.equal(r.days[0]['proven-trip-count'],1);assert(r.diagnostics[0].errors.length);
});
test('short class payload, pagination, wrong dated class and conflicting cross-class trip IDs never certify complete',async()=>{
 for(const change of [f=>f.classes[2].total_entry_trips=2,f=>f.classes[2].class_related_data.date='2026-10-31',f=>f.classes[2].trips[0].trip_id=101]){const f=fixture();change(f);const r=await countDays(options(f));assert.equal(r.days[0].completeness,'partial');}
 await assert.rejects(source('https://wef-schedules-decoded.gombcg.workers.dev/classes/1?show_id=76&customer_id=15',{deadline:Date.now()+30000,request:async()=>new Response(JSON.stringify({next_page:2}),{headers:{'x-rs-decoded':'1'}})}),/Paginated/);
});
test('changed final inventory is failed, per-day failure preserves last totals while other days publish',async()=>{
 const f=fixture();let reads=0;const old=f.read;f.read=async u=>{const r=await old(u);if(new URL(u).pathname==='/schedule'&&new URL(u).searchParams.get('date')==='2026-10-30'&&++reads===2)r.payload.rings=[];return r;};const r=await countDays(options(f));assert.equal(r.failures.length,1);assert.equal(r.days.length,1);
 const rows=show.fields.available_dates.split(', ').map((date,n)=>({id:'day'+n,fields:{'show-day-key':'15|76|'+date,show:[show.id],date,'unique-entry-count':99,'trip-count':111,'day-priority':n}}));
 const api={list:async()=>structuredClone(rows),upsert:async(t,k,fields)=>{for(const f of fields)Object.assign(rows.find(x=>x.fields[k]===f[k]).fields,f);return fields.map(f=>structuredClone(rows.find(x=>x.fields[k]===f[k])));}};
 const out=await publishDays({api,show,clock,readSummaries:async()=>r});assert.equal(out.ok,false);assert.equal(rows[0].fields['unique-entry-count'],99);assert.equal(rows[0].fields['trip-count'],111);assert.equal(rows[0].fields['latest-attempt-status'],'Failed');assert.equal(rows[1].fields['unique-entry-count'],1);assert.deepEqual(rows.map(r=>r.fields['day-priority']),[0,1]);
});
test('request deadline withholds unknown totals and still completes without substitute data',async()=>{const f=fixture();const r=await countDays({...options(f),deadline:0});assert.equal(r.days.length,0);assert.equal(r.failures.length,2);assert.equal(f.calls.length,0);});
test('source uses decoded proxy only, bounded retry and no response detail in errors',async()=>{
 let count=0;const r=await source('https://wef-schedules-decoded.gombcg.workers.dev/classes/1?show_id=76&customer_id=15',{deadline:Date.now()+30000,request:async()=>{count++;return count===1?new Response('PRIVATE',{status:502}):new Response('{"trips":[]}',{headers:{'x-rs-decoded':'1'}});}});assert.equal(count,2);assert.deepEqual(r.payload,{trips:[]});
 await assert.rejects(source('https://evil.example/classes/1',{deadline:Date.now()+30000,request:async()=>{throw Error('must not call');}}),/Unapproved/);
 await assert.rejects(source('https://wef-schedules-decoded.gombcg.workers.dev/classes/1?show_id=76&customer_id=15',{deadline:Date.now()+30000,request:async()=>new Response('{}')}),/decoding/);
});

test('invalid JSON diagnostics never disclose response detail',async()=>{await assert.rejects(source('https://wef-schedules-decoded.gombcg.workers.dev/classes/1?show_id=76&customer_id=15',{deadline:Date.now()+30000,request:async()=>new Response('PRIVATE_HORSE_NAME',{headers:{'x-rs-decoded':'1'}})}),e=>e.message==='Invalid count JSON response');});
