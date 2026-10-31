"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),zlib=require('node:zlib'),crypto=require('node:crypto');
const {runShows}=require('../collector'),{archiveFile}=require('../../wef-tenants/archive'),{readShowFocus}=require('../../wef-schedules/show-source');
function fixture(t){
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'shows-calendar-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));const history=path.join(dir,'history');fs.mkdirSync(history);
 let now='2026-10-30T22:54:00Z',sid=76,fail=false,n=0;
 const source=()=>({show:{show_id:sid,facility_id:516,show_name:'Fixture',start_date:'2026-10-29T00:00:00.000Z',end_date:'2026-11-04T00:00:00.000Z'},show_date:'2026-10-30',rings:[]});
 const payload=source(),body=Buffer.from(JSON.stringify(payload)),sha=crypto.createHash('sha256').update(body).digest('hex'),file=path.join(history,sha+'.json.gz');fs.writeFileSync(file,zlib.gzipSync(body));
 const row={id:'canonical',fields:{'show-key':'15|76',show_id:76,customer_id:15,facility_id:516,show_name:'Fixture',start_date:'2026-10-29',end_date:'2026-11-04',selected_date:'2026-10-30',available_dates:'2026-10-29, 2026-10-30, 2026-10-31, 2026-11-01, 2026-11-02, 2026-11-03, 2026-11-04',is_default:true,last_checked:'2026-10-30T22:49:00Z','default-payload-file':file,'default-payload-sha256':sha}};
 const tables={shows:[row],'run-logs':[],heartbeat:[],sources:[],'err-logs':[],'show-days':[]},requests=[];
 const api={list:async name=>structuredClone(tables[name]),upsert:async(name,key,fields)=>{for(const f of fields){let r=tables[name].find(r=>r.fields[key]===f[key]);if(!r){r={id:'r'+(++n),fields:{}};tables[name].push(r);}Object.assign(r.fields,structuredClone(f));}return structuredClone(fields.map(f=>tables[name].find(r=>r.fields[key]===f[key])));},call:async(base,name,method,body)=>{for(const r of body.records)Object.assign(tables[name].find(x=>x.id===r.id).fields,r.fields);}};
 const opts={api,clock:()=>now,statePath:path.join(dir,'focus.json'),archiveRoot:history,dayStopPath:path.join(dir,'days-stop.json'),archive:(...a)=>archiveFile(history,...a),publishDays:async()=>({ok:true,days:[],errors:[]}),fetchJson:async url=>{requests.push(url);if(fail)throw Error('source failed');return new URL(url).pathname==='/ring'?{show:{show_id:sid}}:source();}};
 return{tables,requests,opts,dir,set:(at,id=sid,bad=false)=>{now=at;sid=id;fail=bad;},run:extra=>runShows({...opts,...extra}),consume:()=>readShowFocus({read:()=>api.list('shows'),now,statePath:path.join(dir,'consumer.json'),archiveRoot:history})};
}

test('two scheduled-function fixture invocations count every day without Data reads or entity/raw-payload persistence',async t=>{
 t.mock.method(Date,'now',()=>Date.parse('2026-10-31T12:00:00Z'));const f=fixture(t);f.set('2026-10-31T12:00:00Z');let calls=0;const originalFetch=global.fetch;t.after(()=>global.fetch=originalFetch);
 const sid=76,start='2026-10-29',end='2026-11-04';global.fetch=async url=>{calls++;const u=new URL(url);assert.equal(u.hostname,'wef-schedules-decoded.gombcg.workers.dev');let p;
 if(u.pathname==='/schedule'){const date=u.searchParams.get('date'),cid=Number(date.replaceAll('-',''));p={show:{show_id:sid,start_date:start,end_date:end},show_date:date,rings:[{ring_number:6,classes:[{class_id:cid}]}]};}
 else{const cid=Number(u.pathname.split('/').at(-1)),v=String(cid),date=v.slice(0,4)+'-'+v.slice(4,6)+'-'+v.slice(6);p={show_id:sid,class:{class_id:cid},class_related_data:{date,ring:6},total_entry_trips:1,trips:[{entry_id:123,trip_id:cid,horse:'PRIVATE_HORSE_NAME',rider:'PRIVATE_RIDER_NAME'}]};}
 return new Response(JSON.stringify(p),{headers:{'x-rs-decoded':'1'}});};
 const list=f.opts.api.list;f.opts.api.list=async(table,base)=>{assert.notEqual(base,'appdl16HJ5ETm8N69');assert(!['runs','collections','entries','horses','riders','trips'].includes(table));return list(table);};
 for(let n=0;n<2;n++){const r=await f.run({publishDays:require('../show-days').publishDays});assert.equal(r.ok,true,JSON.stringify(r));assert.equal(r.show_days.rows,7);if(n===0)assert.equal(r.show_days.created,7);assert.equal(r.direct_counts.request_count,21);assert.equal(r.archives.length,0);assert(r.observability.sources.some(s=>s.key==='direct-day-counts'));assert(!r.observability.sources.some(s=>s.key==='season-counts'));assert.equal(f.tables['show-days'].length,7);assert(f.tables['show-days'].every(r=>r.fields['unique-entry-count']===1&&r.fields['trip-count']===1));assert.equal(f.tables['show-days'].find(r=>r.fields.date==='2026-10-30').fields['day-priority'],0);assert(!JSON.stringify(f.tables).includes('PRIVATE_'));}
 assert.equal(calls,42);assert.equal(f.tables['run-logs'].length,2);assert.equal(f.tables.heartbeat.length,1);
});

test('failed class response retains previous totals, publishes safe other days, and marks run/heartbeat Partial with durable count stop',async t=>{
 t.mock.method(Date,'now',()=>Date.parse('2026-10-31T12:00:00Z'));const f=fixture(t);f.set('2026-10-31T12:00:00Z');const prior=global.fetch;t.after(()=>global.fetch=prior);
 const date='2026-10-29';f.tables['show-days'].push({id:'existing',fields:{'show-day-key':'15|76|'+date,show:['canonical'],date,'unique-entry-count':99,'trip-count':199,'day-priority':-1}});
 global.fetch=async url=>{const u=new URL(url);let p;if(u.pathname==='/schedule')p={show:{show_id:76,start_date:'2026-10-29',end_date:'2026-11-04'},show_date:u.searchParams.get('date'),rings:u.searchParams.get('date')===date?[{ring_number:6,classes:[{class_id:1}]}]:[]};else p={show_id:76,class:{class_id:1},class_related_data:{date,ring:6},total_entry_trips:1,trips:[{entry_id:1}]};return new Response(JSON.stringify(p),{headers:{'x-rs-decoded':'1'}});};
 const r=await f.run({publishDays:require('../show-days').publishDays});assert.equal(r.ok,false);assert.equal(r.discovery_ok,true);assert.equal(f.tables.heartbeat[0].fields.result,'Partial');assert.equal(f.tables['run-logs'][0].fields.result,'Partial');assert(fs.existsSync(f.opts.dayStopPath));assert.equal(f.tables['show-days'][0].fields['unique-entry-count'],99);assert.equal(f.tables['show-days'][0].fields['trip-count'],199);assert.equal(f.tables['show-days'][0].fields['latest-attempt-status'],'Failed');assert.equal(r.show_days.rows,6);assert(f.tables['err-logs'].length>0);
});
