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
 const api={list:async name=>structuredClone(tables[name].map(r=>name==='show-days'?{...r,fields:{...r.fields,'is_default (from show)':(r.fields.show||[]).map(id=>tables.shows.find(s=>s.id===id)?.fields.is_default===true?true:null)}}:r)),upsert:async(name,key,fields)=>{for(const f of fields){let r=tables[name].find(r=>r.fields[key]===f[key]);if(!r){r={id:'r'+(++n),fields:{}};tables[name].push(r);}Object.assign(r.fields,structuredClone(f));}return structuredClone(fields.map(f=>tables[name].find(r=>r.fields[key]===f[key])));},call:async(base,name,method,body)=>{for(const r of body.records)Object.assign(tables[name].find(x=>x.id===r.id).fields,r.fields);}};
 const opts={cleanup:async()=>({result:'Skipped'}),browserDiscovery:async()=>{throw Error('Browser unavailable fixture');},api,clock:()=>now,statePath:path.join(dir,'focus.json'),archiveRoot:history,dayStopPath:path.join(dir,'days-stop.json'),archive:(...a)=>archiveFile(history,...a),publishDates:async()=>({ok:true,days:[],errors:[]}),fetchJson:async url=>{requests.push(url);if(fail)throw Error('source failed');return new URL(url).pathname==='/ring'?{show:{show_id:sid}}:source();}};
 return{tables,requests,opts,dir,set:(at,id=sid,bad=false)=>{now=at;sid=id;fail=bad;},run:extra=>runShows({...opts,...extra}),consume:()=>readShowFocus({read:()=>api.list('shows'),now,statePath:path.join(dir,'consumer.json'),archiveRoot:history})};
}

test('priority phase runs despite count stop, preserves selected anchor and skips midnight repeat',async t=>{
 const f=fixture(t);const show=f.tables.shows[0];f.tables['show-days']=[29,30,31].map((d,i)=>({id:'d'+i,fields:{'show-day-key':'15|76|2026-10-'+d,show:[show.id],date:'2026-10-'+d,'trip-count':123}}));
 fs.writeFileSync(f.opts.dayStopPath,'{"error":"old stop"}');f.set('2026-10-30T23:04:00Z');let r=await f.run();assert.equal(r.day_priority.status,'Verified');assert.deepEqual(f.tables['show-days'].map(r=>r.fields['day-priority']),[-2,-1,0]);assert(f.tables['show-days'].every(r=>r.fields['trip-count']===123));assert.equal(fs.readFileSync(f.opts.dayStopPath,'utf8'),' {"error":"old stop"}'.trim());
 f.set('2026-10-31T04:04:00Z');r=await f.run();assert.equal(r.day_priority.status,'Skipped');assert.deepEqual(f.tables['show-days'].map(r=>r.fields['day-priority']),[-2,-1,0]);
});
test('discovery before19 labels every current date and marks old-show days inactive; repeat skips',async t=>{
 const f=fixture(t),show=f.tables.shows[0],{updatePriority}=require('../day-priority');show.fields.start_date='2026-10-20';
 f.tables.shows.push({id:'different',fields:{'show-key':'15|75',customer_id:15,show_id:75,start_date:'2026-10-01',end_date:'2026-10-04',is_default:false}});
 f.tables['show-days']=[{id:'old',fields:{'show-day-key':'15|76|2026-10-20',show:[show.id],date:'2026-10-20','day-priority':88}},{id:'today',fields:{'show-day-key':'15|76|2026-10-30',show:[show.id],date:'2026-10-30'}},{id:'other',fields:{'show-day-key':'15|75|2026-10-01',show:['different'],date:'2026-10-01','day-priority':77}}];
 const opts={api:f.opts.api,show,statePath:path.join(f.dir,'priority.json')};let r=await updatePriority({...opts,now:'2026-10-30T16:00:00Z',discovered:true});assert.equal(r.status,'Verified');assert.equal(f.tables['show-days'][0].fields['day-priority'],-10);assert.equal(f.tables['show-days'][1].fields['day-priority'],0);assert.equal(f.tables['show-days'][2].fields['day-priority'],999);
 assert.equal(f.tables['show-days'][2].fields.last_checked_status,'Not active');assert.equal(f.tables['show-days'][1].fields.last_checked_status,'Active');assert(f.tables['show-days'].every(r=>r.fields.last_checked==='2026-10-30T16:00:00Z'));
 r=await updatePriority({...opts,now:'2026-10-30T23:04:00Z'});assert.equal(r.status,'Verified');assert.equal((await updatePriority({...opts,now:'2026-10-30T23:09:00Z'})).status,'Skipped');
});
test('duplicate inventory fails before writes and durably stops only priority phase',async t=>{
 const f=fixture(t),show=f.tables.shows[0],{updatePriority}=require('../day-priority');const row={id:'x',fields:{show:[show.id],date:'2026-10-30','show-day-key':'15|76|2026-10-30'}};f.tables['show-days']=[row,structuredClone(row)];let writes=0;f.opts.api.call=async()=>{writes++;};const opts={api:f.opts.api,show,now:'2026-10-30T23:04:00Z',statePath:path.join(f.dir,'p.json')};await assert.rejects(updatePriority(opts),/duplicate/);assert.equal(writes,0);assert(fs.existsSync(opts.statePath+'.stopped.json'));await assert.rejects(updatePriority(opts),/stopped/);
});

test('ambiguous multi-show link is withheld before writes',async t=>{const f=fixture(t),show=f.tables.shows[0];f.tables['show-days']=[{id:'ambiguous',fields:{show:[show.id,'other'],date:'2026-10-30','show-day-key':'15|76|2026-10-30'}}];let writes=0;f.opts.api.call=async()=>{writes++;};await assert.rejects(require('../day-priority').updatePriority({api:f.opts.api,show,now:'2026-10-30T23:04:00Z',statePath:path.join(f.dir,'p.json')}),/Invalid/);assert.equal(writes,0);});

test('normal wake initializes v1 state once; unchanged repeat preserves verification timestamp',async t=>{
 const f=fixture(t),show=f.tables.shows[0],statePath=path.join(f.dir,'p.json'),{updatePriority}=require('../day-priority');
 f.tables['show-days']=[{id:'day',fields:{show:[show.id],date:'2026-10-30','show-day-key':'15|76|2026-10-30','___trip-count':42}}];
 fs.writeFileSync(statePath,JSON.stringify({version:1,anchor:'2026-10-30',show:show.id,date:'2026-10-30',evening:false}));
 const opts={api:f.opts.api,show,statePath};assert.equal((await updatePriority({...opts,now:'2026-10-30T16:00:00Z'})).status,'Verified');
 assert.equal((await updatePriority({...opts,now:'2026-10-30T16:05:00Z'})).status,'Skipped');
 assert.equal(f.tables['show-days'][0].fields.last_checked,'2026-10-30T16:00:00Z');assert.equal(f.tables['show-days'][0].fields['___trip-count'],42);
});

test('new-show rollover changes former day0 to inactive999 and stamps both shows',async t=>{
 const f=fixture(t),old=f.tables.shows[0],{updatePriority}=require('../day-priority'),statePath=path.join(f.dir,'p.json');
 f.tables['show-days']=[{id:'old-day',fields:{show:[old.id],date:'2026-10-30','show-day-key':'15|76|2026-10-30'}}];
 await updatePriority({api:f.opts.api,show:old,now:'2026-10-30T16:00:00Z',statePath});old.fields.is_default=false;
 const next={id:'new',fields:{'show-key':'15|77',show_id:77,customer_id:15,is_default:true,start_date:'2026-11-05',end_date:'2026-11-08',selected_date:'2026-11-05'}};f.tables.shows.push(next);
 f.tables['show-days'].push({id:'new-day',fields:{show:[next.id],date:'2026-11-05','show-day-key':'15|77|2026-11-05'}});
 const r=await updatePriority({api:f.opts.api,show:next,now:'2026-11-02T06:04:00Z',statePath,discovered:true});assert.equal(r.active,1);assert.equal(r.inactive,1);
 assert.deepEqual(f.tables['show-days'].map(r=>[r.fields['day-priority'],r.fields.last_checked_status]),[[999,'Not active'],[0,'Active']]);
 assert(f.tables['show-days'].every(r=>r.fields.last_checked==='2026-11-02T06:04:00Z'));
});

test('failed status readback never stamps last_checked and retains existing stop behavior',async t=>{
 const f=fixture(t),show=f.tables.shows[0],statePath=path.join(f.dir,'p.json');f.tables['show-days']=[{id:'day',fields:{show:[show.id],date:'2026-10-30','show-day-key':'15|76|2026-10-30'}}];
 f.opts.api.call=async()=>{};
 await assert.rejects(require('../day-priority').updatePriority({api:f.opts.api,show,now:'2026-10-30T16:00:00Z',statePath}),/readback/);
 assert.equal(f.tables['show-days'][0].fields.last_checked,undefined);assert(fs.existsSync(statePath+'.stopped.json'));
});

test('lookup disagreement is withheld before writes',async t=>{
 const f=fixture(t),show=f.tables.shows[0];f.tables['show-days']=[{id:'day',fields:{show:[show.id],date:'2026-10-30','show-day-key':'15|76|2026-10-30'}}];
 const list=f.opts.api.list;f.opts.api.list=async name=>{const rows=await list(name);if(name==='show-days')rows[0].fields['is_default (from show)']=[null];return rows;};
 let writes=0;f.opts.api.call=async()=>{writes++;};await assert.rejects(require('../day-priority').updatePriority({api:f.opts.api,show,now:'2026-10-30T16:00:00Z',statePath:path.join(f.dir,'p.json')}),/lookup/);assert.equal(writes,0);
});
