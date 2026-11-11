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
test('normal repeat runs retain source date/time, heartbeat and logs; calendar change keeps native ID and schedules consumes it',async t=>{
 const f=fixture(t),source=f.tables.shows[0].fields.last_checked;
 assert.equal((await f.run()).ok,true);assert.equal(f.requests.length,0);
 f.set('2026-10-30T23:04:00Z');const r=await f.run();assert.equal(r.ok,true,r.errors.join(';'));assert.equal(r.discovery.show_day,'Advanced');
 const row=f.tables.shows[0];assert.equal(row.id,'canonical');assert.equal(row.fields.selected_date,'2026-10-31');assert.equal(row.fields.source_selected_date,'2026-10-30');assert.equal(row.fields.last_checked,source);
 assert.equal((await f.consume()).show.fields.sgl_default_day,'2026-10-31');
 f.set('2026-10-30T23:09:00Z');assert.equal((await f.run()).discovery.show_day,'Retained');assert.equal(f.requests.length,0);assert.equal(f.tables['run-logs'].length,3);assert.equal(f.tables.heartbeat.length,1);
});
test('Sunday poll repeats after three hours until show changes then stops until next Sunday',async t=>{
 const f=fixture(t);await f.run();f.set('2026-11-02T03:04:00Z');let r=await f.run();assert.equal(r.ok,true,r.errors.join(';'));assert.equal(f.requests.length,2);
 f.set('2026-11-02T03:09:00Z');await f.run();assert.equal(f.requests.length,2);
 f.set('2026-11-02T06:04:00Z',77);r=await f.run();assert.equal(r.ok,true,r.errors.join(';'));assert.equal(f.requests.length,4);assert.equal(r.show_id,77);assert.equal(f.tables.shows[0].fields.is_default,false);
 f.set('2026-11-02T09:04:00Z',78);await f.run();assert.equal(f.requests.length,4);assert.equal(f.tables.shows.length,2);
});
test('failed source logs linked error, retains authority time and does not retry on five-minute wakes',async t=>{
 const f=fixture(t);await f.run();const before=structuredClone(f.tables.shows);f.set('2026-11-02T03:04:00Z',76,true);const first=await f.run();assert.equal(first.ok,false);assert.deepEqual(f.tables.shows,before);assert.equal(f.requests.length,1);assert.equal(f.tables['err-logs'].length,2);assert.equal(f.tables.sources[0].fields['last-result'],'Failed');
 f.set('2026-11-02T03:09:00Z');const second=await f.run();assert.equal(second.ok,false);assert.match(second.errors[0],/awaiting/);assert.equal(f.requests.length,1);assert.deepEqual(f.tables.shows,before);
});
test('existing count stop stays byte-identical while calendar and heartbeat proceed',async t=>{
 const f=fixture(t);fs.writeFileSync(f.opts.dayStopPath,'{"error":"prior count failure"}');let calls=0;f.set('2026-10-30T23:04:00Z');const r=await f.run({publishDays:async()=>{calls++;throw Error('must not retry');}});
 assert.equal(r.discovery_ok,true);assert.equal(r.ok,true);assert.equal(calls,0);assert.equal(f.tables.heartbeat[0].fields.result,'Success');assert.equal(fs.readFileSync(f.opts.dayStopPath,'utf8'),' {"error":"prior count failure"}'.trim());
});
test('tampered source archive, wrong selection evidence and ambiguous canonical inventory fail closed',async t=>{
 for(const mode of ['hash','calendar','duplicate']){const f=fixture(t);if(mode==='hash')f.tables.shows[0].fields['default-payload-sha256']='0'.repeat(64);if(mode==='calendar')Object.assign(f.tables.shows[0].fields,{selection_basis:'calendar',selection_at:'2026-10-30T18:00:00Z'});if(mode==='duplicate')f.tables.shows.push(structuredClone(f.tables.shows[0]));const before=structuredClone(f.tables.shows);const r=await f.run();assert.equal(r.ok,false,mode);assert.deepEqual(f.tables.shows,before);assert.equal(f.requests.length,0);}
});
test('archive failure prevents calendar mutation',async t=>{const f=fixture(t);f.set('2026-10-30T23:04:00Z');const before=structuredClone(f.tables.shows);const r=await f.run({archive:async()=>{throw Error('disk failed');}});assert.equal(r.ok,false);assert.deepEqual(f.tables.shows,before);});
test('adapter still rejects cross-base writes and deletes',async()=>{const api=require('../airtable').airtable({request:async()=>{throw Error('network forbidden');}});await assert.rejects(api.call('other','shows','PATCH',{}));await assert.rejects(api.call(require('../settings').BASE_ID,'shows','DELETE',{}));});

test('normal collector runs the real cleanup check independently without changing discovery or archives',async t=>{
 const f=fixture(t);
 f.tables.cleanup=['run-logs','err-logs','heartbeat'].map((Name,i)=>({id:'control'+i,fields:{Name,cleanup:'checked'}}));
 f.tables['cleanup-logs']=[];
 f.opts.api.removeCleanup=async(table,rows,options)=>{
  await options.authorize(rows);
  const ids=rows.map(r=>r.id);
  f.tables[table]=f.tables[table].filter(r=>!ids.includes(r.id));
  await options.onDeleted(ids);
 };
 const cleanup=require('../cleanup').runCleanup;
 const first=await f.run({cleanup});
 assert.equal(first.ok,true,first.errors.join(';'));
 assert.equal(first.cleanup.result,'Skipped');
 assert.equal(first.cleanup.counts['run-logs'],1);
 assert.equal(first.archives.length,0);
 assert.equal(f.requests.length,0);
 assert(fs.existsSync(path.join(f.dir,'cleanup-state.json')));
 f.set('2026-11-02T03:04:00Z',76,true);
 const failed=await f.run({cleanup});
 assert.equal(failed.ok,false);
 assert.equal(failed.cleanup.result,'Success',failed.cleanup.error);
 assert.equal(failed.cleanup.counts['run-logs'],1);
 assert.equal(failed.cleanup.actions.find(a=>a.table==='run-logs').before,2);
});
test('consumer freshness expires without authority heartbeat even when source date is retained',async t=>{const f=fixture(t);await f.run();f.set('2026-10-30T23:25:00Z');await assert.rejects(f.consume(),/stale/);});

const captured=require('./fixtures/browser-schedule.json');
const browserSource=require('../browser-discovery');
function browserFound(){return browserSource.normalize({url:'https://www.wellingtoninternational.com/showgrounds/show-schedule/?date=2026-11-07&sid=200000077',defaultShow:200000077,payload:structuredClone(captured)});}
test('browser primary creates linked show-days and archive, no proxy requests; repeat preserves keys and source time',async t=>{
 const f=fixture(t);await f.run();f.set('2026-11-07T23:00:00Z');let calls=0;
 const opts={browserDiscovery:async()=>{calls++;return browserFound();},publishDates:require('../show-days').publishDates};
 let r=await f.run(opts);assert.equal(r.ok,true,r.errors.join(';'));assert.equal(r.discovery.method,'Browser');assert.equal(f.requests.length,0);assert.equal(calls,1);
 const show=f.tables.shows.find(x=>x.fields.show_id===200000077);assert.equal(show.fields.source_selected_date,'2026-11-07');assert.equal(f.tables.shows[0].fields.is_default,false);
 assert.deepEqual(f.tables['show-days'].map(x=>[x.fields.date,x.fields['day-priority']]),[['2026-11-05',-2],['2026-11-06',-1],['2026-11-07',0],['2026-11-08',1]]);
 assert(r.archives.some(x=>x.kind==='default-browser'));assert(r.archives.some(x=>x.kind==='default-schedule'));assert(!r.archives.some(x=>x.kind==='default-ring'));
 assert.deepEqual(r.observability.sources.map(x=>x.key),['default-browser']);assert.equal((await f.consume()).show.fields.sgl_default_day,'2026-11-07');
 const ids=f.tables['show-days'].map(x=>x.id),sourceTime=show.fields.last_checked;f.set('2026-11-07T23:05:00Z');r=await f.run(opts);assert.equal(r.ok,true,r.errors.join(';'));assert.equal(calls,1);assert.equal(f.requests.length,0);assert.equal(show.fields.last_checked,sourceTime);assert.deepEqual(f.tables['show-days'].map(x=>x.id),ids);
});
test('browser failure uses original ring/schedule pair only, with linked failure evidence',async t=>{
 const f=fixture(t);await f.run();f.set('2026-11-02T03:04:00Z');const r=await f.run();assert.equal(r.ok,true,r.errors.join(';'));assert.equal(r.discovery.method,'Proxy fallback');assert.deepEqual(f.requests.map(x=>new URL(x).pathname),['/ring','/schedule']);assert.equal(r.observability.sources.find(x=>x.key==='default-browser').result,'Failed');assert(r.error_records.length>0);
});
test('both discovery methods fail without replacing canonical show or retrying the next wake',async t=>{
 const f=fixture(t);await f.run();const before=structuredClone(f.tables.shows);f.set('2026-11-02T03:04:00Z',76,true);const first=await f.run();assert.equal(first.ok,false);assert.deepEqual(f.tables.shows,before);assert.equal(f.requests.length,1);f.set('2026-11-02T03:09:00Z');assert.equal((await f.run()).ok,false);assert.equal(f.requests.length,1);
});
test('browser archive failure is not a browser discovery failure and never invokes fallback',async t=>{
 const f=fixture(t);await f.run();f.set('2026-11-07T23:00:00Z');const before=structuredClone(f.tables.shows);const r=await f.run({browserDiscovery:async()=>browserFound(),archive:async()=>{throw Error('archive fixture failure');}});assert.equal(r.ok,false);assert.equal(f.requests.length,0);assert.deepEqual(f.tables.shows,before);
});
