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
 const opts={api,clock:()=>now,statePath:path.join(dir,'focus.json'),archiveRoot:history,dayStopPath:path.join(dir,'days-stop.json'),archive:(...a)=>archiveFile(history,...a),publishDays:async()=>({ok:true,days:[],errors:[]}),fetchJson:async url=>{requests.push(url);if(fail)throw Error('source failed');return new URL(url).pathname==='/ring'?{show:{show_id:sid}}:source();}};
 return{tables,requests,opts,dir,set:(at,id=sid,bad=false)=>{now=at;sid=id;fail=bad;},run:extra=>runShows({...opts,...extra}),consume:()=>readShowFocus({read:()=>api.list('shows'),now,statePath:path.join(dir,'consumer.json'),archiveRoot:history})};
}

test('normal repeats manage dates without count requests, Data reads or historical count mutations',async t=>{
 const f=fixture(t),legacy={error:'prior count timeout'};fs.writeFileSync(f.opts.dayStopPath,JSON.stringify(legacy));
 const days=f.tables.shows[0].fields.available_dates.split(', ');for(const date of days)f.tables['show-days'].push({id:'day'+date,fields:{'show-day-key':'15|76|'+date,show:['canonical'],date,'unique-entry-count':17,'trip-count':21,'latest-attempt-status':'Failed','publication-status':'Unknown'}});
 const prior=structuredClone(f.tables['show-days']),list=f.opts.api.list;f.opts.api.list=async(name,base)=>{assert(!base||base===require('../settings').BASE_ID);assert(!['runs','collections','entries','trips'].includes(name));return list(name);};
 const native=global.fetch;t.after(()=>global.fetch=native);global.fetch=async()=>{throw Error('No count/provider requests permitted');};
 for(const at of ['2026-10-31T22:59:00Z','2026-10-31T23:04:00Z','2026-10-31T23:09:00Z']){f.set(at);const r=await f.run({readSummaries:()=>{throw Error('No Data or direct counts');},publishDays:()=>{throw Error('Retired count publisher must not run');}});assert.equal(r.ok,true,JSON.stringify(r));assert.equal(r.direct_counts,undefined);}
 assert.equal(f.tables.shows[0].fields.selected_date,'2026-11-01');assert.equal(f.tables['show-days'].find(x=>x.fields.date==='2026-11-01').fields['day-priority'],0);
 for(const before of prior){const after=f.tables['show-days'].find(r=>r.id===before.id);for(const [k,v]of Object.entries(before.fields))assert.deepEqual(after.fields[k],v);}
 assert.equal(f.requests.length,0);assert.equal(f.tables.heartbeat[0].fields.result,'Success');assert.equal(f.tables['run-logs'].length,3);assert.equal(fs.readFileSync(f.opts.dayStopPath,'utf8'),JSON.stringify(legacy));
});
test('missing canonical dates are created with existing keys and links, without count metadata',async t=>{
 const f=fixture(t);const native=global.fetch;t.after(()=>global.fetch=native);global.fetch=async()=>{throw Error('No count requests');};
 const r=await f.run();assert.equal(r.ok,true,JSON.stringify(r));assert.equal(r.show_days.created,7);assert.equal(f.tables['show-days'].length,7);
 for(const row of f.tables['show-days']){const offset=(Date.parse(row.fields.date)-Date.parse('2026-10-30'))/86400000;assert.deepEqual(Object.keys(row.fields).sort(),['date','day-priority','last_checked','last_checked_status','show','show-day-key']);}
 const ids=f.tables['show-days'].map(r=>r.id);assert.equal((await f.run()).ok,true);assert.deepEqual(f.tables['show-days'].map(r=>r.id),ids);
});
test('date publication refuses duplicate keys and wrong existing native links',async()=>{
 const {publishDates}=require('../show-days'),show={id:'show',fields:{'show-key':'15|76',customer_id:15,show_id:76,start_date:'2026-10-29',end_date:'2026-10-29',available_dates:'2026-10-29'}};
 for(const rows of [[{fields:{'show-day-key':'15|76|2026-10-29'}},{fields:{'show-day-key':'15|76|2026-10-29'}}],[{fields:{'show-day-key':'15|76|2026-10-29',show:['wrong'],date:'2026-10-29'}}]])await assert.rejects(publishDates({show,api:{list:async()=>rows,upsert:async()=>assert.fail('must not repair')}}),/Duplicate|identity/);
});
