'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const {cleanup,TABLES,quickRows,eligible}=require('./candidate/cleanup');
function setup(t,names=TABLES){
 const runtime=fs.mkdtempSync(path.join(os.tmpdir(),'wef-cleanup-offline-'));t.after(()=>fs.rmSync(runtime,{recursive:true,force:true}));
 let seq=0,now='2026-11-02T15:00:00.000Z';
 const data={shows:[{id:'recShow',fields:{show_id:77,customer_id:15,is_default:true}}],cleanup:names.map(Name=>({id:'recC'+(++seq),fields:{Name,cleanup:true}}))};
 const row=(table,fields,createdTime='2026-11-01T10:00:00.000Z')=>{const r={id:'recR'+(++seq),createdTime,fields};(data[table]||=[]).push(r);return r;};
 const events=[];
 const api={list:async name=>structuredClone(data[name]||[]),upsert:async(name,key,rows)=>{
  events.push('log:'+name);for(const fields of rows){const old=(data[name]||[]).find(r=>r.fields[key]===fields[key]);if(old)Object.assign(old.fields,structuredClone(fields));else row(name,structuredClone(fields));}return data[name];
 },call:async(base,name,method,body)=>{assert.equal(method,'PATCH');const records=body.records.map(p=>{const r=data[name].find(r=>r.id===p.id);Object.assign(r.fields,p.fields);return r;});return{records};},
 retireCleanup:async(table,rows,policy,receipt,onDeleted)=>{
  assert.equal(require('./candidate/log-retention').verified(receipt).length,rows.length);
  assert(data['cleanup-logs'].some(r=>r.fields['archive-file']===receipt.file));
  events.push('delete:'+table);
  for(const r of rows)assert(eligible(table,r,policy.trigger,policy.show));
  data[table]=data[table].filter(r=>!rows.some(x=>x.id===r.id));onDeleted(rows.map(r=>r.id));
 }};
 const options={runtime,api,clock:()=>now,archive:async(kind,rows)=>{events.push('archive:'+kind);return require('./candidate/archive').archiveFile(path.join(runtime,'history'),kind,rows);}};
 return{data,row,events,options,advance:()=>{now='2026-11-03T15:00:00.000Z';}};
}
test('new show archives all snapshots before deletion, preserves current/future records and catalogs',async t=>{
 const a=setup(t);const old=a.row('live-entries',{'show-id':76,date:'2026-11-01',class:['recOldClass']});
 a.row('live-classes',{'show-id':76,date:'2026-11-01','live-entries':[old.id]});
 const retained=a.row('live-classes',{'show-id':77,date:'2026-11-07',groups:['recKept']});
 a.row('collections',{'show-id':76,'source-kind':'archive-catalog'});
 const r=await cleanup(a.options);assert.equal(r.result,'Success');assert.equal(a.data['live-entries'].length,0);
 assert.deepEqual(a.data['live-classes'],[retained]);assert.equal(a.data.collections.length,1);
 const lastArchive=a.events.map((e,i)=>e.startsWith('archive:')?i:-1).reduce((a,b)=>Math.max(a,b));assert(lastArchive<a.events.findIndex(e=>e.startsWith('delete:')));
 assert.equal(r.actions.find(a=>a.table==='live-entries').deleted,1);
 assert.deepEqual(require('./candidate/log-retention').verified(r.actions.find(a=>a.table==='live-classes').archive)[0].fields['live-entries'],[old.id]);
});
test('same show next day does not trigger show cleanup and repeat does not duplicate actions',async t=>{
 const a=setup(t);await cleanup(a.options);const before=a.data['cleanup-logs'].length;a.advance();
 a.row('live-groups',{'show-id':76,date:'2026-11-01'});
 const r=await cleanup(a.options);assert.equal(r.new_show,false);assert.equal(r.actions.length,0);assert.equal(a.data['cleanup-logs'].length,before);assert.equal(a.data['live-groups'].length,1);
});
test('a subsequent show change triggers again, including same-date old-show records',async t=>{
 const a=setup(t);await cleanup(a.options);a.data.shows[0].fields.show_id=78;
 a.row('live-groups',{'show-id':77,date:'2026-11-02'});const r=await cleanup(a.options);assert.equal(r.new_show,true);assert.equal(a.data['live-groups'].length,0);
});
test('quick cleanup at exactly 1000 selects oldest 500 and excludes business tables',()=>{
 const rows=Array.from({length:1000},(_,i)=>({id:'rec'+i,createdTime:new Date(Date.UTC(2026,10,1,0,i)).toISOString(),fields:{result:'Success',completed:'2026-11-01T12:00:00Z'}}));
 assert.equal(quickRows('heartbeat',rows.slice(0,999)).length,0);assert.equal(quickRows('heartbeat',rows).length,500);assert.equal(quickRows('heartbeat',rows.reverse())[0].id,'rec0');assert.equal(quickRows('live-groups',rows).length,0);
});
test('quick cleanup is recurring, archives/logs 500 and protects current invocation',async t=>{
 const a=setup(t,['heartbeat']);fs.writeFileSync(path.join(a.options.runtime,'cleanup-state.json'),JSON.stringify({show:77}));
 for(let i=0;i<1000;i++)a.row('heartbeat',{'show-id':77,'run-id':String(i),result:'Success',completed:'2026-11-02T14:59:00Z'},new Date(Date.UTC(2026,10,1,0,i)).toISOString());
 a.options.protectedRuns=['0'];const r=await cleanup(a.options);assert.equal(r.actions.length,1);assert.equal(r.actions[0].trigger,'quick-cleanup');assert.equal(r.actions[0].deleted,500);assert(a.data.heartbeat.some(r=>r.fields['run-id']==='0'));assert.equal(a.data.heartbeat.length,500);
 const repeat=await cleanup(a.options);assert.equal(repeat.actions.length,0);
});
test('active logs remain, including blank show IDs; terminal blank-show logs can retire on show discovery',async t=>{
 const a=setup(t,['heartbeat','run-logs','errs-log']);a.row('heartbeat',{result:'Running',started:'2026-11-02T14:59:00Z'});
 a.row('heartbeat',{result:'Success',completed:'2026-11-01T14:00:00Z'});a.row('run-logs',{result:'Running'});
 a.row('errs-log',{'occurred-at':'2026-11-01T14:00:00Z'});
 await cleanup(a.options);assert.equal(a.data.heartbeat.length,1);assert.equal(a.data.heartbeat[0].fields.result,'Running');assert.equal(a.data['run-logs'].length,1);assert.equal(a.data['errs-log'].length,0);
});
test('unchecked tables, references, catalogs and cleanup logs cannot be deleted',async t=>{
 const a=setup(t,['collections','wef-entries.entries','cleanup-logs','meta']);a.row('collections',{'show-id':76});const r=await cleanup(a.options);assert.equal(r.actions.length,0);assert.equal(a.data.collections.length,1);
});
test('archive failure causes no deletion and later invocations do not retry',async t=>{
 const a=setup(t,['live-groups']);a.row('live-groups',{'show-id':76});a.options.archive=async()=>{throw Error('disk full');};
 const r=await cleanup(a.options);assert.equal(r.result,'Error');assert.equal(a.data['live-groups'].length,1);assert.equal(a.data['errs-log'].length,1);
 const second=await cleanup(a.options);assert.equal(second.result,'Blocked');assert.equal(a.data['live-groups'].length,1);
});
test('catalog logging failure prevents deletion',async t=>{
 const a=setup(t,['live-groups']);a.row('live-groups',{'show-id':76});const upsert=a.options.api.upsert;
 a.options.api.upsert=async(t,...args)=>{if(t==='cleanup-logs')throw Error('catalog unavailable');return upsert(t,...args);};
 const r=await cleanup(a.options);assert.equal(r.result,'Error');assert.equal(a.data['live-groups'].length,1);assert(!a.events.some(e=>e.startsWith('delete:')));
});
test('delete failure is recorded once and cleanup stops without retry',async t=>{
 const a=setup(t,['live-groups']);a.row('live-groups',{'show-id':76});let calls=0;a.options.api.retireCleanup=async()=>{calls++;throw Error('uncertain DELETE');};
 const r=await cleanup(a.options);assert.equal(r.result,'Error');assert.equal(r.actions[0].result,'Error');await cleanup(a.options);assert.equal(calls,1);
});
test('canonical identity change after archive stops deletion',async t=>{
 const a=setup(t,['live-groups']);a.row('live-groups',{'show-id':76});const archive=a.options.archive;
 a.options.archive=async(...args)=>{const x=await archive(...args);a.data.shows[0].fields.show_id=78;return x;};
 const r=await cleanup(a.options);assert.equal(r.result,'Error');assert.equal(a.data['live-groups'].length,1);
});
test('old active logs and insufficient complete rows cannot force a 500-record deletion',()=>{
 const rows=Array.from({length:1000},(_,i)=>({id:'rec'+i,createdTime:'2026-11-01T00:00:00Z',fields:{result:i<499?'Success':'Running',completed:'2026-11-01T12:00:00Z'}}));assert.equal(quickRows('run-logs',rows).length,0);
});
