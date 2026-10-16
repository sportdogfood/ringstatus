'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {fixture,payload,NOW}=require('./fixtures/schedule-collector');
const staging=require('../store');
global.fetch=async()=>{throw Error('Network forbidden in offline staging tests');};
function memoryStore(){
 const attempts=[],datasets=new Map();let writes=0;
 return {attempts,datasets,get writes(){return writes;},
  async begin(fields){const r={id:'attempt'+attempts.length,fields:structuredClone(fields)};attempts.push(r);return r;},
  async finish(r,fields){Object.assign(r.fields,structuredClone(fields));return r;},
  async latest(id,date){return structuredClone(attempts.filter(r=>r.fields.status==='complete'&&r.fields['show-id']===id&&r.fields.date===date).at(-1)||null);},
  async writeRows(id,show,date,rows){writes++;datasets.set(id,structuredClone(rows));},
  async loadRows(id){return structuredClone(datasets.get(id)||[]);}
 };
}

const shows=[{fields:{show_id:74,customer_id:15,focus_day:'2026-10-15',start_date:'2026-10-15',end_date:'2026-10-17'}}];
const collect=(store,f)=>staging.collectStagedSchedules({store,io:f.io,shows,now:NOW,clock:()=>NOW});
test('all show days collected with no runtime reads or writes',async()=>{
 const f=fixture(),store=memoryStore();
 for(const key of ['fetchShowTargetRows','fetchLatestHeartbeat','fetchLiveGroupsRows','fetchShowRecordId','fetchExistingRowsForShow','fetchHeartbeatViewRows','airtableCreateRecords','airtablePatchRecords','fetchTableFieldMeta']) f.io[key]=()=>{throw Error('Forbidden runtime call '+key);};
 const r=await collect(store,f);assert.equal(r.ok,true,JSON.stringify(r));assert.equal(store.datasets.size,3);assert.equal(f.rows.length,0);
 assert.equal(staging.consumeStagedCollection,undefined);
});
test('unchanged recurring collection reuses existing datasets',async()=>{
 const store=memoryStore(),f=fixture();assert.equal((await collect(store,f)).ok,true);
 const writes=store.writes;const r=await collect(store,f);assert.equal(r.ok,true);assert.ok(r.days.every(d=>d.unchanged));assert.equal(store.writes,writes);
});
test('source failure preserves last completed data',async()=>{
 const store=memoryStore(),f=fixture();await collect(store,f);const previous=await store.latest(74,'2026-10-15');
 f.io.fetchJson=async()=>{throw Error('source unavailable');};const r=await collect(store,f);assert.equal(r.ok,false);
 assert.deepEqual(await store.latest(74,'2026-10-15'),previous);
});
test('partial write never becomes complete',async()=>{
 const store=memoryStore(),f=fixture();store.writeRows=async()=>{throw Error('batch failed');};
 assert.equal((await collect(store,f)).ok,false);assert.equal(await store.latest(74,'2026-10-15'),null);
});
test('readback corruption prevents completion',async()=>{
 const store=memoryStore(),f=fixture();store.loadRows=async()=>[];
 assert.equal((await collect(store,f)).ok,false);assert.equal(await store.latest(74,'2026-10-15'),null);
});
test('in-progress collection leaves completed snapshot available',async()=>{
 const store=memoryStore(),f=fixture();await collect(store,f);const old=await store.latest(74,'2026-10-15');
 let release,started;const gate=new Promise(r=>release=r),signal=new Promise(r=>started=r);
 const fetch=f.io.fetchJson;f.io.fetchJson=async u=>{started();await gate;return fetch(u);};
 const pending=collect(store,f);await signal;assert.deepEqual(await store.latest(74,'2026-10-15'),old);
 release();assert.equal((await pending).ok,true);
});
test('missing show bounds reject without guessing focus dates',async()=>{
 const store=memoryStore();await assert.rejects(staging.collectStagedSchedules({store,io:fixture().io,shows:[{fields:{focus_day:'2026-10-15'}}],now:NOW}),/Invalid show focus date/);
 assert.equal(store.attempts.length,0);
});
test('same-day keys track adds changes drops and preserve inactive history',()=>{
 const row=(key,name)=>({key,fields:{show_id:74,show_date:'2026-10-15',class_name:name}});
 const before=[row('a','A'),row('b','B'),row('c','C')],after=[row('a','changed'),row('b','B'),row('d','D')];
 const result=staging.reconcileRows(before,after,'2026-10-15');
 assert.deepEqual(result.changes,{added:['d'],changed:['a'],unchanged:['b'],dropped:['c']});
 assert.equal(result.rows.find(r=>r.key==='c').inactive,true);assert.equal(result.rows.find(r=>r.key==='c').dropped_at,'2026-10-15');
 assert.deepEqual(staging.reconcileRows(result.rows,after,'2026-10-16').changes.dropped,[]);
 assert.equal(staging.reconcileRows(result.rows,after,'2026-10-16').rows.find(r=>r.key==='c').dropped_at,'2026-10-15');
 assert.throws(()=>staging.reconcileRows([], [row('a','A'),row('a','B')],'2026-10-15'),/duplicate/);
});
test('empty response cannot drop established records',async()=>{
 const store=memoryStore(),f=fixture();await collect(store,f);const before=await store.latest(74,'2026-10-15');
 const source=f.io.fetchJson;f.io.fetchJson=async u=>{const p=await source(u);p.data=[];return p;};
 const result=await collect(store,f);assert.equal(result.ok,false);assert.ok(result.days.every(d=>/zero-row/.test(d.error)));
 assert.deepEqual(await store.latest(74,'2026-10-15'),before);
});
test('returning dropped record reactivates without deleting history',()=>{
 const row={key:'a',fields:{show_id:74,show_date:'2026-10-15',class_name:'A'}};
 const dropped=staging.reconcileRows([row],[],'2026-10-15');
 const back=staging.reconcileRows(dropped.rows,[row],'2026-10-16');
 assert.deepEqual(back.changes.added,['a']);assert.equal(back.rows[0].inactive,undefined);assert.equal(back.rows[0].dropped_at,undefined);
});
test('all days reconcile independently and repeated drops are not reissued',async()=>{
 const store=memoryStore(),f=fixture();const source=f.io.fetchJson;
 f.io.fetchJson=async u=>{const p=await source(u);p.data.push({...p.data[0],class_number:22,class_groupxclasses_id:20,class_group_id:21});return p;};
 assert.equal((await collect(store,f)).ok,true);
 f.io.fetchJson=source;
 const changed=await collect(store,f);assert.equal(changed.ok,true,JSON.stringify(changed));
 assert.equal(changed.days.length,3);assert.ok(changed.days.every(d=>d.changes.dropped.length===1));
 for(const day of changed.days){const record=await store.latest(74,day.date);const rows=await store.loadRows(record.fields['dataset-id']);assert.equal(rows.length,2);assert.equal(rows.filter(r=>r.inactive).length,1);}
 const repeated=await collect(store,f);assert.ok(repeated.days.every(d=>d.changes.dropped.length===0&&d.unchanged));
});
test('wrong date and malformed payload never replace a saved snapshot',async()=>{
 const store=memoryStore(),f=fixture();await collect(store,f);const before=await store.latest(74,'2026-10-15');
 f.io.fetchJson=async()=>payload('2027-01-01');assert.equal((await collect(store,f)).ok,false);assert.deepEqual(await store.latest(74,'2026-10-15'),before);
 f.io.fetchJson=async()=>({});assert.equal((await collect(store,f)).ok,false);assert.deepEqual(await store.latest(74,'2026-10-15'),before);
});
module.exports={memoryStore};


test('rotating token evidence does not change semantic schedules or stored datasets',async()=>{
 const store=memoryStore(),f=fixture();let token='first';
 f.io.applyScheduleHtmlTimeOverlay=rows=>({rows:rows.map(row=>({...row,fields:{...row.fields,...require('../token-utils').buildSglTokenFields(token)}})),summary:{}});
 await collect(store,f);const writes=store.writes,previous=await store.latest(74,'2026-10-15');
 token='rotated-and-longer';const result=await collect(store,f);
 assert.equal(result.ok,true);assert.equal(store.writes,writes);
 assert.ok(result.days.every(d=>d.unchanged&&d.changes.changed.length===0));
 const latest=await store.latest(74,'2026-10-15'),context=JSON.parse(latest.fields['context-json']);
 assert.equal(latest.fields['dataset-id'],previous.fields['dataset-id']);
 assert.equal(context.token_evidence.rotated_rows,1);
 assert.notEqual(context.observed_fingerprint,latest.fields.fingerprint);
 const saved=await store.loadRows(latest.fields['dataset-id']);assert.equal(saved[0].fields.sgl_token_raw,'first');
 const damaged=structuredClone(saved);damaged[0].fields.sgl_token_raw='damaged';
 assert.throws(()=>staging.verifyDataset(latest,damaged,74,'2026-10-15'),/readback mismatch/);
 token='third';const source=f.io.fetchJson;f.io.fetchJson=async u=>{const p=await source(u);p.data[0].class_name='Real change';return p;};
 const changed=await collect(store,f);assert.ok(changed.days.every(d=>!d.unchanged&&d.changes.changed.length===1));
});

test('valid empty in-range days warn while failed retrieval remains failure',async()=>{
 const store=memoryStore(),f=fixture();const source=f.io.fetchJson;
 f.io.fetchJson=async u=>{const p=await source(u);p.data=[];return p;};
 const result=await collect(store,f);assert.equal(result.ok,true);
 assert.ok(result.days.every(d=>d.warnings[0].code==='empty_show_day'));
 assert.equal(JSON.parse(store.attempts[0].fields['context-json']).warnings[0].level,'WARNING');
 f.io.fetchJson=async()=>{throw Error('unavailable');};
 const failed=await collect(store,f);assert.equal(failed.ok,false);assert.ok(failed.days.every(d=>!d.warnings));
});


test('estimated times cover every show day, changes update snapshots, failure preserves them',async()=>{
 const store=memoryStore(),f=fixture(),seen=[];let time='10:30AM';
 f.io.captureScheduleHtml=async(show,date)=>{seen.push([show,date]);return `<tr class="class_group_row"><td><a href="?cgid=8&amp;ring=2">Class [11]</a></td><td>${time}</td></tr>`;};
 let r=await collect(store,f);assert.equal(r.ok,true);
 assert.deepEqual(seen,[[74,'2026-10-15'],[74,'2026-10-16'],[74,'2026-10-17']]);
 for(const day of r.days) assert.equal((await store.loadRows(day.dataset_id))[0].fields.estimated_start_time,'10:30:00');
 const writes=store.writes;r=await collect(store,f);assert.ok(r.days.every(d=>d.unchanged));assert.equal(store.writes,writes);
 time='1:30PM';r=await collect(store,f);assert.ok(r.days.every(d=>d.changes.changed.length===1));
 for(const day of r.days) assert.equal((await store.loadRows(day.dataset_id))[0].fields.estimated_start_time,'13:30:00');
 f.io.captureScheduleHtml=async()=>{throw Error('capture timeout');};r=await collect(store,f);
 assert.equal(r.ok,true);assert.ok(r.days.every(d=>d.unchanged&&d.warnings.some(w=>w.code==='estimated_times_incomplete')));
 for(const day of r.days) assert.equal((await store.loadRows(day.dataset_id))[0].fields.estimated_start_time,'13:30:00');
});


test('collector processes today then tomorrow before fallback dates and keeps zero-entry classes',async()=>{
 const store=memoryStore(),f=fixture(),seen=[];
 const fetch=f.io.fetchJson;f.io.fetchJson=async u=>{const payload=await fetch(u);for(const r of payload.data)r.total_trips=0;return payload;};
 f.io.captureScheduleHtml=async(show,date)=>{seen.push(date);return '<tr class="class_group_row"><td><a href="?cgid=8&amp;ring=2">Class [11]</a></td><td>8 AM</td></tr>';};
 const range=[{fields:{...shows[0].fields,start_date:'2026-10-14',end_date:'2026-10-18'}}];
 const r=await staging.collectStagedSchedules({store,io:f.io,shows:range,now:NOW,clock:()=>NOW});
 assert.equal(r.ok,true,JSON.stringify(r));assert.deepEqual(seen,['2026-10-15','2026-10-16','2026-10-17','2026-10-18','2026-10-14']);
 for(const day of r.days){const rows=await store.loadRows(day.dataset_id);assert.equal(rows.length,1);assert.equal(rows[0].fields.total_trips,0);assert.equal(rows[0].fields.estimated_start_time,'08:00:00');}
});
