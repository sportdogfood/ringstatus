"use strict";
// Read normal receipts and Airtable only. Does not invoke sources or collectors.
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),zlib=require('node:zlib');
const root=path.resolve(__dirname,'..'),dir=path.join(__dirname,'evidence/consumer-connection');
async function verify(kind){
 assert.ok(['entries','live'].includes(kind));
 const home=path.join(root,'wef-'+kind),receipt=JSON.parse(fs.readFileSync(path.join(home,'runtime/last-run.json')));
 const cutoff=JSON.parse(fs.readFileSync(path.join(dir,'cutover.json'))).at;
 assert.ok(receipt.started>cutoff,'No post-deployment normal receipt yet');
 const evidence={at:new Date().toISOString(),kind,receipt,result:'FAIL'};
 try{
  assert.equal(receipt.ok,true,receipt.errors.join('; '));
  assert.equal(receipt.show_source.base,'appCaSmL819KK7UFJ');
  assert.equal(receipt.show_source.record,'rec4En9UWd6lkT1Oh');
  const api=require(path.join(home,'airtable')).airtable(),data={};
  const tables=kind==='entries'?['entries','collections','run-logs','heartbeat',require(path.join(home,'settings')).SYNC_TABLE]:['live-rings','live-groups','live-classes','live-entries','collections','run-logs','heartbeat','wef-schedules.schedules','wef-entries.entries'];
  for(const t of tables)data[t]=await api.list(t);
  const log=data['run-logs'].find(r=>r.fields['run-id']===receipt.run_id);
  assert.equal(log?.fields.result,'Success');assert.equal(log.fields.completed,receipt.completed);
  assert.equal(data.heartbeat[0].fields['checked-at'],receipt.completed);assert.equal(data.heartbeat[0].fields.result,'Success');
  for(const a of receipt.archives){assert.equal(crypto.createHash('sha256').update(zlib.gunzipSync(fs.readFileSync(a.file))).digest('hex'),a.sha256);}
  assert.equal(log.fields['archive-count'],receipt.archives.length);
  const before=JSON.parse(fs.readFileSync(path.join(dir,'records-before.json')));
  const eq=(a,b)=>assert.deepEqual([...(a||[])].sort(),[...(b||[])].sort());
  const stable=(rows,old,key)=>{for(const r of rows){const prior=old.find(o=>o.fields[key]===r.fields[key]);if(prior)assert.equal(r.id,prior.id,'Existing identity replaced');}require('../wef-tenants/airtable').unique(rows,key);};
  if(kind==='entries'){
   stable(data.entries,before.entries,'entry-key');stable(data.collections,before.collections,'collection-id');
   const active=data.entries.filter(r=>r.fields.status==='active');assert.equal(active.length,receipt.rows);
   const schedules=data[require(path.join(home,'settings')).SYNC_TABLE];
   for(const r of active){assert.equal(r.fields.schedules.length,1);const s=schedules.find(s=>s.id===r.fields.schedules[0]);assert.equal(s?.fields['stage-key'],r.fields['schedule-key']);assert.equal(s.fields['class-id'],r.fields['class-id']);assert.equal(s.fields.date,r.fields.date);const old=before.entries.find(o=>o.fields['entry-key']===r.fields['entry-key']);if(old)eq(r.fields.schedules,old.fields.schedules);}
   for(const c of data.collections.filter(r=>r.fields['show-id']===receipt.show_id)){assert.equal(c.fields.status,'complete');assert.equal(c.fields['row-count']||0,(c.fields.entries||[]).length);}
  }else{
   for(const [t,key]of Object.entries({'live-rings':'ring-key','live-groups':'group-key','live-classes':'class-key','live-entries':'live-entry-key'}))stable(data[t],before.live[t],key);
   const schedules=data['wef-schedules.schedules'],groups=data['live-groups'],classes=data['live-classes'];
   let links=0;
   for(const c of classes){const expected=[];for(const gid of c.fields.groups||[]){const g=groups.find(g=>g.id===gid);assert.ok(g);const matches=schedules.filter(s=>s.fields.status==='active'&&s.fields['show-id']===c.fields['show-id']&&s.fields.date===c.fields.date&&s.fields['class-id']===c.fields['class-id']&&s.fields['stage-key']?.startsWith(g.fields['group-key']+'_'));assert.equal(matches.length,1,'Exactly one active schedule per class/group');expected.push(matches[0].id);}eq(c.fields.schedules,[...new Set(expected)]);links+=expected.length;}
   for(const g of groups){const expected=schedules.filter(s=>s.fields.status==='active'&&s.fields['show-id']===g.fields['show-id']&&s.fields.date===g.fields.date&&s.fields['stage-key']?.startsWith(g.fields['group-key']+'_')&&(g.fields.classes||[]).some(id=>classes.find(c=>c.id===id)?.fields['class-id']===s.fields['class-id']));eq(g.fields.schedules,expected.map(r=>r.id));}
   let binderLinks=0;for(const e of data['live-entries']){const c=classes.find(c=>(e.fields.class||[]).includes(c.id));assert.ok(c);assert.equal(e.fields['entry-key'],[e.fields['show-id'],e.fields.date,e.fields['entry-id'],c.fields['class-id']].join('|'));const expected=data['wef-entries.entries'].filter(r=>r.fields['entry-key']===e.fields['entry-key']);eq(e.fields.entry,expected.map(r=>r.id));binderLinks+=expected.length;const raw=JSON.parse(e.fields['source-json']);assert.equal(Number(raw.live.id),e.fields['trip-id']);assert.equal(Number(raw.identity.entry_id),e.fields['entry-id']);}
   evidence.scheduleLinks=links;evidence.binderLinks=binderLinks;assert.deepEqual(receipt.warnings,[]);
  }
  for(const [file,hash]of Object.entries(JSON.parse(fs.readFileSync(path.join(dir,'protected-hashes.json')))))assert.equal(crypto.createHash('sha256').update(fs.readFileSync(path.join(root,file))).digest('hex'),hash,'Protected file '+file);
  evidence.result='PASS';evidence.data=data;
 }catch(e){evidence.error=e.message;}
 fs.writeFileSync(path.join(dir,kind+'-'+receipt.run_id+'.json'),JSON.stringify(evidence,null,2));
 console.log(JSON.stringify({...evidence,data:undefined,receipt:{run:receipt.run_id,started:receipt.started,completed:receipt.completed,show_source:receipt.show_source,counts:receipt.counts,rows:receipt.rows,errors:receipt.errors}},null,2));
 if(evidence.result!=='PASS')process.exitCode=1;
 return evidence;
}
if(require.main===module)verify(process.argv[2]).catch(e=>{console.error(e.message);process.exitCode=1;});
module.exports={verify};
