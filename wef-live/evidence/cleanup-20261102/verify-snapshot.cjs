'use strict';
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict'),crypto=require('node:crypto'),zlib=require('node:zlib');
const label=process.argv[2];if(!/^[a-z0-9-]+$/.test(label||''))throw Error('Need snapshot label');
const snapshot=JSON.parse(fs.readFileSync(path.join(__dirname,label+'.json'),'utf8'));
const before=JSON.parse(fs.readFileSync(path.join(__dirname,'before.json'),'utf8'));
const run=snapshot.receipts['cleanup-last-run.json'];assert(run,'No completed cleanup receipt');assert.equal(run.result,'Success');assert(!snapshot.receipts['cleanup-stop.json'],'Cleanup stop remains');
const verified=[];
for(const a of run.actions){
 assert(['Success','Skipped'].includes(a.result));const catalog=snapshot.tables['cleanup-logs'].filter(r=>r.fields['cleanup-id']===a.id);assert.equal(catalog.length,1);
 const c=catalog[0].fields;assert.equal(c.result,a.result);assert.equal(c['records-deleted'],a.deleted||0);assert.equal(c['records-archived'],a.rows.length);
 if(a.rows.length){
  const bytes=zlib.gunzipSync(fs.readFileSync(a.archive.file));assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),a.archive.sha256);
  const records=JSON.parse(bytes);assert.deepEqual(records,a.rows);assert.equal(c['archive-file'],a.archive.file);assert.equal(c['archive-sha256'],a.archive.sha256);
  const remaining=new Set(snapshot.tables[a.table].map(r=>r.id));assert(records.every(r=>!remaining.has(r.id)),'Archived old record remains');
  for(const r of records){const original=before.tables[a.table].find(x=>x.id===r.id);if(original)assert.deepEqual(r.fields,original.fields,'Original fields/native links changed before archive');}
 }
 verified.push({table:a.table,trigger:a.trigger,archived:a.rows.length,deleted:a.deleted||0,file:a.archive?.file,sha256:a.archive?.sha256});
}
const priorReceiptFiles=fs.readdirSync(__dirname).filter(n=>/^normal-[0-9]+\.json$/.test(n)&&n!==label+'.json');
// A repeat run must preserve the previous immutable action logs and not create duplicate cleanup actions.
if(!run.new_show){assert.equal(run.actions.filter(a=>a.trigger==='new-show').length,0);}
const terminal=snapshot.tables.heartbeat.filter(r=>Object.values(snapshot.receipts).some(p=>p&&p.run_id===r.fields['run-id']&&p.lane));
assert.equal(terminal.length,3);assert(terminal.every(r=>r.fields.completed&&r.fields.result!=='Running'));
const lanes=['groups','rings','classes'].map(n=>snapshot.receipts['lane-'+n+'-last-run.json']);assert(lanes.every(r=>r?.ok&&r.completed));
const result={status:'PASS',snapshot:label,at:snapshot.at,cleanup_run_id:run.run_id,new_show:run.new_show,show:run.show,
 started:run.started,completed:run.completed,actions:verified,total_archived:verified.reduce((n,a)=>n+a.archived,0),total_deleted:verified.reduce((n,a)=>n+a.deleted,0),
 lane_runs:lanes.map(r=>({lane:r.lane,run_id:r.run_id,started:r.started,completed:r.completed,show:r.scope?.show,date:r.scope?.date,issues:r.issues?.length,stage_errors:r.stage_errors?.length})),
 quick_cleanup_production_proof:verified.some(a=>a.trigger==='quick-cleanup')?'Observed':'Unverified; threshold was not reached',
 limits:['No forced show/day/source changes','Native reciprocal links to deleted old records disappear; original values are archived','Author review, not independent review']};
const file=path.join(__dirname,label+'-verified.json');if(fs.existsSync(file))throw Error('Preserve verified evidence');fs.writeFileSync(file,JSON.stringify(result,null,2));console.log(JSON.stringify(result));
