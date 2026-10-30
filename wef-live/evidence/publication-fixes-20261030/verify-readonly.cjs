const fs=require('fs'),z=require('zlib'),crypto=require('crypto'),assert=require('assert/strict'),path=require('path');
const root=__dirname,base=path.resolve(root,'../..'),read=p=>JSON.parse(fs.readFileSync(p,'utf8').replace(/^\uFEFF/,''));
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
const archive=a=>{const b=z.gunzipSync(fs.readFileSync(a.file));assert.equal(hash(b),a.sha256);return readBuffer(b);};
const readBuffer=b=>JSON.parse(b.toString());
const {FIELD,TARGETS}=require('../../publication-api');
const token=require('../../settings').AIRTABLE_TOKEN;
const api=require('../../airtable').airtable();
const keys=['customer-id','customer_id','show-id','show_id','ring-id','ring_id','ring-number','ring_number','group-id','class-id','trip-id','entry-id','horse-id','rider-id','trainer-id','ring-key','stage-key','group-key','class-key','schedule-key','trip-key','date','ring_date','ring','source-uuid'];
function protectedValues(f,table,lane){const o={};for(const [k,v]of Object.entries(f))if(k!==FIELD&&(keys.includes(k)||Array.isArray(v)))o[k]=v;if(table==='schedules'&&lane==='schedules')o.status=f.status;if(table==='trips'){o.status=f['record-status'];const s=JSON.parse(f['source-json']);o.source=Object.fromEntries(['trip_id','entry_id','horse_id','rider_id','trainer_id','entryxclasses_uuid'].map(k=>[k,s[k]??null]));}return o;}
async function getRows(lane,table,ids){const t=TARGETS[lane],out=[];for(let i=0;i<ids.length;i+=70){const q=new URLSearchParams({filterByFormula:'OR('+ids.slice(i,i+70).map(id=>`RECORD_ID()="${id}"`).join(',')+')',pageSize:'100'});const res=await fetch(`https://api.airtable.com/v0/${t.base}/${t.tables[table]}?${q}`,{headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(15000)});assert(res.ok,`GET ${lane}/${table} ${res.status}`);const r=await res.json();assert(!r.offset);out.push(...r.records);}return out;}
(async()=>{
 const deploy=read(root+'/deployed.json');for(const f of deploy.files)assert.equal(hash(fs.readFileSync(f.path)),f.sha256);
 const report={verifiedAt:new Date().toISOString(),mode:'Saved normal receipts and bounded Airtable GET only',runs:[]};
 for(const id of process.argv.slice(2)){
  const r=read(base+'/runtime/'+id+'.json');assert(r.ok);assert(r.counts.entries>0&&r.counts.classes>0&&r.counts.rings>0);assert(Date.parse(r.started)>Date.parse(deploy.at));assert.equal(r.logging_errors.length,0);
  const artifacts=r.archives.filter(a=>a.kind.startsWith('publication-')).map(a=>({receipt:a,body:archive(a)}));
  const input=artifacts.find(a=>a.receipt.kind==='publication-observations');assert(input);const source=input.body;
  for(const a of source.sourceArchives)archive(a);
  const run={id,started:r.started,completed:r.completed,scope:r.scope,counts:r.counts,lanes:r.publication.lanes,verified:[],uncertain:[],metadataChanges:[],flags:[],logs:{},freshnessMs:[],reconciled:[]};
  const destinations={};
  for(const lane of Object.keys(TARGETS)){
   const stats=r.publication.lanes[lane];assert(stats.planned>0);assert.equal(stats.planned,stats.patched+stats.unchanged+stats.deferred+stats.uncertain,'Count conservation');
   const intents=artifacts.filter(a=>a.receipt.kind===`publication-${lane}-intent`);
   let verified=0;
   for(const a of intents){const intent=a.body,{table,id:recordId,key}=intent.destination;const out=artifacts.filter(x=>x.receipt.kind===`publication-${lane}-outcome`&&x.body.intent.sha256===a.receipt.sha256);const outcome=out.find(x=>x.body.outcome==='Verified');
    if(!outcome){assert(out.some(x=>x.body.outcome==='Unknown'));run.uncertain.push({lane,table,id:recordId,intent:a.receipt});continue;}
    const checked=outcome.body.readback;assert.equal(checked.id,recordId);assert.equal(checked.fields[FIELD],intent.after);assert.deepEqual(protectedValues(checked.fields,table,lane),protectedValues(intent.before.fields,table,lane));
    const previous=intent.before.fields[FIELD]?JSON.parse(intent.before.fields[FIELD]).components:{};const envelope=JSON.parse(intent.after);assert.equal(envelope.destination.key,key);
    for(const [level,c]of Object.entries(envelope.components))if(c.revision!==previous[level]?.revision){assert.equal(c.scope.show,r.scope.show);assert.equal(c.scope.date,r.scope.date);assert.equal(c.scope.customer,r.scope.customer);const age=Date.parse(intent.at)-Date.parse(c.observedAt);assert(age>=-60000&&age<=360000);run.freshnessMs.push(age);assert.equal(c.observedAt,intent.source[level].observedAt);if(level==='group'&&lane==='data'){const g=source.groups.find(g=>g['group-key']===c.sourceKey);assert(g);const raw=JSON.parse(g['source-json']);assert.equal(c.values.hasJson,Object.hasOwn(raw,'has_JSON')?raw.has_JSON:null);run.flags.push({id:recordId,value:c.values.hasJson});}}
    const changes=Object.keys({...intent.before.fields,...checked.fields}).filter(k=>k!==FIELD&&JSON.stringify(intent.before.fields[k])!==JSON.stringify(checked.fields[k]));if(changes.length)run.metadataChanges.push({lane,table,id:recordId,fields:changes});
    const group=lane+'|'+table;destinations[group]??=new Map();destinations[group].set(recordId,{key,fields:checked.fields});
    run.verified.push({lane,table,id:recordId,intent:a.receipt,outcome:outcome.receipt,changes});verified++;
   }
   assert.equal(verified,stats.patched,'Verified patch tally');
   run.reconciled.push(...artifacts.filter(a=>a.receipt.kind===`publication-${lane}-reconcile`).map(a=>({lane,outcome:a.body.outcome,intent:a.body.intent,receipt:a.receipt})));
  }
  for(const [group,records]of Object.entries(destinations)){const [lane,table]=group.split('|'),rows=await getRows(lane,table,[...records.keys()]);assert.equal(rows.length,records.size,'Published destination remains');for(const row of rows){const prior=records.get(row.id),envelope=JSON.parse(row.fields[FIELD]);assert.equal(envelope.destination.key,prior.key);assert.deepEqual(protectedValues(row.fields,table,lane),protectedValues(prior.fields,table,lane),'Current identity/native-link readback');}}
  for(const table of ['heartbeat','run-logs']){const q='?'+new URLSearchParams({filterByFormula:`{run-id}="${id}"`});const rows=(await api.call('appgzosUcVqA4vZdf',table,'GET',undefined,q)).records;assert.equal(rows.length,1);assert(rows[0].fields.completed);assert.match(rows[0].fields.summary,/uncertain/);run.logs[table]=rows[0];}
  const q='?'+new URLSearchParams({filterByFormula:`{run-id}="${id}"`});run.errorRows=(await api.call('appgzosUcVqA4vZdf','errs-log','GET',undefined,q)).records.length;
  report.runs.push(run);console.log(JSON.stringify({id,counts:run.counts,lanes:run.lanes,verified:run.verified.length,flags:run.flags,metadataChanges:run.metadataChanges.length,reconciled:run.reconciled.map(x=>({lane:x.lane,outcome:x.outcome})),logs:Object.fromEntries(Object.entries(run.logs).map(([k,v])=>[k,v.fields.result]))}));
 }
 fs.writeFileSync(root+'/normal-run-verification.json',JSON.stringify(report,null,2));
})().catch(e=>{console.error(e.stack);process.exitCode=1});
