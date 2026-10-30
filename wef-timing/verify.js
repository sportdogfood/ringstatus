"use strict";
// Independent read-only acceptance: never invokes runner/entry or source acquisition.
const fs=require('node:fs'),path=require('node:path');
const {api,ENGINE,TABLES}=require('./api');
const {hash}=require('./pace');
async function verify(){
  const receipt=JSON.parse(fs.readFileSync(path.join(__dirname,'runtime/last-run.json'),'utf8'));
  const a=api({token:process.env.WEF_TIMING_AIRTABLE_TOKEN,deadline:Date.now()+45000});
  const answers=await a.list(ENGINE,TABLES.answers),checkpoints=await a.list(ENGINE,TABLES.checkpoint);
  const keys=answers.map(r=>r.fields['answer-key']);if(new Set(keys).size!==keys.length)throw Error('Duplicate snapshot keys');
  const current=checkpoints.find(r=>r.fields['checkpoint-key']==='ring-pace-runner');
  if(!receipt.ok||receipt.errors.length)throw Error('Normal run did not report business success');
  if(current?.fields['run-id']!==receipt.run_id)throw Error('Receipt/checkpoint run mismatch; observe next completed normal run');
  const created=answers.filter(r=>r.fields['answer-key'].includes('|'+receipt.run_id+'|'));
  if(created.length!==receipt.counts.written)throw Error('Written count/readback mismatch');
  for(const row of created){const f=row.fields,p=JSON.parse(f['payload-json']),refs=JSON.parse(f['input-refs-json']);
    const observedAt=p.interval?p.interval.to:refs.find(x=>x?.table==='show-collection')?.observed_at;
    // Hash includes schedule evidence time even for measured intervals.
    const scheduleAt=refs.find(x=>x?.table==='show-collection')?.observed_at;
    if(hash({policy:'ring-pace-v1',payload:p,inputRefs:refs,observedAt:scheduleAt})!==f['content-hash'])throw Error('Snapshot hash mismatch');
    if(p.selected_pace_minutes_per_trip!==null&&Math.round(p.selected_pace_minutes_per_trip*10000)/10000!==f['pace-minutes-per-trip'])throw Error('Pace field/payload mismatch');
    if(f['observed-at']!==(observedAt||''))throw Error('Source observation mismatch');
  }
  const latest=new Map();for(const r of answers){const k=r.fields['scope-key'];if(!latest.has(k)||r.fields['evaluated-at']>latest.get(k).fields['evaluated-at'])latest.set(k,r);}
  const numeric=created.filter(r=>r.fields['pace-minutes-per-trip']>0);
  const result={verified_at:new Date().toISOString(),run:receipt.run_id,started:receipt.started,completed:receipt.completed,ok:true,
    counts:receipt.counts,total_snapshots:answers.length,covered_group_scopes:latest.size,expected_group_scopes:JSON.parse(current.fields['state-json']).scope_count,
    checkpoint_outcome:current.fields['last-outcome'],checkpoint_duration_ms:current.fields['duration-ms'],duplicates:0,verified_hashes:created.length,
    clock_verified:receipt.clock_verified,numeric_created:numeric.length,observed_interval_created:created.filter(r=>r.fields['pace-method']==='observed_interval').length,
    example:numeric.length?Object.fromEntries(Object.entries(numeric[0].fields).filter(([k])=>['answer-key','show-id','date','ring-number','group-id','pace-method','pace-minutes-per-trip','observed-at'].includes(k))):null};
  fs.mkdirSync(path.join(__dirname,'evidence'),{recursive:true});
  fs.writeFileSync(path.join(__dirname,'evidence','normal-'+receipt.run_id+'.json'),JSON.stringify(result,null,2));
  fs.writeFileSync(path.join(__dirname,'evidence','receipt-'+receipt.run_id+'.json'),JSON.stringify(receipt,null,2));
  console.log(JSON.stringify(result));
}
verify().catch(e=>{console.error(e.message);process.exitCode=1;});
