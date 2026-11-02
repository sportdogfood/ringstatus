from pathlib import Path
import shutil, json, datetime

root=Path(__file__).resolve().parent.parent
dest=root/'evidence'/'delete-only-20261102'
dest.mkdir(exist_ok=False)
for name in ('baseline','candidate'):
    (dest/name).mkdir()
    for p in root.glob('*.js'): shutil.copy2(p,dest/name/p.name)
prior=root/'evidence'/'agreed-rules-20261102'
for name in ('live-cadence.js','lanes.js','cleanup.js'): shutil.copy2(prior/'candidate'/name,dest/'candidate'/name)
for p in prior.glob('*.test.js'): shutil.copy2(p,dest/p.name)
for name in ('task-before.xml','task-prepared.xml'): shutil.copy2(prior/name,dest/name)
p=dest/'candidate'/'cleanup.js'
s=p.read_text()
s=s.replace("clock=()=>new Date().toISOString(),archive,", "clock=()=>new Date().toISOString(),")
s=s.replace(" archive||=((kind,rows)=>require('./archive').archiveFile(path.join(runtime,'history'),kind,rows));\n",'')
s=s.replace("'records-archived':action.archive?action.rows.length:0", "'records-archived':0")
s=s.replace("  if(action.archive){fields['archive-file']=action.archive.file;fields['archive-sha256']=action.archive.sha256;}\n",'')
s=s.replace("'archive-file':action.archive?.file||''", "'archive-file':''")
s=s.replace("  // Capture every original native-link snapshot before deleting any linked row.", "  // Log the planned exact targets before deletion; cleanup is delete-only.")
s=s.replace("   if(a.rows.length){a.archive=await archive('cleanup-'+a.trigger+'-'+a.table,a.rows);if(JSON.stringify(require('./log-retention').verified(a.archive))!==JSON.stringify(a.rows))throw Error('Cleanup archive readback mismatch');}\n",'')
s=s.replace('protectedRuns:protectedIds},a.archive,','protectedRuns:protectedIds},')
p.write_text(s)
p=dest/'candidate'/'airtable.js'
s=p.read_text().replace('async function retireCleanup(table,rows,policy,receipt,onDeleted=()=>{})','async function retireCleanup(table,rows,policy,onDeleted=()=>{})')
s=s.replace("const cleanup=require('./cleanup'),util=require('node:util'),fs=require('node:fs'),path=require('node:path');", "const cleanup=require('./cleanup'),util=require('node:util');")
s=s.replace("    if(!receipt?.file||!path.resolve(receipt.file).startsWith(path.resolve(settings.RUNTIME,'history')+path.sep))throw Error('Cleanup archive outside history');\n",'')
s=s.replace("    if(JSON.stringify(require('./log-retention').verified(receipt))!==JSON.stringify(rows))throw Error('Cleanup archive rows mismatch');\n",'')
p.write_text(s)
# Adapt the existing offline fixtures to the owner's delete-only contract.
p=dest/'cleanup.test.js';s=p.read_text()
s=s.replace('policy,receipt,onDeleted','policy,onDeleted')
s=s.replace("  assert.equal(require('./candidate/log-retention').verified(receipt).length,rows.length);\n  assert(data['cleanup-logs'].some(r=>r.fields['archive-file']===receipt.file));", "  assert(data['cleanup-logs'].some(r=>r.fields.table===table&&r.fields.result==='Running'));\n  assert(!fs.existsSync(path.join(runtime,'history')));")
s=s.replace("new show archives all snapshots before deletion, preserves current/future records and catalogs", "new show deletes only old-show rows without archives, preserves current/future rows and catalogs")
s=s.replace(" const lastArchive=a.events.map((e,i)=>e.startsWith('archive:')?i:-1).reduce((a,b)=>Math.max(a,b));assert(lastArchive<a.events.findIndex(e=>e.startsWith('delete:')));", " assert(!a.events.some(e=>e.startsWith('archive:')));assert(!fs.existsSync(path.join(a.options.runtime,'history')));")
s=s.replace(" assert.deepEqual(require('./candidate/log-retention').verified(r.actions.find(a=>a.table==='live-classes').archive)[0].fields['live-entries'],[old.id]);", " assert(r.actions.every(a=>!a.archive));assert(a.data['cleanup-logs'].every(r=>r.fields['records-archived']===0&&!r.fields['archive-file']));")
s=s.replace('quick cleanup is recurring, archives/logs 500', 'quick cleanup is recurring, deletes/logs 500 without archives')
start=s.index("test('archive failure causes")
end=s.index("test('catalog logging failure",start)
s=s[:start]+"""test('an injected archive function is never called by delete-only cleanup',async t=>{
 const a=setup(t,['live-groups']);a.row('live-groups',{'show-id':76});a.options.archive=async()=>{throw Error('archive forbidden');};
 const r=await cleanup(a.options);assert.equal(r.result,'Success');assert.equal(a.data['live-groups'].length,0);assert(!fs.existsSync(path.join(a.options.runtime,'history')));
});

"""+s[end:]
start=s.index("test('canonical identity change after archive")
end=s.index("test('old active logs",start)
s=s[:start]+"""test('canonical identity change after action logging stops deletion',async t=>{
 const a=setup(t,['live-groups']);a.row('live-groups',{'show-id':76});const upsert=a.options.api.upsert;
 a.options.api.upsert=async(...args)=>{const x=await upsert(...args);if(args[0]==='cleanup-logs')a.data.shows[0].fields.show_id=78;return x;};
 const r=await cleanup(a.options);assert.equal(r.result,'Error');assert.equal(a.data['live-groups'].length,1);
});

"""+s[end:]
p.write_text(s)
p=dest/'day-trigger.test.js';s=p.read_text().replace('policy,receipt,onDeleted','policy,onDeleted').replace("assert.deepEqual(require('./candidate/log-retention').verified(receipt),chosen);", "assert(!fs.existsSync(path.join(runtime,'history')));")
p.write_text(s)
p=dest/'adapter.test.js';s=p.read_text()
start=s.index(" const root=path.join(settings.RUNTIME")
end=s.index(' let current=',start)
s=s[:start]+" const rows=[{id:'recOld',createdTime:'2026-11-01T00:00:00Z',fields}];\n"+s[end:]
s=s.replace('api,rows,receipt,policy','api,rows,policy').replace(',a.receipt','')
s=s.replace('accepts archived reciprocal-link removals', 'accepts expected reciprocal-link removals after deletion')
s=s.replace('stops on default-show change or archive tampering', 'stops on default-show change without needing an archive')
s=s.replace(" await assert.rejects(a.api.retireCleanup('live-groups',a.rows,a.policy,{...a.receipt,sha256:'bad'}),/hash mismatch/);assert.equal(a.deletes(),0);", " assert.equal(a.deletes(),0);")
p.write_text(s)
(dest/'TASK.md').write_text('''# Delete-only correction — November 2, 2026

Owner explicitly approved correction and redeployment in the current chat. Supersedes prior archive-before-delete for cleanup. Delete only: no new cleanup archive files. Existing historical archives/catalogs remain untouched.

New show: delete nonmatching-show records in checked owned tables. New Eastern day only: inspect heartbeat/run-logs/errs-log and delete oldest 500 completed eligible records when total >=1000. Same show/day: no cleanup inventory. Preserve active/current rows and exact pre-delete/readback checks. No repair, manual collector or observer.

Also deploy already-approved cadence: hourly actual status checks/logs 24h, three-minute details only when live=true and canonical priority-0 equals today, 05:00–19:00 Eastern. Existing hidden task acts as quiet local three-minute dispatcher. Stage8 remains held; old24h retention stays disabled. Other collectors untouched.

Delta: cleanup.js, airtable.js retireCleanup only, lanes.js, new live-cadence.js, existing task duration. No schema/dependency upgrade. Two preexisting cleanup-reporting P2 findings remain separate; no opportunistic fixes. Rollback baseline retained; never overwrite intervening edits or retry failed business execution without authority.
''')
print(dest)
