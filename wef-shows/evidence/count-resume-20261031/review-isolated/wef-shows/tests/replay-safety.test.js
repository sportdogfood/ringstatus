const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),zlib=require('node:zlib');
const {bridge,reader}=require('../day-source'),{archivedRow}=require('../day-archive'),{hash}=require('../show-days');
const fixture=require('./fixtures/day-summary.json');
const replay=input=>bridge(input,{root:process.env.WEF_DAY_TEST_SOURCE_ROOT});
test('replay still rejects a changed hash after valid input',async()=>{
 assert.deepEqual(await replay(fixture),fixture.expected);
 const changed=structuredClone(fixture);Object.values(changed.collections)[0]['retained-sha256']='0'.repeat(64);
 await assert.rejects(replay(changed));
 assert.deepEqual(await replay(fixture),fixture.expected);
});
test('replay rejects missing retained evidence and changed provenance',async()=>{
 for(const mutate of [v=>delete v.collections[Object.keys(v.collections)[0]],v=>Object.values(v.collections)[0]['_record-id']='wrong']){
  const changed=structuredClone(fixture);mutate(changed);await assert.rejects(replay(changed));
 }
});
test('archive reader validates bytes, payload, show and path before replay',async t=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'shows-archive-test-'));t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
 const part={'retained-json':'{"test":true}'};part['retained-sha256']=hash(part['retained-json']);
 const bytes=zlib.gzipSync(JSON.stringify(part)),ref={key:'frozen',sha256:part['retained-sha256'],scope:'dated-inventory-page',archive:{format:'wef-data-evidence-v1',path:'wef-data/shows/15/200000076/retained-evidence/parts/'+part['retained-sha256']+'.json.gz',bytes:bytes.length,sha256:hash(bytes)}};
 const file=path.join(root,ref.archive.path);fs.mkdirSync(path.dirname(file),{recursive:true});fs.writeFileSync(file,bytes);
 assert.deepEqual(archivedRow(root,ref,'15|200000076'),{...part,'collection-key':ref.key,scope:ref.scope,status:'Verified retained observation',_archive:ref.archive});
 assert.throws(()=>archivedRow(root,ref,'15|999'),/scope/);
 for(const change of [r=>r.archive.sha256='0'.repeat(64),r=>r.archive.bytes++,r=>r.archive.path='../outside',r=>r['record-id']='recWrong']){
  const bad=structuredClone(ref);change(bad);assert.throws(()=>archivedRow(root,bad,'15|200000076'));
 }
 const state={sequence:1,phase:'Complete','show-key':'15|200000076',authority:['15|200000076','2026-10-29','2026-11-01','2026-10-30'],'authority-before':['15|200000076','2026-10-29','2026-11-01','2026-10-30'],observations:[ref]};
 const row={fields:{'run-key':'run',status:'Verified completed revision','state-json':JSON.stringify(state)}};let replays=0;
 const read=reader({list:async table=>{assert.equal(table,'runs','archive references must not be fetched as Airtable rows');return[row];}},{runtime:root,archiveRoot:root,replay:async input=>{replays++;assert.deepEqual(input.collections.frozen,archivedRow(root,ref,'15|200000076'));return[];}});
 await read({fields:{'show-key':'15|200000076',customer_id:15,show_id:200000076,start_date:'2026-10-29',end_date:'2026-11-01',selected_date:'2026-10-30'}});assert.equal(replays,1);
 fs.writeFileSync(file,Buffer.from('tampered'));assert.throws(()=>archivedRow(root,ref,'15|200000076'),/hash\/length/);
});
