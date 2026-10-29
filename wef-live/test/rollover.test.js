"use strict";
const test=require('node:test'),assert=require('node:assert/strict');
const {canRetire}=require('../rollover');
test('retirement scope permits old dates and same-date other shows only',()=>{
  const scope={show:75,date:'2026-10-23'};
  assert(canRetire({'show-id':74,date:'2026-10-23'},scope));
  assert(canRetire({'show-id':75,date:'2026-10-22'},scope));
  for(const f of [{'show-id':75,date:'2026-10-23'},{'show-id':74,date:'2026-10-24'}, {date:'2026-10-23'},{'show-id':74,date:'bad'}])assert(!canRetire(f,scope));
  assert(!canRetire({'show-id':74,date:'2026-10-23'},'2026-10-23'));
});
test('same-date old-show adapter deletion requires an exact verified archive',async()=>{
  const fs=require('fs'),path=require('path'),z=require('zlib'),crypto=require('crypto');
  const root=path.join(require('../settings').RUNTIME,'history');
  const dir=fs.mkdtempSync(path.join(root,'offline-rollover-'));
  const rows=[{id:'offline-old',fields:{'show-id':74,date:'2026-10-23',entry:['preserved-in-archive']}}];
  const file=path.join(dir,'fixture.json.gz'),bytes=Buffer.from(JSON.stringify(rows));
  const receipt={file,sha256:crypto.createHash('sha256').update(bytes).digest('hex')};
  const calls=[];const api=require('../airtable').airtable({pause:async()=>{},request:async(url,options)=>{
    calls.push(options.method);return {ok:true,json:async()=>({records:[{id:'offline-old',deleted:true}]})};
  }});
  try{
    fs.writeFileSync(file,z.gzipSync(bytes));
    await assert.rejects(()=>api.retire('live-entries',rows,{show:75,date:'2026-10-23'},{...receipt,sha256:'bad'}),/verification/);
    assert.equal(calls.length,0);
    await api.retire('live-entries',rows,{show:75,date:'2026-10-23'},receipt);
    assert.deepEqual(calls,['DELETE']);
  }finally{fs.unlinkSync(file);fs.rmdirSync(dir);}
});
