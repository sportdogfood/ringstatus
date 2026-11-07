const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),vm=require('node:vm');
test('alert-owned local request, natural completion receipts, once per workflow, pending stays',async t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'v2-alert-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
 const runtime=path.join(dir,'runtime','cleanup');fs.mkdirSync(runtime,{recursive:true});
 for(const workflow of ['wef-data','wef-data-linking'])fs.mkdirSync(path.join(dir,'runtime','logging',workflow,'run-logs'),{recursive:true});
 const module={exports:{}};vm.runInNewContext(fs.readFileSync(path.join(__dirname,'candidate/cleanup-local-logs.js'),'utf8'),{module,require,process,setTimeout,__dirname:dir});const helper=module.exports;
 const root=path.join(dir,'runtime','logging','wef-data'),pending=path.join(root,'run-logs','pending.json'),row={id:'pending',workflow:'wef-data',base:'appFixture',started:'2026-11-07T00:00:00Z',status:'Failed',completed:'2026-11-07T00:01:00Z'};fs.writeFileSync(pending,JSON.stringify(row));
 assert.equal(helper.prune(root,{workflow:'wef-data',base:'appFixture'}).runs,0);assert(!fs.existsSync(path.join(root,'cleanup-receipt.json')));
 const req=helper.request(runtime,{run_id:77,event_id:'calendar-day|2026-11-07'},['run-logs','err-logs']);
 const wait=helper.wait(req);helper.prune(root,{workflow:'wef-data',base:'appFixture'});helper.prune(path.join(dir,'runtime','logging','wef-data-linking'),{workflow:'wef-data-linking',base:'appFixture'});
 assert.equal((await wait).length,2);assert.equal(fs.readFileSync(pending,'utf8'),JSON.stringify(row));
 const receipt=fs.readFileSync(path.join(root,'cleanup-receipt.json'),'utf8');helper.prune(root,{workflow:'wef-data',base:'appFixture'});assert.equal(fs.readFileSync(path.join(root,'cleanup-receipt.json'),'utf8'),receipt);
 const failed=helper.request(runtime,{run_id:78,event_id:'next'},['run-logs','err-logs']);
 fs.unlinkSync(path.join(root,'cleanup-receipt.json'));fs.mkdirSync(path.join(root,'cleanup-receipt.json'));
 assert.throws(()=>helper.prune(root,{workflow:'wef-data',base:'appFixture'}));
 helper.prune(path.join(dir,'runtime','logging','wef-data-linking'),{workflow:'wef-data-linking',base:'appFixture'});
 assert.equal((await helper.wait(failed)).find(r=>r.workflow==='wef-data').result,'Error');
});
