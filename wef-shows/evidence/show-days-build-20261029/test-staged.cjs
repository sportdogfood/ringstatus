// Offline harness: resolves proven dependencies from their real location, but
// compiles only the staged collector. Never invokes a scheduled entry point.
const fs=require('node:fs'),path=require('node:path'),Module=require('node:module');
const root=path.resolve(__dirname,'../..'),stage=path.join(__dirname,'staged');
const filename=path.join(root,'collector.js'),m=new Module(filename,module);
m.filename=filename;m.paths=Module._nodeModulePaths(root);
const realRequire=Module.createRequire(filename);
m.require=id=>id==='./show-days'?require(path.join(stage,'show-days')):realRequire(id);
m._compile(fs.readFileSync(path.join(stage,'collector.js'),'utf8'),filename);
require.cache[filename]=m;
let source=fs.readFileSync(path.join(root,'tests/shows.test.js'),'utf8');
source=source.replace('const options={api,',"const options={publishDays:async()=>({ok:true,days:[],errors:[]}),readSummaries:async()=>({days:[]}),dayStopPath:path.join(dir,'day-stop.json'),api,");
source+=`
test('day publication failure preserves discovery and stops only day retries',async t=>{
 const f=fixture(t);let attempted=0;
 const options={...f.options,publishDays:async()=>{attempted++;throw Error('day failure');}};
 const first=await runShows(options);assert.equal(first.discovery_ok,true);assert.equal(first.ok,false);assert.equal(f.tables.shows.length,1);assert.equal(f.tables.heartbeat[0].fields.result,'Partial');
 const second=await runShows(options);assert.equal(second.discovery_ok,true);assert.equal(second.ok,false);assert.equal(attempted,1);assert.equal(f.requests.length,4);assert.ok(fs.existsSync(f.options.dayStopPath));
});`;
const testfile=path.join(root,'tests/shows.test.js'),tm=new Module(testfile,module);tm.filename=testfile;tm.paths=Module._nodeModulePaths(path.dirname(testfile));tm._compile(source,testfile);
