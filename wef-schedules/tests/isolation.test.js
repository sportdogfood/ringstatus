"use strict";
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),{execFileSync}=require('node:child_process');
const root=path.resolve(__dirname,'..');
test('private dependency graph has no imports outside package',()=>{
 const seen=new Set();
 const manifest=JSON.parse(fs.readFileSync(path.join(root,'package.json'),'utf8'));
 const lock=JSON.parse(fs.readFileSync(path.join(root,'package-lock.json'),'utf8'));
 assert.deepEqual(manifest.dependencies,{playwright:'1.62.1'});
 function external(target){
  if(require('node:module').isBuiltin(target))return;
  assert.ok(Object.hasOwn(manifest.dependencies,target),'Undeclared dependency '+target);
  const resolved=require.resolve(target,{paths:[root]});
  const privateRoot=fs.realpathSync(path.join(root,'node_modules',target));
  assert.ok(privateRoot.startsWith(fs.realpathSync(root)+path.sep),'Dependency outside private package');
  assert.ok(fs.realpathSync(resolved).startsWith(privateRoot+path.sep),'Dependency resolved outside private install');
  const installed=JSON.parse(fs.readFileSync(path.join(privateRoot,'package.json'),'utf8'));
  assert.equal(installed.version,manifest.dependencies[target]);
  assert.equal(lock.packages['node_modules/'+target].version,installed.version);
  assert.ok(lock.packages['node_modules/'+target].integrity);
 }
 assert.throws(()=>external('unapproved-dependency'),/Undeclared dependency/);
 function visit(file){if(seen.has(file))return;seen.add(file);const text=fs.readFileSync(file,'utf8');
 for(const match of text.matchAll(/require\(['"]([^'"]+)['"]\)/g)){
 const target=match[1];if(!target.startsWith('.')){external(target);continue;}
 const resolved=require.resolve(path.resolve(path.dirname(file),target));assert.ok(resolved.startsWith(root+path.sep),resolved);visit(resolved);
 }}visit(path.join(root,'collect.js'));assert.ok(seen.has(path.join(root,'normalizer.js')));
});
test('legacy settings cannot change private base paths or credentials',()=>{
 const script=`const s=require(${JSON.stringify(path.join(root,'settings.js'))});console.log(JSON.stringify(s));`;
 const env={...process.env,AIRTABLE_TOKEN:'legacy-secret',AIRTABLE_BASE_ID:'appLegacy',SCHEDULE_STAGING_BASE_ID:'appWrong',SCHEDULE_FOCUS_FILE:'C:/legacy/focus',SGL_FETCH_SCRIPT:'C:/legacy/fetch.ps1',SGL_FETCH_SESSION_JSON:'C:/legacy/session.json',MANUAL_SCHEDULE_FALLBACK_DIRS:'C:/legacy',WEF_SCHEDULES_AIRTABLE_TOKEN:'private-test-token'};
 const s=JSON.parse(execFileSync(process.execPath,['-e',script],{env,encoding:'utf8'}));
 assert.equal(s.AIRTABLE_TOKEN,'private-test-token');assert.equal(s.SCHEDULE_STAGING_BASE_ID,'appUq5Qz9kARqFija');assert.equal(s.AIRTABLE_BASE_ID,'');assert.equal(s.MANUAL_SCHEDULE_FALLBACK_DIRS,undefined);
 for(const key of ['SCHEDULE_FOCUS_FILE','SCHEDULE_COLLECTION_LOCK','SGL_PAYLOAD_ROOT','EARLY_SCHEDULE_PAYLOAD_DIR','MANUAL_SCHEDULE_PAYLOAD_DIR','MANUAL_SCHEDULE_HTML_DIR','SGL_FETCH_SCRIPT','SGL_FETCH_SESSION_JSON'])assert.ok(s[key].startsWith(root+path.sep),key);
 assert.ok(!JSON.stringify(s).includes('legacy-secret'));
});
test('private fallback and PowerShell paths stay inside package',()=>{
 const core=require('../collector-core');for(const f of [core.scheduleFallbackDirs,core.manualScheduleFallbackDirs,core.scheduleHtmlFallbackDirs])for(const dir of f())assert.ok(path.resolve(dir).startsWith(root+path.sep),dir);
 const ps=fs.readFileSync(path.join(root,'sgl_fetch.ps1'),'utf8');assert.ok(!ps.includes('actions-runner'));assert.ok(!ps.includes("'User', 'Machine'"));
});
test('copied package runs offline tests without its original checkout',t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'wef-private-proof-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));
 fs.cpSync(root,dir,{recursive:true,filter:p=>!p.includes(path.sep+'runtime')});
 const childEnv={...process.env};delete childEnv.NODE_TEST_CONTEXT;
 const result=execFileSync(process.execPath,['--test','--test-reporter=tap','tests/schedules_standalone.test.js','tests/schedules_staging.test.js'],{cwd:dir,env:childEnv,encoding:'utf8'});assert.ok(Number(result.match(/# tests (\d+)/)[1])>=23); assert.match(result,/# fail 0/);
});

test('copied PowerShell request retains all five parameters',()=>{
 const file=path.join(root,'sgl_fetch.ps1');assert.ok(!fs.readFileSync(file).includes(Buffer.from('\r\r\n')));
 if(process.platform!=='win32') return;
 const command="$t=$null;$e=$null;$a=[System.Management.Automation.Language.Parser]::ParseFile('"+file.replaceAll("'","''")+"',[ref]$t,[ref]$e); if($e.Count){throw 'Parser errors'}; $c=$a.FindAll({param($n) $n -is [System.Management.Automation.Language.CommandAst] -and $n.GetCommandName() -eq 'Invoke-WebRequest'},$true); @($c.CommandElements | Where-Object {$_ -is [System.Management.Automation.Language.CommandParameterAst]} | ForEach-Object ParameterName) | ConvertTo-Json -Compress";
 const names=JSON.parse(execFileSync('powershell.exe',['-NoProfile','-Command',command],{encoding:'utf8'}));
 assert.deepEqual(names,['UseBasicParsing','Uri','WebSession','Headers','TimeoutSec']);
});
