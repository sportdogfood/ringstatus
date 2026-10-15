import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {readFile} from 'node:fs/promises';
import {recognizeTenantsFixture} from '../test-support/recognize-tenants-fixture.mjs';
import {accessHash} from '../src/lib/rs-inputs-access.js';
import {TABLES} from '../src/lib/rs-tenant-submission-store.js';
import {createPersistence} from '../src/assets/rs-inputs/persistence.js';

const origin='https://ringstatus.com';
const playwrightRequire=createRequire('C:/Users/gombc/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/package.json');
const {chromium}=playwrightRequire('playwright');
const native=await readFile(new URL('../src/assets/rs-recognition/native-client.js',import.meta.url),'utf8');
const adapter=await readFile(new URL('../recognize-agent/extension/adapter.js',import.meta.url),'utf8');
const html=`<!doctype html><style>[aria-hidden=true]{display:none}</style>
<div id="rs-native-recognize" data-rs-native-enabled="true" data-rs-native-click-ownership="adapter">
<div data-rs-native-overlay><div data-rs-native-state="login"><p data-rs-native-feedback></p><input name="identifier" autocomplete="tel"><button data-rs-native-action="phone-login">Continue</button><button data-rs-native-action="login">Phone entry</button><button data-rs-native-action="recovery">SMS option</button></div>
${['recognized','profile','recovery','received','unavailable'].map(s=>'<div data-rs-native-state="'+s+'"><p data-rs-native-feedback></p></div>').join('')}</div></div>
<script type="module">import {mountNativeRecognition} from '/native.js';
const root=document.getElementById('rs-native-recognize');
window.app=mountNativeRecognition({root,baseUrl:'https://ringstatus.com/test/rs-recognition/',silentRecognition:true,launcherPath:'/rs-member',present:({state,busy})=>{root.setAttribute('aria-busy',String(busy));for(const view of root.querySelectorAll('[data-rs-native-state]'))view.setAttribute('aria-hidden',String(view.dataset.rsNativeState!==state));},navigate:path=>{history.pushState(null,'',path);root.setAttribute('aria-busy','false');}});
window.loaded=true;</script>`;

test('access timeout releases busy state and the agent can retry the same recognized identity',{timeout:30000},async()=>{
 const f=await recognizeTenantsFixture(),browser=await chromium.launch({headless:true,channel:'msedge'});
 const context=await browser.newContext(),page=await context.newPage();
 const timeoutHtml=html.replace('root,baseUrl:',`root,timeoutMs:100,fetchImpl:async(url,options)=>{if(new URL(url).pathname.endsWith('/access')&&!window.accessTimedOut){window.accessTimedOut=true;return new Promise((resolve,reject)=>options.signal.addEventListener('abort',()=>reject(new DOMException('Timed out','AbortError')),{once:true}));}return fetch(url,options);},baseUrl:`);
 await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());assert.equal(url.origin,origin);
  if(url.pathname==='/rs-recognize')return route.fulfill({contentType:'text/html',body:timeoutHtml});
  if(url.pathname==='/native.js')return route.fulfill({contentType:'application/javascript',body:native});
  const response=await f.dispatch(new Request(req.url(),{method:req.method(),headers:await req.allHeaders(),...(req.postData()?{body:req.postData()}:{})}));
  return route.fulfill({status:response.status,headers:Object.fromEntries(response.headers),body:await response.text()});
 });
 try{
  await page.goto(origin+'/rs-recognize');await page.waitForFunction(()=>window.app?.snapshot().stage==='entry');
  await page.addScriptTag({content:adapter});await page.evaluate(()=>window.agent=RecognizeTestAdapter(document,location));
  await page.evaluate(()=>agent.perform('submit_credential','6318752160'));
  const state=await page.evaluate(()=>app.snapshot());assert.equal(state.busy,false);assert.ok(state.allowedActions.includes('retry'));
  await page.evaluate(()=>agent.perform('retry'));await page.waitForURL('**/rs-member');
  assert.equal(f.code,null);assert.equal(f.rows.tblfkRSJAEMzuzApR.length,1);
 }finally{await context.close();await browser.close();f.db.close();}
});

for(const credential of ['phone','pin','invite','sms'])for(const mode of ['native','agent-adapter'])for(const mobile of [false,true])test(`${credential} ${mode} ${mobile?'mobile':'desktop'}: same-browser recognition through Tenants and real preference processor`,{timeout:90000},async()=>{
 const f=await recognizeTenantsFixture(),browser=await chromium.launch({headless:true,channel:'msedge'});
 const context=await browser.newContext({viewport:mobile?{width:390,height:844}:{width:1280,height:800},isMobile:mobile});
 f.person.fields.member_pin='0123';
 if(credential==='invite')Object.assign(f.person.fields,{input_invite_hash:await accessHash('0123456'),input_invite_expires_at:new Date(Date.now()+600000).toISOString()});
 const value={phone:'6318752160',pin:'0123',invite:'0123456',sms:'6318752160'}[credential];
 const page=await context.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await context.route('**/*',async route=>{
  const req=route.request(),url=new URL(req.url());
  assert.equal(url.origin,origin,'unexpected browser network');
  if(url.pathname==='/rs-recognize')return route.fulfill({status:200,contentType:'text/html',body:html});
  if(url.pathname==='/native.js')return route.fulfill({status:200,contentType:'application/javascript',body:native});
  if(!url.pathname.startsWith('/test/rs-inputs/'))return route.abort();
  const response=await f.dispatch(new Request(req.url(),{method:req.method(),headers:await req.allHeaders(),...(req.postData()?{body:req.postData()}: {})}));
  return route.fulfill({status:response.status,headers:Object.fromEntries(response.headers),body:await response.text()});
 });
 try{
  await page.goto(origin+'/rs-recognize');await page.waitForFunction(()=>window.loaded&&document.querySelector('[data-rs-native-state="login"]').getAttribute('aria-hidden')==='false');
  if(mode==='agent-adapter'){
   await page.addScriptTag({content:adapter});await page.evaluate(()=>window.agent=RecognizeTestAdapter(document,location));
   if(credential!=='phone')await page.evaluate(kind=>agent.perform('show_'+kind),credential);
   const result=await page.evaluate(({credential,value})=>agent.perform(credential==='sms'?'request_code':'submit_credential',value),{credential,value});assert.equal(result.ok,true);
  }else{if(credential==='sms')await page.locator('[data-rs-native-action="recovery"]').click();await page.locator('[name="identifier"]').fill(value);await page.locator('[data-rs-native-action="phone-login"]').click();}
  if(credential==='sms'){
   await page.waitForFunction(()=>document.querySelector('[name="identifier"]').autocomplete==='one-time-code');
   assert.match(f.code,/^\d{6}$/);
   if(mode==='agent-adapter')await page.evaluate(code=>agent.perform('verify_code',code),f.code);
   else {await page.locator('[name="identifier"]').fill(f.code);await page.locator('[data-rs-native-action="phone-login"]').click();}
  }else assert.equal(f.code,null,'non-SMS recognition must not send');
  await page.waitForURL('**/rs-member');
  const cookies=await context.cookies();assert.ok(cookies.some(c=>c.name==='__Host-rs_recognition_device'&&c.httpOnly&&c.secure));
  const call=async(path,body,headers={})=>page.evaluate(async({path,body,headers})=>{const r=await fetch(path,{method:body?'POST':'GET',headers:{...(body?{'Content-Type':'application/json'}:{}),...headers},...(body?{body:JSON.stringify(body)}:{})});return {status:r.status,body:await r.json()};},{path,body,headers});
  const access=await call('/test/rs-inputs/access');assert.equal(access.body.actor.id,f.actor);
  const state=await call('/test/rs-inputs/state',null,{'X-RS-Tenants':'barn-rider-v1','X-RS-Actor':f.actor});assert.equal(state.status,200,JSON.stringify(state));
  let saved=await call('/test/rs-inputs/subscriptions?view=preferences',null,{'X-RS-Actor-Scope':f.actor});assert.equal(saved.status,200);assert.equal(saved.body.preferences.enabled,false);
  const before=f.rows[TABLES.sub].length;
  const mismatch=await call('/test/rs-inputs/subscriptions',{action:'preferences',requestId:'wrong-account-1',expectedRevision:2,preferences:f.preferences},{'X-RS-Actor-Scope':'different_actor'});assert.equal(mismatch.status,409);assert.equal(mismatch.body.error,'account_changed');assert.equal(f.rows[TABLES.sub].length,before);
  for(const enabled of [true,false]){
   const requestId='fixture-save-'+enabled,preferences={...f.preferences,enabled,variables:{...f.preferences.variables,class_started:{enabled:true,value:''}}};
   const body={action:'preferences',requestId,expectedRevision:saved.body.revision,preferences};
   const pending=await call('/test/rs-inputs/subscriptions',body,{'X-RS-Actor-Scope':f.actor});assert.equal(pending.status,202,JSON.stringify(pending));
   await f.processPreferences(f.rows[TABLES.sub].at(-1));
   const result=await call('/test/rs-inputs/subscriptions?view=submission&request_id='+requestId,null,{'X-RS-Actor-Scope':f.actor});assert.equal(result.body.state,'Applied',JSON.stringify(result));
   saved=await call('/test/rs-inputs/subscriptions?view=preferences',null,{'X-RS-Actor-Scope':f.actor});assert.equal(saved.body.preferences.enabled,enabled);
   assert.equal((await call('/test/rs-inputs/subscriptions',body,{'X-RS-Actor-Scope':f.actor})).body.state,'Applied');
  }
  assert.equal(f.target.fields.consent_state,'Revoked');assert.equal(f.target.fields.revision,4);assert.deepEqual(f.prior,f.original);assert.equal(f.rows[TABLES.event].length,3);
  await page.goto(origin+'/rs-recognize');await page.waitForURL('**/rs-member');
  assert.equal(f.rows.tblfkRSJAEMzuzApR.length,1,'return must not create another device');
  assert.ok(f.rows.tblBgvP7pYSv8o20j.length>0);assert.ok(f.rows.tblWjbASVMIjFLyW8.length>0);
  assert.deepEqual(errors,[]);
 }finally{await context.close();await browser.close();f.db.close();}
});

test('device draft keeps uncertain writes; only rejected revision can be replaced by explicit preparation',async()=>{
 let state=null;const storage={read:async()=>state,mutate:async(key,fn)=>{const changed=fn(state);state=changed.state;return changed.result;}};
 const p=createPersistence({scope:{actorId:'a',resource:'preferences',tenantId:'personal'},storage});
 await p.edit({enabled:false});const op=await p.prepare({requestId:'save-one',expectedRevision:2});
 await p.fail(op,'TimeoutError');assert.equal(await p.rejectRevision(op),false);assert.equal((await p.load()).pending.requestId,'save-one');
 await p.fail(op,'record_changed');assert.equal(await p.rejectRevision(op),false,'an admitted processor rejection is not safe to replace');
 await p.fail(op,'intake_record_changed');await p.refresh({enabled:true},3);assert.equal(await p.rejectRevision(op),true);
 assert.deepEqual((await p.load()).draft,{enabled:false});assert.equal((await p.load()).dirty,true);
 const next=await p.prepare({requestId:'save-two',expectedRevision:3});assert.equal(next.body.expectedRevision,3);
});
