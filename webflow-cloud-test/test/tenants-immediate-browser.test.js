import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile,mkdir} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {recognizeTenantsFixture} from '../test-support/recognize-tenants-fixture.mjs';
import {TABLE} from '../src/lib/rs-tenant-barn.js';
const req=createRequire('C:/Users/gombc/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/package.json');
const {chromium}=req('playwright');
const origin='https://ringstatus.com',path='/rs-barn-onboarding-setup-v24';
const archived=await readFile('C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/tenants-connections-20261010/rs-barn-onboarding-setup-v24.html','utf8');
// Actual saved Webflow markup; external scripts/styles removed for deterministic
// local behavior testing. This does not claim current published visual parity.
const html=archived.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'')+`<style>[hidden],.rs-barn-v24-state-hidden{display:none!important}</style><script type="module">import {prepareBarnOnboarding} from '/test/rs-inputs/native-barn.js';window.app=prepareBarnOnboarding(document,location.origin+'/test/rs-inputs/native-barn.js');</script>`;
const native=await readFile(new URL('../src/assets/rs-barn-onboarding/native-client.js',import.meta.url),'utf8');
for(const mobile of [true,false])test(`Tenants ${mobile?'mobile':'desktop'} load/edit/immediate display/failure/reload/retry/add`,{timeout:90000},async()=>{
 const f=await recognizeTenantsFixture();f.insert(TABLE.roles,{rec_name:'owner'});
 const recognized=await f.dispatch(new Request(origin+'/test/rs-inputs/native-recognition?operation=action',{method:'POST',headers:{Origin:origin,'Content-Type':'application/json'},body:JSON.stringify({action:'credential',kind:'phone',value:'6318752160',request_id:crypto.randomUUID()})}));
 const identity=await recognized.json();assert.equal(identity.verified,true);
 const browser=await chromium.launch({headless:true,channel:'msedge'}),context=await browser.newContext({viewport:mobile?{width:390,height:844}:{width:1280,height:900},isMobile:mobile});
 await context.addCookies([{name:'__Host-rs_recognition_device',value:identity.device_token,url:origin,secure:true,httpOnly:true,sameSite:'Lax'}]);
 const page=await context.newPage(),errors=[],writes=[];page.on('pageerror',e=>errors.push(e.message));
 page.setDefaultTimeout(30000);
 let release,failFirst=true,blocked=false;
 await context.route('**/*',async route=>{
  const request=route.request(),url=new URL(request.url());
  if(url.origin!==origin)return route.abort();
  if(url.pathname===path)return route.fulfill({contentType:'text/html',body:html});
  if(url.pathname==='/test/rs-inputs/native-barn.js')return route.fulfill({contentType:'text/javascript',body:native});
  if(!url.pathname.startsWith('/test/rs-inputs/'))return route.abort();
  if(url.pathname.endsWith('/record')){
   writes.push(JSON.parse(request.postData()));
   if(failFirst){failFirst=false;blocked=true;await new Promise(r=>release=r);return route.fulfill({status:503,contentType:'application/json',body:JSON.stringify({ok:false,error:'write_outcome_unknown'})});}
  }
  const response=await f.dispatch(new Request(request.url(),{method:request.method(),headers:await request.allHeaders(),...(request.postData()?{body:request.postData()}:{})}));
  const data=await response.clone().json();
  if(data.error==='tenant_save_pending'){
   const row=f.rows[TABLE.submissions].find(r=>r.fields['Processing status']==='Pending'&&JSON.parse(r.fields['Submitted changes']).kind==='barn-rider');
   if(row)await f.processBarn(row);
  }
  return route.fulfill({status:response.status,headers:Object.fromEntries(response.headers),body:await response.text()});
 });
 try{
  await page.goto(origin+path);await page.waitForFunction(()=>window.app?.getState()?.barns.length===1&&!app.isBusy());
  assert.equal(await page.locator('.rs-barn-v24-barn-name').textContent(),'Fixture Barn');
  await page.locator('[aria-label="Edit barn"]').click();await page.waitForFunction(()=>!app.isBusy()&&document.querySelector('#Barn-name').value==='Fixture Barn');await page.locator('#Barn-name').fill('Immediate Barn');
  await page.locator('#email-form .rs-barn-v24-primary').click();
  await page.waitForFunction(()=>document.querySelector('.rs-barn-v24-barn-name').textContent==='Immediate Barn');
  assert.equal(await page.locator('#email-form').getAttribute('data-rs-save-state'),'Saving');
  assert.equal(await page.locator('.rs-barn-v24-root').evaluate(n=>n.inert),false);
  assert.equal(f.rows[TABLE.barn].length,0,'UI feedback precedes canonical write');
  while(!blocked)await new Promise(r=>setTimeout(r,10));release();
  await page.waitForFunction(()=>document.querySelector('#email-form .rs-barn-v24-primary').textContent==='Retry'&&!app.isBusy());
  assert.equal(await page.locator('#Barn-name').inputValue(),'Immediate Barn');
  await page.reload();await page.waitForFunction(()=>window.app&&!app.isBusy()&&document.querySelector('#Barn-name').value==='Immediate Barn');
  assert.equal(await page.locator('.rs-barn-v24-barn-name').textContent(),'Immediate Barn');
  await page.locator('#email-form .rs-barn-v24-primary').click();
  await page.waitForFunction(()=>document.querySelector('#email-form').getAttribute('data-rs-save-state')==='Saved'&&!app.isBusy());
  assert.equal(new Set(writes.filter(w=>w.kind==='barn').map(w=>w.requestId)).size,1);
  assert.equal(f.rows[TABLE.barn].length,1);assert.equal(f.rows[TABLE.barn][0].fields.tenant_official_name,'Immediate Barn');
  await page.evaluate(()=>app.add('riders'));await page.locator('#Rider-name').fill('Immediate Rider');
  await page.locator('#email-form-3 .rs-barn-v24-primary').click();
  await page.waitForFunction(()=>[...document.querySelectorAll('.rs-barn-v24-row-name')].some(n=>n.textContent==='Immediate Rider'));
  assert.equal(await page.locator('#email-form-3').getAttribute('data-rs-save-state'),'Saving');
  await page.waitForFunction(()=>document.querySelector('#email-form-3').getAttribute('data-rs-save-state')==='Saved'&&!app.isBusy());
  assert.equal(f.rows[TABLE.riders].length,1);assert.equal(f.rows[TABLE.riders][0].fields.name,'Immediate Rider');
  await page.reload();await page.waitForFunction(()=>window.app?.getState()?.riders.length===1&&!app.isBusy());
  assert.equal((await page.evaluate(()=>app.getState())).barns[0].name,'Immediate Barn');
  assert.deepEqual(errors,[]);await mkdir('test-evidence',{recursive:true});await page.screenshot({path:`test-evidence/tenants-${mobile?'mobile':'desktop'}-functional.png`,fullPage:true});
 }catch(error){console.log('BROWSER FAILURE',await page.evaluate(()=>({busy:window.app?.isBusy(),error:document.querySelector('#email-form').getAttribute('data-rs-save-error'),input:document.querySelector('#Barn-name')?.value,feedback:[...document.querySelectorAll('.rs-barn-v24-error')].map(n=>n.textContent)})),errors);throw error;}finally{release?.();await context.close();await browser.close();f.db.close();}
});
