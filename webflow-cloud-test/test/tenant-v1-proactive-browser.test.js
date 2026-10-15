import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {profileFixture} from '../test-support/tenant-profile-fixture.mjs';
const {chromium}=createRequire('C:/Users/gombc/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/package.json')('playwright');
const origin='https://ringstatus.com',path='/rs-barn-onboarding-setup-v24';
const archived=await readFile('C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/tenants-cause-audit-20261015/live-page.html','utf8');
const html=archived.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'')+`<style>[hidden],.rs-barn-v24-state-hidden{display:none!important}</style><script type="module">import {prepareBarnOnboarding} from '/test/rs-inputs/native-barn.js';window.app=prepareBarnOnboarding(document,location.origin+'/test/rs-inputs/native-barn.js');</script>`;
const native=await readFile(new URL('../src/assets/rs-barn-onboarding/native-client.js',import.meta.url),'utf8');
for(const width of [390,1280])test('proactive profile preserves second edit during delayed save '+width,{timeout:240000},async()=>{
 const f=await profileFixture(),browser=await chromium.launch({headless:true,channel:'msedge'}),context=await browser.newContext({viewport:{width,height:844}}),page=await context.newPage();
 await context.addCookies([{name:'__Host-rs_recognition_device',value:f.identity.device_token,url:origin,secure:true,httpOnly:true,sameSite:'Lax'}]);
 let release,hold=true;const writes=[],errors=[];page.on('pageerror',e=>errors.push(e.message));page.setDefaultTimeout(40000);
 await context.route('**/*',async route=>{
  const r=route.request(),u=new URL(r.url());if(u.origin!==origin)return route.abort();
  if(u.pathname===path)return route.fulfill({contentType:'text/html',body:html});
  if(u.pathname.endsWith('/native-barn.js'))return route.fulfill({contentType:'text/javascript',body:native});
  if(!u.pathname.startsWith('/test/rs-inputs/'))return route.abort();
  if(u.pathname.endsWith('/record')){writes.push(JSON.parse(r.postData()));if(hold){hold=false;await new Promise(resolve=>release=resolve);}}
  const response=await f.dispatch(new Request(r.url(),{method:r.method(),headers:await r.allHeaders(),...(r.postData()?{body:r.postData()}:{})}));
  if(u.pathname.endsWith('/record'))console.log('record response',response.status);return route.fulfill({status:response.status,headers:Object.fromEntries(response.headers),body:await response.text()});
 });
 try{
  console.log('stage: load');await page.goto(origin+path);await page.waitForFunction(()=>app?.getState()?.selectedBarn&&!app.isBusy());
  await page.getByRole('button',{name:'Edit my profile',exact:true}).click();
  const form=page.locator('#email-form-2'),field=form.locator('[data-rs-profile-field="displayName"]');
  console.log('stage: first edit');await field.fill('First local edit');await form.locator('.rs-barn-v24-secondary').click();
  await page.waitForFunction(()=>document.querySelector('#email-form-2').hidden);
  assert.equal(await page.locator('.rs-barn-v24-root').evaluate(n=>n.inert),false);
  console.log('stage: second edit');await page.locator('.rs-barn-v24-row').filter({hasText:'First local edit'}).locator('.rs-barn-v24-edit-button').click();await field.fill('Newest local edit');await form.locator('.rs-barn-v24-secondary').click();
  await page.waitForFunction(()=>[...document.querySelectorAll('.rs-barn-v24-row-name')].some(n=>n.textContent==='Newest local edit'));
  console.log('stage: queued');for(let i=0;writes.length===0&&i<600;i++)await new Promise(r=>setTimeout(r,50));assert.equal(writes.length,1,'same-record successor waits in device queue');
  await page.locator('.rs-barn-v24-row').filter({hasText:'Newest local edit'}).locator('.rs-barn-v24-edit-button').click();await field.fill('Unsubmitted typing survives');
  console.log('stage: release');while(!release)await new Promise(r=>setTimeout(r,5));release();
  await page.waitForFunction(()=>app.getState().users.some(r=>r.displayName==='Newest local edit'));
  assert.equal(await field.inputValue(),'Unsubmitted typing survives');assert.equal(writes.length,2);assert.equal(writes[1].expectedRevision,writes[0].expectedRevision+1);
  await page.reload();await page.waitForFunction(()=>app?.getState()?.users.some(r=>r.displayName==='Newest local edit'));
  assert.deepEqual(errors,[]);
 }catch(error){console.log('failure state',await page.locator('#email-form-2').getAttribute('data-rs-save-state'),await page.locator('#email-form-2').getAttribute('data-rs-save-error'),writes,errors);throw error;}finally{release?.();await browser.close();f.db.close();}
});
