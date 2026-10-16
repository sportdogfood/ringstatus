import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {profileFixture} from '../test-support/tenant-profile-fixture.mjs';
const {chromium}=createRequire('C:/Users/gombc/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/package.json')('playwright');
test('native sign-out activation navigates existing route and preserves device drafts',{timeout:90000},async()=>{
 const origin='https://ringstatus.com',path='/rs-barn-onboarding-setup-v24';
 const archived=await readFile('C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/tenants-cause-audit-20261015/live-page.html','utf8');
 const html=archived.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi,'').replace(/<link\b[^>]*>/gi,'')+`<style>[hidden],.rs-barn-v24-state-hidden{display:none!important}</style><script type="module">import {prepareBarnOnboarding} from '/test/rs-inputs/native-barn.js';window.app=prepareBarnOnboarding(document,location.origin+'/test/rs-inputs/native-barn.js');</script>`;
 const native=await readFile(new URL('../src/assets/rs-barn-onboarding/native-client.js',import.meta.url),'utf8');
 const f=await profileFixture(),browser=await chromium.launch({headless:true,channel:'msedge'}),context=await browser.newContext(),page=await context.newPage();
 await context.addCookies([{name:'__Host-rs_recognition_device',value:f.identity.device_token,url:origin,secure:true,httpOnly:true,sameSite:'Lax'}]);
 await context.route('**/*',async route=>{const r=route.request(),u=new URL(r.url());if(u.origin!==origin)return route.abort();
  if(u.pathname===path)return route.fulfill({contentType:'text/html',body:html});
  if(u.pathname==='/rs-recognize')return route.fulfill({contentType:'text/html',body:'<h1>Existing recognition route</h1>'});
  if(u.pathname.endsWith('/native-barn.js'))return route.fulfill({contentType:'text/javascript',body:native});
  if(!u.pathname.startsWith('/test/rs-inputs/'))return route.abort();
  const response=await f.dispatch(new Request(r.url(),{method:r.method(),headers:await r.allHeaders(),...(r.postData()?{body:r.postData()}:{})}));return route.fulfill({status:response.status,headers:Object.fromEntries(response.headers),body:await response.text()});});
 try{await page.goto(origin+path);await page.waitForFunction(()=>app?.getState()?.selectedBarn&&!app.isBusy());
  await page.evaluate(()=>{localStorage.setItem('unrelated-pending-draft','preserve');document.addEventListener('click',e=>e.preventDefault());const header=document.createElement('div');header.style.cssText='position:fixed;left:120px;right:0;top:0;height:44px;z-index:999';document.body.append(header);});
  const button=page.getByRole('button',{name:'Not you? Sign out',exact:true});
  assert.ok(await button.evaluate(el=>el.getBoundingClientRect().width<120),'sign-out hit area stays outside header overlay');await button.click();await page.waitForURL(origin+'/rs-recognize?sign_out=1');
  assert.equal(await page.evaluate(()=>localStorage.getItem('unrelated-pending-draft')),'preserve');
 }finally{await browser.close();f.db.close();}
});
