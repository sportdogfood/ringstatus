import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {webcrypto} from 'node:crypto';
const before=fs.readFileSync('wef-data/runtime/wef-decoder-runner/worker.js','utf8');
const after=fs.readFileSync(new URL('./runner-class-candidate.js',import.meta.url),'utf8');
async function headers(code,route){let captured;const app=new Function('fetch','crypto',code.replace('export default','return'))(async(url,options)=>{captured={url,headers:options.headers};return Response.json({class:{class_id:123},trips:[]});},webcrypto);assert.equal((await app.fetch(new Request('https://preview.test'+route),{SGL_TOKEN:'offline',SGL_COOKIE:'offline-cookie'})).status,200);return captured;}
test('schedule URL and working headers are unchanged',async()=>{const r='/schedule?customer_id=15&show_id=200000078&date=2026-11-11';assert.deepEqual(await headers(before,r),await headers(after,r));});
test('class detail keeps its URL/auth and gets the existing working headers',async()=>{const r='/classes/123?customer_id=15&show_id=200000078';const a=await headers(before,r),b=await headers(after,r);assert.equal(a.url,b.url);assert.equal(a.headers.Authorization,b.headers.Authorization);assert.equal(a.headers.Cookie,b.headers.Cookie);assert.ok(a.headers.path);assert.equal(b.headers.path,undefined);assert.match(b.headers['User-Agent'],/Windows NT.*Edg\/158/);assert.equal(b.headers['sec-ch-ua-platform'],'"Windows"');});
test('people and class-directory requests are unchanged',async()=>{for(const r of ['/people/123?pid=123&show_id=200000078&customer_id=15','/classes?customer_id=15&show_id=200000078'])assert.deepEqual(await headers(before,r),await headers(after,r));});
