import fs from 'node:fs/promises';
import path from 'node:path';
import {webcrypto} from 'node:crypto';
const root=process.cwd(),evidence=path.join(root,'workers/wef-schedules-decoded/evidence/returned-token-20261111');
const source=await fs.readFile(path.join(root,'wef-data/runtime/wef-decoder-runner/worker.js'),'utf8');
const relay=await fs.readFile(path.join(root,'wef-data/runtime/wef-decoder-runner/runner-relay.mjs'),'utf8');
const env=await fs.readFile(path.join(root,'wef-data/runtime/collector.env'),'utf8');
const cookie=env.match(/^\s*SGL_COOKIE=(.*)$/m)?.[1]?.trim().replace(/^['"]|['"]$/g,'');
if(!cookie)throw Error('Existing runner cookie unavailable');
const candidate=source.replace('if (pathname === "/schedule") {','if (pathname === "/schedule" || /^\\/classes\\/\\d+$/.test(pathname)) {');
if(candidate===source)throw Error('Expected existing header guard missing');
const functions=relay.slice(relay.indexOf('let cachedToken;'),relay.indexOf('const expected ='));
const sourceToken=new Function('SGL_COOKIE','recordRenewal','logEnv',functions+';return sourceToken;')(cookie,async()=>true,{});
const token=await sourceToken();
const results=[];
for(const [mode,code]of [['installed',source],['class-uses-existing-working-headers',candidate]]){
 const logs=[];
 const app=new Function('crypto','console',code.replace('export default','return'))(webcrypto,{error:s=>logs.push(String(s).split(cookie).join('[redacted]').split(token).join('[redacted]'))});
 const response=await app.fetch(new Request('https://preview.test/classes/200031471?customer_id=15&show_id=200000078'),{SGL_TOKEN:token,SGL_COOKIE:cookie});
 const body=await response.json();
 results.push({mode,status:response.status,decoded:response.headers.get('x-rs-decoded')==='1',classId:body.class?.class_id,trips:body.trips?.length,idsPopulated:Array.isArray(body.trips)&&body.trips.every(t=>Number.isFinite(t.trip_id)&&Number.isFinite(t.entry_id)),errors:logs});
 await new Promise(r=>setTimeout(r,1000));
}
await fs.writeFile(path.join(evidence,'runner-class-preview.json'),JSON.stringify({sameURL:true,sameCookie:true,sameRunnerRenewalToken:true,results},null,2));
await fs.writeFile(path.join(evidence,'runner-class-candidate.js'),candidate);
console.log(JSON.stringify(results));
