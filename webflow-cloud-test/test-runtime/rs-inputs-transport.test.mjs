import { Miniflare } from 'miniflare';
import test from 'node:test';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import assert from 'node:assert/strict';
const root=fileURLToPath(new URL('../',import.meta.url));
const config=JSON.parse(await readFile(new URL('../wrangler.json',import.meta.url),'utf8'));
const entry=`
import {createAccessStore} from './src/lib/rs-inputs-access.js';
import {createAirtableInputStore} from './src/lib/rs-inputs-airtable.js';
import {createZohoTokenProvider} from './src/lib/rs-inputs-zoho-auth.js';
import {createCrmInputStore} from './src/lib/rs-inputs-crm.js';
export default {async fetch(request){
const env={RS_INPUTS_BASE_ID:'app9kOZdIaGyKk5uG',RS_INPUTS_WRITE_MODE:'isolated-trial',AIRTABLE_TOKEN:'fixture',ZOHO_CLIENT_ID:'fixture',ZOHO_CLIENT_SECRET:'fixture',ZOHO_REFRESH_TOKEN:'fixture'};
const actions={access:()=>createAccessStore({env}).byHash('a'.repeat(64)),airtable:()=>createAirtableInputStore({env,minimumIntervalMs:0}).list('barn'),oauth:()=>createZohoTokenProvider({env})(),crm:()=>createCrmInputStore({token:'fixture',mappings:{barns:{module:'RS_Trial_Barns',fields:{entity_uid:'UID',name:'Name'}}}}).list('barns')};
try{return Response.json({value:await actions[new URL(request.url).pathname.slice(1)]()});}catch(e){return Response.json({code:e.code,status:e.status,diagnostic:e.storageDiagnostic});}
}};`;
const {outputFiles}=await build({stdin:{contents:entry,resolveDir:root},bundle:true,format:'esm',platform:'browser',write:false});
for(const redirect of [false,true]) test(redirect?'native Cloudflare fetch rejects redirects without following credentials':'native Cloudflare fetch reaches each input provider',async()=>{
 let hits=0,followed=0;
 const outboundService=async req=>{
  hits++;const u=new URL(req.url);
  if(u.hostname==='redirect.invalid'){followed++;return Response.json({});}
  if(redirect)return new Response('',{status:302,headers:{Location:'https://redirect.invalid/leak'}});
  let body;
  if(u.hostname==='api.airtable.com')body={records:[]};
  else if(u.hostname==='accounts.zoho.com')body={access_token:'fixture-token',expires_in:3600,api_domain:'https://www.zohoapis.com'};
  else if(u.pathname.endsWith('/org'))body={org:[{zgid:'941333935'}]};
  else if(u.pathname.includes('/settings/modules/'))body={modules:[{api_name:'RS_Trial_Barns',api_supported:true}]};
  else if(u.pathname.endsWith('/settings/fields'))body={fields:[{api_name:'UID',unique:true},{api_name:'Name'}]};
  else if(u.pathname.endsWith('/RS_Trial_Barns'))body={data:[],info:{more_records:false}};
  else throw new Error('Unexpected fixture request');
  return Response.json(body);
 };
 const mf=new Miniflare({modules:true,script:outputFiles[0].text,compatibilityDate:config.compatibility_date,compatibilityFlags:config.compatibility_flags,outboundService});
 try{
  for(const [name,value] of Object.entries({access:null,airtable:[],oauth:'fixture-token',crm:[]})){
   const result=await(await mf.dispatchFetch('http://test.local/'+name)).json();
   if(redirect){assert.equal(result.status,name==='crm'?502:503);assert.ok(result.code);}
   else assert.deepEqual(result,{value});

  }
  assert.equal(followed,0);assert.equal(hits,redirect?4:7);
 }finally{await mf.dispose();}
});
