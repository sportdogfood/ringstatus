'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {airtable}=require('../airtable');
function fixture({loseResponse=false}={}){
 const rows=Array.from({length:250},(_,i)=>({id:'old'+i,fields:{'entry-key':'old-key-'+i,'source-json':'retained'}})),calls=[];let posts=0,serial=0;
 const request=async(url,options)=>{
  const u=new URL(url),method=options.method,filter=u.searchParams.get('filterByFormula');calls.push({method,filter,created:posts>0});
  if(method==='POST'){
   posts++;const values=JSON.parse(options.body).records;const saved=values.map(r=>({id:'created'+(++serial),fields:r.fields}));rows.push(...saved);
   if(loseResponse){loseResponse=false;throw Error('Lost create response');}
   return{ok:true,json:async()=>({records:structuredClone(saved)})};
  }
  assert.equal(method,'GET');let selected=rows;
  if(filter){const keys=[...filter.matchAll(/\{entry-key\}=("(?:[^"\\]|\\.)*")/g)].map(m=>JSON.parse(m[1]));assert(keys.length<=20&&keys.length>0);selected=rows.filter(r=>keys.includes(r.fields['entry-key']));}
  const start=Number(u.searchParams.get('offset')||0),page=selected.slice(start,start+100);
  return{ok:true,json:async()=>({records:structuredClone(page),...(start+100<selected.length?{offset:String(start+100)}:{})})};
 };
 return{api:airtable({request,pause:async()=>{}}),rows,calls,get posts(){return posts;}};
}
test('batch readback targets at most twenty exact unchanged keys despite unrelated rows',async()=>{
 const f=fixture(),fields=Array.from({length:25},(_,i)=>({'entry-key':'existing-shape|'+i,'source-json':'exact-'+i}));
 const saved=await f.api.upsert('entries','entry-key',fields);assert.equal(saved.length,25);assert.equal(f.posts,3);
 const readbacks=f.calls.filter(c=>c.method==='GET'&&c.created);assert(readbacks.length>0);assert(readbacks.every(c=>c.filter));
 const ids=saved.map(r=>r.id);const repeated=await f.api.upsert('entries','entry-key',fields);assert.deepEqual(repeated.map(r=>r.id),ids);assert.equal(f.posts,3);assert.equal(f.rows.length,275);
});
test('lost create response is read back by original keys, never created twice',async()=>{
 const f=fixture({loseResponse:true}),fields=[{'entry-key':'unchanged|quote"key','source-json':'exact'}];
 const first=await f.api.upsert('entries','entry-key',fields),again=await f.api.upsert('entries','entry-key',fields);
 assert.equal(first[0].id,again[0].id);assert.equal(f.posts,1);assert.equal(f.rows.filter(r=>r.fields['entry-key']===fields[0]['entry-key']).length,1);
});
