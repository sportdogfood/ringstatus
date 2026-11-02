"use strict";
// Local Airtable adapter. Live delete-only retirement; original values and exact deletions remain verified.
const settings = require("./cleanup-settings");
const same=(a,b)=>JSON.stringify(a==null||a===''?null:a)===JSON.stringify(b==null||b===''?null:b)||((a==null||a==='')&&(b===false||Array.isArray(b)&&!b.length));
function unique(rows,key){const seen=new Set();for(const r of rows){const k=r.fields[key];if(k==null||k===''||seen.has(k))throw Error('Missing or duplicate '+key);seen.add(k);}}
function airtable({
  token = settings.AIRTABLE_TOKEN,
  request = globalThis.fetch,
  pause = (ms) => new Promise((r) => setTimeout(r, ms)),
} = {}) {
  const writable = new Set(['cleanup','cleanup-logs','err-logs']),
    unresolved = new Set();
  const readable=new Map([[settings.BASE_ID,new Set([...writable,...require('./cleanup').TABLES,'wef-shows.shows'])]]);
  async function call(base, table, method = "GET", body, query = "", limit = {}) {
    if (!readable.get(base)?.has(table)) throw Error("Unapproved read target");
    if (
      method !== "GET" &&
      (base !== settings.BASE_ID ||
        !writable.has(table) ||
        !["POST", "PATCH"].includes(method))
    )
      throw Error("Read-only source or forbidden mutation");
    await pause(250);
    const r = await request(
      `https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}${query}`,
      {
        method,
        headers: {
          Authorization: "Bearer " + token,
          "Content-Type": "application/json",
        },
        ...(body ? { body: JSON.stringify(body) } : {}),
        signal: (() => { const remaining=(limit.deadline ?? (Date.now()+30000))-Date.now(); if(remaining<=0)throw Error("Publication lane budget exhausted"); return AbortSignal.timeout(Math.min(30000,remaining)); })(),
      },
    );
    if (!r.ok) throw Error(`Airtable ${method} ${table} HTTP ${r.status}`);
    return r.json();
  }
  async function list(table, base = settings.BASE_ID, limit) {
    const rows = [];
    let offset;
    do {
      const q = new URLSearchParams({
        pageSize: "100",
        ...(offset ? { offset } : {}),
      });
      const d = await call(base, table, "GET", undefined, "?" + q, limit);
      rows.push(...d.records);
      offset = d.offset;
    } while (offset);
    return rows;
  }
  const equal = (a, b) =>
    Array.isArray(a) && Array.isArray(b)
      ? JSON.stringify([...a].sort()) === JSON.stringify([...b].sort())
      : same(a, b);
  async function verify(table, key, fields) {
    const rows = await list(table);
    unique(rows, key);
    return fields.map((f) => {
      const r = rows.find((r) => r.fields[key] === f[key]);
      if (!r) throw Error("Missing saved " + table + " " + f[key]);
      for (const [k, v] of Object.entries(f))
        if (!equal(r.fields[k], v))
          throw Error("Readback mismatch " + table + "." + k);
      return r;
    });
  }
  async function upsert(table, key, fields) {
    if (!writable.has(table)) throw Error("Read-only destination");
    if (!fields.length) return [];
    unique(
      fields.map((fields) => ({ fields })),
      key,
    );
    const before = await list(table);
    unique(before, key);
    const creates = [],
      patches = [];
    for (const f of fields) {
      const old = before.find((r) => r.fields[key] === f[key]);
      if (old) {
        const delta = Object.fromEntries(
          Object.entries(f).filter(([k, v]) => !equal(old.fields[k], v)),
        );
        if (Object.keys(delta).length)
          patches.push({ id: old.id, fields: delta });
      } else {
        const id = table + "|" + f[key];
        if (unresolved.has(id)) throw Error("Creation outcome unresolved");
        creates.push({ fields: f });
      }
    }
    for (let i = 0; i < patches.length; i += 10)
      await call(settings.BASE_ID, table, "PATCH", {
        records: patches.slice(i, i + 10),
      });
    for (let i = 0; i < creates.length; i += 10) {
      const batch = creates.slice(i, i + 10);
      try {
        await call(settings.BASE_ID, table, "POST", { records: batch });
      } catch (error) {
        try {
          await verify(
            table,
            key,
            batch.map((r) => r.fields),
          );
        } catch {
          for (const r of batch) unresolved.add(table + "|" + r.fields[key]);
          throw Error("Creation outcome unresolved: " + error.message);
        }
      }
      await verify(
        table,
        key,
        batch.map((r) => r.fields),
      );
    }
    return verify(table, key, fields);
  }
  async function retireCleanup(table,rows,policy,onDeleted=()=>{}) {
    const cleanup=require('./cleanup'),util=require('node:util');
    if(!cleanup.TABLES.includes(table)||!Number.isSafeInteger(policy.show)||policy.show<=0||
      !['new-show','quick-cleanup'].includes(policy.trigger)||
      rows.some(r=>!cleanup.eligible(table,r,policy.trigger,policy.show,api.runShows)||api.protectedIds.has(r.id)))throw Error('Forbidden cleanup scope');
    if(policy.trigger==='quick-cleanup'&&rows.length!==500)throw Error('Quick cleanup requires exactly 500 records');
    const afterDeleted=fields=>Object.fromEntries(Object.entries(fields).filter(([k])=>!settings.COMPUTED[table]?.includes(k)).map(([k,v])=>[k,
      Array.isArray(v)&&v.every(id=>typeof id==='string'&&/^rec[a-zA-Z0-9]+$/.test(id))?v.filter(id=>!policy.deletedIds.has(id)):v
    ]).filter(([,v])=>!Array.isArray(v)||v.length));
    for(let i=0;i<rows.length;i+=10){
      // Check canonical identity and exact original values immediately before each delete batch.
      if(await cleanup.canonical({list})!==policy.show)throw Error('Default show changed before cleanup deletion');
      const batch=rows.slice(i,i+10),q=new URLSearchParams({filterByFormula:'OR('+batch.map(r=>`RECORD_ID()="${r.id}"`).join(',')+')'});
      const current=(await call(settings.BASE_ID,table,'GET',undefined,'?'+q)).records;
      if(current.length!==batch.length||batch.some(r=>{const c=current.find(c=>c.id===r.id);return !c||
        !util.isDeepStrictEqual(afterDeleted(r.fields),afterDeleted(c.fields))||!cleanup.eligible(table,c,policy.trigger,policy.show,api.runShows);}))throw Error('Cleanup record changed before deletion');
      await pause(250);const query=new URLSearchParams();for(const r of batch)query.append('records[]',r.id);
      const response=await request(`https://api.airtable.com/v0/${settings.BASE_ID}/${encodeURIComponent(table)}?${query}`,{method:'DELETE',headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(30000)});
      if(!response.ok)throw Error('Cleanup deletion HTTP '+response.status);
      const d=await response.json();if(d.records?.length!==batch.length||d.records.some(r=>!r.deleted)||JSON.stringify(d.records.map(r=>r.id).sort())!==JSON.stringify(batch.map(r=>r.id).sort()))throw Error('Uncertain cleanup deletion');
      onDeleted(batch.map(r=>r.id));
      if((await call(settings.BASE_ID,table,'GET',undefined,'?'+q)).records.length)throw Error('Cleanup deleted record still present');
    }
  }
  const api={list,upsert,call,retireCleanup,runShows:{},protectedIds:new Set()};
  api.prepare=async()=>{
    const logs=await list('run-logs'),errors=await list('err-logs'),heartbeats=await list('heartbeat');
    const {showOf,protectedRows}=require('./cleanup-fields');
    api.protectedIds=protectedRows(logs,errors,heartbeats);
    for(const r of logs){const sid=showOf('run-logs',r);if(sid!==null){api.runShows[r.id]=sid;if(r.fields['run-key'])api.runShows[r.fields['run-key'].split('|')[0]]=sid;}}
    for(const r of errors){const sids=[...new Set((r.fields.run||[]).map(id=>api.runShows[id]).filter(Number.isSafeInteger))];if(sids.length===1)api.runShows[r.id]=sids[0];}
  };
  return api;
}
module.exports={airtable};
