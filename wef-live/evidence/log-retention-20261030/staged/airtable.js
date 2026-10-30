"use strict";
// Local Airtable adapter. Destination-only writes; retirement requires a verified local archive.
const settings = require("./settings");
const { same, unique } = require("./runtime-utils");
function airtable({
  token = settings.AIRTABLE_TOKEN,
  request = globalThis.fetch,
  pause = (ms) => new Promise((r) => setTimeout(r, ms)),
} = {}) {
  const writable = new Set(Object.keys(require("./schema").tables)),
    unresolved = new Set();
  const readable = new Map([
    [
      settings.BASE_ID,
      new Set([...writable, "wef-schedules.schedules"]),
    ],
    [settings.SHOWS_BASE, new Set(["shows"])],
    [settings.SCHEDULES_BASE, new Set(['schedules','show-collection'])],
  ]);
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
  async function retire(table, rows, scope, receipt) {
    if (
      !["live-rings", "live-groups", "live-classes", "live-entries", "collections"].includes(
        table,
      ) ||
      rows.some((r) => !require('./rollover').canRetire(r.fields, scope) ||
        (table === 'collections' && !['rings','groups','class'].includes(r.fields['source-kind'])))
    )
      throw Error("Forbidden retirement scope");
    const fs = require("node:fs"),
      zlib = require("node:zlib"),
      crypto = require("node:crypto"),
      path = require("node:path");
    const root = path.resolve(settings.RUNTIME, "history") + path.sep;
    if (!receipt?.file || !path.resolve(receipt.file).startsWith(root))
      throw Error("Missing retirement archive");
    const bytes = zlib.gunzipSync(fs.readFileSync(receipt.file));
    if (
      crypto.createHash("sha256").update(bytes).digest("hex") !==
        receipt.sha256 ||
      JSON.stringify(JSON.parse(bytes)) !== JSON.stringify(rows)
    )
      throw Error("Retirement archive verification failed");
    for (let i = 0; i < rows.length; i += 10) {
      await pause(250);
      const query = new URLSearchParams();
      for (const row of rows.slice(i, i + 10))
        query.append("records[]", row.id);
      const r = await request(
        `https://api.airtable.com/v0/${settings.BASE_ID}/${encodeURIComponent(table)}?${query}`,
        {
          method: "DELETE",
          headers: { Authorization: "Bearer " + token },
          signal: AbortSignal.timeout(30000),
        },
      );
      if (!r.ok) throw Error("Retirement HTTP " + r.status);
      const result = await r.json();
      if (
        result.records?.length !== rows.slice(i, i + 10).length ||
        result.records.some((r) => !r.deleted) ||
        JSON.stringify(result.records.map(r=>r.id).sort()) !== JSON.stringify(rows.slice(i,i+10).map(r=>r.id).sort())
      )
        throw Error("Incomplete retirement");
    }
  }
  async function oldErrors(cutoff,maxRows,accept=()=>true){
    const rows=[];let offset;do{const q=new URLSearchParams({pageSize:'100',filterByFormula:`IS_BEFORE({occurred-at},'${cutoff}')`,...(offset?{offset}:{})});const d=await call(settings.BASE_ID,'errs-log','GET',undefined,'?'+q);rows.push(...d.records.filter(accept));offset=d.offset;}while(offset&&rows.length<maxRows);return rows.slice(0,maxRows);
  }
  async function retireLogs(table,rows,cutoff,receipt,parents){
    const policy=require('./log-retention');
    if(!policy.TABLES.includes(table)||!Number.isFinite(Date.parse(cutoff))||Date.parse(cutoff)>Date.now()-86400000||rows.some(r=>!policy.eligible(table,r,cutoff,parents)))throw Error('Forbidden log retirement');
    const path=require('node:path');if(!path.resolve(receipt.file).startsWith(path.resolve(settings.RUNTIME,'history')+path.sep))throw Error('Log archive outside history');
    if(JSON.stringify(policy.verified(receipt))!==JSON.stringify(rows))throw Error('Log archive rows mismatch');
    for(let i=0;i<rows.length;i+=10){const batch=rows.slice(i,i+10);const q=new URLSearchParams({filterByFormula:'OR('+batch.map(r=>`RECORD_ID()="${r.id}"`).join(',')+')'});const current=(await call(settings.BASE_ID,table,'GET',undefined,'?'+q)).records;
      if(current.length!==batch.length||batch.some(r=>{const c=current.find(c=>c.id===r.id);return !c||!require('node:util').isDeepStrictEqual(c.fields,r.fields)||!policy.eligible(table,c,cutoff,parents);}))throw Error('Log changed before deletion');
      await pause(250);const query=new URLSearchParams();for(const r of batch)query.append('records[]',r.id);
      const response=await request(`https://api.airtable.com/v0/${settings.BASE_ID}/${encodeURIComponent(table)}?${query}`,{method:'DELETE',headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(30000)});
      if(!response.ok)throw Error('Log retirement HTTP '+response.status);const d=await response.json();if(d.records?.length!==batch.length||d.records.some(r=>!r.deleted)||JSON.stringify(d.records.map(r=>r.id).sort())!==JSON.stringify(batch.map(r=>r.id).sort()))throw Error('Uncertain log deletion');
      const remaining=(await call(settings.BASE_ID,table,'GET',undefined,'?'+q)).records;if(remaining.length)throw Error('Deleted log still present');
    }
  }
  async function fieldNames(table) {
    if(!readable.get(settings.BASE_ID).has(table)) throw Error('Unapproved metadata target');
    await pause(250);
    const r=await request(`https://api.airtable.com/v0/meta/bases/${settings.BASE_ID}/tables`,{
      headers:{Authorization:'Bearer '+token},signal:AbortSignal.timeout(30000)});
    if(!r.ok) throw Error('Reference schema HTTP '+r.status);
    const d=await r.json(),t=d.tables.find(t=>t.name===table);
    if(!t)throw Error('Missing reference schema');
    return t.fields.map(f=>f.name);
  }
  return { list, upsert, call, retire, retireLogs, oldErrors, fieldNames, publicationEnabled: true };
}
module.exports = { airtable };
