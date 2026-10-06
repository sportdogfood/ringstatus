// Work-only connector relay for the recorded isolated integration run.
// Not deployed or imported by runtime routes. Requires authorized MCP tools,
// fresh table metadata, and a context.written array containing this run's IDs.
export async function relayAirtableRequest(q, context){
 const { tools } = context;
 const prefix=context.fixturePrefix||'rs_live_20261005_2215';
 if(!/^rs_(?:live|auth)_[A-Za-z0-9_]{8,48}$/.test(prefix))throw new Error('Invalid fixture namespace');
 const parsed=/^(https:\/\/[^/]+)([^?]*)(?:\?(.*))?$/.exec(q.url);if(!parsed)throw new Error('Invalid bridge URL'); const query=new Map((parsed[3]||'').split('&').filter(Boolean).map(x=>{const [k,...v]=x.split('=');return[decodeURIComponent(k),decodeURIComponent(v.join('=').replace(/\+/g,' '))];}));const url={origin:parsed[1],pathname:parsed[2],searchParams:query},parts=url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
 if(url.origin!=='https://api.airtable.com'||parts[0]!=='v0'||parts[1]!=='app9kOZdIaGyKk5uG'||parts.length>4)throw new Error('Wrong bridge target');
 const t=context.tables.find(t=>t.name===parts[2]);
 if(!t||t.name==='quick-references')throw new Error('Unknown bridge table');
 const field=name=>{const f=t.fields.find(f=>f.name===name);if(!f||name.startsWith('legacy_'))throw new Error('Unknown runtime field '+name);return f;};
 const ids=t.fields.filter(f=>!f.name.startsWith('legacy_')).map(f=>f.id);
 let r;
 const args={baseId:parts[1],tableId:t.id};
 if(q.method==='GET'){
   const formula=url.searchParams.get('filterByFormula');
   let filters;
   if(formula){
    const m=/^\{([a-z_]+)\} = '((?:\\.|[^'])*)'$/.exec(formula);
    if(!m||!['device_token','entity_uid','event_uid','session_event_uid','idempotency_key','person_uid','input_invite_hash'].includes(m[1]))throw new Error('Unsupported bridge filter');
    filters={operands:[{operator:'=',operands:[field(m[1]).id,m[2].replace(/\\(['\\])/g,'$1')]}]};
   }
   for(const key of url.searchParams.keys())if(!['filterByFormula','maxRecords','offset'].includes(key))throw new Error('Unsupported query '+key);
   r=await tools.mcp__codex_apps__airtable_list_records_for_table({...args,fieldIds:ids,...(filters?{filters}:{}),...(parts[3]?{recordIds:[parts[3]]}:{}),...(url.searchParams.get('offset')?{cursor:url.searchParams.get('offset')}:{ }),pageSize:Math.min(100,Number(url.searchParams.get('maxRecords')||100))});
 }else{
   if(!['POST','PATCH'].includes(q.method))throw new Error('Unsupported method');
   const incoming=q.body?.records||(q.body?.fields?[{id:parts[3],fields:q.body.fields}]:null);
   if(!Array.isArray(incoming)||incoming.length>10)throw new Error('Unexpected mutation payload');
   const written=context.written;
   for(const rec of incoming){
    if(rec.id&&!written.includes(rec.id))throw new Error('Refusing mutation of pre-existing record');
    if(!rec.id){
      const v=rec.fields;
      const own=t.name.startsWith('rs_input_')?([`${prefix}_actor`,`${prefix}_person`].includes(v.owner_uid)||[`${prefix}_actor`,`${prefix}_person`].includes(v.actor_uid)):(Object.values(v).some(x=>typeof x==='string'&&(x.startsWith(prefix+'_')||x.startsWith('inputs_'+prefix+'_'))));
      if(!own)throw new Error('Refusing mutation outside fixture namespace');
    }
   }
   const records=incoming.map(rec=>({...rec,fields:Object.fromEntries(Object.entries(rec.fields).map(([k,v])=>[field(k).id,v]))}));
   if(q.method==='POST')r=await tools.mcp__codex_apps__airtable_create_records_for_table({...args,records,fieldIds:ids});
   else {
    if(q.body.performUpsert){
     const keys=q.body.performUpsert.fieldsToMergeOn;
     if(!Array.isArray(keys)||keys.length!==1||!['entity_uid','event_uid'].includes(keys[0]))throw new Error('Unsupported upsert key');
     for(const rec of incoming.filter(rec=>!rec.id)){
      const matches=await tools.mcp__codex_apps__airtable_list_records_for_table({...args,fieldIds:[field(keys[0]).id],filters:{operands:[{operator:'=',operands:[field(keys[0]).id,rec.fields[keys[0]]]}]},pageSize:2});
      if(matches.isError||!matches.structuredContent?.records||matches.structuredContent.nextCursor||matches.structuredContent.records.length>1)throw new Error('Cannot establish unique fixture upsert target');
      if(matches.structuredContent.records.some(row=>!context.written.includes(row.id)))throw new Error('Refusing upsert into pre-existing record');
     }
    }
    r=await tools.mcp__codex_apps__airtable_update_records_for_table({...args,records,fieldIds:ids,...(q.body.performUpsert?{performUpsert:{fieldIdsToMergeOn:q.body.performUpsert.fieldsToMergeOn.map(k=>field(k).id)}}:{})});
   }
 }
 if(r.isError||!r.structuredContent?.records)throw new Error('Airtable connector returned an error');
 const out=r.structuredContent.records.map(rec=>({id:rec.id,createdTime:rec.createdTime,fields:Object.fromEntries(Object.entries(rec.cellValuesByFieldId||{}).map(([id,v])=>{
   const f=t.fields.find(f=>f.id===id);if(!f)throw new Error('Unknown returned field');
   if(f.type==='singleSelect')v=v?.name??v;
   if(f.type==='multipleSelects')v=v.map(x=>x.name??x);
   if(f.type==='multipleRecordLinks')v=v.map(x=>x.id??x);
   return[f.name,v];
 }))}));
 if(q.method!=='GET')context.written=[...new Set([...context.written,...out.map(r=>r.id)])];
 context.calls=(context.calls||0)+1;
 if(parts[3]&&q.method==='GET')return{id:q.id,status:out.length?200:404,body:out[0]||{error:'NOT_FOUND'}};
 if(parts[3]&&q.method==='PATCH'&&!q.body.records)return{id:q.id,status:200,body:out[0]};
 return{id:q.id,status:200,body:{records:out,...(r.structuredContent.nextCursor?{offset:r.structuredContent.nextCursor}:{})}};
}
