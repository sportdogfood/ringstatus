'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { localDay } = require('../wef-timing/purge');
const { instant } = require('../wef-timing/class-timing');
const { base, CLASS_BASE, TABLES, unique } = require('./linker');
const { snapshots } = require('./trip-status');
const cleaner = require('./cleanup');

const calculatedFields = ['calculated-order-of-go','calculated-estimated-go-time','calculated-at-time','go-time-start-in','go-time-end-in'];
const auditTables = ['runs','errors','heartbeat'];
const logTables = { 'run-logs': 'runs', heartbeat: 'heartbeat', 'errs-log': 'errors' };
const quote = JSON.stringify;
const eq = (field, value) => `{${field}}=${quote(value)}`;
const and = (...conditions) => `AND(${conditions.join(',')})`;
const any = conditions => `OR(${conditions.join(',')})`;
const dateField = day => `DATETIME_FORMAT({date},'YYYY-MM-DD')=${quote(day)}`;
const byIds = ids => any(ids.map(id => `RECORD_ID()=${quote(id)}`));
const prefixed = (key, prefix) => `LEFT({${key}},${prefix.length})=${quote(prefix)}`;
const chunks = function* (rows) { for (let i = 0; i < rows.length; i += 10) yield rows.slice(i, i + 10); };
const blank = value => value === '' ? null : value ?? null;
const same = (a,b) => JSON.stringify(blank(a)) === JSON.stringify(blank(b)) || (instant(a) !== null && instant(a) === instant(b));
const matches = (row, fields) => row && Object.entries(fields).every(([name,value]) => same(row.fields[name], value));

function airtable({ token = process.env.WEF_TIMING_AIRTABLE_TOKEN, request = fetch, deadline, pause = ms => new Promise(resolve => setTimeout(resolve, ms)) }) {
  if (!token) throw Error('Timing Airtable token missing');
  async function call(table, method, body, query = '') {
    if (!Object.hasOwn(TABLES, table)) throw Error('Unknown Trip table');
    if (method === 'DELETE' && !auditTables.includes(table)) throw Error('Trip deletion target forbidden');
    if (method === 'PATCH' && !['snapshots','trips',...auditTables].includes(table)) throw Error('Trip write target forbidden');
    if (table === 'trips' && method === 'PATCH' && body.records.some(row => !row.id || Object.keys(row.fields).some(name => !calculatedFields.includes(name)))) throw Error('Only Trip calculated fields may be updated');
    await pause(210);
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw Error('Trip runner deadline reached');
    const target = table === 'classStatus' ? CLASS_BASE : base;
    const response = await request(`https://api.airtable.com/v0/${target}/${TABLES[table]}${query}`, {
      method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(Math.min(30000, remaining)),
    });
    if (!response.ok) throw Error(`Trip Airtable ${method} ${table} HTTP ${response.status}`);
    return response.json();
  }
  return {
    base,
    async list(table, formula, latestField) {
      const rows = [];
      let offset;
      do {
        const q = new URLSearchParams({ pageSize:'100', ...(formula ? { filterByFormula:formula } : {}), ...(offset ? { offset } : {}) });
        if (latestField) {
          q.set('maxRecords','1');
          q.set('sort[0][field]',latestField);
          q.set('sort[0][direction]','desc');
        }
        const result = await call(table, 'GET', null, '?' + q);
        rows.push(...result.records);
        offset = result.offset;
      } while (offset);
      return rows;
    },
    upsert: (table, key, fields) => call(table, 'PATCH', { performUpsert:{ fieldsToMergeOn:[key] }, records:fields.map(value => ({ fields:value })) }),
    update: (table, records) => call(table, 'PATCH', { records }),
    async remove(table, ids) {
      for (const batch of chunks(ids)) {
        const q = new URLSearchParams();
        for (const id of batch) q.append('records[]', id);
        await call(table, 'DELETE', null, '?' + q);
      }
    },
  };
}

function stateStore(runtime) {
  const file = path.join(runtime, 'state.json');
  return {
    load: () => fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : {},
    save(state) {
      fs.mkdirSync(runtime, { recursive: true });
      const temp = file + '.' + crypto.randomUUID();
      fs.writeFileSync(temp, JSON.stringify(state), { flag: 'wx', flush: true });
      fs.renameSync(temp, file);
    },
  };
}

async function upsertVerified(api, table, key, fields) {
  await api.upsert(table, key, fields);
  const saved = await api.list(table, any(fields.map(row => eq(key, row[key]))));
  if (saved.length !== fields.length || fields.some(f => !matches(unique(saved.filter(row => row.fields[key] === f[key])), f))) throw Error(`${table} readback mismatch`);
}

async function cleanLogs({ api, state, save, show, now, runId, deadline }) {
  const control = state.cleanup || (state.cleanup = {});
  if (control.pending) return { ok:false, reason:'Unfinished Trip cleanup intent requires reconciliation', deleted:0 };
  if (deadline - Date.now() < 30000) return { ok:true, reason:'Cleanup deferred by runner budget', deleted:0 };
  const event = cleaner.due(control, show, now);
  const controls = await api.list('cleanup');
  const checked = cleaner.LOGS.filter(name => controls.filter(row => (row.fields['\uFEFFName'] ?? row.fields.Name) === name && row.fields.cleanup === 'checked').length === 1);
  if (!checked.length) return { ok:true, deleted:0, reason:'No supported checked tables' };
  const rows = {};
  for (const name of cleaner.LOGS) rows[name] = await api.list(logTables[name]);
  const resolved = control.resolvedErrorIds || [];
  const protection = { complete:true, pending:false, recordIds:[], resolvedErrorIds:resolved,
    runIds:[runId, ...rows.heartbeat.map(row => row.fields['run-id']),
      ...rows['run-logs'].filter(row => !row.fields.completed || row.fields.result === 'Running').map(row => row.fields['run-id']),
      ...rows['errs-log'].filter(row => !resolved.includes(row.id)).map(row => row.fields['run-id'])].filter(Boolean) };
  const trims = control.trims || (control.trims = {});
  const actions = [];
  for (const name of checked) {
    const table = logTables[name];
    const transport = { base, async read(_name, ids) {
        const records=[];
        for(let i=0;i<ids.length;i+=50) records.push(...await api.list(table,byIds(ids.slice(i,i+50))));
        return records;
      }, remove: (_name, ids) => api.remove(table, ids),
      beginIntent: async intent => { control.pending = { name, ids:intent.records.map(row => row.id), event }; save(); return name; },
      finishIntent: async (_id, result) => { trims[name] = result.trimPending; delete control.pending; save(); } };
    const result = await cleaner.cleanup({ api:transport, table:name, rows:rows[name], event, now, checked:true, protection, pendingTrim:trims[name] });
    trims[name] = result.trimPending;
    actions.push({ table:name, ...result });
    save();
  }
  control.show = show;
  if (event.trigger === 'daily-1900' || event.dailyDue) control.dailyDay = event.day;
  save();
  return { ok:true, event:event.trigger, deleted:actions.reduce((total,a) => total+a.deleted,0), actions };
}

async function run({ deadline=Date.now()+160000, now=new Date().toISOString(), runId=crypto.randomUUID(),
  api=airtable({ deadline }), store=stateStore(path.join(__dirname,'runtime')), clock=()=>new Date().toISOString() }={}) {
  const day=localDay(now), state=store.load(), save=()=>store.save(state);
  const report={ run_id:runId, started:now, day, ok:false, evaluated:0, snapshots:0, marked:0, estimated:0, issues:[] };
  let show;
  const runLog = { 'run-id':runId, date:day, started:now, result:'Running' };
  const audit = async (label, work) => {
    try { await work(); }
    catch (error) { report.ok=false; report.issues.push({ reason:`${label}: ${error.message}` }); }
  };
  try {
    const selected=unique((await api.list('shows')).filter(row => row.fields.is_default === true));
    show=Number(selected?.fields.show_id);
    const customer=Number(selected?.fields.customer_id), prefix=`${customer}|${show}|${day}|`;
    if (!Number.isSafeInteger(show) || show<=0 || !Number.isSafeInteger(customer) || customer<=0 || selected.fields['show-key']!==`${customer}|${show}`) throw Error('Default show missing or ambiguous');
    report.show=show;
    runLog['show-id']=show;
    await upsertVerified(api,'runs','run-id',[runLog]);
    const dayRow=unique(await api.list('days',and(eq('show-day-key',`${customer}|${show}|${day}`),dateField(day))));
    if (!dayRow) {
      report.reason='No verified show-day for today';
    } else {
      const input={};
      for (const name of ['trips','rings','groups','classes']) input[name]=await api.list(name,eq('date',day));
      input.schedules=await api.list('schedules',prefixed('schedule-key',prefix));
      input.ringStatus=[];
      for(const ring of input.rings.filter(row=>row.fields['ring-key']?.startsWith(`${show}|${day}|`))) {
        input.ringStatus.push(...await api.list('ringStatus',and(eq('scope-key',`ring-current-group|${prefix}${ring.fields['ring-id']}`),`{evaluated-at}<=${quote(now)}`),'evaluated-at'));
      }
      input.classStatus=await api.list('classStatus',and(eq('show-id',show),dateField(day)));
      const cycle=state.cycle?.show===show&&state.cycle.day===day ? state.cycle : null;
      const priorCycles=[...new Set([runId,cycle?.id,cycle?.id===runId?cycle.prior:null].filter(Boolean))];
      const previous=await api.list('snapshots',and(eq('show-id',show),dateField(day),any(priorCycles.map(id=>`RIGHT({snapshot-key},${id.length+1})=${quote('|'+id)}`))));
      const { output, issues }=snapshots({ input, customer, show, day, at:now, runId, previous });
      report.evaluated=output.length;
      report.issues.push(...issues);
      for (const batch of chunks(output)) {
        const rows=batch.map(item=>item.snapshot);
        const fresh=rows.filter(row=>!previous.some(p=>p.fields['snapshot-key']===row['snapshot-key']));
        for (const row of rows) {
          const saved=previous.filter(p=>p.fields['snapshot-key']===row['snapshot-key']);
          if (saved.length && !matches(unique(saved),row)) throw Error('Existing Trip cycle snapshot differs');
        }
        if (fresh.length) await upsertVerified(api,'snapshots','snapshot-key',fresh);
        report.snapshots+=rows.length;
        report.estimated+=rows.filter(row=>row['estimated-go-time']).length;
        const ids=batch.map(item=>item.trip.id), latest=await api.list('trips',byIds(ids));
        const writes=[];
        for (const item of batch) {
          const current=unique(latest.filter(row=>row.id===item.trip.id));
          const sourceFields=['trip-key','last-seen','source-updated-at','record-status','order-of-go','scratch','gone','gone-in'];
          if (!matches(current,Object.fromEntries(sourceFields.map(field=>[field,item.trip.fields[field]])))) {
            report.issues.push({ key:item.trip.fields['trip-key'], reason:'Trip source changed before calculated fields could be saved' });
            continue;
          }
          writes.push({ id:item.trip.id, fields:item.calculated });
        }
        if (writes.length) {
          await api.update('trips',writes);
          const saved=await api.list('trips',byIds(writes.map(row=>row.id)));
          if (saved.length!==writes.length || writes.some(row=>!matches(unique(saved.filter(r=>r.id===row.id)),row.fields))) throw Error('Calculated Trip fields readback mismatch');
          report.marked+=writes.length;
        }
      }
      state.cycle={ show,day,id:runId,prior:cycle?.id===runId?cycle.prior:cycle?.id };
      save();
    }
    report.ok=true;
  } catch (error) { report.issues.push({ reason:error.message }); }
  report.completed=clock();
  for (const batch of chunks(report.issues.map((issue,index)=>({ 'error-key':`${runId}|${index}`, 'run-id':runId, date:day,
    'occurred-at':report.completed, ...(show?{'show-id':show}:{}),message:issue.key?`${issue.key}: ${issue.reason}`:issue.reason })))) {
    await audit('Error log',()=>upsertVerified(api,'errors','error-key',batch));
  }
  const result=()=>report.ok?(report.issues.length?'Partial':report.reason?'Skipped':'Success'):'Failed';
  await audit('Run log',()=>upsertVerified(api,'runs','run-id',[{...runLog,completed:report.completed,result:result(),summary:JSON.stringify(report)}]));
  await audit('Heartbeat',()=>upsertVerified(api,'heartbeat','heartbeat-key',[{ 'heartbeat-key':'trip-calculator','run-id':runId,date:day,
    ...(show?{'show-id':show}:{}),started:now,completed:report.completed,'checked-at':report.completed,result:result(),summary:JSON.stringify(report) }]));
  if (Number.isSafeInteger(show)&&show>0) {
    try { report.cleanup=await cleanLogs({ api,state,save,show,now:clock(),runId,deadline }); }
    catch(error) { report.cleanup={ ok:false,reason:error.message }; }
  }
  return report;
}

module.exports = { run, airtable, cleanLogs };
