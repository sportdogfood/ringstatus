const assert = require('node:assert/strict');
const {cleanup} = require('../../../cleanup');
const {base} = require('../../../schema.json');
const now = '2026-11-08T23:00:00Z';
const show = 200000077;
const makeRow = i => ({id: 'rec'+String(i).padStart(14,'0'), createdTime: new Date(Date.parse(now)-10000+i).toISOString(), fields: {result:'Success', completed:now, 'run-id':'run-'+i}});
const rows = Array.from({length:1000}, (_,i)=>makeRow(i));
const saved = new Map(rows.map(r=>[r.id,r]));
let persisted;
const api = {
  base,
  read: async (_table, ids) => ids.map(id=>saved.get(id)).filter(Boolean),
  beginIntent: async () => 'cleanup-intent',
  remove: async (_table, ids) => {
    if (!persisted) { const newRow = makeRow(1000); saved.set(newRow.id,newRow); }
    for (const id of ids) saved.delete(id);
  },
  finishIntent: async (_intent, result) => { persisted = result; },
};
const args = {api, table:'run-logs', rows, event:{trigger:'log-limit',show,day:'2026-11-08'}, now, checked:true, protection:{complete:true,pending:false,recordIds:[],runIds:[],resolvedErrorIds:[]}};
(async()=>{
  const first = await cleanup(args);
  const second = await cleanup({...args, rows:[...saved.values()], pendingTrim:persisted.trimPending});
  console.log(JSON.stringify({first, second, actualRows:saved.size, expectedMaximum:500}));
  assert(saved.size <= 500, 'A later normal check must continue an unfinished trim after a concurrent log append');
})().catch(error=>{ console.error(error); process.exitCode=1; });
