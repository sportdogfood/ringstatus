"use strict";
// Local packaging only. This file does not call Airtable or execute business work.
const fs = require('node:fs'), path = require('node:path');
function buildEvents(schema, anchor) {
  const table = name => {const t = schema.tables.find(t => t.name === name); if (!t) throw Error('Missing table ' + name); return t;};
  const fields = (name, names) => names.map(n => {const f = table(name).fields.find(f => f.name === n); if (!f) throw Error('Missing field ' + name + '.' + n); return f.id;});
  const readScript = n => fs.readFileSync(path.join(__dirname, n), 'utf8');
  const script = ['event-core.js', 'events.js', 'linker.js'].map(readScript).join('\n') + `
const cfg = input.config();
await resultsLogged(base, cfg.operation, async () => {
  if (cfg.operation === 'event') {const result = await resultsEvent(base, cfg); await linkResults(base); return result;}
  const result = await resultsEvent(base, {reconcile: true}); await linkResults(base); return result;
});
`;
  const define = (name, trigger, config, source = script) => ({name, trigger, nodes: [{key: 'results', type: 'customScript', inputs: {script: source, inputObj: config}}]});
  const definitions = [];
  for (const source of ['wef-data.trips', 'wef-data.results']) {
    const watch = source.endsWith('trips') ? ['trip-key', 'class', 'entry', 'horse', 'rider', 'gone-in', 'source-json', 'class_related_data.results_verified (from class)'] :
      ['result-key', 'trip', 'class', 'entry', 'score1', 'score2', 'score3', 'score4', 'score5', 'score6', 'time-one', 'time-two', 'time-three', 'faults-one', 'faults-two', 'faults-three', 'time-fault-one', 'time-fault-two', 'time-fault-three', 'placing', 'position', 'points-earned', 'prize-money'];
    for (const type of ['recordCreated', 'recordUpdated']) definitions.push(define('Results events — ' + source + ' ' + (type === 'recordCreated' ? 'created' : 'changed'),
      {type, inputs: {tableId: table(source).id, ...(type === 'recordUpdated' ? {watchFields: fields(source, watch)} : {})}},
      {operation: 'event', source, recordId: {$ref: 'trigger', path: ['id']}}));
  }
  definitions.push(define('Results events — reconcile and empty links', {type: 'cron', inputs: {type: 'minutely', config: {width: 10}, start: anchor}}, {operation: 'reconcile'}));
  const cleanupScript = ['event-core.js', 'cleaner.js'].map(readScript).join('\n') +
    '\nawait resultsLogged(base, "cleanup", () => cleanResults(base));\n';
  definitions.push(define('Results cleanup — normal run', {type: 'cron', inputs: {type: 'minutely', config: {width: 10}, start: anchor}}, {}, cleanupScript));
  return {script, definitions};
}
if (require.main === module) {
  const [schemaPath, outputPath, anchor] = process.argv.slice(2);
  const result = buildEvents(JSON.parse(fs.readFileSync(schemaPath, 'utf8').replace(/^\uFEFF/, '')), anchor);
  fs.writeFileSync(outputPath, JSON.stringify(result, null, 2) + '\n');
}
module.exports = {buildEvents};
