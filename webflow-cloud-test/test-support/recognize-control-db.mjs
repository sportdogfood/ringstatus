import { DatabaseSync } from 'node:sqlite';
import { readFileSync } from 'node:fs';
import { beforeEach, afterEach } from 'node:test';
// Unit-test binding backed by real SQLite. Workers/D1 behavior is separately
// exercised with two Miniflare Workers and persistent storage.
export function controlDatabase() {
  const sql = new DatabaseSync(':memory:');
  sql.exec(readFileSync(new URL('../migrations/recognize-control/0001_claims.sql', import.meta.url), 'utf8'));
  return { close: () => sql.close(), prepare(text) {
    return { bind(...values) {
      return { async run() { const result = sql.prepare(text).run(...values); return { meta: { changes: result.changes } }; },
        async first() { return sql.prepare(text).get(...values) || null; } };
    } };
  } };
}
export function installControlDatabase(...bindings) {
  let db;
  beforeEach(() => { db = controlDatabase(); for (const env of bindings) env.RS_RECOGNITION_CONTROL_DB = db; });
  afterEach(() => db.close());
}
