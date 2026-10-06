import test from 'node:test';
import assert from 'node:assert/strict';
import { relayAirtableRequest } from '../test-support/rs-inputs-mcp-controller.mjs';

function fixture(existing, written = []) {
  let mutations = 0;
  const context = { written, calls: 0, tables: [{ id: 'tblFixture', name: 'rs_input_barns', fields: [{ id: 'fldEntity', name: 'entity_uid', type: 'singleLineText' }, { id: 'fldOwner', name: 'owner_uid', type: 'singleLineText' }] }], tools: {
    async mcp__codex_apps__airtable_list_records_for_table(args) {
      assert.deepEqual(args.filters.operands[0].operands, ['fldEntity', 'rs_requested_canonical_id']);
      return { structuredContent: { records: existing } };
    },
    async mcp__codex_apps__airtable_update_records_for_table(args) {
      mutations++;
      return { structuredContent: { records: [{ id: existing[0]?.id || 'recNewFixture', cellValuesByFieldId: args.records[0].fields }] } };
    }
  } };
  const request = { id: 1, url: 'https://api.airtable.com/v0/app9kOZdIaGyKk5uG/rs_input_barns', method: 'PATCH', body: { performUpsert: { fieldsToMergeOn: ['entity_uid'] }, records: [{ fields: { entity_uid: 'rs_requested_canonical_id', owner_uid: 'rs_live_20261005_2215_actor' } }] } };
  return { context, request, mutations: () => mutations };
}
test('connector relay rejects a pre-existing upsert target even with the fixture owner', async () => {
  const f = fixture([{ id: 'recPreexisting' }]);
  await assert.rejects(relayAirtableRequest(f.request, f.context), /pre-existing record/);
  assert.equal(f.mutations(), 0);
});
test('connector relay permits upsert of a record created by this test run', async () => {
  const f = fixture([{ id: 'recOwned' }], ['recOwned']);
  assert.equal((await relayAirtableRequest(f.request, f.context)).body.records[0].id, 'recOwned');
  assert.equal(f.mutations(), 1);
});
test('connector relay permits a new fixture upsert and records its provider ID', async () => {
  const f = fixture([]);
  assert.equal((await relayAirtableRequest(f.request, f.context)).body.records[0].id, 'recNewFixture');
  assert.deepEqual(f.context.written, ['recNewFixture']);
  assert.equal(f.mutations(), 1);
});
