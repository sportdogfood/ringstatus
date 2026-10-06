import test from 'node:test';
import assert from 'node:assert/strict';
import { handleInputRoute } from '../src/pages/rs-inputs/[operation].js';

test('existing Recognize source works before the clean input base is configured', async () => {
  const calls = [];
  const personId = 'rec' + 'p'.repeat(14);
  const result = await handleInputRoute({
    request: new Request('https://example.test/test/rs-inputs/recognition?device_token=fixture-token'),
    locals: { rsInputActor: { id: 'fixture-actor', profile: { personUid: 'fixture-person' } } }
  }, { RS_INPUTS_RECOGNITION_BASE_ID: 'apptdhhNzduxm5gjn', AIRTABLE_TOKEN: 'fixture-only' }, async url => {
    calls.push(String(url));
    return Response.json(String(url).includes('rs_devices_test')
      ? { records: [{ id: 'rec' + 'd'.repeat(14), fields: { status: 'test', person: [personId] } }] }
      : { id: personId, fields: { person_uid: 'fixture-person', person_name: 'Fixture', status: 'test', access_level: 'member' } });
  });
  assert.equal(result.status, 200);
  assert.equal((await result.json()).profile.person_uid, 'fixture-person');
  assert.equal(calls.length, 2);
  assert.ok(calls.every(url => url.includes('/apptdhhNzduxm5gjn/')));
});

test('recognition binding does not permit input records in the existing operational base', async () => {
  const response = await handleInputRoute({ request: new Request('https://example.test/test/rs-inputs/state'), locals: { rsInputActor: { id: 'fixture-actor' } } }, {
    RS_INPUTS_RECOGNITION_BASE_ID: 'apptdhhNzduxm5gjn', RS_INPUTS_BASE_ID: 'apptdhhNzduxm5gjn', AIRTABLE_TOKEN: 'fixture-only'
  }, () => assert.fail('No provider request is allowed'));
  assert.equal(response.status, 503);
  assert.equal((await response.json()).error, 'clean_input_base_required');
});

test('existing-base binding does not create an authenticated actor', async () => {
  const response = await handleInputRoute({ request: new Request('https://example.test/test/rs-inputs/recognition?device_token=fixture-token'), locals: {} }, { RS_INPUTS_RECOGNITION_BASE_ID: 'apptdhhNzduxm5gjn', AIRTABLE_TOKEN: 'fixture-only' }, () => assert.fail('No unauthenticated provider read'));
  assert.equal(response.status, 401);
});
