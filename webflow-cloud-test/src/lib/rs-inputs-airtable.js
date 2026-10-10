import { InputError } from './rs-inputs.js';

const INPUT_BASE = 'app9kOZdIaGyKk5uG';
const tables = { barn: 'tblRvTwo3HYPUkZou', users: 'tblYgoeLEey05xgw9', riders: 'tblnd2ToLs7dTzLAM', horses: 'tblpyyaOMgjLzLvkP', locations: 'tblEuwOr1rUKnj1j3' };
const fields = { id: 'entity_uid', barnId: 'barn_uid', name: 'name', email: 'email', userId: 'user_uid', riderId: 'rider_uid', locationId: 'location_uid', address: 'address', recognitionPersonId: 'recognition_person_uid', revision: 'revision', ownerUid: 'owner_uid', requestUid: 'request_uid', requestHash: 'request_hash' };
const escape = value => String(value).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
export function createAirtableInputStore({ env, fetchImpl = fetch, minimumIntervalMs = 225 }) {
  const base = env.RS_INPUTS_BASE_ID;
  if (base !== INPUT_BASE) throw new InputError('clean_input_base_required', 503);
  if (!env.AIRTABLE_TOKEN) throw new InputError('storage_credentials_missing', 503);
  let lastRequestAt = 0;
  async function call(table, { method = 'GET', body, formula } = {}) {
    const url = new URL(`https://api.airtable.com/v0/${base}/${encodeURIComponent(table)}`);
    if (formula) url.searchParams.set('filterByFormula', formula);
    const all = [];
    do {
      // Pace this isolated trial's sequential requests. This is not a global
      // rate limiter or a concurrency guarantee across Worker instances.
      const pause = Math.max(0, minimumIntervalMs - (Date.now() - lastRequestAt));
      if (pause) await new Promise(resolve => setTimeout(resolve, pause));
      lastRequestAt = Date.now();
      let response;
      try { response = await fetchImpl(url, { method, redirect: 'manual', headers: { Authorization: `Bearer ${env.AIRTABLE_TOKEN}`, 'Content-Type': 'application/json' }, ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(15000) }); }
      catch { throw new InputError(method === 'GET' ? 'storage_unavailable' : 'write_outcome_unknown', 503); }
      if (!response.ok) throw new InputError(method === 'GET' ? 'storage_unavailable' : 'write_outcome_unknown', 503);
      let result;
      try { result = await response.json(); } catch { throw new InputError(method === 'GET' ? 'storage_unavailable' : 'write_outcome_unknown', 503); }
      if (!Array.isArray(result.records)) throw new InputError('invalid_storage_response', 502);
      all.push(...result.records);
      if (!result.offset || method !== 'GET') break;
      url.searchParams.set('offset', result.offset);
    } while (true);
    return all;
  }
  function writable() {
    if (env.RS_INPUTS_WRITE_MODE !== 'isolated-trial') throw new InputError('storage_concurrency_not_qualified', 503);
  }
  const decode = row => Object.fromEntries(Object.entries(fields).filter(([, field]) => row.fields?.[field] !== undefined).map(([key, field]) => [key, row.fields[field]]));
  return {
    capabilities: { atomicCompareAndSwap: false, atomicAudit: false, usage: 'isolated-trial-only' },
    async list(kind) {
      if (!tables[kind]) throw new InputError('invalid_kind');
      const result = (await call(tables[kind])).map(decode);
      if (new Set(result.map(row => row.id)).size !== result.length || result.some(row => !row.id || !row.barnId || !row.name || !Number.isInteger(row.revision))) throw new InputError('invalid_storage_records', 502);
      return result;
    },
    async put(kind, record, { expectedRevision } = {}) {
      writable();
      if (!tables[kind]) throw new InputError('invalid_kind');
      const matches = await call(tables[kind], { formula: `{entity_uid} = '${escape(record.id)}'` });
      if (matches.length > 1) throw new InputError('ambiguous_entity_id', 409);
      const current = matches[0];
      if (current && current.fields.revision !== expectedRevision) throw new InputError('record_changed', 409);
      if (!current && expectedRevision !== undefined) throw new InputError('record_changed', 409);
      const mapped = Object.fromEntries(Object.entries(fields).filter(([key]) => record[key] !== undefined).map(([key, field]) => [field, record[key]]));
      // This writer is qualified only for isolated-trial. Classify new records
      // using existing schema choices; never infer or overwrite legacy lifecycle.
      // Lifecycle does not grant access or consent, and is not caller-controlled.
      if (!current) Object.assign(mapped, { status: 'Active', record_mode: 'Test' });
      // Upsert stabilizes retries; this read + write is NOT an atomic conditional update.
      await call(tables[kind], { method: 'PATCH', body: { performUpsert: { fieldsToMergeOn: ['entity_uid'] }, records: [{ fields: mapped }] } });
    },
    async event(eventId) {
      const rows = await call('tblwts3huk3w1ACjh', { formula: `{event_uid} = '${escape(eventId)}'` });
      if (rows.length > 1) throw new InputError('ambiguous_audit_event', 409);
      if (!rows[0]) return null;
      try { return { inputHash: rows[0].fields.input_hash, record: JSON.parse(rows[0].fields.result_json) }; }
      catch { throw new InputError('invalid_audit_record', 502); }
    },
    async appendEvent(event) {
      writable();
      const existing = await call('tblwts3huk3w1ACjh', { formula: `{event_uid} = '${escape(event.eventId)}'` });
      if (existing.length > 1) throw new InputError('ambiguous_audit_event', 409);
      // Preserve existing audit classification, including unclassified legacy
      // rows. This preflight does not provide cross-instance atomicity.
      await call('tblwts3huk3w1ACjh', { method: 'PATCH', body: { performUpsert: { fieldsToMergeOn: ['event_uid'] }, records: [{ fields: { event_uid: event.eventId, actor_uid: event.actorId, entity_uid: event.record.id, barn_uid: event.barnId, action: event.action, kind: event.kind, request_uid: event.requestId, input_hash: event.inputHash, result_json: JSON.stringify(event.record), occurred_at: event.occurredAt, outcome: 'committed', ...(!existing.length ? { record_mode: 'Test' } : {}) } }] } });
    }
  };
}
