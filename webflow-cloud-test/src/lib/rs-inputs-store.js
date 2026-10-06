import { InputError } from './rs-inputs.js';
import { createAirtableInputStore } from './rs-inputs-airtable.js';
import { createCrmInputStore, ACCOUNTS_TRIAL_PREFIX, ACCOUNTS_TRIAL_MAPPING } from './rs-inputs-crm.js';
import { runtimeZohoTokenProvider } from './rs-inputs-zoho-auth.js';

// Verified Ringstatus Accounts field API names, 2026-10-06. Barns alone enter this
// bounded trial; recognition, roster records and the existing audit stay Airtable.
export const CRM_BARN_PREFIX = ACCOUNTS_TRIAL_PREFIX;
export const CRM_BARN_MAPPING = { barns: ACCOUNTS_TRIAL_MAPPING };

export function createCrmBarnInputStore({ crm, airtable, writeMode }) {
  function decode(row) {
    const id = typeof row.entity_uid === 'string' && row.entity_uid.startsWith(CRM_BARN_PREFIX)
      ? row.entity_uid.slice(CRM_BARN_PREFIX.length) : '';
    if (!/^rs_[a-f0-9]{32}$/.test(id) || typeof row.name !== 'string' || !row.name.trim() ||
        typeof row.owner_uid !== 'string' || !row.owner_uid || !Number.isSafeInteger(row.revision) || row.revision < 1 ||
        !/^[a-f0-9]{64}$/.test(row.request_uid || '') || !/^[a-f0-9]{64}$/.test(row.request_hash || '') ||
        !/^\d+$/.test(row.record_id || '') || !Number.isFinite(Date.parse(row.modified_time))) {
      throw new InputError('invalid_crm_barn_record', 502);
    }
    return { id, barnId: id, name: row.name, ownerUid: row.owner_uid, revision: row.revision,
      requestUid: row.request_uid, requestHash: row.request_hash };
  }
  async function rows() {
    const result = await crm.list('barns');
    const decoded = result.map(row => ({ source: row, record: decode(row) }));
    if (new Set(decoded.map(row => row.record.id)).size !== decoded.length) throw new InputError('ambiguous_entity_id', 409);
    return decoded;
  }
  function translate(error) {
    if (error.code === 'stale_revision') throw new InputError('record_changed', 409);
    if (error.code === 'crm_write_outcome_unknown') throw new InputError('write_outcome_unknown', 503);
    throw error;
  }
  return {
    requestNamespace: 'crm-barns',
    capabilities: { atomicCompareAndSwap: false, atomicAudit: false, usage: 'isolated-trial-only' },
    async list(kind) {
      return kind === 'barn' ? (await rows()).map(row => row.record) : airtable.list(kind);
    },
    async put(kind, record, { expectedRevision } = {}) {
      if (kind !== 'barn') return airtable.put(kind, record, { expectedRevision });
      if (writeMode !== 'isolated-trial') throw new InputError('storage_concurrency_not_qualified', 503);
      const current = (await rows()).find(row => row.record.id === record.id);
      if (current ? current.record.revision !== expectedRevision : expectedRevision !== undefined) throw new InputError('record_changed', 409);
      if (!/^rs_[a-f0-9]{32}$/.test(record.id) || record.barnId !== record.id ||
          record.revision !== (current?.record.revision || 0) + 1 ||
          current && current.record.ownerUid !== record.ownerUid) throw new InputError('invalid_crm_barn_record', 409);
      const values = { name: record.name, owner_uid: record.ownerUid, revision: record.revision,
        request_uid: record.requestUid, request_hash: record.requestHash };
      try {
        if (current) {
          const { owner_uid, ...patch } = values;
          await crm.update('barns', current.source.record_id, patch, { modifiedTime: current.source.modified_time });
        }
        else await crm.create('barns', { entity_uid: `${CRM_BARN_PREFIX}${record.id}`, ...values });
      } catch (error) { translate(error); }
    },
    // Keep request replay evidence separate when the explicit storage target changes.
    event(eventId) { return airtable.event(`crm-barns:${eventId}`); },
    appendEvent(event) { return airtable.appendEvent({ ...event, eventId: `crm-barns:${event.eventId}` }); }
  };
}

export function createInputStore({ env, fetchImpl = fetch }) {
  const mode = env.RS_INPUTS_BARN_STORAGE || 'airtable';
  if (!['airtable', 'zoho-crm'].includes(mode)) throw new InputError('invalid_barn_storage', 503);
  const airtable = createAirtableInputStore({ env, fetchImpl });
  if (mode === 'airtable') return airtable;
  const crm = createCrmInputStore({ getToken: runtimeZohoTokenProvider(env, fetchImpl), mappings: CRM_BARN_MAPPING, fetchImpl });
  return createCrmBarnInputStore({ crm, airtable, writeMode: env.RS_INPUTS_WRITE_MODE });
}
