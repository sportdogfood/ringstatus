// Bounded trial adapter. Module/field names must come from target metadata.
const EXPECTED_ORG = "941333935";
const API = "https://www.zohoapis.com/crm/v8";
const KINDS = new Set(["barns", "locations"]);
const FIELDS = new Set(["entity_uid", "barn_uid", "name", "address", "location_uid", "revision", "request_uid"]);

export class CrmInputError extends Error {
  constructor(code, status = 502, details = {}) {
    super(code);
    this.name = "CrmInputError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

export function createCrmInputStore({ token, mappings, fetchImpl = fetch } = {}) {
  if (typeof token !== "string" || !token.trim()) throw new CrmInputError("missing_crm_token", 503);
  const config = structuredClone(mappings || {});
  for (const [kind, mapping] of Object.entries(config)) {
    if (!KINDS.has(kind) || !/^RS_Trial_[A-Za-z0-9_]+$/.test(mapping?.module || "")) throw new CrmInputError("invalid_crm_mapping", 503);
    if (!mapping.fields?.entity_uid || !mapping.fields?.name) throw new CrmInputError("missing_identity_mapping", 503);
    const used = new Set();
    for (const [key, apiName] of Object.entries(mapping.fields)) {
      if (!FIELDS.has(key) || !/^[A-Za-z][A-Za-z0-9_]*$/.test(apiName) || used.has(apiName)) throw new CrmInputError("invalid_crm_field_mapping", 503);
      used.add(apiName);
    }
  }

  async function request(path, { method = "GET", body, headers = {} } = {}) {
    let response;
    try {
      response = await fetchImpl(`${API}${path}`, {
        method, redirect: "error",
        headers: { Authorization: `Zoho-oauthtoken ${token}`, "Content-Type": "application/json", ...headers },
        ...(body ? { body: JSON.stringify(body) } : {})
      });
    } catch {
      // A write timeout has an unknown commit outcome. Never retry creation here.
      throw new CrmInputError(method === "GET" ? "crm_unavailable" : "crm_write_outcome_unknown", 502);
    }
    if (response.status === 204) return { data: [], info: { more_records: false } };
    const data = await response.json().catch(() => null);
    if (response.status === 412) throw new CrmInputError("stale_revision", 409);
    if (!response.ok) throw new CrmInputError("crm_request_failed", response.status, { provider_code: data?.code || "unknown" });
    if (!data || typeof data !== "object") throw new CrmInputError("invalid_crm_response");
    return data;
  }

  function mappingFor(kind) {
    if (!KINDS.has(kind) || !config[kind]) throw new CrmInputError("unconfigured_crm_entity", 503);
    return config[kind];
  }

  async function preflight(kind) {
    const org = await request("/org");
    // Organization API has both a CRM record id and zgid; compare the observed zgid.
    if (org.org?.length !== 1 || String(org.org[0].zgid) !== EXPECTED_ORG) throw new CrmInputError("wrong_crm_organization", 503);
    const selected = kind ? [kind] : Object.keys(config);
    if (!selected.length) throw new CrmInputError("missing_crm_mappings", 503);
    for (const current of selected) {
      const mapping = mappingFor(current);
      const moduleResult = await request(`/settings/modules/${encodeURIComponent(mapping.module)}`);
      const module = moduleResult.modules?.find(item => item.api_name === mapping.module);
      if (!module?.api_supported) throw new CrmInputError("crm_module_unavailable", 503);
      const fieldResult = await request(`/settings/fields?module=${encodeURIComponent(mapping.module)}`);
      const byName = new Map((fieldResult.fields || []).map(field => [field.api_name, field]));
      for (const apiName of Object.values(mapping.fields)) {
        if (!byName.has(apiName)) throw new CrmInputError("crm_field_unavailable", 503, { field: apiName });
      }
      // Names are mutable. Duplicate prevention requires the mapped canonical ID.
      const identity = byName.get(mapping.fields.entity_uid);
      if (!identity.unique || typeof identity.unique === "object" && !Object.keys(identity.unique).length) throw new CrmInputError("crm_identity_not_unique", 503);
    }
    return { ok: true, organization: EXPECTED_ORG, kinds: selected };
  }

  function encode(mapping, entity, creating) {
    if (!entity || typeof entity !== "object" || Array.isArray(entity)) throw new CrmInputError("invalid_entity", 400);
    if (creating && (typeof entity.entity_uid !== "string" || !entity.entity_uid.trim() || typeof entity.name !== "string" || !entity.name.trim())) throw new CrmInputError("missing_entity_identity", 400);
    if (!creating && "entity_uid" in entity) throw new CrmInputError("immutable_entity_identity", 400);
    const result = {};
    for (const [key, value] of Object.entries(entity)) {
      const field = mapping.fields[key];
      if (!field) throw new CrmInputError("unmapped_entity_field", 400, { field: key });
      if (value !== null && typeof value !== "string" && typeof value !== "number") throw new CrmInputError("invalid_field_value", 400);
      if (typeof value === "number" && !Number.isFinite(value)) throw new CrmInputError("invalid_field_value", 400);
      if (key === "revision" && (!Number.isSafeInteger(value) || value < 1)) throw new CrmInputError("invalid_revision", 400);
      result[field] = value;
    }
    if (!Object.keys(result).length) throw new CrmInputError("empty_update", 400);
    return result;
  }

  function decode(mapping, row) {
    const result = { record_id: String(row.id), modified_time: row.Modified_Time || null };
    for (const [key, apiName] of Object.entries(mapping.fields)) if (apiName in row) result[key] = row[apiName];
    return result;
  }

  async function verifyRecordScope(mapping, recordId) {
    if (!/^\d+$/.test(String(recordId))) throw new CrmInputError("invalid_crm_record_id", 400);
    const result = await request(`/${encodeURIComponent(mapping.module)}/${encodeURIComponent(recordId)}?fields=${encodeURIComponent(Object.values(mapping.fields).join(","))}`);
    if (result.data?.length !== 1 || String(result.data[0].id) !== String(recordId)) throw new CrmInputError("crm_record_unavailable", 404);
    return result.data[0];
  }

  function committed(result) {
    const item = result.data?.[0];
    if (result.data?.length !== 1 || item?.status !== "success" || !item.details?.id) {
      throw new CrmInputError(item?.code === "DUPLICATE_DATA" ? "duplicate_entity" : "crm_write_failed", item?.code === "DUPLICATE_DATA" ? 409 : 502, { provider_code: item?.code || "unknown" });
    }
    return { record_id: String(item.details.id), modified_time: item.details.Modified_Time || null };
  }

  return {
    preflight,
    async list(kind) {
      const mapping = mappingFor(kind);
      await preflight(kind);
      const rows = [];
      // Bounded trial intentionally refuses silent truncation or unbounded scans.
      for (let page = 1; page <= 10; page++) {
        const result = await request(`/${encodeURIComponent(mapping.module)}?fields=${encodeURIComponent([...Object.values(mapping.fields), "Modified_Time"].join(","))}&per_page=200&page=${page}`);
        if (!Array.isArray(result.data) || result.data.some(row => !row?.id || !row[mapping.fields.entity_uid])) throw new CrmInputError("invalid_crm_response");
        rows.push(...result.data.map(row => decode(mapping, row)));
        if (!result.info?.more_records) return rows;
      }
      throw new CrmInputError("crm_trial_record_limit", 409);
    },
    async create(kind, entity) {
      const mapping = mappingFor(kind);
      const fields = encode(mapping, entity, true);
      await preflight(kind);
      const result = await request(`/${encodeURIComponent(mapping.module)}`, { method: "POST", body: { data: [fields], trigger: [], skip_feature_execution: [{ name: "cadences" }] } });
      return { ...entity, ...committed(result) };
    },
    async update(kind, recordId, patch, { modifiedTime } = {}) {
      const mapping = mappingFor(kind);
      const fields = encode(mapping, patch, false);
      if (typeof modifiedTime !== "string" || !/^\d{4}-\d{2}-\d{2}T/.test(modifiedTime) || !Number.isFinite(Date.parse(modifiedTime))) throw new CrmInputError("missing_modified_time", 400);
      await preflight(kind);
      await verifyRecordScope(mapping, recordId);
      const result = await request(`/${encodeURIComponent(mapping.module)}/${encodeURIComponent(recordId)}`, { method: "PUT", headers: { "If-Unmodified-Since": modifiedTime }, body: { data: [fields], trigger: [], skip_feature_execution: [{ name: "cadences" }] } });
      return { ...patch, ...committed(result) };
    }
  };
}
