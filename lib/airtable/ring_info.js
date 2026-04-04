/**
 * Airtable Automation: When record enters view → GET /ring/{ring_id}?customer_id=15
 * Writes back ONLY:
 *   ring_name, ring_number, ring_id
 */

const CUSTOMER_ID = 15;
const BASE_URL = "https://broad-tooth-b8ed.gombcg.workers.dev/ring";

const TABLE_NAME = "watch_rings"; // <-- CHANGE to your table name

const FIELD_RING_ID = "ring_id";
const FIELD_RING_NAME = "ring_name";
const FIELD_RING_NUMBER = "ring_number";

// INPUTS from Automation
const { recordId, ring_id } = input.config();

if (!recordId) throw new Error("Missing input: recordId");
if (!ring_id) throw new Error("Missing input: ring_id (map your {ring_id} field into the script input)");

const table = base.getTable(TABLE_NAME);

// Build endpoint: /ring/{ring_id}?customer_id=15
const url = `${BASE_URL}/${encodeURIComponent(ring_id)}?customer_id=${encodeURIComponent(CUSTOMER_ID)}`;

let res;
try {
  res = await fetch(url, { method: "GET" });
} catch (err) {
  throw new Error(`Fetch failed: ${err.message}`);
}

if (!res.ok) {
  const txt = await res.text().catch(() => "");
  throw new Error(`Endpoint error ${res.status}: ${txt}`);
}

let data;
try {
  data = await res.json();
} catch (err) {
  throw new Error(`Invalid JSON response: ${err.message}`);
}

// Expecting payload.ring.{ring_name, ring_number, ring_id}
const ring = data?.ring;
if (!ring) throw new Error("Response missing `ring` object");

const out = {
  [FIELD_RING_ID]: ring.ring_id ?? ring_id,
  [FIELD_RING_NAME]: ring.ring_name ?? null,
  [FIELD_RING_NUMBER]: ring.ring_number ?? null,
};

await table.updateRecordAsync(recordId, out);

output.set("endpoint", url);
output.set("updated", out);
