-- Control metadata only. User entities and event detail remain in Airtable.
-- Do not expire an uncertain claim: delayed retries must not recreate writes.
CREATE TABLE IF NOT EXISTS recognize_claims (
  claim_key TEXT PRIMARY KEY,
  state TEXT NOT NULL CHECK (state IN ('claimed', 'complete')),
  result_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
