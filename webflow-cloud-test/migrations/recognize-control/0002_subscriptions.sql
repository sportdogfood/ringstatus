-- Subscription write reservations only. Airtable remains the entity/audit store.
-- No expiry: a crashed or uncertain worker must not overlap a delayed external write.
-- Parent owns applying this additive migration to RS_RECOGNITION_CONTROL_DB.
CREATE TABLE IF NOT EXISTS subscription_write_reservations (
  actor_uid TEXT PRIMARY KEY,
  request_uid TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  worker_token TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('working', 'uncertain', 'idle')),
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
