-- Private recognition projection. Refreshed outside visitor requests.
CREATE TABLE IF NOT EXISTS recognize_iam_people(person_uid TEXT PRIMARY KEY, profile_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS recognize_iam_keys(kind TEXT NOT NULL, lookup_key TEXT NOT NULL, person_uid TEXT NOT NULL, expires_at INTEGER, PRIMARY KEY(kind,lookup_key,person_uid));
CREATE INDEX IF NOT EXISTS recognize_iam_lookup ON recognize_iam_keys(kind,lookup_key);
CREATE TABLE IF NOT EXISTS recognize_iam_devices(token_hash TEXT PRIMARY KEY,person_uid TEXT NOT NULL,expires_at INTEGER NOT NULL,retired INTEGER NOT NULL DEFAULT 0);
