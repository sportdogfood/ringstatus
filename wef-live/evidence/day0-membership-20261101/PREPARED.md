# Prepared day-0 membership change

November 1, 2026. Not deployed. Production JS hashes match the frozen 21-file baseline. No Airtable schema/record or task changes.

24 targeted offline checks passed in the actual node --test invocation. This is author-executed offline proof, not independent review or scheduled acceptance. Tests cover the saved 25-group / 43-appearance payload, two-miss progression, reappearance, repeated class appearances by group, incomplete inventory preservation, no false-live/Completed/has_JSON collection gates, canonical priority-zero selection, links and logging error isolation. Existing tests were copied and changed expectations are explicit; original release tests remain intact.

Candidate lanes.js removes collection skips on false/failed live status, Completed groups and has_JSON=false; selects canonical Shows day-priority 0 without a freshness/entry-count gate. Pure membership.js supplies the transition rule and per-group appearance identities. It is deliberately not wired to Airtable until required additive storage is approved. No complete-integration claim.

## Exact prerequisite

Current live-classes keys identify 41 unique details, not the 43 group-class appearances. Changing those keys would disrupt current entry/schedule links. Proposed additive storage is specified in schema-proposal.json: record-status on live-groups, plus live-class-appearances with one row per show/date/group/class and links to existing details/groups. No existing keys or records are replaced.

Root owner AGENTS rule: approval does not authorize extra schema/contract changes; a necessary expansion requires a concrete proposal and explicit approval before implementation. This is the reason schema creation/integration and deployment remain pending. Do not add schema, deploy a partial candidate or mark this complete before that decision.

Stages 8/9 stay on hold. No observers, manual collections, record repairs, provider calls, new scheduler, dependency upgrades or other collectors' changes.
