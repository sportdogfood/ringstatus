# Current scope: link the schedules shared into Entries

Only active local shared schedules determine what Entries processes. Accepted Data supplies matching participation/group evidence; unrelated Data failures do not block supported links. No source-base schedule parity or dated-health replay. Missing evidence is reported and processing continues; ambiguous or duplicate schedule keys are Conflict. Actual read/write errors remain errors.

Existing keys, native links, helpers, provenance and source timestamps are preserved. No absence retirement, source acquisition or tenant selection. Group membership plus class-date mapping does not establish an operational group occurrence.

Latest release evidence: evidence/shared-schedules-20261029/. Accepted through normal 14:39 and 14:44 Eastern runs, 29 October 2026. See release.md.

# Current Entries deployment - 29 October 2026

Data-input switch deployed at 17:27:23 UTC after independent review and 36 offline checks. Two normal scheduled runs verified at 13:29 and 13:34 Eastern: both business-ok Partial, no processing errors. Evidence: evidence/data-input-switch-20261029/.

Entries links accepted retained Data to canonical Shows and read-only Schedules: rings, groups, classes, horses, riders and actual trainers. No direct source acquisition or tenant selection. CWF is outside this module and its acceptance. Optional Season references are nonblocking.

Existing five-minute task, launcher, keys, native links and helpers remain unchanged. Historical task name wef-entries-cwf-collector does not mean a tenant dependency. Module heartbeat key: wef-entries.

Source observation times and revision provenance are preserved. Replay never refreshes source time. No consumer TTL is invented. Automatic absence retirement is disabled. No retained class evidence fails unavailable; explicit complete zero-trip classes succeed. Ordinary class/date links continue while uncertain group links are flagged.

Added fields: source-trainer-ids, schedule-link-status, schedule-link-basis, group-link-status, link-issues. Future results/messages/waze links do not block supported linking.

## Operation and evidence

The existing task calls run-hidden.vbs -> run.ps1 -> entry.js every five minutes. entry.js holds collector.lock and saves runtime/last-run.json. Only scheduled execution is operational proof.

See [release.md](release.md) for accepted run IDs and [scope.md](scope.md) for responsibilities. Exact hashes, schema readback, task XML and before/after Airtable snapshots are in evidence/data-input-switch-20261029/.

Historical README content is preserved in evidence/data-input-switch-20261029/README-before-finalization.md; its tenant-driven instructions are retired.

Downstream Live sync field exposure and consumer freshness policy remain separately owned work. Entries release does not claim Live or CWF consumer compatibility.

Latest accepted refinement: ambiguous schedule links show Conflict; missing links stay unresolved. Verified through normal 14:09 and 14:14 Eastern runs on 29 October. See release.md.
