# CWF synced show/ring/schedule references

Owner approved replacing collector-maintained show/ring duplicates with existing
synced references, keeping schedule data read-only and tenant edits in CWF. Test
and verify normal scheduled runs before retiring duplicates. No manual runs.

Baseline: deployed tenant collector after trainer-name, all-active rider and
direct rider/horse releases. Exact source and complete relevant Airtable record/
schema snapshots are under `wef-tenants/evidence/synced-references-20261021/`.
Candidate testing is isolated from the scheduled checkout and private runtime.

Approved references in CWF base `appxKPBtdl2Rdq7CS`:

- wef-schedules.shows: tblNABa50eiRKfZS4.
- wef-schedules.rings: tblK8EqyEVUktiLOI.
- wef-schedules.schedules: tbl3QiMIXd6Spi336.

Live preflight: 2 shows, 24 rings and 435 schedules. Current default is source-
selected ESP Fall III, October22-25. Freshness, unique keys and all native linked
show/ring/schedule references verified. Existing browser sync settings show
Edit source records Off for all three; no sync settings were changed.

The collector reads these tables by ID. A stale, missing, ambiguous or internally
incomplete synced graph stops entry processing before people requests or entry
replacement/retirement. A consistently empty schedule is still valid. No manual
sync is triggered. The adapter rejects business mutations to all three synced
tables and the old show-details/rings tables by name or ID.

New CWF link fields: entries.shows, entries.synced-rings, entries.schedules,
collections.shows, rings-helper.synced-rings. These target existing synced record
IDs; no reference copies are created. Schedule-link opens the local synced
schedule record for the same stable schedule key. The shared tenant-index keeps
its existing shared show link using a key/ID-only lookup in the shared shows.
Existing source/date normalization, helper identity/owner fields, ring-walks,
direct horse/rider associations, logs, archives and task cadence are retained.

Historical show-details/rings links are omitted from new writes, so their values
remain intact. Following two successful normal scheduled runs, retire duplicates
by renaming tables to retired-show-details and retired-rings and labeling their
old inbound CWF fields retired-*. No records or fields are deleted. The frozen
tables preserve historical relationships; normal collection stops maintaining
these duplicates. Existing archive-before-delete protections remain unchanged.

Offline: regression reproduced missing write guards and local synced references.
65 tenant tests pass, covering native IDs/permalinks, preserved legacy links,
partial sync protection, empty data, duplicate keys, stale shows, read-only guards
and schema idempotence/conflicts, migrated read-only verification/repeat comparison
and rejection of recreated target tables with different IDs. Review identified
two pre-deployment gaps (legacy verifier expectations and missing schema target-ID
guard); both were corrected and independently re-reviewed. No remaining blockers.

Schema-only configuration added the five native link fields and verified their
reciprocals, writing zero business records. Code deployed
2026-10-22T02:00:22.387Z (21 October, 10:00 p.m. Eastern). Exact deployed hashes
and field IDs are in deployment.json and schema-configuration.json. All 65 deployed
tests passed. Both five-minute task cadence and existing source workflows remain.

First normal scheduled run: 21 October, 10:02 p.m. Eastern, ID
`7a7823b3-fe37-4667-a129-e57e9cffbf19`, completed 10:03:42 p.m., task exit 0.
Verified synced references: 2 shows, 24 rings, 435 schedules. All 6 entries and
12 collections use correct native synced links. All 36 rider and 75 horse
identities, static helper values and rider/horse/entry associations preserved;
six ring helpers and six walking records preserved. All 16 archive hashes verify;
heartbeat/run-log report Success. Legacy reference records are byte-equivalent
to the pre-run freeze snapshot. Protected source hashes are unchanged.

Second normal scheduled run: 21 October, 10:07 p.m. Eastern, ID
`f943e786-949f-4095-89f5-3b8f97b22ed8`, completed 10:08:55 p.m., task exit 0.
The same counts, native references, helper values, associations, archive hashes
and protected sources passed. Repeat comparison passed across 13 tables, with
exactly one new run-log. Heartbeat and run-log report Success for both runs.

After both normal runs passed, duplicate tables were retired at 10:10 p.m.
Eastern by renaming them retired-show-details and retired-rings. Historical
inbound fields were labeled retired-* with their original field IDs, linked
targets and values preserved. No records or fields were deleted or repaired.

The final field-name comparison detected the approved rings -> retired-rings
rename. A separate comparison by stable table/record/field IDs confirmed every
saved value and link unchanged, and the read-only synced verifier passed after
retirement at 10:12 p.m. Eastern. No product code changed after that comparison.
Both task XML snapshots are identical. No manual collector or manual sync ran.

Final evidence: final-verification.json, both verified-<run-id>.json files,
retirement.json and post-retirement-summary.json in the evidence directory.
