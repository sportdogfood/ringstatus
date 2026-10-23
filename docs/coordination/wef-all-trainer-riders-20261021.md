# CWF riders from all active trainers

Owner authorization: include riders from all active trainers and link them to
entries; preserve existing helpers and uniqueness rules. Test and verify normal
scheduled runs only. No manual collection or record repairs.

Baseline: existing deployed `wef-tenants/collector.js` including the accepted
trainer-name refresh, and `wef-tenants/standing-roster.js`. Exact pre-change bytes,
hashes, task XML and Airtable identities are retained in
`wef-tenants/evidence/all-trainer-riders-20261021/`.

Three production lines change: standing rider reads use all active trainer IDs;
standing rider associations match every active trainer; successful current-entry
datasets contribute riders for all active trainers. Existing normalization,
identity checks, stable keys, owner helper preservation, archive protection,
entry-link construction, source decoding and schedule configuration stay in place.

README scope was updated without changing its existing encoding. Tests reproduce
the former exclusions, then verify additional rider creation/linking, repeated-run
stable identities, inactive trainer exclusion, shared rider associations and name
collision rejection. All 49 tenant tests pass on the staged candidate and deployed
files. Independent read-only review reported no actionable findings.

Read-only preflight: 33 existing riders; three additional current-entry riders
(50121 Isabela de Sousa, 81017 Jacqueline Rosenthal, 80970 Taylor Cowles); no
uniqueness conflicts. Existing source roster still returns 33 riders. No records
were changed during preflight.

Tested code was deployed at 2026-10-22T01:01:07.410Z (21 October 9:01 p.m.
Eastern), while the collector lock was absent. Two tested files were promoted to
the existing checkout used by the scheduled task. No task or route changes.

The evidence folder's `verify.cjs` only
reads Airtable and existing run receipts/archives; it never imports or invokes a
collector. Successful checks write `verified-<run-id>.json` acceptance receipts.

First normal scheduled run: 21 October, 9:02 p.m. Eastern, run ID
`0936b0ea-84c1-4ffb-97e1-66bbb0e6d74e`, completed 9:03:43 p.m., task exit 0.
Airtable now has 36 unique riders, including the three expected additions, and
all six entries link to the correct rider and retain their prior record IDs and
other links. The original 33 rider helpers and 75 horse helpers retain their IDs
and static values. All trainer configuration is unchanged. Heartbeat and run-log
report Success; all 16 recorded archive hashes verified. Protected source files
and task XML are unchanged.

Repeat normal scheduled run: 21 October, 9:07 p.m. Eastern, run ID
`ed1f698c-12f6-4e25-bc24-c02f168ac5b1`, completed 9:08:54 p.m., task exit 0.
All 36 rider record IDs are stable, with no uniqueness conflicts or duplicates.
The six entry IDs and rider/horse/ring/trainer/schedule links are correct. Existing
helper static values and trainer configuration remain unchanged. All 16 archives
verified again; heartbeat and run-log report Success. Scheduled acceptance PASS
across both runs. No manual runs or record repairs were performed.
