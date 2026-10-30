# Data + Live ring pace

Task: `wef-timing-data`, three minutes, IgnoreNew, interactive Windows user. Same shared pace arithmetic; separate checkpoint and lock from the existing Schedules lane. No manual starts.

Sources: fresh canonical Shows context selects exactly one producer-owned Data immutable head. Full chain/hash validation and a reread fence precede use. Original dated inventory observation times govern freshness. Data inventory supplies group/class coverage; Live supplies current-day interval counters only when class rosters match exactly. No producer acquisition or writes.

Destination: wef-timing appYOykKIy1kAquhP, timing-answers tblhIjSjeGs5td23c and engine-checkpoints tblfWyTqeyvIvhAjb. Source-lane data_live. No Engine/source/collector writes. Existing Schedules lane continues in wef-engine.

Purge: same policy as existing lane, confined to owned Data snapshots. Fresh working-day inventory plus canonical current date, confirmed across two distinct scheduled invocations. Only earlier days and explicitly previously observed retired shows. Current/future dates and unrelated lanes preserved. Intent saved before deletion, exact returned IDs and absence readback required. No natural transition is fabricated for proof.

Limits: Every discovered eligible group is evaluated per tick; 165-second hard deadline; Data projection budget 24 seconds. Source movement/availability defers outputs and purge. Hard failures disable only this task for owner review. No same-day retention compaction is authorized; recurring snapshots consume Airtable capacity. Producer chain growth may eventually exceed the projection budget; that defers the input rather than calling collectors.

Offline: 45 Node tests; five inert launcher cases through WScript and Windows PowerShell 5.1.26100.9767. Real read-only source diagnostic: stable current head, four inventories, 227 groups. These are not scheduled execution proof.

Live acceptance: three ordinary scheduled runs passed at 20:37, 20:38 and 20:39 Eastern on 29 October 2026. Run IDs 91c0ea3d-1d21-4c31-bd6a-a88ac5f1560b, 2d25b033-ba52-4c16-9e1f-11bf708091ce and a9cb5843-cfc0-4ba3-a9dd-c351e6c08ecb. Wrote 80, 80 and 67 snapshots; 227 distinct group scopes covered, zero duplicates, hashes read back. 60 numerical schedule estimates and 167 explicitly unavailable groups. All 227 lacked Live observations. Purge authority confirmed; zero eligible old snapshots existed, so no natural DELETE occurred. Original lane readback also passed; protected collector file hashes and preexisting task XML matched deployment capture. Evidence: evidence/data-normal-*.json, data-receipt-*.json, data-coverage.json and data-protection-check.json. Observed interval pace and natural rollover DELETE remain unproved until real source conditions occur. Class timing, messaging, results, publish and legacy calculators remain outside this change.

Independent read-only review found unrelated-head snapshot churn; corrected before deployment and covered by head-only advancement regression.

Full-coverage correction, 29 October 2026: scheduled runs 21:04 and 21:07 Eastern each evaluated all 227 groups with zero deferred; verified snapshot readbacks were 147 and 227, respectively. 46 offline tests passed. Missing Live observations and natural rollover deletion remain unverified.
