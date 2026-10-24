# WEF records owner acknowledgments — 23 October 2026

Coordination/documentation record. Four existing owner chats explicitly acknowledged the opening contract in wef-data-boundaries-working.md. Acknowledgment accepts responsibility; it does not prove every connection is implemented. No production acceptance gates are retroactively changed by this record.

## Accepted ownership and exact references

### wef-shows verification and accepted handoffs — recorded 24 October

**VERIFIED for its agreed scope as of 24 October 2026, 11:21 a.m. Eastern**, using the shows owner's inspected evidence in wef-shows/README.md. Twelve normal runs from 10:24 through 11:19 were Success. Latest run 8290957b-6acf-43c5-9527-cb18f50ff5a1 completed 11:19:03–11:19:09; preceding 456df403-84b7-4172-8ec9-f0183767c99e completed 11:14:01–11:14:08. Heartbeat matched; six archives were SHA256-verified by the owner. One canonical default record rec4En9UWd6lkT1Oh: show 200000075, ESP Fall III, October 22–25, source-selected October 24. This records timestamped evidence, not a new production check.

Shows-to-schedules, shows-to-entries and shows-to-live authority handoffs are **ACCEPTED** through the explicit owner acknowledgments recorded below. Shows alone writes canonical show context; readers independently use customer_id|show_id/source identity and retain their own writers and verification. Shows triggers none. Schedules owns its mirror and local hash-verified full-payload archive consumption; that same-machine dependency remains. Missing/ambiguous/inconsistent/over-30-minute-stale authority is not valid empty. Shared-source edits require coordinated scope.

Live trip 200398240/class 200030429 remains a separate downstream source-identity disagreement; it does not invalidate the verified shows contract. Accepted authority handoffs do not imply complete downstream horse/rider/participation/live integration. Historical source and protected-file failures remain unchanged.

| Owner chat | Accepted writer / reader boundary | Base and table identities |
|---|---|---|
| wef-shows — 01a16976-f0a6-72a1-a75f-75de7f19a815 | Writes canonical show identity, dates, source-selected context, default and freshness; downstream readers recur independently. Holds cross-consumer source-repair history. | appCaSmL819KK7UFJ; shows tblJA5CUon5s1sWib |
| wef-schedules — 01a14082-b22f-73b1-bba7-16781892a55d | Writes dated schedule/group/class/ring identities and collection evidence; reads canonical shows. Does not own participation or live observations. | appUq5Qz9kARqFija; schedules tblqiTFqqO3zk2KJC; rings tbliUwvXYdMhvJ3Fm; local shows tblBq3gXC0TYBCmMZ; show-collection tblvGpwxGjXXNLKAU; heartbeat tblj077efYbhlznOJ |
| wef-entries — 01a1691d-3bd7-7eb2-af17-b9ba39a7501e | Writes shared selected participation and provenance; reads shows, schedules, tenant parameters/favorites. Accepts broader participation-linking responsibility; universal helper directory is not implemented. | appSxF4pYMBaxpTZd; entries tblqkWmrsYMs2hyQ6; collections tbltCxLRlhpdHdHSw; run-logs tblCWYgZs49eOuQ7e; heartbeat tbloQWorYyYtTIv8T; local schedules sync tblVcbGJHhzLYZRxn |
| wef-live — 01a16902-240a-7280-8661-90b0d8d7325a | Writes live observations and source-backed trip identities independently of tenant selections; reads canonical shows and local schedule/entry syncs. Untracked participation may remain unmatched. | appgzosUcVqA4vZdf; live-rings tblw8bodkRbCItxIH; live-groups tblMhQtjpvyODEUSn; live-classes tblNaM9ehOJFsrveV; live-entries tblf7550e7UGT8ZSF; local schedules tblGPXIUnD0sfTEIX; local entries tblfj77GmXxuiAoB4 |

CWF appxKPBtdl2Rdq7CS consumes entries sync tbl7ychn5HELB3HTr. Parameter tables: tenant-details tblUoIrtdQ2Wg7DjB and trainers tbljBHTXEJn71RRIl. Local helpers/references: horses tblxMpG75KL3Nmt1v, riders tblll5l7eG5wc27uP, shows tblNABa50eiRKfZS4, rings tblK8EqyEVUktiLOI, schedules tbl3QiMIXd6Spi336. CWF owns its existing helper edits and six local entry links. Entries reads favorites tblyp3d7BZtZmqAVM in tenants app7BXcw801n19QKF; final persistent tenant/roster ownership remains unresolved.

## Keys and behavior acknowledged

- Shows: show-key = customer_id|show_id. Missing, ambiguous, inconsistent or over-30-minute-old authority fails dependent collection; retain last verified context and log failure. No rediscovery fallback or upstream execution trigger. Missing default show is not a successful empty collection. Schedules' full-payload archive handoff currently requires the same machine.
- Schedules: retain exact stage-key; do not reconstruct guessed keys. ring is ring number, not ring_id; class-number is display identity, not class-id. ring-key = customer_id|show_id|ring_date|ring_number. source-selected date/is_selected_date can be upcoming and does not establish TODAY.
- Schedule freshness uses completed dated collections and heartbeat, not unchanged row changed-at. Valid identified zero-row dates succeed; an empty-day Warning is not itself a failed collection. Consumer-specific freshness/partial-input rules still require reconciliation.
- Entries: entry-key = show-id|date|entry-id|class-id. Horse, participation and trip identities remain distinct. Retain schedule-key matching stage-key; resolve native links using destination-local records. trainer-ids means feed association, not confirmed trainer identity.
- Entries' source/local schedule comparison includes retained dropped rows and full current-show parity. Do not filter dropped rows from the entire sync. Participation matching excludes dropped schedules and currently requires one active show/date/class match. Multiple valid active matches remain an explicit contract/implementation gap.
- Live: keys use show/date/ring, show/date/group, show/date/class and show/date/class/trip. Binder match uses show/date/entry/class. Schedule matching uses active rows and matching group prefix. Native links use local sync IDs. Only the actual live day qualifies.
- Live show authority freshness is 30 minutes. Valid empties succeed; failures preserve affected observations and prevent retirement. Skips preserve observations and report the reason separately.
- CWF requires successful current-show entries heartbeat, completed trainer/day collections and fresh active synced entries within 30 minutes. Missing/stale/conflicting/incomplete input fails explicitly, with no duplicate source fallback. Central failed collections retain previous participation; no-longer-selected participation is retained as dropped.

## Privacy and limits

Private-only follows and collection ownership are excluded from the CWF team sync. Live references identity fields only, excluding private selection reasons. Neither shared sync proves per-user permissions for private trainers. Non-empty private follows have offline coverage only. Current entries scope explicitly uses CWF trainer feeds 8778, 50121 and 80970; arbitrary tenant/trainer configuration is not implemented. Persistent helper aliases/history and shared roster ownership are not settled; current roster maintenance remains in CWF.

## Evidence and historical reconciliation

- Shows/schedules original acceptance: wef-shows/evidence/release-verification.json. Direct consumers: wef-shows/evidence/consumer-connection/.
- Entries original acceptance: wef-entries/evidence/release-verification.json. Schedules owner reported latest saved entries receipt at 17:14 Eastern: Success, 9 entries, no errors. This is runner receipt evidence, not a fresh Airtable readback by this coordinator.
- CWF approved migration: wef-tenants/evidence/entries-consumer/README.md and repeat-proof.json. Shadow 13:12/13:17; cutover 13:19:54; consumer proof 13:22/13:27 Eastern: 9 entries, zero people-endpoint fetches, helpers/local reciprocal links/original records preserved.
- Live: wef-live/evidence/continue-on-error-normal-runs.json and activity-gate-normal-runs.json. Live owner reported normal 17:12 and 17:17 skips after all 19 TODAY groups completed; Airtable run-logs/heartbeat readback, existing observations retained, five-minute checks enabled. False-live-status skip branch remains offline-tested only. No new runs were triggered by the acknowledgment request. Live was completing separately owner-authorized work when it received that request.
- The source-fix protected-drift check remains a historical failed gate. Entries identifies the wef-tenants/airtable.js, collector.js and settings.js edits as its separately approved migration. That explains scope overlap; it does not retroactively pass the other release's protected-file check.
- Companion diagram labels 'live verification blocked by HTTP 502' and 'Current collector continues unchanged' are stale. CWF cadence stayed unchanged; consumption behavior changed. Shows README also retains an outdated entries-reference description. These sources were not edited in this acknowledgment pass; historical failures remain intact.

## Outstanding owners and concrete next steps

The coordinator is authorized to manage the four records chats. Other chats were discovered, not contacted or assigned through this acknowledgment pass:

| Required contract | Discovered candidate owner | Status / next decision |
|---|---|---|
| Timing | wef-timing-coordinator — 01a169bc-0bd6-77d3-b7b4-283a07061e90 | Acknowledgment pending; settle input keys, freshness, dropped/inactive/missing cases and calculation ownership. |
| Results | wef-results-coordinator — 01a169d7-0bcd-7ca1-8b39-4e124da8b363 | Acknowledgment pending; settle entry/trip joins, corrections and history. |
| Messages | wef-messages-coordinator — 01a169cc-d938-76f0-9ae4-bbccf7266599 | Acknowledgment pending; confirm actual base/name, subscriptions, private context and answer freshness. |
| Waze | wef-waze-coordinator — 01a169d0-8192-7851-b414-7aed7a065cea | Acknowledgment pending; settle user/tenant/location context, visibility and retention. |
| Tenants / CWF broader contract | wef-tenants-coordinator — 01a169c2-de92-71e1-9857-084838862ae7 | CWF migration accepted by entries owner only; broader parameter/helper/privacy ownership still pending tenant owner. |
| Publication | wef-publish-coordinator — 6abfd653-0db0-83ea-85b2-e7983280a0c4 | Candidate is ChatGPT-backed; not contacted. Settle scoped presentation and freshness; no private selection reasons. |
| Persistent horse/rider/trainer/location/ring helpers | No dedicated authoritative owner confirmed | Owner decision required; do not create duplicate directories or infer ownership from a chat name. |

Next: resolve entries' single-match versus live's multi-group relationship; settle persistent helper/tenant ownership and consumer freshness; obtain owner-authorized external acknowledgments. These discussions do not block proven CWF consumption or authorize production changes. Broader timing/results/messages/Waze/publication connections remain unimplemented or unverified until their owners supply evidence.
