# Messages recipient contract — 10 October 2026

Read-only reconciliation by /root/recipient_contract_review. Model and effort: unknown (not exposed). Repository HEAD a5235466eed1b094a26db14ab34b514726abf91d. Only this document written; no Airtable writes, runtime/code changes, browser tests, SMS, deployment or cutover. Airtable CLI and overview skills read. Instructions read: AGENTS.md, specialist-shared-contract-20261010.md, messages-specialist-handoff-20261010.md, new-airtable-base-guide.md.

## Current source precedence

The deployed source remains Recognize app9kOZdIaGyKk5uG, subscriptions tblbpALv7flu3NNEE. Tenants app7BXcw801n19QKF is the intended membership/preferences owner; its staged copy does not win over that deployed destination. Evidence: .git/ringstatus-control/tenants-connections-20261010/checkpoint.json records deployment 6aa4b4de-a837-40a6-9c33-341041e38b50, commit 18e6e993aeaff6fd53168dac4d6f1b7b46baf195, successful 2026-10-10T16:18:23.4027124Z control-plane readback and RS_INPUTS_BASE_ID app9kOZdIaGyKk5uG. This review reads that coordinator evidence, not a new deployment check.

Captured source-contract.txt lines 84–105 pins subscriptions and guards the base. Lines 95–96 forbid engine/barn/user scope and Active status on personal preference records. Lines 357–376 preserve exact JSON definitions, IANA timezone, on/off and string timing values and normalize phone to E.164. Lines 378–415 derive ownership from verified actor, retain revision/audit controls and save Test with Draft/Granted or Revoked/Revoked. A saved Granted preference is not activated delivery.

Tenants optin-options live description explicitly makes preferences_json the compatibility source until verified cutover. Therefore readable Choice/Value rows are a reconciled representation, not an independently authoritative preference master. No conflict was observed today. Conflict handling after edits, migration/cutover and future precedence between sources remain unresolved. Neither Messages nor this review may choose a cutover default.

Historical native UI proof: docs/coordination/sms-optin-connection-20261009.md, Revision306. Recognize record recbLNY3M6un3KEHH saved explicit consent then revoked it, retained 30 minutes and 10:15, and reloaded SMS off. This is historical form persistence proof, not delivery proof or fresh Recognize record inspection.

## Live Tenants reconciliation

Live connector schema/record reads on 10 October: optins tblCX61u6pjSLrP5S (1 record), optin-options tblFHcfIFjGuoE2hp (18), rules tbldkZBRCOMlbYydg (15), tenant-memberships tblFf4aWQYA2RRsWo (13), tenant-assignments tblnFOT8lGxCXD4Xh (0), favorites tblyp3d7BZtZmqAVM (0). All record pages returned no continuation. Table/schema reads identify IDs; no identity by table-name guess.

Copied opt-in recGOGga4HDUSIZXL: entity_uid rs_876181ac66a07a225f9a7b9967dd6be3; owner/Person target this_sms_test; revision 2; consent_state Revoked; status Revoked; record_mode Test; revoked_at 2026-10-09T16:44:16.668Z; explicit-save consent source and sms-preferences-v1 notice. JSON enabled=false and timeZone=America/New_York. Phone remains present and matches JSON but is intentionally omitted from this document. Membership, Managed by, Notification scope, Selected favorites, engine_scope, barn_uid, user_uid and alert_types are blank. Those blanks are unresolved, not inherited permissions.

Comparison normalized omitted Airtable Value to empty string. Each JSON key occurs exactly once, links solely to recGOGga4HDUSIZXL and has equal Choice/Value. No missing or extra keys.

| JSON key | Choice | Value | Option record ID | Result |
|---|---|---|---|---|
| class_start_late | Off | (blank) | recbqabDvfomz6qBV | PASS |
| class_start_early | Off | (blank) | reclZ5v61Gm4D2hdF | PASS |
| class_starts_in1 | On | 30 | recoYFhnTPm42bQsR | PASS |
| class_starts_in2 | Off | (blank) | recrkTYziXwgyNhz5 | PASS |
| class_started | Off | (blank) | recOwGzuk5MKKXzp1 | PASS |
| class_underway | Off | (blank) | rechme4ROYaiLfV3r | PASS |
| class_completed | Off | (blank) | rece0nQmfAXy0Hpln | PASS |
| trip_starts_in1 | Off | (blank) | recJVk6WT36NzwALq | PASS |
| trip_starts_in2 | Off | (blank) | recrk31s8C0alF7dB | PASS |
| rider_oog | Off | (blank) | recByYpgA5oQW1jaM | PASS |
| rider_oog10 | Off | 10 | rechJFEbpXm8IRWGt | PASS |
| rider_started | Off | (blank) | recSN0mJZ40vlpIeQ | PASS |
| rider_first_score | Off | (blank) | recZDA2owk2ZkuQAh | PASS |
| rider_first_time | Off | (blank) | recCcdG61Mxc8sNF8 | PASS |
| rider_results | Off | (blank) | rec4B2cCqcF3S6X1D | PASS |
| groom_tasks_at1 | On | 10:15 | recp7Vo3gBqhITHwG | PASS |
| groom_tasks_at2 | Off | (blank) | recPmruc6cma4X3TU | PASS |
| groom_tasks_at3 | Off | (blank) | recfRHeiySkrHkr2c | PASS |

PASS: 18/18 choices, values and parent links match. On choices do not overcome SMS off, Revoked consent or Test mode. Four minute keys preserve lead times; rider_oog10 is an entry count, not minutes; three groom task keys are HH:mm clock times interpreted with the retained IANA timezone. Disabled values must remain stored. Native definitions are captured in native-alerts.js line 7; seed/timezone line 17. Minute presets: first class/trip 30/45/60/75/90; second 15/30/45/60. Entry presets 7/10/15; default 10. These are existing UI semantics, not confirmed engine trigger mappings.

Field IDs: preferences_json fldDxqMx5Kd7SCZ3Q; consent_state fldBX5pMx4y0WCcYA; revoked_at fldG5Rw9HKvJUzCx8; Membership fldLTONZPqPRUytXs; Managed by fldN7BSB1X1oqmKC2; Notification scope fldhYgtWe5yNAD9Oy; Selected favorites fld0URBJmfebDV5ld. Options: Option fldw8aGJzGPaGF2rY, Opt-in fldjTyeplSOcvzQ78, Choice fld9ShkYLyGBnI0rY, Value fldpIWmcBbt406mcJ.

## Permitted recipient and scope constraints

These are agreed rules/schema constraints, not runtime enforcement proof:

- Recognition supplies identity; it does not grant tenant access (rule recv6w0fkMflxwKvj). Resolve exactly one recipient Membership to Person and Tenant before activation. Membership status choices are Enabled/Disabled; all 13 inspected membership statuses are blank. No Enabled membership or owner authority is established by these records.
- Owner-managed groom preferences (rec7d5jLEQ6oopApf) require Managed by Person to resolve to an Enabled owner membership in the recipient's same tenant. Manager control never grants recipient consent or overrides revoked_at (recj82LxxgYMCDHe1). No manager or recipient membership is resolved for the copied opt-in.
- Grooms see only assigned items (recNiinaKosbR8rJU). Assignments require one membership and one horse in that tenant's enabled roster; only Active applies. Empty assignment table establishes no permitted groom targets.
- Riders' personal follows remain separate from tenant roster (rec8DmBYp4gkppvfs). Favorites require one Horse/Rider/Trainer target and same membership; Selected favorites cannot import another tenant's follows. Empty favorites table supplies no favorite targets.
- Staff/rider overlap must reuse identity (reciN8zKchl8GIzcc); membership recY4u3SdjpBW5dNZ demonstrates imported rider/staff/groom role overlap but has blank status. The earlier groom-not-rider wording and overlap schema require an explicit policy for combined roles before a union can widen scope. Do not duplicate sends because a person has multiple roles; delivery identity/event deduplication remains to be designed and verified.
- Tenant benefits tbleGDeOz4XDIMAGd (Ad-free, Pro Schedule, Website logo) are separate from personal SMS consent (recj82LxxgYMCDHe1). Schema inspected; benefit records not needed for this reconciliation.
- Primary trainer roster includes horses/riders (recBP2PRsap4PJwz0); Additional trainer adds horses only (rece3mBFTtIf7t6H1). Owner disabled roster excludes tenant targets (recmilbdaNHETosmf). Membership survives show/week/day filters (rec0BfK14Xwy5RnyK); absence from entries is not membership revocation.

Notification scope choices are Tenant roster, Personal favorites, Assignments, Selected favorites. engine_scope choices WEC/WEF; target_type Barn/Person/Horse/Show/Ring. These schemas do not settle inheritance, selection conflicts, or map Rider/Trainer favorites into legacy target_type. Current form has no barn/engine selector; no default engine, tenant, show, week or day may be invented.

## Remaining decisions and next acceptance

1. Tenants/Recognize must agree a mapping and separately authorize cutover from Recognize subscriptions to Tenants optins/options, including ownership identifiers, consent audit history, revision/conflict behavior and exact target persistence readback. Existing Tenants next task is recjNCFtq6dJB7sk3, recorded in coordinator checkpoint; shared adapter reservation required before edits.
2. Resolve explicit recipient membership and authorized same-tenant owner manager; agree combined-role scope and exact deduplication key. Missing statuses must not be treated as Enabled and copied roles are not access grants.
3. Define precedence/intersection for Notification scope, selected favorites, active assignments, disabled roster, engine/target and show/week/day context. Confirm what each of 18 choices consumes from WEF trigger evidence, including entry-count versus time predictions, timezone/day boundaries and task source.
4. Define suppression/STOP handling and reconciliation between provider suppression and app consent before consumer activation. Neither inbound text, identity lookup, option On, tenant benefit nor owner management is a consent grant.
5. Verify an authorized Messages consumer using explicit consent, eligible non-Test records, permitted scope and real event/provider evidence after owner-approved implementation. Current reconciliation does not verify recipient selection, sends or delivery.

Responsibility for these decisions belongs to the existing domain coordinators and owner approvals; no human execution owner or completed handoff is invented. This subtask is complete as read-only contract reconciliation; implementation and operational acceptance remain open.

