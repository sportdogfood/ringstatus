# Entities source-table preparation — 2026-10-09

## Completed

Six tables created in Entities (`app7BXcw801n19QKF`), retaining existing field names, types, choices and stable application IDs. 45 records copied and every populated field compared with connector readback. Airtable record IDs are newly assigned; application UIDs were preserved. No new identity keys were invented. Blank fields remain blank. Original Recognize records remain intact; no application settings, runtime code or deployments changed.

| Table | Recognize source ID | Entities destination ID | Records |
|---|---|---|---|
| rs_input_barns | tblRvTwo3HYPUkZou | [tblNiTqrwR5Pc1czs](https://airtable.com/app7BXcw801n19QKF/tblNiTqrwR5Pc1czs) | 4 |
| rs_input_users | tblYgoeLEey05xgw9 | [tblyTBgrgdhT3Tcg7](https://airtable.com/app7BXcw801n19QKF/tblyTBgrgdhT3Tcg7) | 6 |
| rs_input_locations | tblEuwOr1rUKnj1j3 | [tblVvj8LGidoV45yF](https://airtable.com/app7BXcw801n19QKF/tblVvj8LGidoV45yF) | 3 |
| rs_source_ids | tbl0oVtuFNRy7zWsX | [tblRswgKjA9dt3PED](https://airtable.com/app7BXcw801n19QKF/tblRswgKjA9dt3PED) | 0 |
| rs_input_events | tblwts3huk3w1ACjh | [tbluecPFIorOytASP](https://airtable.com/app7BXcw801n19QKF/tbluecPFIorOytASP) | 31 |
| rs_input_subscriptions | tblbpALv7flu3NNEE | [tblCX61u6pjSLrP5S](https://airtable.com/app7BXcw801n19QKF/tblCX61u6pjSLrP5S) | 1 |

The empty source-ID table was retained as requested. Membership and assignment tables were not copied, following the owner's exclusion from the allocation. Horse and rider source tables were not copied into Entities; those have separate designated bases. The existing blank Table 1 / Grid view example was preserved.

## Critical correction: two-way sync and Astro

Owner enabled record creation in an example synced table. That permits supported Airtable UI creation; it does not permit API edits or creation in a destination synced table. Prior assistant statements did not distinguish these paths and were incomplete.

Official source checked 2026-10-09: https://support.airtable.com/articles/8435404541-two-way-syncing-in-airtable

Required model: Entities owns profile data; Astro reads/writes its source tables directly. Recognize can display the necessary shared subset. Recognition identities/devices/phone verification/sessions remain in Recognize. General profile CRUD must not write into a synced destination table.

## Exact remaining work

1. Check the existing deployed source and preserve recognition routing separately from profile routing. Inspected existing source checkout: `C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus` (checkout inspection is not fresh deployment verification).
2. Existing `webflow-cloud-test/src/lib/rs-inputs-airtable.js` lines 3–10 pins Inputs to Recognize. Its store currently addresses barns/users/riders/horses/locations through one base. Route barns/users/locations/events to Entities and horses/riders to their designated source bases, after matching those existing schemas and UIDs. Do not copy horses/riders into Entities as a workaround.
3. `rs-inputs-access.js` also pins RS_INPUTS_BASE_ID to Recognize. Separate profile storage validation from recognition identity access; retain the existing recognition identity lookup and cookie behavior.
4. `rs-input-subscription-store.js` pins both Recognize base and subscription/event table IDs. Point it to the verified Entities IDs above; update the corresponding destination check in `rs-input-subscriptions.js`. Preserve opt-in and consent behavior.
5. Verify deployed credentials cover the designated sources without exposing tokens. Prepare and test only these routing changes; no UI redesign or replacement authentication system.
6. Before cutover, reconcile any source changes since this snapshot by stable UID. Do not reinsert all records or overwrite newer edits. Until cutover, current forms continue using original Recognize tables; Entities copies are staging.
7. Configure only the actually needed shared views back into Recognize. Do not expose every profile field or full edit history simply because sharing is available. The sharing configuration has not been changed in this preparation.
8. Test the existing published add/edit/save/reload and opt-in/opt-out flows through their intended UI. Connector copy/readback is migration-preparation evidence, not workflow proof.
9. Remove/retire old profile tables only after references and automations are checked and the replacement path is verified. No tables were deleted in this work.

## Reversal

Before cutover the original runtime remains intact. The six new Entities tables can remain as staged copies; no reversal of live behavior is necessary. Do not delete them if new owner edits have occurred. After any future cutover, reverse only the routing changes using the saved prior configuration and reconcile newer data before rollback.

