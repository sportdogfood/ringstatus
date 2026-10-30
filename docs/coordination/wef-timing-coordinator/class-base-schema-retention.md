# wef-class-calculator schema and retention policy

Verified October 30, 2026 through Airtable schema readback.

## Scope and evidence

Target: `wef-class-calculator` (`appeqxxJyPGS1iqdm`).
Source: `wef-ring-pace` (`appYOykKIy1kAquhP`), verified by live base search and full schema reads before and after creation. The coordinator README still refers to this source ID by its historical name, wef-timing.

The separately authorized schema build created the six tables below, with 67 fields total. Field names, types, primary fields and type-specific options match the source. Table descriptions identify the class-calculator scope and explicitly state that creation does not activate writers, retention or runners. The target's existing `meta` table and the source base schema were unchanged on readback.

No records were created, edited or deleted. No code, workflow, scheduled task or production run was changed or invoked. This shell does not move existing class outputs, wire a writer to this base, or establish class-runner operation. The previously failed Timing runner remains outside this authorization and stopped.

## Exact table and field schema

Fields are listed in verified order. A dash in Options means no type-specific configuration was returned. Text fields remain text; this copy does not infer date types or add class-specific columns. Class/occurrence details remain part of the existing answer contract's scope and JSON payload, subject to separately approved writer integration.

| Table | Target table ID | Primary field | Fields |
| --- | --- | --- | --- |
| timing-answers | `tblmoBFySYW2TkDHf` | `answer-key` | 18 |
| engine-checkpoints | `tblJzYflUc6STJRI2` | `checkpoint-key` | 10 |
| run-logs | `tblyI9N4ehBLoRI2S` | `run-id` | 14 |
| heartbeat | `tblPkpcdX1gYWYgMA` | `heartbeat-key` | 10 |
| errs-log | `tblQXKCLO4rxw9WK8` | `error-key` | 9 |
| code-reviews | `tblPUqiquhOTxac93` | `routine` | 6 |

### timing-answers

| Field | Field ID | Airtable type | Options |
| --- | --- | --- | --- |
| `answer-key` (primary) | `fldRSQkTzr4OsOfP1` | `singleLineText` | — |
| `scope-key` | `fldFM1B6pBCdUdtO7` | `singleLineText` | — |
| `answer-kind` | `fld5c9WKxDSYKbhFF` | `singleLineText` | — |
| `source-lane` | `fldZcCyqrCGuTMeWF` | `singleLineText` | — |
| `show-id` | `fldg7macbD9L3me8h` | `number` | `{"precision":0}` |
| `date` | `fld40Xfl9lkauMcxq` | `singleLineText` | — |
| `ring-number` | `fldOy3YNESCCrx6IY` | `number` | `{"precision":0}` |
| `group-id` | `fldSY0kedIIMFp7hH` | `number` | `{"precision":0}` |
| `phase` | `fldotzSJs0atrLovd` | `singleLineText` | — |
| `availability` | `fldcos1nFE96iHdLa` | `singleLineText` | — |
| `pace-method` | `fld4YCr6hIJn1nulI` | `singleLineText` | — |
| `pace-minutes-per-trip` | `fldBlQh3XEpVEQZl2` | `number` | `{"precision":4}` |
| `evaluated-at` | `flduyNIUCgv19s8kk` | `singleLineText` | — |
| `observed-at` | `fldTMV4TEvYZflGp4` | `singleLineText` | — |
| `reason-codes-json` | `fldz8TbnBEn42BmNi` | `multilineText` | — |
| `input-refs-json` | `fldeqfhr3Wl9AcFyo` | `multilineText` | — |
| `payload-json` | `fldFBkeIOgYaUoRSg` | `multilineText` | — |
| `content-hash` | `fldFKIvhdS02Rbvuc` | `singleLineText` | — |

### engine-checkpoints

| Field | Field ID | Airtable type | Options |
| --- | --- | --- | --- |
| `checkpoint-key` (primary) | `fldoby7ilVZ759z7j` | `singleLineText` | — |
| `run-id` | `fldY0m8QoKzaGuTCY` | `singleLineText` | — |
| `started-at` | `fldbURXHQ5Qyv5cmP` | `singleLineText` | — |
| `completed-at` | `fldiyexkwkQD6kmZw` | `singleLineText` | — |
| `last-outcome` | `fldwTstTMnWNweu7R` | `singleLineText` | — |
| `duration-ms` | `fld3SHxxNvclcF6Lm` | `number` | `{"precision":0}` |
| `counts-json` | `fld3kolBc4A3BtTvc` | `multilineText` | — |
| `state-json` | `fldninYlY1iOV6Nkl` | `multilineText` | — |
| `error-summary` | `fldipoUh75THnWVGr` | `multilineText` | — |
| `policy-version` | `fldLMrojz8z4MWXdF` | `singleLineText` | — |

### run-logs

| Field | Field ID | Airtable type | Options |
| --- | --- | --- | --- |
| `run-id` (primary) | `fldpUgvqncoEJDoc6` | `singleLineText` | — |
| `started` | `fldRkOQadeKc2wpyV` | `singleLineText` | — |
| `completed` | `fldkemWcB0NmWIXdf` | `singleLineText` | — |
| `result` | `fldwCoHV8qFKRBWNG` | `singleLineText` | — |
| `show-id` | `fldm0jyXQ0eRNzYmv` | `number` | `{"precision":0}` |
| `date` | `fldx8Rn1KNgb7hARu` | `singleLineText` | — |
| `summary` | `flddawQl8n3Lbv6Cw` | `multilineText` | — |
| `errors` | `fldZ51OjbKnpFR28X` | `multilineText` | — |
| `ring-count` | `fldQ74QH8Il18Bx9X` | `number` | `{"precision":0}` |
| `group-count` | `fldr2mDwp9mNzriJG` | `number` | `{"precision":0}` |
| `class-count` | `fldUyHcJvMdT6uRxX` | `number` | `{"precision":0}` |
| `entry-count` | `fldfkuFws1lMxXICL` | `number` | `{"precision":0}` |
| `archive-count` | `fldDblnSHg9qhlPmo` | `number` | `{"precision":0}` |
| `archives` | `fldF6ESPyFKvujUFY` | `multilineText` | — |

### heartbeat

| Field | Field ID | Airtable type | Options |
| --- | --- | --- | --- |
| `heartbeat-key` (primary) | `fldywWJp1aC6f34Os` | `singleLineText` | — |
| `checked-at` | `fldwrJuW2pK7fvcC0` | `singleLineText` | — |
| `show-id` | `fldUGAEx5rMFyb6ae` | `number` | `{"precision":0}` |
| `date` | `flduVDJNIjAIM0ZZd` | `singleLineText` | — |
| `result` | `fldgGKEaldvVMdQJN` | `singleLineText` | — |
| `summary` | `fldm1q4aPUuBPc35w` | `multilineText` | — |
| `errors` | `fldvhBGww0JmOxFvk` | `multilineText` | — |
| `run-id` | `fldfgHhn1aaFAPLOP` | `singleLineText` | — |
| `started` | `fldgcuzimcWNbMaOo` | `singleLineText` | — |
| `completed` | `fldYiOG2FvVAwLaFd` | `singleLineText` | — |

### errs-log

| Field | Field ID | Airtable type | Options |
| --- | --- | --- | --- |
| `error-key` (primary) | `fldMK8JcxCMKjFUzO` | `singleLineText` | — |
| `run-id` | `fldCXAc2o9tnrrbOI` | `singleLineText` | — |
| `occurred-at` | `fldcKniOC1eGLoWO4` | `singleLineText` | — |
| `show-id` | `fldsPBETAtx8h3VsC` | `number` | `{"precision":0}` |
| `date` | `fld3euEQX1win2Biu` | `date` | `{"dateFormat":{"name":"iso","format":"YYYY-MM-DD"}}` |
| `message` | `fld0uoC09srkP77YH` | `multilineText` | — |
| `err-class` | `fld8AUSU7NozUppkl` | `number` | `{"precision":0}` |
| `err-trip` | `fldNzADqmkuqPQc53` | `number` | `{"precision":0}` |
| `err-group` | `fldbVTuCMoBUgkEOD` | `number` | `{"precision":0}` |

### code-reviews

| Field | Field ID | Airtable type | Options |
| --- | --- | --- | --- |
| `routine` (primary) | `fldYKPKaBfvdXxENA` | `singleLineText` | — |
| `last-review` | `fldiFejPO0mIkgcwS` | `date` | `{"dateFormat":{"name":"iso","format":"YYYY-MM-DD"}}` |
| `last-result` | `fldwWvS7xs8NcU1gh` | `singleLineText` | — |
| `next-review` | `fldzTRrjA71Il9bwq` | `date` | `{"dateFormat":{"name":"iso","format":"YYYY-MM-DD"}}` |
| `cadence` | `fldB2yTzuTNHEdxxS` | `singleLineText` | — |
| `notes` | `fldDhYxJ5jxkKHABN` | `multilineText` | — |

## Required retention policy

This is a documented requirement, not an implemented or verified archive/cleanup process. No numeric retention periods have been approved in this task. Until the missing decisions, implementation and verification are complete, this policy permits no deletion.

| Table | Protected records | Retirement requirement |
| --- | --- | --- |
| timing-answers | Current and future show-day answers for every show; answers with unresolved identity, coverage or downstream dependencies | Fresh verified show/date authority, complete relevant Data coverage, successful calculation and output readback; then verified archive and durable index before any approved deletion |
| engine-checkpoints | Active checkpoints, incomplete runs, pending deletion intents and unresolved receipts; state needed for recovery or idempotency | Only superseded state with no remaining dependency may be considered; archive full state and verify index before deletion |
| run-logs | Incomplete or failed runs under investigation, logs referenced by active checkpoints, pending intents or unresolved findings | Approved retention period and dependency clearance, followed by verified archive and index |
| heartbeat | Current operational/control state and evidence needed to diagnose incomplete runs or unresolved failures | Only superseded, dependency-free history after an approved period; verified archive and index |
| errs-log | Unresolved errors and evidence referenced by runs, checkpoints or reviews | Resolution and dependency clearance plus approved period; verified archive and index |
| code-reviews | Open findings, pending verification and reviews supporting active changes or unresolved failures | Closed, dependency-free evidence after approved period; verified archive and index; do not treat review completion as deployment or scheduled proof |

Common requirements:

1. Preserve complete records, Airtable record IDs, all fields, links and source base/table identity in compressed local archives. Preserve references even where the current schema stores them as text/JSON rather than native linked-record fields. Data/Live input archives do not substitute for archiving class-calculator outputs.
2. Read back the complete archive, verify record coverage/count and content hash, and retain verification evidence. A filename or successful write alone is insufficient.
3. Before any deletion, persist and read back a durable archive index in the class-calculator Airtable base. Required information is archive path, scope, record count, content hash, verification and cleanup status. Exact table/field design is not yet approved; none of the six copied tables is claimed to implement this index.
4. Evaluate each category's eligibility independently. Eligibility in one table never authorizes deletion in another. Protect current/future dates across all shows, not merely the currently selected show; unresolved date/identity or incomplete coverage blocks affected retirement.
5. Preserve exact deletion intents, deletion responses and absence readback. Failed archive/index verification blocks deletion. An uncertain deletion stops for owner review; no blind retry or record repair.
6. Preserve incomplete runs, active checkpoints and pending intents regardless of age. Recheck protection and dependencies before deleting an approved set. A failed run must not trigger cleanup.
7. Archive files and their index must remain available while required by records, recovery or evidence. Their eventual retention/disposal periods also remain undecided; no archive deletion is authorized here.

## Remaining decisions and implementation gaps

- Owner decision: retention periods by table/category, including archive and index retention. No durations are inferred.
- Owner approval: durable Airtable archive-index structure and archive storage location/access responsibility. Full records plus verified hashes and index readback are mandatory regardless of the selected design.
- Separate implementation authorization: route class outputs to this base, wire operational writers and implement/test archive/index/cleanup safeguards. No writer or archive/index implementation is delivered by this schema build.
- Separate operating authorization and proof: deployment, any new or resumed runner, normal scheduled verification, and any recurring code-review schedule. The failed runner is not restarted by creating tables.
- Existing coordinator decision remains open: today-only versus future show-day calculation coverage. Regardless of the choice, current/future records remain protected from retirement.

## Verification result

PASS: all six target table schemas match the live source for field names, types, options and primary fields.
PASS: source table/field schemas and target meta unchanged in before/after reads.
PASS: six table-creation calls succeeded and independent list/schema reads confirmed their IDs and all 67 fields.
NOT IMPLEMENTED / NOT VERIFIED: operational writers, archive files, durable index, retention cleanup, deployment and scheduled operation. Schema verification is not workflow proof.

