# WEC Lanewise Workflow Concept

Date: 2026-07-10

## Status And Use

This document preserves the current workflow concept discussion in one place so it does not have to be reconstructed in future sessions.

This is:

- a concept reference;
- a responsibility and ownership map;
- a record of confirmed intent and current implementation references.

This is not:

- an implementation contract;
- approval to change the current production workflow;
- approval to deploy, migrate, rebuild, or delete anything;
- proof that a proposed responsibility already exists in code.

Future discussions must use this document before reinterpreting the workflow. Any proposed implementation change must be reviewed and approved separately.

## Scope Beyond WEC

This concept is not limited to WEC. It was developed and proven conceptually using WEC as the working dataset, but its tight ringwise, classwise, entrywise, riderwise, and listwise scope is intended to remain stable as new shows and schedules are added. WEF may later be incorporated into this same model. Its current legacy workflow is more robust, but in retrospect it is also too dense and collects substantial data that does not directly support riders' immediate needs. Future WEF work should adapt its useful capabilities to this focused scope rather than carrying forward unnecessary legacy complexity. This scope statement does not itself authorize a WEF migration or production change.

## Primary Ownership Question

Every state, calculation, alert, list, and output must first answer:

```text
Is this ringwise, classwise, entrywise, or riderwise?
```

Listwise is the read-only publishing concern that exposes the prepared lane data.

## Control

`focus_show` is Airtable owned.

```text
focus_show
  show_no
  focus_day
  live-enrichment
  results_enabled
```

- `show_no` selects the show.
- `focus_day` selects the show date.
- `live-enrichment` controls whether Live enrichment is enabled.
- `results_enabled` controls whether Results processing is enabled.
- Catalyst and workflow functions consume these controls; they do not independently redefine their values.

## Responsibility Map

| Responsibility | Owned surface | Purpose | Intent | Current workflow reference | Working disposition |
|---|---|---|---|---|---|
| show | `focus_show` | Select the show, focus day, and enabled options. | Keep workflow scope and controls Airtable owned. | `getActiveFocusShow()` | Keep. |
| classes and normalize | `hs_update_schedule`, `hs_class_start_times` | Collect schedule classes and establish class identities and start times. | Produce the current Classwise list. | `runStage2UpdateSchedule()`, `scheduleRowForProof()`, `classStartRowsFromUpdateSchedule()` | Modify existing behavior only if separately approved; do not rebuild acquisition. |
| probe | `FAST_probe` | Detect class documents containing relevant trainer evidence. | Avoid parsing irrelevant documents. | `runProbe3A()` | Keep the fast probe behavior. |
| parse | `hs_class_oog_raw` | Hold selected raw class documents and parsing state. | Preserve source evidence for methodical parsing. | `storeClassOogRaw()`, `parseClassOogRaw()` | Keep; align normalization only if separately approved. |
| stage | `hs_class_oog` | Materialize relevant parsed horse, rider, trainer, and entry rows. | Produce staged records required to build entries. | `runProbe3B()`, `upsertClassOog()` adapter | Keep the lane and keyed writes. |
| entries | `entry_go_times` | Prepare current tracked entries and base timing data. | Produce the current Entrywise list. | `entryGoRowsFromClassOog()` | Keep the lane; calculations belong to Time Engine. |
| enrich-static | `hs_horses`, `hs_riders`, `hs_trainers` | Supply maintained identities, aliases, and review data. | Improve matching without blocking the main workflow. | `buildProbeEvidence()` | Hold. Non-blocking. |
| enrich-live | `get_rings`, `ring_status` | Maintain current live and not-live ring state. | Provide Ringwise progression. | `runWecStep5LiveEnrichmentOnly()`, `fetchStep5LiveSource()`, `enrichStep5RuntimeRows()` | Keep source acquisition; modify ownership boundaries only if separately approved. |
| calculate | `time-engine` | Calculate ring, class, entry, and result timing or state. | Keep derived calculations consistent. | `runTimeEngineOnly()` | Keep and modify in place; do not replace. |
| expedite | `time-engine` | Apply calculated state to the correct prepared lane. | Move calculations downstream while preserving lane ownership. | No separate current expeditor; currently combined in `runTimeEngineOnly()` | Add bounded helpers inside the existing lane if approved; no replacement service. |
| alerts | `wec-alerts` | Record qualifying lane transitions as append-only events. | Preserve deduplicated business-event history separately from delivery. | `appendAirtableAlertEvents()` | Keep the table, event keys, and append-only history. |
| outputs, endpoints, logs, lists | `wec-print`, `wec-mobile`, `wec-mobile-pro`, `wec-mobile-entry` | Expose prepared operational views and lists. | Publish current data without calculating workflow state. | `wec-print-live`, `wec-mobile-live`, `buildMobileProPayload()`; no current `wec-mobile-entry` action found | Keep working outputs; add missing surfaces separately only if approved. |

The Working disposition column records the current concept discussion. It is not implementation approval.

## Achievable Main Path

The main path identified as achievable is:

```text
show
→ classes
→ probe
→ parse
→ stage
→ entries
```

This path should be able to progress without waiting for all helper enrichment work.

## Existing Update Schedule Behavior

The current `update_schedule` behavior remains the baseline and is not changed by this concept discussion.

Current behavior:

1. Call `update_schedule.php` for the applicable source scope.
2. Parse the returned schedule rows.
3. Attach `show_no`, `focus_day`, `iso_date`, and `focus_day_key`.
4. Apply the existing inline ring-name handling.
5. Build `ring_const_key` and `class_const_key`.
6. Run current preflight checks.
7. Normalize `class_start_time` and `display_time`.
8. Mark accepted rows `active` and excluded rows `preflight` with a reason.
9. Upsert `hs_update_schedule` and mark missing prior rows dropped rather than deleting them.
10. Write approved Airtable review fields when that mirror behavior is enabled.

## Normalization Concepts

Normalization is currently embedded inside existing workflow functions. The concept discussion identifies it explicitly so the same identity is understood consistently.

### Ring Normalization

Ring normalization applies to `update_schedule`. It does not apply to `get_rings` source acquisition.

Relevant fields:

```text
ring_name
ring_name_normalized
ring_name_prioritized
ring_const_key
```

`ring_visual_key` is deprecated and must not be treated as the canonical identity in new concept work.

### Class Normalization

Class labels require consistent interpretation in schedule processing and again when class documents are parsed.

Example:

```text
source label
  12B) $500 this class name

class_number
  12B

class_name
  $500 this class name
```

`class_number` must remain text so alphanumeric values such as `12B` or `737b` are preserved. The complete original class label must remain available as source evidence.

Class normalization is a concept requirement. It is not authorization to edit the current workflow.

## Preflight

Current preflight checks include:

```text
blank time_text
blank or zero class_no
event_type = 5
ticketed
ticketed schooling
```

Accepted rows remain active. Excluded rows remain visible with `status=preflight` and `preflight_reason` rather than being silently lost.

## FAST Probe

`FAST_probe` is the current fast relevance check for class documents.

Input:

```text
active, non-preflight hs_update_schedule classes
```

Current operation:

1. Fetch the raw `class_oog.php` document using `show_no` and `class_no`.
2. Normalize the searchable document text and helper names.
3. Search the full document for allowed trainer evidence.
4. Record helper horse, rider, and trainer token evidence.
5. If allowed trainer evidence exists, retain the raw HTML for parsing.
6. If no evidence exists, record the checked no-match result.

Current stored evidence includes:

```text
probe_status
probe_attempt_count
probe_attempted_at
probe_finished_at
probe_duration_ms
probe_payload_chars
probe_certainty
probe_reason
probe_raw_stored
```

Current no-match policy permits three total attempts: the original attempt plus two retries. Current certainty is categorical (`high` or `none`), not a percentage.

FAST Probe does not parse horses or entries. It decides whether the source document should continue to parsing.

## Parse And Stage

The current parsing surfaces are:

```text
parse ownership
  hs_class_oog_raw

staged parsed rows
  hs_class_oog
```

Current parsing extracts:

```text
entry_order
entry_no
horse
rider
trainer
```

The parser retains rows matching approved helper evidence, writes staged rows to `hs_class_oog`, and marks the raw document parsed or failed.

The concept also requires consistent class interpretation during parsing. The exact implementation remains unapproved.

## Entries

`entry_go_times` is the current Entrywise prepared list.

It combines staged `hs_class_oog` entries with the applicable class timing state. `go_time` is always an estimate and should become more accurate as live progress is available.

## Helper Hold

Static helper enrichment is held and must not prevent the main path from progressing.

Confirmed relationship shorthand:

```text
show    + trainers
classes + rings
entries + horses
results + riders
```

Owned helper surfaces:

```text
hs_horses
hs_riders
hs_trainers
```

The exact schema and handoff represented by the relationship shorthand remain to be resolved. Future work must not invent those details.

## Live Enrichment By Lane

Live enrichment must also be evaluated ringwise, classwise, entrywise, and riderwise.

| Lane | Live observations |
|---|---|
| ringwise | `time`, `total`, `n_to_go`, `n_gone`, `pace` |
| classwise | `this_class`, `total`, `n_to_go`, `n_gone`, `pace` |
| entrywise | `this_entry`, `n_to_go`, `n_gone` |
| riderwise | estimated result-availability time used to check results only for tracked riders |

`n_go` in discussion refers to the existing `n_gone` field.

`get_rings` and `ring_status` own Live enrichment. `get_orders` is deprecated from the Live lane.

Live or not-live are both valid observed states; the lane continues operating in either state.

## Time Engine Calculate By Lane

Time Engine calculations use the same four-lane ownership model.

| Lane | Current-state surface | Calculated state |
|---|---|---|
| ringwise | `ring_status` | idle, now, nextup, done; late 15, late 30, gate |
| classwise | `class_start_times` | firstup, today, soon, nextup, now, done; `class_is_live`; `starts_in`; `ends_in`; 30/60 thresholds |
| entrywise | `entry_go_times` | firstup, today, soon, nextup, now, done; `go_in`; 20/40 thresholds |
| riderwise | `results_ready` | result availability/change; place, score, time |

`class_is_live` is state within `class_start_times`; it is not a separate prepared list.

`grooms_alert` is an example field/state within `entry_go_times`; it is not a separate lane.

## Position And Time Estimates

Time Engine can estimate state using:

```text
total
n_to_go
n_gone
pace
entry_order
```

`go_time` remains an estimate. As an entry approaches, riders may find position more useful than time.

Example discussed:

```text
entry_order = 18
n_gone     = 14

entries_away = 4
```

The output can retain an estimated time while presenting `4 away` as the more useful near-term state. The exact switch between time display and position display is unresolved and must not be invented.

## Time Engine Expedite By Lane

Expediting follows the same four-lane ownership model.

```text
ringwise
  ring_status
  → update ring state
  → create ring alert when eligible

classwise
  class_start_times
  → update class state, including class_is_live
  → create class alert when eligible

entrywise
  entry_go_times
  → update entry state, including approved entry alert fields
  → create entry alerts when eligible

riderwise
  results_ready
  → update result state
  → create result alerts when eligible
```

The expeditor is not a fifth business lane. It moves each calculation to its owning lane.

## Alerts

`wec-alerts` stores append-only business events.

Alerts are:

- created by the owning ringwise, classwise, entrywise, or riderwise state transition;
- deduplicated by stable event identity;
- not delivery records;
- not reopened, resolved, updated, or deleted as workflow state changes;
- allowed to record a later legitimate transition as a new event.

SMS, email, push, and other delivery behavior remain separate from this concept.

## Outputs By Lane

Outputs follow the same lane ownership model.

| Lane | Surface | Output | Status and calculated fields |
|---|---|---|---|
| ringwise | mobile | rings | `status`: idle, now, nextup, done; `late`: 15, 30, gate |
| classwise | mobile | classes | `status`: firstup, today, soon, nextup, now, done; `starts_in`; `ends_in`; thresholds 30 and 60 |
| entrywise | mobile-pro | entries | `status`: firstup, today, soon, nextup, now, done; `go_in`; thresholds 20 and 40 |
| riderwise | mobile-pro | results | `place`, `score`, `time` |

Read-only lists discussed:

```text
rings
classes
entries
trainers
```

Output data/list concepts discussed:

```text
alerts
times
soon
now
done
ring_status
classes
entries
results
```

Output surfaces discussed:

```text
print
mobile
mobile-pro
```

The current implementation references are `wec-print-live`, `wec-mobile-live`, and `buildMobileProPayload()`. No current `wec-mobile-entry` action was found during this review.

Outputs read the prepared lane state. They do not recalculate or move ownership between lanes.

## Current-To-Concept Differences

These differences are recorded to prevent future sessions from silently presenting the concept as current production behavior:

1. Ring and time normalization are currently embedded inside existing functions rather than implemented as independently named workflow stages.
2. Current class-number handling does not provide the complete reusable alphanumeric class normalization described here.
3. Current Live code performs some class and entry calculations in addition to ring enrichment.
4. Current Time Engine combines calculation, trigger preparation, and direct alert append behavior.
5. There is no separately implemented Time Engine expeditor.
6. Current Results uses broad `hs_result_queue`, `hs_result_classes`, and `hs_class_results` machinery; the Riderwise `results_ready` concept is not yet a proven replacement.
7. `wec-mobile-entry` is discussed as an output surface but was not found as a current action.
8. Static helper relationships are held and intentionally non-blocking.

None of these differences authorizes a repair or migration.

## Drift Guards

Future sessions must follow these rules when using this document:

1. Start by assigning the subject ringwise, classwise, entrywise, or riderwise.
2. Do not convert this concept into an implementation contract without explicit approval.
3. Do not claim that a concept stage already exists because a similarly named helper function exists.
4. Do not move `get_rings` into ring normalization; it belongs to Live enrichment.
5. Do not restore `ring_visual_key` as canonical identity; it is deprecated for new concept work.
6. Do not coerce alphanumeric class numbers such as `12B` or `737b` to numbers.
7. Do not let helper enrichment block the main path.
8. Do not make outputs calculate workflow state.
9. Do not treat `class_is_live` or `grooms_alert` as separate lanes.
10. Keep current production behavior separate from proposed ownership changes.

## Unresolved Items

The following items require later discussion or proof and must remain unresolved until then:

- exact helper relationship schemas and handoffs;
- exact ring prioritization rules;
- exact reusable class-normalization implementation;
- exact formulas and gates for late, firstup, soon, nextup, now, and done;
- exact switch from estimated minutes to entries-away display;
- exact Riderwise result source, key, readiness calculation, and polling behavior;
- exact expeditor function boundaries;
- exact `wec-mobile-entry` output contract.
