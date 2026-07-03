# WEC Catalyst Cadence Step 3 Class OOG Contract

## Purpose

Step 3 is the bounded, checkpointed `class_oog` stage for the active WEC show/day. It consumes the locked Step 2 `hs_update_schedule` output, probes HorseShowing `class_oog.php` only in bounded chunks, materializes only active-trainer entry matches, mirrors those matches for Airtable visibility, and stops after `hs_class_oog`.

This contract exists to prevent broad class OOG materialization, repeated all-day probing, and accidental downstream execution.

## Active Focus Source

Step 3 resolves the active focus dynamically from Airtable `focus_show`.

It must use:

- active `show_no`
- active `focus_day`
- active `focus_show` record id
- active pause/lock/live fields as read-only context

Step 3 must not hardcode a focus day and must not use old-day fallback rows.

## Actions

Step 3 action:

```text
wec-step3-class-oog
```

Step 3 cleanup action:

```text
wec-step3-clean-active-class-oog
```

The cleanup action is limited to the active focus and may clean only:

- Catalyst `hs_class_oog`
- Airtable mirror `hs_class_oog`

It must not delete legacy Airtable `class_oog` rows or any downstream rows.

## Source And Outputs

Step 3 source table:

```text
Catalyst hs_update_schedule
```

Step 3 output table:

```text
Catalyst hs_class_oog
```

Step 3 Airtable mirror:

```text
hs_class_oog
```

Step 3 checkpoint location:

```text
hs_heartbeat
```

Checkpoint key pattern:

```text
show_no|focus_day|step3-checkpoint
```

Example:

```text
14909|2026-07-02|step3-checkpoint
```

## Step 2 Handoff

Step 3 is tied to Step 2 through current-day `hs_update_schedule` rows.

Latest locked handoff evidence:

```text
hs_update_schedule current-day rows = 75
preflight rows = 25
non-preflight rows = 50
```

Step 3 must recompute the locked Step 2 preflight rule and use only the non-preflight rows as candidate class inputs.

## Active Trainer Entry Scope

Active trainers come from Airtable `trainers`.

The active entry-number scope comes from trainer-linked `entries` records.

Latest active entry scope evidence:

```text
active entry_no count = 38
```

Step 3 must materialize only parsed class OOG rows where `entry_no` matches the active trainer entry-number scope.

Trainer-name matching is not the primary contract when entry numbers are available.

## Bounded Chunk Behavior

Step 3 must probe `class_oog.php` in bounded chunks.

Approved chunk proof used:

```text
step3_offset = 0
step3_limit = 8
```

Step 3 may process the next unchecked chunk on later cadence runs. It must not probe all 50 non-preflight classes in one unbounded run.

## No-Repeat Checkpoint Behavior

Step 3 stores checked class state in `hs_heartbeat` under the checkpoint key.

For each checked class it records:

- `update_schedule_key`
- `class_no`
- `ring_day_no`
- `ring_no`
- schedule signature
- last checked timestamp
- parsed row count
- matched row count
- skipped broad row count

On repeat cadence, Step 3 compares the current `hs_update_schedule` row signature against the checkpoint.

If the class has already been checked and the signature is unchanged, Step 3 must not call HorseShowing for that class again.

## Re-Probe Rules

Step 3 may re-probe a previously checked class only when one of these is true:

- active `focus_show` changed
- active trainer entry scope changed
- `hs_update_schedule` row signature changed
- checkpoint is missing or corrupt
- explicit `step3_force=1`

No TTL re-probe is locked in this contract.

## Latest PASS Evidence

Chunk 1:

```text
offset = 0
limit = 8
parsed rows = 200
matched rows = 2
broad nonmatching skipped = 198
```

Repeat same chunk:

```text
class_oog requests = 0
skipped already checked rows = 8
```

Next chunk:

```text
offset = 8
limit = 8
parsed rows = 146
matched rows = 8
broad nonmatching skipped = 138
```

Checkpoint after next chunk:

```text
checked classes = 16
next unchecked index = 16
```

Mirror counts after next chunk:

```text
Catalyst hs_class_oog count = 10
Airtable hs_class_oog count = 10
```

## Stop Condition

Step 3 stops after `hs_class_oog`.

The stage is complete for a chunk when:

- the bounded chunk was evaluated
- active-entry matches were written to Catalyst `hs_class_oog`
- active-entry matches were mirrored to Airtable `hs_class_oog`
- broad nonmatching rows were skipped
- checkpoint was updated
- no downstream lanes ran

## Lanes Not Allowed In Step 3

Step 3 must not run:

- `update_schedule_staging`
- `class_start_times`
- `entry_go_times`
- `get_orders`
- `get_rings`
- alerts
- print/PDF/UI/AppSail/Barn Board

## Next Required Gate

Before Step 3 is wired into a cadence action, complete the remaining Step 3 chunks across all 50 non-preflight classes.

The next gate must prove:

- all 50 non-preflight classes were checked through bounded chunks
- repeated unchanged chunks no-op
- final Catalyst `hs_class_oog` count equals active-entry matched rows
- final Airtable `hs_class_oog` count equals Catalyst `hs_class_oog`
- no downstream/get_orders/get_rings/alerts ran

Only after that proof should Step 3 cadence wiring be considered.
