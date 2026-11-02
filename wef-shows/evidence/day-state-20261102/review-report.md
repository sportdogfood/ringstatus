# Independent show-day state review

Reviewer: /root/shows_review. Applied local Code Review skill, approved-change
boundary and recurring-delivery checks. Exact time/hashes are in the receipt.

No actionable defect found in the frozen candidate. Code review COMPLETE/PASS.
Independent targeted tests: 19/19 PASS (tool 900475), exact command/runtime/times
and separate output streams in review-command-log.json. Deployment and normal-run
readback remain unverified by this reviewer.

Reviewed actual before/candidate runtime and fixture diffs, task.md, saved human
request excerpts, schema.json and before-show-days.json. The recorded human request
explicitly covers Active/Not active, noncurrent priority 999, ISO last_checked and
the existing user-added default lookup, followed by approval to edit. One runtime
file changes; three test files update lookup fixtures/expected new fields, with
README/scope documenting that delta. No dependencies, runtime/framework, tasks,
source cadence, collectors, counts, schema expansion beyond the two approved
fields, alert transport or other module edits are introduced.

The saved REST evidence contains [true] for current show 200000077 and [null] for
old show 200000076. Schema snapshot identifies the lookup as multipleLookupValues,
last_checked as dateTime and last_checked_status as singleSelect. The snapshot
does not contain select options; the writer reports those names verified. Actual
successful writes/readback of both choices remain part of normal-run acceptance.

updatePriority checks native parent, unique show-day key, parent date range,
single lookup value and consistency with parent default. Current days receive
relative date offsets across the show range and Active; noncurrent days receive
999 and Not active. No IDs, links, historical count values or source clocks are
patched. Existing collector calls remain unchanged: discovery/normal priority
phase and a forced phase after missing canonical dates are created. Version 2
initialization enables one normal-wake bootstrap, then retains existing due-work
skips; midnight does not advance planning priority.

Business changes are read back first, authority checked again, then last_checked
is patched/read back. Its value is the phase's supplied ISO check time, not a new
source observation. Failure preserves the priority stop and withholds checkpoint
success. Timestamp PATCH batches are not transactional: a later batch failure may
leave earlier timestamps written, but only after all business values passed
readback; no repair or retry is introduced. Skipped phases preserve timestamps.

Tests cover old/current rollover, version-1 bootstrap and repeat skip, all current
offsets, old 999, mismatched lookup, duplicate/multiple-parent withholding, failed
business readback without timestamp, and actual collector fixture preservation of
count cancellation, old stop bytes and native IDs. Inputs are mock APIs/local temp
fixtures only; no business workflow or provider was invoked.

Downstream read-only trace: wef-live/publication-api.js priorityZero matches exact
show-day key/date/native parent and priority===0. Old priority 999 does not qualify;
this review makes no Live implementation changes or operational claims.

Protected hashes are checked in review-protected.json. Required remaining proof:
existing normal scheduled wake initializes both shows correctly, native IDs and
historical fields unchanged, ISO timestamp/status readback, success logs/heartbeat,
then a normal repeat preserves the verified timestamp when skipped. Existing
automatic Records alert blocker is unchanged. Receipt PASS establishes artifact
integrity, not deployment or scheduled operation.
