# Shows workspace instructions

## Weekday notice tolerance - approved November 4, 2026

The existing Codex daily scheduler is the trigger;19:00Eastern remains its target.
The normal weekday notice accepts a trigger from18:40Eastern (up to20minutes early)
through the remainder of that local date. It does not require an exact19:00match.
This applies every day, not only to the initial failed wake. One event per Eastern
trigger-date retains all five recipients, durable send intent, duplicate protection,
actual acknowledgment checks and cleanup-reminder-only restrictions.
The notice announces the next weekday/date; it does not change show day0/priorities.

The November4 18:57:03 wake was rejected by the former strict rule; that evidence
is preserved. The revised normal route is ACTIVE and read back against review;
22independentofflinechecks PASS. Normal delivery and all five acknowledgments are
PENDING the next native occurrence (November5,19:00Eastern target). No manual replay.
See [tolerance deployment](evidence/weekday-tolerance-20261104/deployment.json).

## Daily weekday notice - approved November 3, 2026

One native Codex automation, `wef-shows-daily-weekday-notice`, is ACTIVE daily at
19:00 America/New_York in the existing Shows chat. It sends the evening weekday/date
handoff and cleanup reminder to Records, Timing, Messages, Waze and Publish, even
when there is no show-day. This notice does not change show day0/priorities or
execute cleanup. The locked collector and all existing tasks remain unchanged.

Actual acknowledgments from all five are required, with durable send intent,
deduplication and receipt follow-up in the same daily workflow. The only runtime
write is `.git/ringstatus-control/wef-shows-weekday-notice-state.json`.
First eligible normal run: November 4, 2026 at19:00Eastern; no November3backfill.
Configuration readback and independent19offlinechecks PASS. Actual scheduled sends
and five receipts remain PENDING that first normal run; no manual runs/sends made.
See [deployment and remaining proof](evidence/weekday-notice-20261103/deployment.json).
This narrow owner approval does not deploy the held hourly heartbeat change or
install automatic NEW SHOW/NEW SHOW-DAY discovery delivery.

## LOCKED - owner decision November 2, 2026

Verified deployed baseline: `day-state-20261102`. Shows owns show identity/dates,
planning day0, day-priority and existing archives/heartbeat/logs. Current show-days
are Active/relative priority; historical days are Not active/999, with ISO last_checked.
New-show discovery stays Sunday22:00 Eastern with three-hour retries; day selection
stays after19:00. Deployed heartbeat/run-logs remain every five minutes.
Routine runs and the existing read-only Monday review continue unchanged.

Pending, outside verified lock: hourly out-of-show heartbeat/logging is prepared,
reviewed and registered but NOT deployed; automatic discovery delivery to all five
coordinators is NOT configured. The required recipient policy is in
[discovery-notices.md](discovery-notices.md). Do not treat either pending item as live.
No further implementation/deployment without explicit owner approval reopening the
specific requirement. Read-only defect investigation is not repair authorization.
Preserve cancelled counts, historical evidence, archives and existing stops.
Lock record: [locked-20261102](evidence/locked-20261102/lock.json).

## Discovery notice recipients - owner instruction November 2, 2026

For verified NEW SHOW and NEW SHOW-DAY events, Shows sends the same evidence
directly to wef-records-coordinator, wef-timing-coordinator,
wef-messages-coordinator, wef-waze-coordinator and wef-publish-coordinator.
Use [discovery-notices.md](discovery-notices.md) for resolved chat IDs, notice
content and delivery boundaries. This supersedes older Records-only recipient
wording below; it does not change collectors or install automatic transport.

Owner instruction November 3: receipt follow-up is part of Shows' task. Confirm
the actual acknowledgment from EACH of the five coordinators, retaining send and
receipt times/evidence. Follow up on missing acknowledgment and report gaps.
Sending alone is not completion; do not confuse recorded notification state with
confirmed receipt. See discovery-notices.md. This adds no unattended monitor.

## Current owner-approved scope - October 31, 2026

The latest approval cancels entry/trip counting and its timeout repair. Only
shows, dates, selected day/day-priority and Records transition alerts remain.
The scope-only runtime release removes count invocation and count-stop dependency;
retain historical counts, evidence and stop files unchanged. Preserve existing
source/calendar/priority rules, archives, heartbeat/logging and task cadence.
No Data dependency, timeout fix, new service/observer/alert framework, schema or
record repair is authorized. Automatic Records delivery remains unimplemented
because no installed runner-to-chat route exists; do not call manual delivery
recurring automation. The failure record below is historical; this newer scope
supersedes its open-count-repair statement and earlier count ownership text.


## Owner correction and failure record - October 31, 2026 - v1.0

This dated entry applies to the wef-shows owning chat. The owner requested it
after missed follow-through. It adds no runtime, Airtable, deployment or repair
permission. Current owner-approved README/scope amendments supersede older
baseline descriptions below: Shows counts directly through its proxy, independent
of Data. Do not restore the historical Data count dependency.

### Failures to retain

- I sent Records an alert requirement but did not implement or establish automatic
  delivery. Sending a requirement is not completing the requested outcome.
- After verifying the October 31 normal 19:04 Eastern rollover to November 1 and
  four day-priority updates, I failed to send the already-authorized Records
  notification until the owner challenged the omission. There was no demonstrated
  access or permission blocker to that manual handoff.
- I substituted explanations and acknowledgments for completing that authorized
  handoff. The owner, not this agent, decides which requested outcomes to drop,
  defer or replace. Routine execution choices do not authorize ignoring a request.
- The direct-count release tests/review missed native TimeoutError(code23).
  Existing error classification discarded the name, defeating bounded recovery.
  Initial successful runs did not prove timeout handling. The exact reason source
  requests became slow remains unverified; do not invent that explanation.

### Reminders and frequency

- At the start of EVERY owner turn and EVERY resume: recover the active request,
  still-open earlier requirements, approvals, prohibitions and promised handoffs.
  New requests supplement earlier ones unless the owner explicitly replaces them.
- Before EACH mutation, handoff or scope decision: compare the next action with
  the owner's accepted outcome. Do not add work, change ownership, decide a request
  is unnecessary, or treat diagnosis/review as repair approval.
- After EACH verified show or selected-day change observed during authorized work:
  complete the owner-authorized notification to all five coordinators listed in
  discovery-notices.md, with
  old/new values, normal-run time and evidence. Do not wait for another reminder.
  This does not authorize new polling, observers or a new messaging transport.
- Before EVERY final response or milestone handoff: check each requested outcome
  separately. Report completed, still open and specifically blocked items; finish
  executable authorized work first. Message sent, recipient acknowledgment and
  automatic delivery installed are three different claims requiring evidence.
- At EVERY correction/failure or material phase change: apply Rebalance before
  another action. Retain the existing 30-minute active-work context checkpoint;
  these reminders are in-session checks, not new timers or background jobs.

### State at this entry

Records was sent the verified November 1 rollover and priorities in chat
01a169bc-e58a-7de0-94c6-5d3931fcf745; recipient acknowledgment has not been checked.
Automatic discovery alerts remain unimplemented. Count publication is stopped
after the October 31 17:44 run; investigation/review is complete, but no timeout
repair, stop release or new verification run is authorized. Selected-day and
priority publication succeeded independently. Preserve existing workflows and
stops; no manual collector runs or record repairs.

This entry records procedural checks, not a guarantee of future compliance or
unattended alerts. Read it with current owner instructions and current evidence.

Applies only to `wef-shows`. Preserve root AGENTS.md and higher-priority
instructions. Shows remains LOCKED scope v1.4, runtime release
`count-replay-fix-20261030`; documentation/review coordination does not reopen it.

## Load before dependent work

Explicitly use Instruction Loader. Read the physical owner block
[WEF owner operating instructions](../docs/coordination/wef-owner-operating-instructions.md)
at `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/docs/coordination/wef-owner-operating-instructions.md`.
It was actually read in this workspace on 2026-10-30. A link does not confer
access elsewhere: if unavailable, report the specific missing source and continue
only independent authorized work. Read this module's README current-state section,
scope.md, applicable current request and exact linked release evidence.

Use the verified installed RingStatus Operating Skills **0.2.0** under
`C:/Users/gombc/.codex/plugins/cache/created-by-me-remote/ringstatus-operating-skills/0.2.0/skills/`:

- `instruction-loader/SKILL.md`
- `context-compact/SKILL.md`
- `code-review/SKILL.md`
- `rebalancer/SKILL.md` (owner's `$rebalance` alias)

Read/apply their relevant full instructions on startup/resume and material
direction changes. Handoff is for an authorized transfer; do not invent a transfer
or new chat. The existing local `C:/Users/gombc/.agents/skills/code-review/SKILL.md`
was also read and underlies the recorded review. Do not claim package updates or
new versions merely from receiving a message. Distinguish READ, APPLIED and VERIFIED.

## Ownership and hard boundaries

Shows owns canonical show/default discovery and show-day publication in
`appCaSmL819KK7UFJ`. Data supplies retained count evidence directly; Shows is not
the source of all entries. Season coordinator retains Data acquisition and
separately Season retention. Entries links only; Live observes independently;
Records coordinates contracts/reviews; the owner approves changes.

Preserve current discovery/source checks, count replay, original observation times,
Unknown/Stale/verified-zero semantics, native IDs/links, archives and failure stops.
Schedules remains running. Retirement/Data-only migration is planning only; no
ownership transfer or shared-helper deletion is implied by directory names.
Owner correction: no WEF business observers or replacement polling reminders.
Inspect natural acceptance evidence only during authorized work or on request;
leave unobserved gates unverified. Existing collectors and periodic code reviews
continue unchanged. Context bookkeeping is separate, not a business observer.

Codex is not the runner. No manual collector/provider substitute, record repair,
failed-path retry, stop-file clearing, task/cadence change, deployment or code fix
without applicable explicit authorization. Review findings are not fix approval.
Continue independent authorized work and request only the concrete missing approval.

## Review and continuity

Read [review standard](../docs/coordination/wef-code-review-standard.md). Review
material approved changes before deployment and after behavior/verification
failures. Periodic review is separately owner-authorized and Records-managed;
verify actual automation ID/target/status before claiming it runs. Never create
review automation or write review schema/records under this documentation request.
Preserve dated reviews; do not rerun unchanged-release tests/review per collector
invocation. Keep code review, offline tests, deployment, normal-run proof and source
freshness separate.

At task boundaries and first tool boundary after 30 minutes of active work, follow
`../docs/coordination/context-check-procedure.md` and the context-compact skill.
Use this chat's real telemetry/rollout only; unknown is not a fabricated percentage.
The owner's **65% context preparation threshold** overrides the packaged skill's
70% default. Prepare continuity at that threshold when supported by real telemetry;
do not invent usage, force compaction or restart stopped work.
Save an authorized concise private checkpoint before compaction, never raw chat
history in this repository/Airtable; do not claim compaction or restored continuity
without observing and checking it. Resume from current README/scope/release,
preserving approvals, stops, exact evidence and next authorized action.
