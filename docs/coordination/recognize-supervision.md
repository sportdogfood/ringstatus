# Recognize one-off task supervision

Owner requested 8 October 2026: coordinator owns the active-agent record, keeps checking, records failures and updates completion. Applied to the already-authorized Recognize task; no application changes authorized by this monitoring record.

- Coordinator: `01a11632-62fd-73b1-997e-8be4119185de`.
- Worker: `01a11cc4-39cd-7eb2-872f-89ef41193275`.
- Assignment: [Recognize wiring](recognize-wiring-task.md); complete acceptance: [saved plan](recognize-native-astro-plan.md).
- Registry base: `appZahVgD156cMAe3` (agents). Application data/log base stays `app9kOZdIaGyKk5uG` (Recognize).
- Active record: [`recXpqNMKrk6b7tg5`](https://airtable.com/appZahVgD156cMAe3/tblrEo4YLRQtN5AM2/recXpqNMKrk6b7tg5), table `active-threads` / `tblrEo4YLRQtN5AM2`.
- Coordinator ownership is explicit in thread-overview; no owner column exists and no schema was added.
- Monitor: `recognize-one-off-task-supervision`, thread heartbeat every3minutes. First natural scheduled check observed2026-10-08T18:35:58.243Z; read worker snapshot revision9, evidence and updated Airtable. Worker active; no nudge sent. This proves one scheduled check, not continuous reliability.
- Worker start: 2026-10-08T18:26:25Z. Maximum assignment deadline: 2026-10-08T19:26:25Z. Honor earlier recorded limits; no reset. Final15minutes are verification/checkpoint time.
- Earlier runner start.json inspected at first scheduled check: declared start18:24:00Z, implementation cutoff19:09:00Z, deadline19:24:00Z. Honor the earlier19:24 deadline; retain discrepancy between declared start and actual native turn start rather than inventing a reset.
- Small coordinator-owned state: `.git/ringstatus-control/recognize-supervision.json`. Do not edit the worker's implementation or evidence.

## Checks and actions

Use wait_threads with saved cursor and a compact immediate snapshot. Read only new task evidence when necessary. Record actual last check, observed worker state, next action and meaningful evidence references in the active row. Preserve existing owner/scope text. No repeated broad discovery.

Active worker: inspect progress, do not send repeated prompts. Idle unfinished worker: determine cause; only send an exact same-task continuation if executable within authorization/budget. At most one continuation per distinct checkpoint and two automatic continuations total. Persist intent/result to avoid replay. User decisions, publication/deployment approval and expired budgets are not nudging opportunities. No new worker or changed scope.

At deadline, one save-and-stop message if still working, preserve incomplete status and pause this monitor. At an actionable dependency requiring the owner, record it, notify once and pause this monitor until it changes. Notify only meaningful changes, required owner action, failures or verified completion; unchanged polls stay quiet. Do not modify other monitors, including usage alerts and paused coordination proofs.

Completion: independently inspect evidence against the full assignment, including required live/post-publication gates. Update the SAME record with proof and time; set active=false, closed=true; pause this monitor. A paused/blocked/timed-out task remains closed=false; record exact state and whether any work is running. No automatic future task.

## Existing fields for narrow writes

Active table: Name `fldnEf55xuiTjxfxy`; active `fldS179jFXA7WDGOC`; closed `fld6rtJTHUnMAQOez`; thread-overview `flduQTc2694atgAn8`; performance-complaints `flde7K3mLM6pl9wZ5`. Preserve unrelated fields and existing links.

Failure table `rs-agents-complaints-lib` / `tblPRAjSK54F8VA8U`: Name `fld26uoOiURXkOPy3`; active-threads link `fldGQMdzzo8FcKe7R`; complaint `fldTfgHVQZfCSywak`; solution `fld4AAX7I7QwrV0zO`; source `fldVC5hFr52tiM2tq`. Read current schema if connector requires it. Use base-level record IDs for links; do not invent IDs/select choices.

Existing failure `recydcuqDQ6U1zXkp`: unsupported Webflow script/style action calls, linked to this active record. Source: worker turn `01a11cc4-3fec-7213-a607-ac81704d3502` failed calls, observed by parent. Correct guide action names sent to worker: get_page_freeform_code, get_page_scripts and get_styles. Resolution PENDING successful readback. This is not an authentication/Bridge failure diagnosis or a whole-task failure. Do not duplicate the incident.

For new failures, log only observed evidence, impact and verified correction status. Readback each registry mutation. A missing connector is a monitoring blocker; do not fabricate records or switch to application-data writes.

Pause/unwind: pause `recognize-one-off-task-supervision` through automation_update; retain audit records. It does not stop the worker automatically or remove data. Any owner transfer must update the same active record and monitor ownership to avoid two coordinators issuing instructions.
