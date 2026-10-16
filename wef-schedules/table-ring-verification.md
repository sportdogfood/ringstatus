# Enclosing ring identity — local correction, 15 October 2026
Approved local-only correction after the failed scheduled inheritance release. No deployment, task changes or business runs.

Root cause reproduced from actual rendered Wellington Oct16/show200000074 DOM: the grouped787/835/720 row prints08:00 but contains no ring/cgid parameters; its child rows also have no ring links. The enclosing VanKampen table contains other detail links consistently identifying ring3. Previous matcher rejected the correctly parsed time because row ring was null.

Minimal delta in private collector-core.js: preserve table boundaries and derive table ring only when its detail links give one unambiguous ring ID. Rows lacking ring parameters inherit that table identity. Existing explicit row identities remain. Inheritance state resets for every table and every parser call/day. Unknown/conflicting table identity remains unresolved; no header-name guessing or table-index assignment. Existing group/class matching and override logic unchanged.

Fixture tests/fixtures/rendered-vankampen-20261016.html contains the actual group, child and744 row HTML plus observed enclosing table header, extracted from the read-only DOM inspection. It is a relevant-row excerpt, not a complete saved page. Current rendered744 time was08:30 (the earlier screenshot showed08:27); the test uses the captured08:30.

Full offline suite:58 passed,0 failed. Output table-ring-tests.txt. Actual720/787/835 each resolve08:00 within ring3. Tests cover ambiguous/unknown table rings, missing-link inheritance, independent table/day resets, manual overrides and failure preservation. Prior all-day, zero-entry, history and isolation checks pass.

Not deployed. Next release requires explicit owner approval and normal scheduled verification. The existing deployed parser remains unchanged. Local rendered-fixture proof is not a live recurring-run claim.


## Table-ring release15October2026
Deployed exact58-test package at19:54:35Z between scheduled runs; hashes verified. First normal15:55:01Eastern task trigger ended with3221225786 (0xC000013A), with no new collector START line; latest completed snapshots remain from prior15:50trigger. Cause of interruption not established. No claim of live720/787/83508:00 or two-run verification. No flagged manual overrides in baseline: protection locally tested only. No manual runs, restarts, repair, task changes or further code edits. Stopped verification after scheduler failure per runner boundary. Package remains deployed; task remains enabled and unchanged. Evidence:wef-schedules/runtime/table-ring-deployment.json. Next action: establish cause of task interruption before resuming normal-run observation; no manual substitute.


## Two subsequent normal runs verified â€” 15October2026,16:08Eastern
Read-only interruption investigation: TaskScheduler Operational history disabled; no collectorSTART for15:55trigger; reported0xC000013A. Available System/Application/PowerShell events do not establish who/what terminated it. No cause asserted and no settings changed.
Unmodified schedule subsequently ran16:00(start16:00:03,end16:01:06exit0) and16:05(start16:05:02,end16:08:09exit0; exact END retained in collector log). Both four-day attempt archives confirm720,787,835 onOct16 ring3 at08:00. Airtable readback confirms same three existing record IDs with08:00. No manual runs/restarts/repair required. Protectedsourcehashes unchanged. No flagged live overrides existed; preservation proof remains local tests only. Other unmatched rows are not claimed complete. Full evidence updated in runtime/table-ring-deployment.json.
