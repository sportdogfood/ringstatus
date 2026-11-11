# Three legacy local tasks: pause and restore

Date: 7 November 2026, America/New_York. Version: 2026.11.07.1.

## Status: all three tasks are now disabled

Owner approved pausing these tasks and gave permission to restart later. The two previously enabled tasks are now disabled; the third remains disabled. No scripts, credentials, triggers or task actions were changed. No collector was started.

Task state read back before writing:

| Task (Task Scheduler root folder) | Current state | Proposed pause | Restore previous state |
| --- | --- | --- | --- |
| epoch-tagger-local | Enabled / Ready; last run 1 August | Disable future starts | Enable existing task |
| ringstatus-heartbeat | Enabled / Ready; last run 7 November, 4 p.m. Eastern, result 0 | Disable future starts | Enable existing task |
| ringstatus-pipeline-local | Already disabled | Leave disabled | Leave disabled |

The tasks remain installed. Do not delete or recreate them, change their triggers, replace their scripts, or enable all three as a group.

## What the proposed pause affects

The old heartbeat runs the legacy tagger, heartbeat patterns, cadence controller and slot orchestrator. Its dispatched work includes legacy schedules, trips, live feeds, publishing and WEC jobs, subject to existing gates. Pausing it suspends those future dispatched runs, not just SGL requests. Existing legacy schedule logs show HTTP 403; pausing is not a source-error repair.

These tasks do not directly start or control the current decoder runner, tunnel, Data/Fast, current WEF Live, Shows or show-time tasks. Their independent tasks are outside this pause. This is a task-control finding, not proof that no downstream consumer uses legacy-produced data.

## Exact pause procedure — only after owner approval

1. Read the three current states again. If they differ from the table, reconcile before proceeding.
2. Export the existing three task definitions to an agreed backup location before changing them. Keep existing triggers, actions and credentials untouched.
3. Disable only the two currently enabled tasks:

```powershell
Disable-ScheduledTask -TaskPath '\' -TaskName 'epoch-tagger-local'
Disable-ScheduledTask -TaskPath '\' -TaskName 'ringstatus-heartbeat'
```

4. Leave `ringstatus-pipeline-local` disabled.
5. Verify all three show disabled. Check for an already running instance or dispatched child: disabling future starts does not stop work already running. Report any such instance; do not terminate it without separate approval. An in-flight cadence controller can enable the heartbeat again, so its completion and the final disabled state must be checked before claiming a full pause.
6. Record actual pause time and observed final states here. Do not claim this procedure has run until that evidence exists.

No Stop-ScheduledTask, process termination or manual collector execution is authorized by this document.

## Exact restart procedure — restore normal scheduling

After owner approval to resume, enable only the two tasks that were enabled before the pause:

```powershell
Enable-ScheduledTask -TaskPath '\' -TaskName 'epoch-tagger-local'
Enable-ScheduledTask -TaskPath '\' -TaskName 'ringstatus-heartbeat'

Get-ScheduledTask -TaskPath '\' -TaskName 'epoch-tagger-local','ringstatus-heartbeat','ringstatus-pipeline-local' |
    Select-Object TaskName, State, @{Name='Enabled';Expression={$_.Settings.Enabled}}

Get-ScheduledTaskInfo -TaskPath '\' -TaskName 'epoch-tagger-local'
Get-ScheduledTaskInfo -TaskPath '\' -TaskName 'ringstatus-heartbeat'
```

`ringstatus-pipeline-local` stays disabled because that was its original state. Enabling it would be a separate decision, not restoration.

Do not use Start-ScheduledTask or invoke any collector script manually. Enabling returns control to the existing triggers; inspect their next-run times and existing catch-up settings. Resume may begin work according to those settings. Verify the next normal run from its existing logs. An enabled state alone does not prove successful collection.

## Protected tasks and implementation

Do not change decoder relay/tunnel tasks, the shared proxy, one-second pause, fallback, token renewal, agent URLs, current Data/Fast/Live/Shows tasks or show-time cadence. Pausing legacy tasks does not fix the separate decoder listener/fallback outage.

## Execution record

- Pause: EXECUTED and read back; all three disabled. The epoch task required Windows administrator elevation after an initial access-denied response.
- Restart: NOT EXECUTED. Owner granted permission to restore the two previously enabled tasks later; no immediate restart is requested or scheduled. Windows administrator approval may be required for epoch-tagger-local.
- Evidence: [task backups and final state](evidence/legacy-local-task-pause-20261107/verification.json). Before/paused XML definitions are in the same directory.

Pause verification recorded at 2026-11-07T21:02:39.568Z. No matching legacy launcher, tagger, cadence controller or slot-orchestrator process was found during the initial pause check; no process was terminated.
