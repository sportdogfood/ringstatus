# Entries/CWF preservation verification - 29 October 2026

Status: BLOCKED by observed CWF task termination; two postrelease pairs NOT accepted.

Release cutoffs reported by coordinator: Data 13:51:44.624201Z; Shows 14:01:45.331Z.

Entries first fully postrelease ordinary run e1a2e112-e33d-44e9-a5c5-aedab752e014 started 14:04:02.382Z, completed 14:06:08.701Z, ok true, three rows, no errors. Saved receipt: entries-1404.json.

CWF postrelease run 4f8ab51c-2608-4bce-8ce3-52d434b22ead started 14:02:04.209Z. Airtable run-log rec0WWH892a64A9Cz showed Running, no completed timestamp. Local last-run.json remained successful older run d020d4d0-ecd0-45bc-abeb-aa315953357b, started13:52:03.585Z, completed13:57:26.434Z, consumed Entries13:50:49.871Z. That older pair is outside final deployment window.

Read-only Get-ScheduledTaskInfo subsequently reported LastRunTime 10:02:01 Eastern, LastTaskResult267014 (0x41306, terminated), NextRunTime10:12. Subsequent task inspection straddled the next normal trigger: LastRunTime10:07:01 Eastern, result267009 (0x41301, running), stateRunning. Settings ExecutionTimeLimit PT4M, MultipleInstances IgnoreNew. Collector log has START10:02:03.9456864 and START10:07:03.6109385, no corresponding terminal completion in inspected tail. TaskScheduler Operational event query yielded no event evidence; precise termination cause is not proven. Four-minute limit is a possible explanation, not an established root cause.

Stopped acceptance verification on this failure. No task stopped/disabled/retried manually; naturally scheduled runs remain outside Codex control. No repair or code change attempted. A later natural success must not erase this failed acceptance.

Baseline.json retains read-only Airtable snapshots of 13 CWF synced entries (3 current team and10 historical),76 horses,37 riders. Current3 had native show/schedule/ring/horse/rider/trainer links at baseline. Detailed postrelease helper/link preservation and two paired cycles remain unverified. Private-list populated access remains outside this proof. Code-observation.json and task observation XMLs capture current files/configuration, not a predeployment task comparison.

Only local proof artifacts written. No code/schema/records/tasks/source endpoint calls/manual business runs.
