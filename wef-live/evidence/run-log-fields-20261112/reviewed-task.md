# Run-log fields — November 12, 2026

Owner APPROVED TO EDIT: add lane, record-type, record-key, trip-id, class-id and ring-id for future run-logs. Keep collection/existing logs unchanged; test/code review/deploy/verify normal runs; no manual runs or record repairs.

Baseline: exact 27JS cleanup-read-recovery-20261112 deployed manifest verified. Repository main; module Git status clean. Deployment uses existing registered task and current module path. Add six scalar fields only to run-logs. Populate future lane/linker/live-check logs; summary type run, record-key run UUID; changes type from affected table and record key from saved record. IDs from exact saved fields/native relations only; unavailable stays blank. Preserve summary/diff bodies, source calls, business saves/links, cadence, cleanup, decoded pause, no Records alerts. No old log backfill or new collector/observer/task.

Candidate isolated under candidate/wef-live. Review is author review, no independent review requested. Acceptance: offline baseline/candidate comparison of requests/business writes and old logs; normal scheduled native readback of summary/change metadata and unchanged prior logs. No manual source calls.
