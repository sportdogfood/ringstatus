# Detailed collection activity gate - 23 October 2026

Scope: wef-live only. Shared source transport, shows, schedules, entries and CWF unchanged.

The existing five-minute task reads canonical show context, then checks live status.
An explicit false status skips detail collection. Otherwise ListAjax is validated for
the selected show and TODAY. A nonempty set with every group explicitly Completed
also skips. Empty, mixed or malformed responses do not imply completion.

Skipped checks archive the status/group evidence and update run-logs and heartbeat
with result Skipped and the reason. They do not fetch widget/class/decoded detail,
write live data, or retire existing data. Each subsequent scheduled check evaluates
fresh status and groups, allowing collection to resume without an enable action.

24 offline tests passed, including false status, completion scoped to TODAY,
invalid status, empty/mixed groups, data preservation and resumption.
First natural verification: run 0a893b83-b475-4ed1-b9ea-c7fc91e00414,
17:12:13-17:12:18 ET. All 19 groups completed; result Skipped confirmed in Airtable.
Only source-live-status and source-groups archived. Existing 198 entry records retained.
Second natural verification: cf631314-826d-4126-824f-a567e6650892,
17:17:29-17:17:43 ET. Same all-completed skip, no errors, confirmed in Airtable
run-logs and heartbeat. Both runs archived only status and groups, with no detail
collection. Evidence: activity-gate-normal-runs.json. False-status branch and
automatic resumption passed offline tests; those transitions were not observed
in these two normal runs.
No manual runs or record repairs.
