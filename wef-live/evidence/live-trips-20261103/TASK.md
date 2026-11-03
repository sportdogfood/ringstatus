# Day-0 live-trips — November3,2026

Owner explicitly approved the discussed plan and its existing cleanup pattern. Add live-trips within wef-live, deriving records from the decoded classes response already fetched by the classes lane; optional UI live facts supplement exact source trip-ID matches. Do not depend on UI membership or retired Entries reads. No new endpoint/status/count/age gates, source calls for testing, manual collector runs, forced events or repairs. Existing source/class scope/identity checks log errors and preserve valid records.

One trip per customer/show/date/ring/class/entry/source-UUID (numeric trip-ID fallback), aligned with Data's established composite pattern. Include scalar source horse/rider/trainer/owner IDs/names, show-level entry-key(show|entry), class/ring/group/schedule native links when available, legacy-live-entry reference when already collected. Do not fabricate entity IDs or add reverse Entries/Data dependencies. Source show/date are direct fields. Preserve all old live-entries records/keys/links, continue current writes during transition; no retirement in this release.

Add a trip's own live-trip-results, plus live-trip-messages and live-trip-comments with native trip links. Results use the source's existing result mapping/verification facts; do not send messages or synthesize comments. Message/comment authoring/delivery remains with downstream owners. No extra bases or entity collectors.

Add the four new show tables to existing fixed cleanup whitelist and checked cleanup control configuration: new show clears them; new-calendar/3hour logging cleanup remains exclusively heartbeat/run-logs/errs-log, current/active/diff protections unchanged. Cleanup delete-only; no new schedule/observer. Existing task/cadence/Stage8 hold and other collectors untouched.

Baseline: current24JS, existing Windows task and live Airtable schema captured before edits. Review/offline checks before deployment; installed readback and normal scheduled proof separate from naturally populated day0 acceptance (source presentlyfalse, priority0Nov5). Do not claim empty runs prove populated trip/link coverage.
