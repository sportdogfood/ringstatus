# Sol Medium — independent WEF schedules maintenance

## Monday review schedule authorized — 20 October 2026

Owner requested actual routine code review every Monday. Created active thread heartbeat `wef-collector-monday-code-review`, Mondays 09:00 America/New_York; first planned review October 26. This specifically supersedes the earlier no-new-AI-monitor authorization boundary for this bounded weekly code review only. It does not change any collector, heartbeat, epoch or pipeline business cadence. Review is read-only for code/business state; only existing Airtable code-reviews tracking record recPMdSYjGfYV6M6Y in base appUq5Qz9kARqFija/table tblaLOJqekCeyvKgV may be updated with actual review date/result/evidence and following Monday. Local fixture tests only when warranted by changed code. No automatic fixes/deployments/manual business runs/repairs/new agents/other automations. Keep unchanged results quiet; notify material findings, failure or required owner action.

Current last-result is Passed — run/archive logging release verified (October 20), not an active error. The duplicate-create finding from October 16 is resolved; Airtable notes now distinguish it explicitly from the current result. The previous next-review date October 23 is superseded by October 26. Full release evidence remains runtime/run-logs-release-20261020/.



## Default-day priority deployed and verified — 15 October,17:37 Eastern

Owner-approved collect.js deployed only to independent package;64 local tests passed. Normal17:30 and17:35 triggers exited0. Both API discoveries returned default day2026-10-15; each collected15,16,17,18 in that order, with all show days included. Per-day captures0/46/155/139 matched, no capture errors/unmatched. Existing emptyOct15 warning retained. Airtable collection readback confirmed completion. Manual focus remains unchanged. No manual runs, scheduler or protected workflow changes. Source-day16 priority is fixture-proven, NOT live-observed: API still returned15 despite owner's website observation16. Preserve that distinction; do not invent or force a default day. Evidence: wef-schedules/runtime/default-day-deployment.json. Maintenance agent should adopt this update on its next authorized task, not redeploy or repeat completed work.

Prepared handoff only. Do not start old implementation, deployment or verification assignments. Runtime collection already continues through Windows Task Scheduler; Codex is not the runner.



## Role, model and first action

Use GPT-6.1 Sol, reasoning Medium as owner requested; verify actual chat settings instead of claiming a prompt changed the model. Maintain only the independent WEF schedules package. First load applicable AGENTS.md and this handoff, then STATUS.md and the current-status section of the plan. Confirm those sources are accessible. Continue with one bounded read-only changed-evidence check if maintenance is requested; do not replay the full conversation, discovery, tests or two-run acceptance. Report only a material finding or concrete owner action. No new agent/chat or recurring AI monitor is authorized by this handoff. No operational restart.



## Locations and connections

Repository: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus

Editable isolated source: C:/Users/gombc/.codex/worktrees/wef-schedule-collector/ringstatus/wef-schedules

Deployed private package: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/wef-schedules

Plan: docs/coordination/wef-schedules-only-activation-plan-20261015.md

Resume/status: deployed package STATUS.md

Airtable connector: wef-schedules appUq5Qz9kARqFija; meta tblwBvigcmqRlSYUb; collections tblvGpwxGjXXNLKAU; schedules tblqiTFqqO3zk2KJC. Use connector before browser. Windows task wef-schedules-collector is ENABLED, five-minute cadence, IgnoreNew, interactive user, run.ps1. Read-only task/log inspection via PowerShell. No Cloudflare, Twilio, Webflow or legacy Airtable integration is required here. Secrets remain private in runtime/collector.env; never print or copy them to prompts.



## Locked workflow

Reuse the independent copies of proven WEF code; no reinvention or shared imports. Discover SGL default show; collect every day in that show's date range. Prioritize today/tomorrow, retain later days as fallback. Empty dates within the range generate anomaly warnings; owner confirmedOct15 empty. Between-show gaps are normal: Oct19 default may point to a show beginningOct21/22. Do not require contiguous shows.

One current Airtable row per show/date/source key, readable schedule fields. Preserve separate gzip history before cleanup. Rotating transport tokens do not count as business changes. Added/changed/dropped/reactivated records use existing guards; failed/incomplete captures cannot invent drops or times. Do not modify manual fields/overrides. Estimate inheritance stays within ring/day. Existing group IDs take precedence; missing-ID groups match their complete existing class-number set. Repeated and zero-entry classes included; absent/ambiguous groups remain unresolved. No guessed nearest/first time. Previous times survive capture failure.



## Completed evidence — do not repeat

## Current status — 15 October 2026, 16:51 Eastern

Group-matching release acceptance PASS. Two consecutive normal scheduled triggers16:40:01 and16:45:01 exited0; completed16:43:14 and16:50:42. Each captured Friday46, Saturday155, Sunday139 active rows; Thursday0 (owner-confirmed empty). Every populated day had zero capture errors and zero unmatched rows. Airtable readback matched all340 active times, zero blanks/mismatches. Repeated classes and zero-entry groups included.28 previously blank dropped records stayed blank; missing times were not invented. Manual override tests pass, but no flagged live override cases existed.



The16:45 run overlapped16:50; IgnoreNew prevented another instance. Five-minute trigger is not a guarantee of five-minute completion. Task remains enabled and unchanged. Earlier16:35 Saturday capture failure is retained: zero parsed timed rows,130 prior values preserved; exact underlying cause unavailable because HTML/browser diagnostics were not saved. Two later clean runs do not erase that failure.



No immediate repair, redeployment, schema migration or re-verification assignment remains. Maintenance is read-only until a new authorized change. Future default-show transition across a gap and live manual-override cases remain naturally observed coverage, not failures or permission to manufacture data.



Evidence: wef-schedules/runtime/two-clean-runs-20261015.json; group-matching-deployment.json; group-matching-airtable-readback.json; collector-2026-10-15.log; history/attempts and history/datasets. Local suite61 passed. Source repeated-group-verification.md contains test scope.



## Skills to load, narrowly

Read actual available SKILL.md files and record loaded paths; do not claim skills loaded from names alone. Applicable AGENTS.md is mandatory. Airtable overview skill for schema/records; systematic-debugging for an actual failure; verification-before-completion for acceptance claims; code review via available review capability before an approved new release. Use source catalog paths when versions move. Known catalog paths:

- C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/airtable/7.0.0/skills/airtable-overview/SKILL.md

- C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/superpowers/6.4.2/skills/systematic-debugging/SKILL.md

- C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/superpowers/6.4.2/skills/verification-before-completion/SKILL.md

- C:/Users/gombc/.codex/plugins/cache/openai-curated-remote/superpowers/6.4.2/skills/requesting-code-review/SKILL.md

Do not load Recognize/Tenants or unrelated UI skills. Follow repository context-check procedure at boundaries and after30minutes; keep output compact and use narrow saved evidence.



## Maintenance and authorization

First check only added logs/latest complete collections against the saved checkpoint. Distinguish scheduled trigger, actual START, completion, warning and failure. Exit0 alone is insufficient: inspect estimated_times errors/unmatched and Airtable readback. Runtime/history/current.json identifies datasets; immutable attempts preserve prior runs. Do not rewrite those runner-managed checkpoints. Keep maintenance notes in meta and STATUS.md with timestamp and exact evidence. Existing task remains responsible for recurrence.

Do not execute collect.js, run.ps1, Start-ScheduledTask, direct business endpoints or scrapers. Do not repair records, alter settings/task cadence, disable workflows, redeploy or edit code without new authorization. A failure permits read-only evidence collection, not a retry or patch; obtain separate APPROVED TO EDIT as AGENTS.md requires. No shared/root files, existing heartbeat/pipeline/epoch tasks, other bases or workflows may change. No unsolicited architecture, schema additions or monitoring automation.

If capture fails again, preserve exact saved evidence and distinguish known failure point from unknown cause. Current capture uses a fixed5second wait and saves no failed HTML; do not assert slow rendering as proven. A bounded rendered-row readiness check plus failure evidence is a possible future proposal, NOT an approved change. Never restart completed token-comparison, staging, inheritance or group-matching work.



## Run/archive logging verified - 20 October 2026, 09:26 Eastern

Airtable run-logs is live: one record per actual collector run, readable per-day rows/changes/warnings/errors and incremental archive details (kind, show/date scope, count, file, SHA256 and cleanup outcome). Normal triggers 09:20:01/09:25:01 exited 0, completed 09:20:48/09:25:44. Exactly two distinct run records; four verified attempt archives each. Both report 435 active rows, all four October 22-25 dates complete, no time errors/unmatched rows and only the empty-October-22 warning. All 435 schedule IDs and values remained unchanged; no duplicate log rows or schedule growth. Local suite 75 PASS; release review no material findings. Single-attempt log POST avoids duplicate-create retries; uncertain outcomes use read-only stable-ID lookup. Existing heartbeat/epoch/pipeline/collector task definitions and protected sources unchanged. No manual runs or repairs.

Latest 1000 runs stay in Airtable; older completed logs are verified locally before slot reuse, with that archive noted on the new run. Existing previous-show archives are preserved locally, not retroactively imported. Retirement, failure and retention branches are fixture-proven, not live-observed during these two unchanged runs. Logging cannot report to Airtable when Airtable itself is unavailable or process startup fails; existing local log retains available errors. Do not repeat release implementation/acceptance. Evidence: runtime/run-logs-release-20261020/deployment.json and two-runs-airtable.json.



## Duplicate schedule-key correction verified - 20 October 2026, 09:52 Eastern

Owner-approved P2 correction deployed to private current-store.js and its tests only. Verified survivor first, then exact archive-before-delete of scoped extras, then final unique survivor/field readback. Post-write duplicates from repeated creates now reach the existing safe reconciliation path. Existing transport, schedule/manual/drop behavior and other workflows preserved. Local suite 80 PASS, targeted 14 PASS; release reviewer no material findings. Two normal triggers 09:45:01/09:50:01 exited 0, ended 09:45:49/09:50:52. Each reports all four October 22-25 dates complete, 435 rows, no errors/unmatched times and only the empty-October-22 warning. All 435 record IDs/values unchanged; zero duplicates; eight actual attempt archive files/hash entries verified. No live duplicate case was fabricated: duplicate/failure recovery is fixture-proven, normal operation independently observed. No manual runs or record repairs. Protected source hashes and collector/heartbeat/epoch/pipeline task definitions unchanged. P2 review finding resolved. Next Monday code review remains October 26 at 09:00 Eastern. Evidence: runtime/duplicate-key-release-20261020/deployment.json; root docs/coordination/wef-schedules-duplicate-key-fix-20261020.md.

# Show/ring tables accepted — 20 October 2026, 11:42 Eastern

Owner-approved independent collector addition is complete. show-details, rings and rings-helper exist in appUq5Qz9kARqFija, with linked schedules. selected_date is source-selected October 22, not the computer date or legacy focus_day. Daily rings reuse existing dated payloads; static helper identity accepts ID/number/name within facility and preserves owner details. Source/deployment paths remain unchanged. Normal 11:35 and 11:40 triggers exited 0; both verify 1 show, 24 daily rings, 6 selected October 22 rings, 6 helpers and 435 schedule links. Repeat record IDs/data stable; all original schedule fields/IDs preserved; no duplicates. Eight actual archive entries/files verified. 97 local tests PASS; independent final review no material findings. Six existing private modules, 16 protected sources and all four task definitions unchanged. No manual runs/repairs. Do not redeploy or repeat acceptance. Evidence: wef-schedules/runtime/show-rings-release-20261020; exact table IDs, behavior and proof in docs/coordination/wef-show-rings-release-20261020.md.

