# Resume here — RingStatus coordination, 10 October 2026

Prepared handoff from chat 01a11632-62fd-73b1-997e-8be4119185de. No new chat dispatched. This packet replaces reading the long conversation; inspect linked evidence only as needed.

## Immediate next task

Review/implement the two already identified Recognize gaps, preserving its existing working implementation: silent public-page recognition without navigation, then writing the existing sessions summary. Qualify the complete journey afterward. Do not repeat discovery, redesign authentication or create another database. User's current request is this handoff and context monitoring; it does not itself authorize publishing or live tests.

## Workspace and source

- Project: ringstatus, local project ID 7073fdbc-f525-4251-aedc-63fc27eaf755.
- Repository: C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus.
- Deployed-source checkout: C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus/webflow-cloud-test.
- Latest verified deployed commit: 18e6e993aeaff6fd53168dac4d6f1b7b46baf195, branch work/inputs-crm-20261006. Naming compatibility only; 44 recognition/access tests and build passed.
- Cloud app d7d97751-20e1-4148-a5cf-ee58671c128a; environment 110f06dd-c1ea-4839-98af-d829cbe77941; mount /test; deployment 6aa4b4de-a837-40a6-9c33-341041e38b50 succeeded 2026-10-10T15:33:38.686Z.
- Preserve unrelated/uncommitted work. Inspect before editing; no reset, cleanup or replacement.

## Base state and exact references

- Agents https://airtable.com/appZahVgD156cMAe3: global registry and handoffs. Its records are evidence only after readback; stale Working statuses occurred.
- Recognize https://airtable.com/app9kOZdIaGyKk5uG: index tblZ6CTQoaqkHJvey, tasks tbltfO97Vy1StVVkO, system checkpoint tblTlw48kcRoEX6yf/recB69Oe39xZ0nsAe.
- Tenants https://airtable.com/app7BXcw801n19QKF: index tbl5kWiI8OAfWdzRp, tasks tblXxPquz9jtdQM3n. Formerly Entities; tenant means barn/container.
- Naming verified: all 29 Recognize and 39 Tenants physical tables match their index names and stable links. 21 renamed. One word where practical, otherwise lowercase-hyphens; ___ reserved for deprecated. Data and fields retained. Naming is complete; runtime is not.
- Core Recognize: people tbly1PM5iFYqVzKSm, recognition-devices tblfkRSJAEMzuzApR, sessions tblBgvP7pYSv8o20j, footprints tblWjbASVMIjFLyW8, sms-verification-requests tblxC4SYtzYhJNcJS, rs-phone-aliases-test tblgDWKi0Bb6OcoqS. Some ___ tables still serve existing runtime consumers; prefix never authorizes deletion.

## Recognize direction and remaining tasks

Discovery recmzhSVu14Ka4ahn is Done. Evidence: docs/coordination/recognize-current-binding-review-20261010.md (baseline before latest alias-ID-only release).

1. recpJKchoFBgvqyj9: /, /ring-status and /about-ringstatus still load legacy client. Reuse native silent operation with explicit no-navigation behavior. Public pages never force phone entry or redirect.
2. rec7FIzlG6f9Qd9f3: existing sessionStorage session UID and footprints writer exist; separate sessions table has no writer. Connect person/device/session/activity using existing mechanisms, preserving footprint deduplication; no new infrastructure.
3. recEOLQ5QiYvVKQot: actual desktop/mobile journey remains unverified. Include same-browser code entry, silent return, expiry/revocation, correct session/footprint links and working Inputs regression.

Preserve native Webflow styling and /rs-recognize binding; success goes to /rs-member. Keep original /recognize proof. Cookie intended expiry 365 days, browser clearing possible. Phone verification uses existing random OTP and Airtable-native Twilio automation, not new Twilio Verify. Only 16318752160 is an authorized test recipient; verify current live-send scope before sending. Cached state or IP alone does not authorize identity. Recognition does not own tenant membership or profile editing.

## Other unfinished work, not to silently drop or activate

Tenants schema worker created tenant-trainers, tenant-roster and tenant-assignments, reused favorites/optin-options. Read docs/coordination/tenants-schema-completion-20261010.md; next bindings task rec2YJdtU25OHZSDn, then submission wiring/runtime verification. Schema presence is not live authorization enforcement.

Existing Barn Inputs add/edit/save/reload proof covered barns and riders only. Full onboarding/profile, Schedule live data, CSS audit and offline persistent optimistic/dirty-state implementation remain open. SMS opt-in save/reload/opt-out was historically proved; delivery is separate. Profile-data migration remains incomplete; preserve Recognize legacy wiring. Tenants owns membership, roles, assignments, favorites, opt-ins; trainer/horse/rider/ring identities serve multiple downstream systems. Use existing rules/development records, do not infer new relationships from table names.

## Workers and reminders

Internal /root/recognize_completion completed read-only discovery, not implementation. Agents worker recvIfHlAJKjTmbBE and handoff recrZ0pTDSgER8HqZ corrected/read back Completed. /root/tenants_schema_completion also completed its schema assignment. No internal worker was running at last direct check; verify before claiming otherwise. Internal workers are attached to the old dialogue; a new chat must not assume it can resume them.

Four reminders targeting this chat were PAUSED in saved configuration at handoff: recognize-one-off-task-supervision, onboarding-continuation-supervision, onboarding-consistency-audit-supervision, usage-remaining-20-and-10-alerts. No reminder transferred or reactivated. Ignore expired historical deadlines. Old separate worker 01a11cc4-39cd-7eb2-872f-89ef41193275 is historical, not the current discovery worker.

## Instructions, skills and operating limits

Read repository AGENTS.md. Preserve supplied working code/history; diagnose compatibility delta before editing. Codex is not the business runner: no substitute direct execution, record repair or retry loop after workflow failure. Explicit publish authorization is required. No new hooks or resident agents. Keep replies concise with factual completion evidence; never claim dispatch from a saved prompt.

Load only applicable skills from installed catalog: Airtable overview for schema; webflow-cli:cloud for existing Cloud code; webflow-mcp:custom-code-management for page binding; frontend-testing-debugging for browser acceptance; verification-before-completion before completion claims; systematic-debugging only for a reproduced failure. Their exact previously verified paths are in docs/coordination/recognize-completion-handoff-20261010.md. That older document contains superseded dispatch/naming notes: this packet and latest live evidence control.

Original proof: f012c3077 recognition history; WEC packing 647ea24f8 is historical working architecture, not proof that current native bindings work. Never reconstruct it just because current endpoints/base names differ.

## Cost and context control now implemented

Read docs/coordination/context-check-procedure.md. scripts/context-check.py measures actual local token telemetry and saves per-chat state. Run at task boundaries and first tool boundary after 30 active minutes; get account usage once per due check. Initial test detected 183,345 latest input tokens and requested handoff review. Four helper cases passed. No background enforcement is claimed.

Review thresholds are local defaults, not OpenAI limits: latest input >=100k, >=10 percentage-point account consumption since check, or 30 minutes without accepted progress. Prepare a compact handoff instead of importing full history. State file for old chat is .git/ringstatus-control/context-01a11632-62fd-73b1-997e-8be4119185de.json; new chat must use its own.

Usage investigation: docs/coordination/usage-investigation-20261010.md. Last four-hour sample was Astra Medium, 77.6M input including 76.2M cached. Account snapshot 26% remaining is time-specific and not per-chat billing. Recommendation only: test GPT-6.1 Sol Medium/Standard on one bounded task; user has not selected a model change. No model change, new chat, new agent or schedule is implied by this packet.

Reusable base guide: docs/coordination/new-airtable-base-guide.md. Treat Recognize as a structural reference under correction, not a verified end-to-end template.
