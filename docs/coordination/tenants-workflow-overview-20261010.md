# Tenants workflow and persistence

Current structural update: [tenant relationship schema readback](tenants-schema-completion-20261010.md). `tenant-trainers` stores Primary/Additional policy, `tenant-roster` stores tenant-local horse/rider enablement, and `tenant-assignments` stores membership-to-horse assignments. Existing favorites/opt-in options remain separate. These tables are prepared and empty; application validation and runtime integration remain open.

Recorded October 10, 2026. Airtable system record: https://airtable.com/app7BXcw801n19QKF/tblMKWJw8Q6joKc8U/recJzisHwiPPkKG6y

TENANTS — COMPLETE WORKFLOW REFERENCE
Scope: recurring use of tenant data, not the onboarding checklist. This is the agreed model plus explicitly open implementation decisions. Documentation does not prove live operation.

RESPONSIBILITY
Webflow owns native presentation and styling. Astro maps data/actions into those elements and performs server-side identity/access/input checks; it does not replace the design with a styled template.
Recognize owns identification, recognition cookies and recognition sessions. Tenants owns barn details, person-to-barn relationships, role/assignment scope, favorites and preferences. Recognized does not automatically mean authorized for every barn.
Tenants base: app7BXcw801n19QKF. Recognize base: app9kOZdIaGyKk5uG. Messages handles delivery/history; show engines supply current schedule/entry data by endpoints. Presence in a show does not create tenant membership.
tenant_level describes tenant benefits: Ad-free, Pro Schedule, Website logo. Existing levels and benefit assignments are not assumed approved. ___profiles is retired reference, not a separate active master.

NORMAL LOOP
1. On a designated page, recognition is silent when possible. Public pages stay public; only designated gated pages initiate recognition/login when needed.
2. Resolve the person's tenant membership, role and assigned scope. Show cached data for the same recognized account/tenant only; cache is not authorization. Exact logout, account-switch and stale-offline-access rules are open.
3. Render permitted cached data immediately, then request current data through Astro. Cache keeps last-confirmed data separate from pending local edits.
4. Add/edit updates the visible UI immediately and persists the pending change on the device. Dirty means not yet confirmed in the designated Airtable target.
5. Astro receives the operation, validates it and records input-submissions. Each operation needs a stable submission ID reused for retries; logging the submission is not confirmation that the target was updated.
6. Intended automation validates/applies the submission to its designated table and records Applied, Failed or Needs review. Current writer/processor and duplicate prevention are not implemented or proven here.
7. Client obtains final outcome. Only confirmed target persistence clears that operation's dirty state and updates the confirmed cache. A later edit made while an earlier save is in flight must remain pending.
8. Background refresh updates clean data while preserving pending local changes. Conflicts and failures stay visible; do not silently discard edits or claim saved.
9. Offline: show last available permitted data and retain pending edits across refresh/restart where browser storage survives. On return/reconnect, refresh and resume pending operations under the same IDs. Retry limits, ordering and conflict handling require implementation decisions.
10. Other-device changes appear through refresh; no assumed real-time subscription.

PERSISTENCE — AGREED INTENT
Recognition cookie: keep 365-day expiry per owner, subject to browser restrictions. This does not promise 365-day access or persistence.
Person/tenant details: keep useful device cache as long as practical, refresh from the server; exact TTL not selected.
Show data: cache only relevant show-week/day data; fetch current endpoint data for the selected week/day. Retain long-term tenant associations separately so absent-this-week does not remove membership or historical suggestions.
Persistent optimistic UI and dirty changes must survive normal page refresh and restart while storage remains available. Airtable designated tables remain confirmed source of truth; browser cache is fallback, not master.
Do not state all iPhones clear all storage after exactly seven days: that was a concern in discussion, not a verified universal rule. Browser clearing can remove cache/queue; do not promise recovery of a change never submitted.

CADENCE
Page open: use eligible cache immediately, then refresh.
Edit: immediate optimistic display and durable local pending save; submit when connectivity and authorization allow.
Submission arrival: intended automation processes it; no activation claimed.
Confirmation: update device cache and clear only confirmed operation.
Tab return / reconnect: refresh/resume is the proposed implementation trigger supporting agreed persistence.
Foreground periodic refresh: 60 seconds is PROPOSED, NOT LOCKED. Exact interval, debounce/backoff, polling for confirmation, hidden-tab behavior and retry cap remain open.
Show-engine cadence is separate, not inherited from the tenant profile refresh.

STATE MEANINGS
Local pending: persisted on this device; not received/confirmed by server.
Pending: submission exists, final target application unconfirmed.
Applied / Saved: designated target write confirmed with operation/result association.
Failed: unsuccessful; show reason and retain user's work.
Needs review / Conflict: human decision needed; never resolve by guessing or mark saved.
Routine edits do not repeat tenant onboarding. Sensitive owner/role/access edits need a defined policy; the exact review mechanism is not yet implemented.

EXAMPLES
Rename horse: immediate new text + pending indicator -> Astro submission -> automation updates exact horse ID -> confirmed result -> saved. Shared horse renaming authority must be checked.
Offline rider edit: persist local dirty change -> page reload shows it pending -> reconnect submits same operation ID -> confirmed write clears pending.
Second-device update: fresh server data updates clean fields; overlapping dirty changes remain visible for conflict resolution.
Response lost after target write: retry must not create duplicate target/event. Durable idempotency behavior is a required proof, not established by a log table.
Missing horse/rider: red-line form in barn-inputs routes to admin; resolve existing alias before creating duplicates.
Audit-lane: compare a submitted full horse/class list to daily displayed entries, not counts alone; it is separate from technical event logs.

TABLE RESPONSIBILITIES
tenants: barn container/details; tenant_level: benefit definitions.
people: individuals and Recognize reference; tenant-memberships/roles: barn-specific relationship and permitted scope (enforcement remains unverified).
trainers/horses/riders/rings: shared references, never recreate per service.
locations/tenant-locations: venue places and barn/show-week location associations, not live positions.
favorites: personal follows separate from tenant-wide roster.
optins/optin-options: consent, scope and individual choices; owner-managed groom choices do not substitute recipient consent; delivery is separate.
input-submissions: intended Astro intake and application outcome. Existing schema lacks explicit Tenant and sensitive-change review fields; resolve minimal mapping in development before implementation. Membership currently supplies a possible tenant reference but is not proof of a complete validation contract.
events: preserved input-event history; avoid duplicate logging designs.
sources: external stable-ID mapping; ___ tables are preserved legacy evidence.
onboarding-stages: human deployment-readiness checklist; not each add/edit transaction and not automatic deploy enforcement.

PAGES / SOURCES
Owner-designated public: /, /ring-status, /about-ringstatus.
Launcher: /rs-member (historical metadata record: page 6ac91a47a4ac5cd8134b81c0); /rs-recognize recognition entry.
Tenant UI references: /rs-barn-onboarding-setup-v24, /rs-barn-onboarding-alerts (SMS opt-in), /rs-barn-onboarding-feed, /rs-barn-onboarding-schedule-v24.
Current published bindings, gate list, buttons, active Astro routes/build and deployed base mapping need fresh verification; URLs here are owner/history references, not new live tests.

CURRENT PROOF AND LIMIT
Airtable structure and documented consolidation verified this session; People 130 records and 13 source memberships, no access enabled. 18 option values copied with revoked consent retained. Six onboarding stages per 12 tenants, no approval inferred.
Prior barn/rider add/edit/save/reload and SMS preference save/reload/opt-out proofs do not prove the new Tenants-base loop, offline queue, all entity types or SMS delivery.
No tenant runtime migration, automation activation, new application code, deployment or live browser test performed by this documentation task.

FINISH CRITERIA
Prove exact page/build/base/table mappings; load/add/edit/save/reload; offline edit/reconnect; restart with pending changes; lost acknowledgement + duplicate retry; new edit during earlier save; second-device conflict; server validation/failed save; tenant/account isolation and session expiry; consent/assignment limits.
Record evidence per case. Missing automation may use an owner-agreed human step with named responsible person, exact action, result location and resumption condition. Do not claim such a handoff already exists.

