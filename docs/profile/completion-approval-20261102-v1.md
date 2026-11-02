# Finish the existing Profile

Version 1.1.0 — 2 November 2026. Approved by the owner on 2 November 2026: APPROVED TO EDIT and deploy the existing app for those Profile controls and required Airtable additions. Replaces the overcomplicated v1.0.0 proposal.

Already verified live: sign-in, Profile save/reload, failed-request recovery, Team roster and Owner enable/disable. The test Owner now has one Team membership.

Finish the existing controls: User level, Team creation and invitations, photo upload and modified-date sorting. Preserve the current design and one Team membership per User. Use existing Team records; do not migrate to the Name-only teams table.

Proposed Airtable additions: people.user_level (Independent/Preferred), people.profile_photo (attachment), people.modified_at (last modified), and team-invitations with Team, recipient, sender, status and existing revision/request fields. No automatic level assignment. Invitations require explicit acceptance; only a User without membership can join. Preferred Team creation makes the creator Owner.

Adapt the existing Profile client/service/server, verify the changes, then deploy only the existing /test application and test visible controls with marked Test records. Restore temporary test changes. No website publication, upgrades, replacement recognition, new service engines or unrelated cleanup.

Baseline: deployed source 89abf609f816b6c9d10f05d301027c991a33280a in C:/Users/gombc/.codex/worktrees/recognize-isolated-20261101/ringstatus. Preserve the current uncommitted client.js change; identify its owner before editing. Existing release: app d7d97751-20e1-4148-a5cf-ee58671c128a, environment 110f06dd-c1ea-4839-98af-d829cbe77941, branch work/inputs-crm-20261006. Coordinate shared changes before release.

Evidence: .git/ringstatus-control/recognize-model-correction-20261101/profile-e2e-live-results-20261102.json. External services remain separately owned and unconnected; this proposal does not build them or claim they work.

Approval requested: implement these existing Profile controls, make only the listed data additions, and deploy/test the existing application. No broader permission is requested.
