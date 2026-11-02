# Profile review corrections

2 November 2026 — explicitly APPROVED TO EDIT by the owner, including the additional field and manual disposable Test setup. The existing app has not been redeployed.

The approved controls and Airtable additions are prepared. The focused tests and build passed. Independent review then reproduced two missing cases:

- Saving an unchanged uploaded photo clears the attachment and stores its expiring download URL. Preserve the attachment when its displayed URL is unchanged; cover opening, saving, unrelated edits and removal.
- Team creation can reach Airtable but lose its reply before the app records the new Team ID. The existing retry then cannot find that Team or finish its Owner membership. Add one text field, tenants.profile_request_uid, and use the existing operation ID to find and verify the created Team before continuing. Keep the existing save journal; do not introduce another processor or workflow.

Approval requested: these corrections, their focused checks, and that one additional field. The existing app deployment approval remains in effect after checks pass. Preserve the current design, one Team membership per User, existing records and other owners' changes. No upgrade, migration or website publication.

Live acceptance also needs two registered disposable Test Users with no Team membership: one creates the labelled Test Team, the other declines and then accepts its invitation. No eligible existing actor was found. Do not remove settled memberships or invent credentials. Proposed manual test setup, requiring explicit approval: create only two labelled Test people in profiles and corresponding Recognize identities, linked by unique person_uid, without memberships. Proposed reserved fictional test phones +12025550188 and +12025550189 have no current primary-phone match; alias collision check is recorded separately. No PIN, SMS, consent or real-person data is involved. Use only the existing owner-authorized /test/rs-inputs/native-recognition?operation=refresh_iam administration path to load that setup, with Recognize coordination. This is explicitly manual test preparation, not automated registration or workflow proof. Do not add a signup feature. After testing, deactivate the new test identities/Users/Team and restore temporary values; preserve operation evidence and all pre-existing records. No fixture setup or refresh has been performed.

Candidate: C:/Users/gombc/.codex/worktrees/profile-completion-20261102/ringstatus. Evidence: main checkout .git/ringstatus-control/profile-completion-20261102/; independent review in review/. Current production source remains 89abf609f816b6c9d10f05d301027c991a33280a.
