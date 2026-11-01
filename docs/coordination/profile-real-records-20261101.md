# Profile real-record connection — 1 November 2026

Owner-approved delta: connect the current Profile model to Recognize FIRST and real User/Team records; preserve current /recognize and /profile designs; remove embedded demo payloads from /, /ring-status, /about-ringstatus, /recognize and /profile. DO NOT publish or deploy. Baseline local commit833ccbcc5b6b1810164384f6ba187f528c2e82ba, branch codex/profile-real-records-20261101. Existing runtime/dependencies/schema, old Inputs consumers and unrelated page/site code are protected.

## Implemented locally

- Shared Recognize adapter supplies person_uid. Profile reads the server cookie independently and rejects actor switches before record access. GeoIP identity alone does not authorize private Profile data.
- Exact canonical User join through recognition_person_uid, independent of Team membership. First/last names come from existing Recognize People; name/email/phone/photo URL/tags/concurrency fields use existing canonical User fields. PIN is set-only and never returned.
- Team and membership links use actual Airtable record IDs. Only enabled membership in an active Team exposes its roster. Owner-only membership enable/disable preserves roles and protects owners. Multiple memberships are not resolved by choosing the first.
- Existing durable-save journal, revisions, request IDs and readback support retries. Actor/generation fences clear stale UI. Non-secret pending edits survive reload; PIN itself is excluded and same-PIN re-entry can confirm the original operation. Viewer hide preferences remain private per-user display state, not membership edits.
- Current Profile and embedded Recognize styles are byte-preserved from saved model sources. The sample User/credential model and separate seeded location model are removed from production application code. No real business data is embedded in Webflow custom code.

## Existing targets

Profile base app7BXcw801n19QKF: people tblyTBgrgdhT3Tcg7, Team/tenants tbled1saw01Thffs1, memberships tblFf4aWQYA2RRsWo, roles tblRqRbjmqmNfJtIP. Recognize base app9kOZdIaGyKk5uG, People tbly1PM5iFYqVzKSm supplies first_name/last_name and credentials. Existing D1 claims are reused for save/revision metadata; no table or field was created.

## Verification and limits

Sixteen scoped backend/service tests passed. Actual local-browser fixture checks passed shared sign-in -> Profile, email edit/save/reload, Team roster, owner membership disable/enable, logout, and interrupted response -> retained draft after reload -> confirmed save. Fixtures use in-memory D1 and simulated Airtable responses; these are NOT production persistence or live-site proof. Independent read-only source review cleared the bounded connection. Final production build passed. Evidence/logs and exact before/after page code: approved checkout .git/ringstatus-control/recognize-model-correction-20261101/.

Five page code blocks are saved only. Application routes/assets in this branch are not deployed; the Profile draft requires that application release BEFORE any future site publication. No code is pushed to the deployment branch. The current live/staging website remains its prior published version. Do not label all prototype controls complete.

## Real-record gaps requiring a separate owner decision

The model invented Independent/Preferred levels, Team invitations and inline photo persistence. Existing real schema has no User level or Team invitation storage. profile-img is a URL field and cannot store the model's inline file bytes. Last-modified sorting also lacks a real modified timestamp field. These controls have not been implemented with guessed data, permissions or storage. Existing records can be loaded/edited, and existing memberships managed; this does not prove Team creation, invitation acceptance or file uploads.

Smallest proposed backing additions, NOT authorized or applied:

- Existing people table: user_level single select (Independent, Preferred), profile_photo attachment, modified_at last-modified timestamp. Keep existing profile-img URL, credential/access fields and Team roles unchanged. No automatic tier assignment/migration.
- Team invitations table: Invitation ID, Team link, Recipient link, Invited by link, Status (Pending/Accepted/Declined), revision, request_uid and request_hash. Only an active Owner invites; only the recipient accepts/declines; membership is created only on explicit acceptance. Preserve the model's single-Team rule; no inferred switching.
- Store photo uploads on the User record's attachment field using Airtable's supported attachment upload path, without adding a separate media service. Read fresh attachment URLs rather than storing temporary download URLs into profile-img. Reference: https://support.airtable.com/articles/9139007724-attachment-fields-in-airtable and https://support.airtable.com/articles/9671148410-Airtable-attachment-URL-behavior.

No schema expansion or implementation against these proposed fields/table is permitted until explicitly approved. No publication is implied by such approval.
