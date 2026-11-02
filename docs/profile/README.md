# RingStatus Profile

## Owner and purpose

**Owner: rs-profile-coordinator.** Profile begins after Recognize supplies a stable person identity. It owns the current Profile toolbar/panels, User record loading/editing, Teams, memberships, roles and consumer permissions. It does not own recognition, cookies, sign-in/out, identity indexing or the Recognize widget.

Read the [coordinator handoff](../coordination/rs-profile-coordinator-handoff-20261102.md) for exact source, evidence, authority and first action. Existing recipient chat is `01a12690-0e9d-71f0-bb85-000d02307e8d`, currently titled `rs-profile`; preparing the handoff does not rename or replace that chat.

## Contract with Recognize

The existing browser adapter supplies `person_uid` or null. Profile independently validates the server cookie/principal, including server-held `identity_only`, before exposing data. A browser UID is not permission. GeoIP-only identity requires existing credential confirmation. Own Profile does not require Team membership; Team views/actions apply their own checks.

Navigation from the public Recognize widget currently opens `/profile`. Profile's embedded recognition control uses the same adapter. Preserve it rather than building another sign-in flow.

## Existing implementation

Paths are relative to `webflow-cloud-test/`:

- UI: `src/assets/rs-profile/client.js`, `profile.css`, `recognize.css`.
- Browser data adapter: `src/assets/rs-profile/service.js`.
- Server data/permissions/save handling: `src/lib/rs-profile.js`.
- Routes: `src/pages/rs-profile/client.js.ts`, `data.js`, `index.astro`.
- Public endpoints: `/test/rs-profile/client.js`, `/test/rs-profile/data`; standalone entry `/test/rs-profile/`.

Preserve the current `/profile` design. Do not substitute `lp-profile`, a new prototype, legacy `/rs-recognize`, sample users or a parallel adapter.

## Records

Existing canonical base `app7BXcw801n19QKF` (Tenants): User `tblyTBgrgdhT3Tcg7`, Team `tbled1saw01Thffs1`, memberships `tblFf4aWQYA2RRsWo`, roles `tblRqRbjmqmNfJtIP`. Match User `recognition_person_uid` exactly to Recognize `person_uid`; use actual linked User/Team record IDs.

The current adapter still obtains first/last names and manages credential updates through existing Recognize records/helpers. This is a coordinated interface, not authority to move fields, duplicate credential storage or edit Recognize internals. D1 identity, claims and IAM refresh helpers are shared protected dependencies.

## Current status

Deployed source `89abf609f816b6c9d10f05d301027c991a33280a`, Cloud `/test`, success 2 November2026 at12:11:26 UTC. Actual existing-cookie -> published Profile -> existing real test User -> active Team/six-member roster and reload passed. Exact User/Team/membership IDs matched independent Airtable reads. Unauthenticated data returned401. No live Profile/Team data was edited in that verification.

Fresh credentials, live saves/retries, Owner writes, GeoIP confirmation and mobile acceptance were not all reverified in that release. Prior16 scoped tests, source review, local fixture journey and build are separate evidence. User levels, Team creation/invitations, inline uploads and real modified-date sorting remain unimplemented; previous backing proposals remain unapproved.

No implementation, schema change, automatic record repair, push, deployment or website publication is authorized by this documentation handoff. Preserve existing stops and obtain only a genuinely missing decision for the exact next change. Keep replies short and code simple.
