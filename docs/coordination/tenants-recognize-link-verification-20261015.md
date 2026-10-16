# Recognize to Tenants user link — 15 October 2026

## State and authorization

Owner authorized editing and testing the existing link, with no new fields or complexity and no publishing. Correction is local only; not deployed. Previous live two-user workflow evidence remains separate and unchanged.

## Corrected behavior

- Native profile-link uses the existing Tenants direct Astro-to-Airtable save path instead of the legacy Users store.
- Verified recognition identity resolves the existing canonical Tenants Person and assigned Barn. Names or phone numbers are not used to guess a match.
- Existing enabled owner membership is required and preserved. No Person, membership, role or permission is invented by linking.
- Save and reload retain the same User identity and assigned Barn. Replaying the same operation produces no duplicate writes.
- The browser merges confirmed User data into the existing assigned-Barn state using the existing direct-save behavior, without the former 15-second cutoff.
- Wrong-Barn, disabled-membership and mismatched-identity requests are rejected before writes.

## Verification

Local authenticated application-handler tests use fixture storage, including a recognition-issued cookie, assigned-Barn loading, profile link, save, reload, repeated operation and access rejection. They do not establish current live deployment behavior.

- Targeted route, bridge, client and access suite: 46 passed, 0 failed.
- Additional client confirmation/cache-merge test: 1 passed, 0 failed.
- Production build: passed.
- Generated native bundle consistency check: passed.
- Independent read-only review: no definite blocker in the scoped correction.

## Exact files and evidence

Checkout: `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test`

Scoped files:
- `src/lib/rs-tenant-barn.js`
- `src/pages/rs-inputs/[operation].js`
- `src/components/rs-onboarding/api.ts`
- `src/assets/rs-barn-onboarding/native-client.js` (generated)
- `test/rs-tenants-barn-correction.test.js`
- `test/rs-tenants-client.test.js`
- `test/rs-tenants-profile-link.test.js` (new)

Evidence beneath that checkout:
- `test-evidence/profile-link-final-tests.log`
- `test-evidence/profile-link-client-confirmation.log`
- `test-evidence/profile-link-build.log`

Before copies: main repository `.git/ringstatus-control/tenants-profile-link-20261015/`.
Deployed baseline remains `733587411ad328b5535c4549d9b7a81f9a896389`, deployment `f3d7efc7-3e77-4739-934b-b8a997f7530e`.

## Resume precisely here

Do not repeat the already-passed local work or restart stopped agents. No release is authorized by this task. After explicit release authorization, isolate only the scoped files from the shared checkout, deploy the verified correction and verify the recognized assigned-Barn/profile-link save and reload in the live browser.

Existing owner access is the tested boundary. General multi-Barn linking and staff/rider/groom permissions are not established by this correction. Previously open entity forms, broader access behavior and deferred removal of Setup/Add Barn remain outside this task. No new fields, schema changes, automation changes, SMS or static publishing occurred.
