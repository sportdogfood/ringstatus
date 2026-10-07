# RingStatus Recognize — repair, evidence, fragility, and automated verification

Date: October 6, 2026, America/New_York. Evidence timestamps below use UTC when marked Z.
Scope: the isolated Webflow Cloud `/test` application and its invitation/access integration with the existing Recognize implementation. This is not acceptance of the public website, every Recognize branch, Barn setup, CRM writes, SMS, or Catalyst.

## Result and acceptance boundary

**CONFIRMED:** the deployed application completed invitation acceptance, browser confirmation, persistence of the person/device association and a successful recognition audit in Airtable, then recognition on a subsequent page reload. The original `503 storage_unavailable` failure is resolved for this observed journey.

**PARTIAL overall:** that successful journey does not establish production readiness, all failure paths, expired-session recovery, concurrency safety, downstream audit processing, or unattended operation. Automated API and runtime checks are now supplied in the repository; the new live automation has not yet been executed against the deployment.

The user supplied Recognize as an already working model. The repair preserves that as the baseline. The defect identified here was in the added provider-request configuration used by the invitation/access integration. The work does not establish that the original Recognize model required a rebuild.

## Exact targets

| Item | Target |
| --- | --- |
| Repository | `sportdogfood/ringstatus` |
| Working branch | `work/inputs-crm-20261006` |
| Webflow site | `6982268b7543ac3c80151266` |
| Webflow Cloud app | `d7d97751-20e1-4148-a5cf-ee58671c128a` |
| Isolated environment | `110f06dd-c1ea-4839-98af-d829cbe77941` |
| Page | `https://ringstatus.webflow.io/test/onboarding` |
| API prefix | `https://ringstatus.webflow.io/test/rs-inputs/` |
| Trial Airtable base | `app9kOZdIaGyKk5uG` |
| People | `rs_people_test`, `tbly1PM5iFYqVzKSm` |
| Devices | `rs_devices_test`, `tblfkRSJAEMzuzApR` |
| Recognition audits | `rs_recognition_sessions_test`, `tblWjbASVMIjFLyW8` |
| Runtime configuration | compatibility date `2025-03-03`, `nodejs_compat` |
| Barn storage configuration inspected earlier | Zoho CRM; expected organization `zgid=941333935` |

The original recognition base and infrastructure base are not substituted for the trial base. The repair does not migrate data or resume paused Catalyst/SMS work.

## Failure, cause, and minimum repair

The failing request returned `503` and `{ "ok": false, "error": "storage_unavailable" }`. The bounded diagnostic header subsequently showed `transport_error;reason=TypeError` for request `6fdb3f75-0bc3-41a9-8033-62781bd7b033` at `2026-10-07T00:30:26Z`.

**CONFIRMED locally in Cloudflare workerd:** constructing/sending a provider request with `redirect: "error"` throws a TypeError before the provider request is sent. The existing mocked HTTP tests did not exercise that native runtime behavior; two mocked tests explicitly asserted the incompatible value.

The repair changes the request setting to `redirect: "manual"` in:

- `webflow-cloud-test/src/lib/rs-inputs-access.js`
- `webflow-cloud-test/src/lib/rs-inputs-airtable.js`
- `webflow-cloud-test/src/lib/rs-inputs-crm.js`
- `webflow-cloud-test/src/lib/rs-inputs-zoho-auth.js`

All four reject unsuccessful provider responses. Requests do not follow provider redirects. The CRM adapter also converts provider 3xx rejection to a 502 error rather than returning a misleading redirect status to the client. Two existing expectations were corrected and a CRM redirect-rejection regression was added.

The repaired source is commit `1b6e6b33fdc9cadfab616f3178bb04b7d9810a86`. Webflow deployment `3b40c26e-ad57-4e43-9bfb-ea3ca746e7d7` reported success at `2026-10-07T00:37:17.722Z`. Build success alone was not treated as workflow acceptance.

The opt-in diagnostic header remains limited to POST `/test/rs-inputs/access`, the owned isolated trial configuration, and `X-RS-Diagnostic: storage`. It contains bounded provider status/error categories, not raw messages, tokens, request URLs or records.

## Live evidence collected

| Check | Evidence | Finding |
| --- | --- | --- |
| Storage transport after deployment | User's live request at `2026-10-07T00:42:35Z`, request ID `f949577e-909c-4af8-91d8-54f0e5111dee`, body `invalid_invitation` for a deliberately fake token | CONFIRMED: this request passed the earlier storage failure and reached invitation validation. The pasted excerpt did not include the HTTP status line. |
| Actual invitation accepted | User screenshot `image(2).png` shows the invited test profile and confirmation drawer; independent Airtable read shows invitation hash and expiry cleared | CONFIRMED: successful redemption and persistence of consumption |
| Browser confirmation | User screenshot `image(3).png` shows progression to Barn setup; independent Airtable device and audit reads below | CONFIRMED: association and audit stored |
| Returning browser | User's subsequent screenshot `image(4).png`, supplied after the reload instruction, shows the same profile as Recognized | CONFIRMED: user-observed recognition after reload; not an agent-controlled browser trace |

The synthetic test person is `recDOM4IgtPTDpOCL`, canonical UID `rs_edge_20261006_2df31d6b57`, display name `TEST Edge Onboarding 20261006`. Its access is `invited`, status `Active`; the reissued invitation has been consumed. No real person was enrolled for this check.

Independent device read: `recoJcv9SGrYFTFfP`, status `Active`, linked to that person, `last_seen_at=2026-10-07T00:49:02.793Z`, recognition source `Web`.

Independent audit read: `recARiA2t4wdXiNMd`, `event_at=2026-10-07T00:49:03.065Z`, event type `success`, result `matched`, matching person/device links, page path `/test/rs-inputs/recognition`. Its `automation_status` was `queued`. This does **not** prove downstream automation ran.

The test person's access and device were left active so the user's ongoing session was not disrupted. The private invitation file is now consumed and is not a reusable login link. No invitation token, session cookie, Airtable PAT or Zoho credential is included in this report.

## How this integrated path works

1. The operator grants an existing canonical test person invited/approved input access. A random invitation is stored only as a SHA-256 hash, with expiry and a rotated session version.
2. The client reads the invitation from the URL fragment and removes it from the address bar before the API request. It sends the invitation only to the same-origin access endpoint.
3. The access endpoint checks origin, configuration, expiry, eligibility and uniqueness. It clears the invitation, then returns a signed session cookie scoped to `/test/rs-inputs`, with HttpOnly, Secure and SameSite=Strict.
4. Subsequent input/recognition requests verify the cookie and read the current person's access/session version. A browser device token alone does not grant input authorization.
5. Explicit browser confirmation associates the device with the verified person and writes a recognition event. Later recognition reads resolve that association and verify the principal.
6. Barn setup is a downstream step. Arriving at its screen does not verify a saved barn or a CRM write.

## Fragility and limitations that remain

| Area | Inspected behavior / evidence | Consequence |
| --- | --- | --- |
| Session lifetime | Signed session TTL is 8 hours; invitation TTL on issuance is 24 hours; device-cookie lifetime is 1 year | Recognition persistence and access persistence differ. **No automatic session renewal was found in this path.** After session expiry, the device token cannot restore authorization; another invitation is required under the current implementation. Expiry was tested locally, not by waiting eight hours in production. |
| Recovery | `recovery` returns `recovery_delivery_unconfigured`; new-profile action returns `profile_creation_unconfigured` | Self-service recovery/new identity creation is not completed in this integration. |
| Invitation consumption | Airtable lookup followed by update, without atomic compare-and-swap | Sequential replay rejection is tested. Simultaneous redemption is not proven single-use; a lost response can consume the invitation without delivering the cookie. |
| Other writes | Bounded trial adapters do not guarantee atomic entity/audit commits or globally serialized writes | Local retry/conflict tests reduce known failure risks; they do not establish multi-user production concurrency. |
| Logging | Webflow runtime log queries and the user's CLI returned empty results for the relevant requests | Lack of a log result is not evidence of no request or success. Live responses and independent data reads were needed. |
| Request timeouts | Repaired input-provider requests have timeouts; not every reused recognition-provider request has one | Some recognition failures can leave a request waiting longer. A timeout after a mutation may still have an unknown commit outcome. |
| Test cleanup | A request can continue remotely after a client timeout | The new runner revokes access and attempts retirement, but reports `UNVERIFIED_WRITE_OUTCOME` after an uncertain write even if immediate readback looks clean. It never calls this cleanup PASS. |
| Automation event | Observed audit was queued | Processing, recurrence, and downstream notifications remain unverified. |
| Browser coverage | Supplied screenshots and live record reads verify the observed journey | Cross-browser behavior, fresh browser restart, viewport completeness and UI error recovery are not automatically covered by the new API runner. |
| Scope | Original working Recognize behavior includes more than this invitation journey | Phone/PIN, ambiguous matches, updates, retirement/recovery and other branches require their own acceptance evidence. Some have local regression coverage; this report does not claim all are deployed-verified. |

## Why the owner had to act as QC

This was a verification-coverage and execution-access gap. Existing handler tests supplied replacement HTTP functions, so they could not catch native Cloudflare request-option incompatibility. The existing API journey runner required a server-issued session before starting, so it did not automate the invitation bootstrap that was failing. No verified deployed acceptance job or browser UI gate had been established for this path.

The agent's browser surface was blocked by its URL security policy during the attempted live inspection. The user's browser and PowerShell could reach the target. Connector access could inspect records and deploy code but did not supply either the app's private credentials or control of the user's browser. Those are distinct capabilities.

Manual screenshots should not be the routine release gate. Deterministic behavior can be checked on each change; a configured live runner can perform the API journey without owner clicks. A browser job is still needed to cover actual browser storage, reload interactions, button behavior and visual/layout quality.

## Repeatable automation supplied

### Native Cloudflare regression

File: `webflow-cloud-test/test-runtime/rs-inputs-transport.test.mjs`.

It bundles the actual four adapters, runs them through Miniflare/workerd native `fetch`, and reads compatibility settings from the actual `wrangler.json`. Provider endpoints are synthetic outbound services; no real provider credentials or production records are used. It verifies successful requests and redirects rejected without credential forwarding. It would catch the `redirect: "error"` regression that the Node-only mocks missed.

The test pins the already locked dependency versions as explicit dev dependencies: Miniflare `4.20260603.0` and esbuild `0.27.7`. These were the versions installed for the local execution.

### Existing regression suite

`npm run test:recognize` runs the existing input and recognition tests plus the new runner tests. It covers the existing signed-session, principal, origin, invitation, provider, relationship, retry and audit checks. These remain local/simulated evidence, clearly separate from deployed acceptance.

`.github/workflows/recognize-checks.yml` runs installation, these regressions and the native Cloudflare tests on scoped branch pushes and pull requests. This is a test workflow, not proof that Webflow waits for it before deploying. A release-blocking required check/deployment dependency has not been established.

### Deployed API journey

File: `webflow-cloud-test/scripts/rs-recognize-live.mjs`.

It defaults to dry run. Executing requires the exact isolated target, a dedicated person UID beginning `rs_ci_`, an existing active/test person whose name begins `QA CI Recognize`, and a scoped test PAT. It will not automatically create identities, change the owner's test session, use device recognition as authorization, or test arbitrary production URLs.

The runner checks:

1. Dedicated fixture preflight and unauthenticated denial.
2. Fake-invitation rejection and wrong-origin rejection.
3. Fresh invitation issuance and acceptance with expected signed identity/cookie attributes.
4. Invitation replay rejection and independent storage readback of consumption.
5. A fresh signed-session request and the invited profile before device confirmation.
6. Browser association confirmation, identical confirmation retry, and a new recognition request.
7. Independent Airtable readback of exactly one matching device and confirmation audit.
8. Device retirement and retired-device rejection.
9. Access revocation, rejection of the old session, and logout cookie clearing.
10. Cleanup in `finally`, including fresh access/device readback. Uncertain write outcomes remain explicitly unresolved.

It uses synthetic records only. It does not create barns, send messages, activate Catalyst or claim audit automation processing. Test audit/device records are retained for review; repeated runs accumulate labelled test evidence. Hard termination of the runner can prevent `finally`; inspect/reconcile the fixture before retrying in that case.

`.github/workflows/recognize-live.yml` supplies a manual workflow with one-at-a-time concurrency and a stored JSON report. It is intentionally not scheduled or assumed to run after Webflow deployment. The test must target a completed, identified deployment. Manual-dispatch availability from a non-default branch is not assumed; workflow registration/default-branch integration must be verified.

### Commands

From `webflow-cloud-test`, using Node 22.12 or later:

```powershell
npm ci
npm run test:recognize
npm run test:runtime
npm run test:recognize:live
```

The last command is a dry run. On a configured runner:

```powershell
npm run test:recognize:live -- --run
```

Required runner configuration, stored outside source:

| Name | Value / purpose |
| --- | --- |
| `RS_CI_TARGET` | Exactly `https://ringstatus.webflow.io/test` |
| `RS_CI_PERSON_UID` | Dedicated existing synthetic person's canonical UID, beginning `rs_ci_` |
| `RS_CI_AIRTABLE_TOKEN` | Secret PAT with required read/write access to the isolated base's test records; never the browser session cookie |

The GitHub live workflow uses environment `ringstatus-recognize-test`, its `RS_CI_PERSON_UID` variable and `RS_CI_AIRTABLE_TOKEN` secret. Their current remote existence/values were not verified by this work. The current local process lacks the required configuration; the runner correctly returns `BLOCKED` before network access. The current manual test person `rs_edge_20261006_2df31d6b57` is deliberately ineligible for the CI runner.

No copying of Webflow secrets into logs/chat/source is required or permitted. Existing connection authentication does not automatically provision runner secrets.

## What must be true before claiming automated acceptance

- The dedicated fixture and runner credential are configured and verified against the exact isolated base.
- The workflow is registered on an appropriate approved branch and the runner can reach the completed deployment and Airtable normally.
- A live run produces a successful report, including independent storage checks and cleanup, associated with the tested deployed commit.
- A supported browser runner separately verifies invitation UI, real cookie storage, confirmation, reload, and relevant visual/error behavior. This implementation has not added or run that browser job.
- If unattended post-deployment operation is required, the deployment-complete trigger and required-check/release relationship must be established and observed; repeated or scheduled execution must be evidenced separately.

## Verification of the newly supplied automation

All 158 input/recognition tests and both native Cloudflare runtime tests passed locally. The new runner tests execute the actual authenticated route and actual recognition handlers against isolated in-memory REST fixtures, including a normal journey, rejected configuration, refusal of a non-test identity, response loss, cleanup failure and a late-commit race. The last case proves an immediate empty cleanup read cannot produce a false PASS.

The live runner has **not** been run against the deployed app in this work. Its dry run and missing-configuration BLOCKED behavior are verified. GitHub workflow execution is separately reported after source upload; a committed workflow file by itself is not evidence of a run.

## Evidence handling and continuation

Reuse the repaired deployment and the existing live evidence. Do not repeat environment setup, replace the original Recognize flow, issue new invitations to real people, or mark a local/mock result as deployed success. Preserve unknown write outcomes until reconciled. Leave the user's active test session intact during this documentation/automation work.

The immediate technical regression is repaired and the observed returning-browser journey is confirmed. The remaining work is to establish reliable automated acceptance and resolve the documented trial limitations before expanding the readiness claim.
