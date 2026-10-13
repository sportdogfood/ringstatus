# Recognize table-address release — 11 October 2026

Owner request: resolve the failing existing-browser Recognize landing without replacing the proven implementation. Investigation timebox: 16:35:55–17:05:55 UTC.

## Observed failure

The owner's existing Edge tab sent its existing secure HttpOnly recognition cookie. Its normal page-load POST to `/test/rs-inputs/native-recognition?operation=recognize` returned HTTP 502; the response was a Cloudflare/Webflow Bad Gateway HTML page, ray `a48f4a1a2c842685-MIA`. The client's subsequent device lookup returned HTTP 401 `authentication_required`. No phone entry, SMS, cookie clearing or direct substitute production request was performed. Private cookie/token values are excluded.

Current Webflow deployment independently verified through MCP: `6aa4b4de-a837-40a6-9c33-341041e38b50`, source `18e6e993aeaff6fd53168dac4d6f1b7b46baf195`, successful 10 October 15:33:38.686 UTC. Environment `110f06dd-c1ea-4839-98af-d829cbe77941`, app `d7d97751-20e1-4148-a5cf-ee58671c128a`, site `6982268b7543ac3c80151266`, branch `work/inputs-crm-20261006`, mount `/test`.

The deployed input-recognition adapter still uses `rs_people_test` and `rs_devices_test`. Fresh Airtable schema inspection confirms those names do not exist: the tables are `people` (`tbly1PM5iFYqVzKSm`) and `recognition-devices` (`tblfkRSJAEMzuzApR`). Failed lookup responses in this source become `recognition_lookup_failed` HTTP 502. This is a verified compatibility defect; the current HTML gateway response does not independently prove its entire upstream error chain.

## Exact existing correction isolated

Candidate: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus/.git/ringstatus-control/recognize-table-release-20261011/candidate`.

Baseline is the exact deployed commit, exported without mixed working-tree changes. Only two production lines differ:

- `src/lib/rs-inputs-recognition.js`: use existing stable People and Devices table IDs for the designated Recognize base; preserve other explicitly selected bases.
- `src/lib/rs-inputs-access.js`: use the same existing stable People table ID.

This correction already existed in the approved 10 October table-address correction. The unrelated actor-scope and wider Tenants/Recognize changes in the previous prepared packages are excluded. No Webflow presentation, auth policy, cookie format, database, schema, redirect or SMS change is included.

## Current verification and release boundary

Fresh local affected recognition/access/native route/client checks: 74 passed, 0 failed. Astro build completed exit 0. These establish local preparation only. Logs: `scoped-tests.log`, `build.log`; exact production delta: `release.diff`, in the candidate's parent directory. The original first test/build launcher attempts used incorrect local paths and were corrected before executing the successful checks; no production execution was involved.

The preceding approval record explicitly covered these address corrections and local checks, with no release authority. Exact next action: owner approval to deploy this isolated correction to the existing `/test` environment, followed by one normal reload of the same Edge tab and verification of successful silent recognition and `/rs-member` navigation. Preserve the existing cookie and record resulting session/footprint evidence. No SMS is needed for that known-browser test. Any failed live path stops for assessment; no retry loop or alternate production execution.

Rollback reference: the currently deployed commit and deployment identified above; the baseline archive is saved alongside the candidate. Rollback is not pre-authorized or automatically executed.

Recognize remains open until the agreed live acceptance is evidenced; fixing this lookup does not itself prove mobile/desktop OTP, separate session lifecycle or every previously open gate.

## Owner-approved release execution

Owner answered “Deploy the correction now.” The verified existing GitHub-connected deployment branch was still exactly at the baseline commit. A scoped commit `7efc625c664f04c8bc31cfb15811fcd9ad36784c` contains only the two production-line address changes and their two focused existing test files. It was pushed normally, without force, to `work/inputs-crm-20261006`; the user's checkout/index and unrelated pending files were preserved.

Webflow automatically started deployment `e7be573b-1f25-4bcf-b96e-193b1508d00d` at 16:48:46.978 UTC for that exact commit. This is the existing configured deployment path, not an alternate runner or direct production-data command. No static Webflow publication or configuration/automation/schema change was made. Deployment outcome and browser acceptance follow below when observed.

## Verified live outcome — 16:50–16:51 UTC

- Webflow reported successful deployment of exact commit `7efc625c664f04c8bc31cfb15811fcd9ad36784c` at **16:49:42.353 UTC**.
- Normal reload of the owner's original Edge tab, preserving its cookie, produced **HTTP 200 application/json** from the recognition POST and automatically navigated from `/rs-recognize` to `/rs-member`. No button, phone entry or SMS was needed.
- A second normal visit to `/rs-recognize` again produced HTTP 200 and silently returned to `/rs-member`.
- Airtable readback found footprint **`recz12byxUsmDQlKf`**, event time `2026-10-11T16:50:02.703Z`, result `matched`, path `/rs-recognize`, session `547cb765-c2cc-40c3-8d6d-b61906c7db5b`, linked to person `recV9CvYGaTGz7Wzr` and device `reccaXNrPXVzvVmpZ`.
- The displayed launcher remains the owner's existing template: “Hi Alex” and buttons with `#` destinations. That display is not proof of the person's name and is outside this two-line release. No claim that launcher personalization/buttons are complete.
- The browser response body was unavailable through CDP after navigation; no JSON body was fabricated. Network status, actual navigation and matching Airtable record provide the observed proof.

The reported known-browser landing failure is fixed and live-verified. The complete Recognize product task stays open for public-page silent behavior, separate Sessions-table integration, and remaining desktop/mobile code-entry/revocation acceptance. Those prepared changes were not silently included in this release.
