# Recognize completion release

Current deployed baseline: `7efc625c664f04c8bc31cfb15811fcd9ad36784c`. Its stable People/Devices lookup fix and verified silent return must be preserved.

Prepared source: `.git/ringstatus-control/recognize-completion-release-20261011/candidate/webflow-cloud-test` in the approved RingStatus repository. Exact existing nine-file Recognize artifact from `C:/Users/gombc/.codex/recognize-release-20261010/recognize-manifest.json` was hash-verified and assembled on that current baseline. No Tenants delta is included and no new authentication method is introduced.

Scope: existing public silent/no-navigation mode; reusable server-confirmed browser state; existing Sessions-table writer with previously approved pre-write retry correction. Four production files plus five tests. All other current deployed files are preserved. Existing native Webflow page presentation stays in place.

Fresh local verification: 189 relevant recognition/access/adapter/native-client/session tests passed with zero failures. Astro build completed successfully. Logs are `recognize-tests.log` and `build.log` in the candidate's parent directory.

Footer updates: exactly the prepared recognition-loader replacement in `docs/coordination/recognize-public-page-code-preview-20261010.json`, on `/` (`6982268d7543ac3c801512cd`), `/ring-status` (`6ab11bd52642dca0c8e74004`), `/about-ringstatus` (`6ab260d76d76d28aa9be185f`). Live saved footers were read via MCP11Oct and all three still match the saved before-content exactly. Each footer changes4256→4608 characters. Leave all heads and other footer content unchanged. Native source must deploy before updated footer publication.

Publication boundary: MCP site-wide publication could include unrelated pending `/ld` edits (updated11Oct15:31:52Z) and other site changes. Owner should publish only the three designated pages through the normal Webflow page publication controls; do not silently publish unrelated work. Current site-level published timestamp is9Oct18:22:16Z, not a reliable per-page completion claim.

Live acceptance: known and unknown public visits stay uninterrupted; `/rs-recognize` known browser→`/rs-member`; unknown browser phone/code entry stays in the originating browser; 365-day secure cookie; server-confirmed session reuse and linked Sessions/Footprints readback; refresh/return; local negative/revocation tests remain distinguished from live evidence. SMS tests are restricted to owner-authorized16318752160. User enters any received private code in the originating test browser; do not extract codes from Airtable or logs or paste them into chat.

No full completion until these observed gates are recorded. Launcher button destinations remain owner-designated separately; do not invent routes or claim sample text is actual identity.

## Authorized execution, 11 October

Owner approved this exact remaining release and the three footer updates. Webflow publication remains owner-controlled. Scoped commit `785f571c2167c82c214f0e5c81ffcb77208b70e8` was pushed normally to the existing `work/inputs-crm-20261006` deployment branch on verified parent `7efc625c664f04c8bc31cfb15811fcd9ad36784c`. Only the nine manifest files are included. Deployment `94246ddd-cc67-4590-b1c8-52084ce694b7` started at 16:58:15 UTC; success and live evidence must be recorded separately.

Deployment succeeded at `2026-10-11T16:59:15.300Z`. All three approved footer writes succeeded and exact content was read back. Publication was handed to the owner; it has not yet been confirmed.

Live original Edge-browser evidence after this release:
- Normal `/rs-recognize` navigation silently reached `/rs-member` twice, with no phone/button entry. Second recognition POST returned 200.
- New Sessions row `recsWl2W6lQ4awspG` was created through the normal page flow for session `547cb765-c2cc-40c3-8d6d-b61906c7db5b`, person `recV9CvYGaTGz7Wzr`, device `reccaXNrPXVzvVmpZ`.
- Second normal visit reused that same row: `started_at` stayed `16:59:35.832Z`; `last_activity_at` advanced to `16:59:54.181Z`; status remained active. Readback returned exactly one matching row.
- Device cookie is Secure, HttpOnly, SameSite=Lax, path `/`, with 365 days remaining. No token value recorded.
- Browser sessionStorage has the same session UID and server-confirmed state with `confirmed:true`; confirmed_at `1791737994581`.
- Existing linked Footprint `recz12byxUsmDQlKf` provides matched recognition evidence for the same session/person/device, recorded during the earlier approved table-address release.

Remaining human/live acceptance: owner publication of the three saved public footer updates, then public known/unknown no-interruption checks; owner desktop InPrivate same-browser code-entry and silent return; mobile confirmation. Do not treat these as verified based on local tests. The existing launcher still has sample text and owner-unwired buttons; those are not identity or navigation proof.

### Publication and public-page verification, 17:02 UTC

Owner confirmed publication. Fresh normal visits to `/`, `/ring-status`, and `/about-ringstatus` each sent the recognition request and received 200 while remaining on the requested public page. No forced launcher navigation occurred. Same Sessions row last_activity_at advanced to `2026-10-11T17:02:19.181Z`; start/person/device remained unchanged. This verifies known-browser public recognition after publication. Owner also reported their device recognized and redirected to the correct landing; whether that involved an unknown browser and SMS code is awaiting clarification. Unknown-browser public behavior and desktop/mobile SMS acceptance are not inferred from this report.

Owner clarified immediate redirect without SMS and reported private mode also recognized. Read-only inspection of the new test tab `685189921` found a recognition cookie identical to the original recognized browser cookie (boolean comparison only; no token recorded). This test therefore exercised an existing recognized identity, not an unknown browser. Do not infer a new identity bypass or claim OTP verification. The next required human step is a genuinely fresh private browser session; preserve the original working session.

### Owner test confirmation

Following the fresh InPrivate instructions, the owner replied “this worked.” Recorded as owner-reported desktop success. This reply supplies no new SMS-code details or browser capture; preserve the distinction between owner confirmation and directly observed evidence. No code or deployment changed following this confirmation.
