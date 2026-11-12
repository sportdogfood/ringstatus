# Recognize + Profile: behavior, storage and runbook

Document version: **1.1 — 2026-11-12** (repository publication; behavior unchanged). Evidence date: **2026-11-11**, America/New_York. This is a dated operational record, not a claim that later deployments are identical.

Scope: existing Recognize and Profile v3 integration through Astro/Webflow Cloud. Documentation only. No runtime changes, record changes, deployment, new synchronization route or new behavior is authorized by this document. Schedule owns its own datasets and is not reimplemented here.

Canonical repository path: docs/recognize-profile-runbook/README.md on shared main. The [shared data and UI contract](../profile/README.md) and this runbook must be read from the same repository commit. Local checkout paths below identify historical evidence, not the canonical distribution location.

## Version and deployment record

| Item | Verified reference |
| --- | --- |
| Source application | `webflow-cloud-test` in the RingStatus repository |
| Reviewed checkout | `C:/Users/gombc/.codex/worktrees/profile-first-load-20261111` |
| First-load correction | `291c645461a33239dffbe332410366a42dc7b0c0` |
| Desktop-only sizing correction / reviewed HEAD | `16291a95b665d61413d04b02cc04c6dd9f0d032e` |
| Existing deployment branch | `work/inputs-crm-20261006` |
| First-load deployment | `c6741363-de4d-4530-b82b-f9b0bf539016`, recorded successful 2026-11-11 19:30:27 UTC |
| Desktop deployment | `a487762c-f9dc-4c76-ac41-522e3100edf4`, recorded successful 2026-11-11 19:44:39 UTC |
| Webflow site | `6982268b7543ac3c80151266` |
| Cloud application / environment | `d7d97751-20e1-4148-a5cf-ee58671c128a` / `110f06dd-c1ea-4839-98af-d829cbe77941` |
| Existing live Profile route | `https://ringstatus.com/test/rs-profile-v3/` and the website embed |
| Recognize route | `/test/rs-inputs/native-recognition` |
| Profile data route | `/test/rs-profile/data` |

Do not confuse this deployed Astro source with an older `chatgpt.site` prototype. The main repository checkout may be on another branch; use the recorded commit when comparing this release.

## Rules: how Recognize and Profile work together

1. Public browsing does not wait for Profile, Airtable, history export or coordinates. Signed-out users see the Recognize control, not private Profile content.
2. On arrival, inspect existing device state and start silent server recognition. A matching retained Profile snapshot can render before the recognition response; it is display context, not a replacement for server authorization. Pending logout, expired identity and account/device mismatches must not restore another account's private display.
3. Without a usable local identity, the person submits the existing phone/PIN/invite form. The current server uses its Cloudflare D1 identity snapshot. The measured credential and recognition responses made zero Airtable calls.
4. An authenticated recognition response supplies the user ID and minimum name. Profile immediately seeds that minimum identity and shows the launcher. It does not wait for the full Airtable-backed Profile response. GeoIP-only identity is explicitly excluded from this authenticated bootstrap.
5. Full Profile data hydrates behind the visible minimum. The minimum identity is not an editable complete record: Edit waits for the actual record rather than opening empty fields.
6. Existing packages load independently. The observed background order was Profile, roster, Team, horses. A requested missing package takes priority; background work then resumes. Reuse loaded device data instead of starting all datasets again on each click. Priority interruption/resumption was not re-tested in the latest live acceptance run.
7. Save retains the requested edit locally and allows the user to continue. Pending is not confirmed Saved. Use the existing server save route and identifiers; preserve newer local edits against older responses and keep operations for the same record ordered. PINs must not enter persistent Profile snapshots or pending storage.
8. Logout immediately removes the private display and returns to Recognize. Credentials/confirmation are cleared or retired through the existing flow. Account-scoped snapshots and pending work are retained separately; they must never be displayed or submitted as another user.
9. Profile retention is separate from show/date selection. A show change must not discard Profile or pending edits. Access rejection clears the cached Profile display. Local snapshots have no fixed expiration timer.
10. No new loaders, exporters, stores, gates, schema changes or replacement components are implied by these rules. Use the existing implementation. Changes require the owner's applicable scope and publication authority.

## What the user sees while signing in

There is **no loading spinner, progress animation or changing button label** in the current Recognize submit handler. Sign in is disabled while the request is running and retains its label. Errors are displayed by the existing form. This is an open feedback gap, not a completed enhancement.

## Cookies

These are the Recognize/Profile-related cookies verified in the source, not an inventory of unrelated website cookies. Never copy actual cookie/token values into a report.

| Cookie | Contents / purpose | Renewal and removal |
| --- | --- | --- |
| `__Host-rs_recognition_device` | Server-issued recognition token | Successful recognition renews cookie and D1 device expiry to 365 days. Credential sign-in issues/renews it. Logout expires it. `Path=/; HttpOnly; Secure; SameSite=Lax`. |
| `__Host-rs_recognition_signed_out` | Value `1`, prevents GeoIP automatically restoring an identity after logout | Set on logout for 365 days; cleared on successful credential sign-in. `Path=/; HttpOnly; Secure; SameSite=Lax`. |
| `__Secure-rs_input_access` | Related existing inputs-access cookie; not the Profile snapshot | Recognition logout explicitly expires it at `Path=/test/rs-inputs`, with HttpOnly, Secure and SameSite=Strict. Issuance/normal lifetime was not audited in this review. |

Local storage alone is not proof that the server cookie is valid. Cookies and browser storage are origin-specific; a prototype origin does not share the live site's stored identity.

## Device storage inventory

`<user>` means the current user ID, not a shared record for every person. Storage schema suffixes are their existing versions; they are not this README's version.

| Location / exact key | Stored data | Update / lifetime |
| --- | --- | --- |
| localStorage `rs_recognition_device_token_v1` | Browser recognition token | Created/updated by the recognition flow; removed on logout in the current widget adapter. |
| localStorage `rs_recognition_device_expires_v1` | Millisecond expiry timestamp | Set to now + 365 days after successful recognition; cleared on logout. |
| localStorage `rs_recognition_identity_v1` | `personUid`, `deviceToken`, `expiresAt` | Retained with Profile; cleared on logout/unrecognized outcome. It is not permission to bypass server access checks. |
| localStorage `rs_recognition_pending_history_v1` | Pending history event UUIDs | Removed as export is confirmed. Failure retains the queue; not a condition for displaying Profile. |
| localStorage `rs_recognition_pending_signout_v1` | Pending device-retirement request | Stored before logout completion; removed after confirmation. Prevents restoring signed-out private display while retirement is pending. |
| sessionStorage `rs_native_recognition_session_v1` | Session UUID | Created when needed and rotated by existing logout/history handling. Tab-session storage. |
| sessionStorage `rs_native_recognition_confirmation_v1` | `session_uid`, `confirmed`, `confirmed_at` | Successful recognition sets confirmation; cleared for revalidation/logout. |
| localStorage `rs_profile_snapshot_v1:<user>` | `uid`, `deviceFingerprint`, confirmed state including loaded permitted Profile/Team/horse data | Updated after confirmed reads/saves. No fixed TTL. PIN removed from stored profiles. Removed on access rejection; ordinary logout retains account-scoped data. |
| localStorage `rs_profile_pending_v1:<user>` | User/fingerprint, save body and operation identifiers, sent/retry/error flags | Updated with pending work; removed when empty. PIN removed and `needsPin` retained where necessary. |
| localStorage `rs_profile_hidden_v1:<user>` | User-scoped hide/show choices | On preference changes. |
| localStorage `rs_profile_v3_view:<user>` | UI preferences, including visible sections, tab/group, sort, layout and collapsed state | On UI changes. Activity, agent messages and horses are excluded from this object. |
| localStorage `rs_profile_geoip_v1:<user>` | `uid`, `deviceFingerprint`, `geoip: {latitude, longitude, source, observedAt}` | Successful optional coordinate ping. No fixed TTL; only matching user/device metadata is used. |
| localStorage `ringstatus-sms-alert-optins-v2:<user>` | Version 2 preferences: enabled flag, time zone, variables with enabled/value settings | Existing UI saves locally. This is not Airtable or SMS subscription confirmation. The model also declares legacy v1 key `ringstatus-sms-alert-optins-v1`. |
| localStorage `rs_profile_v3_horses:<user>` | Separate browser-only horse-editor records | Existing local horse form writes here. Do not confuse it with server-loaded horses in the Profile snapshot or claim these edits saved to Airtable. |

No actual phone numbers, PINs, tokens, raw IPs or coordinate values are included in this document. Unrelated `lp-*` storage, Schedule IndexedDB packages and other applications' preferences are outside this inventory.

## GeoIP identity hints versus coordinates

**Identity hint:** Recognize derives a hashed lookup key from the request IP and stores the association with a person in D1 `recognize_iam_keys` with kind `geoip` and a 365-day expiry. Authenticated recognition/sign-in learns or renews that association. A unique valid association may produce an `identity_only` result. It must not grant private Profile access. A signed-out cookie suppresses that route. The raw IP is not the persisted lookup key.

**Coordinate ping:** after initial Profile read completes, the client quietly requests `/test/rs-profile/data?view=geoip` with the existing identity and actor scope. The server first uses Cloudflare request coordinates. If absent, it may request GeoJS using the request IP. This fallback transmits the IP to GeoJS to perform the lookup; the returned Profile metadata stores coordinates/source/time, not raw IP.

Coordinate source is `cloudflare` or `geoip`. Latitude/longitude must be finite and within valid ranges. Client timeout is 2.5 seconds; GeoJS fallback timeout is 1.5 seconds. Failure does not delay or interrupt Profile. The ping is attempted once per user/device fingerprint per page runtime; even a failed attempt is not automatically retried in that same runtime by this function. It is not an eight-minute coordinate refresh.

No browser location permission is requested by this path. GeoIP coordinates are approximate, not browser GPS. The previously discussed permission-based precise-location option is not established as implemented by this review. This coordinate endpoint does not establish an Airtable coordinate-save workflow.

## Refresh cadence

- Browser schedules the existing refresh at eight minutes and schedules the next tick after the preceding attempt finishes. It is not an exact wall-clock cron and does not run with the page closed; browser timer throttling can delay it.
- Refresh the largest prepared Profile view: Team if loaded, otherwise roster if loaded, otherwise Profile. Refresh horses when requested or loaded.
- Existing conditional requests and merge handling avoid replacing unchanged state and protect pending edits. A changed-only result does not itself prove Airtable is supplying record-level deltas; do not claim that without tracing the server query.
- Cookie expiry renewal happens on recognition, not on this Profile timer. Coordinates have their own once-per-runtime behavior above. History export is separate from minimum Profile display.

## Recorded performance and verification

| Scenario | Recorded result, 2026-11-11 |
| --- | --- |
| First phone sign-in, no Profile snapshot | About 1.23 seconds from credential request to minimum Profile. Name/launcher appeared about 23 ms after Recognize responded. Both recognition calls reported `X-RS-Airtable-Calls: 0`. |
| Full initial Profile response | About 8.876 seconds for the real request in this test. Minimum Profile was already visible. An additional deliberate 8-second browser-only hold separately proved it did not block the minimum display. |
| Returning reload | Cached name/launcher observed at 1.0675 seconds after page start, at least 337.8 ms before recognition completed and 1.5294 seconds before Profile response. This is not a claim of zero page-start cost. |
| Logout then sign back in | Retained name appeared 14.4 ms after recognition reply. Private display disappeared on logout. |
| Corrected names | Lainey Owner and Lainey Rider verified in actual Recognize responses and Profile headings after existing IAM refresh. |
| Desktop/mobile | Desktop 820px shell, scale 1, right aligned; launcher about 393px. Mobile 393px viewport: full available width, square shell corners, launcher above footer. Dark/light editor fit and toggle/logout checked. |

These are individual observed measurements, not latency guarantees or a multi-run performance distribution. This acceptance did not perform business record saves, force a priority interruption, wait through an eight-minute changed-data cycle, or retest every previously listed website page. Do not label those checks newly passed.

## Known trouble and remaining proof

| Status | Issue | Required handling / evidence |
| --- | --- | --- |
| Open | No sign-in spinner or changing progress label | Disabled button only. No feedback enhancement is authorized by this README. |
| Open performance observation | Full Profile response took about 8.876 seconds | Keep minimum identity independent. Measure full-data latency separately; do not report 1.23 seconds as complete-data availability. |
| Resolved on evidence date; recurrence possible | Recognize said RingStatus Test while Profile said Lainey Owner | Airtable names were already correct; D1 identity snapshot was stale. Existing authenticated owner `refresh_iam` returned success, then Owner and Rider names matched live. Direct source edits need the existing refresh path; do not hardcode a name in UI. |
| Existing limitation | Coordinates can remain old after a failed ping; no recurring coordinate update | Inspect `observedAt`; a previous stored value is not evidence of a successful current ping. |
| Existing limitation | Opt-in and separate horse-editor saves are browser-only | Do not present them as Airtable-confirmed changes or active SMS subscriptions. |
| Verification gap | Eight-minute changed-data merge, priority promotion/resumption, save/retry/conflict and every embed page | Source behavior is documented; the latest live run did not independently prove every case. Use the runbook below before claiming current end-to-end acceptance. |
| Historical, not re-established as current | 503/history-session conflicts, stale deployment/style, wrong Schedule navigation, missing launcher icons, oversized/empty shell | Earlier thread reports do not establish current failure or current resolution. Reproduce against the exact deployed version before edits. Latest proof covers only the explicit rows above. |
| Operational risk | Clearing storage destroys retained pending work; old-account snapshots coexist | Do not clear production storage to troubleshoot. Use an isolated test context and verify account isolation. Never infer active identity from merely finding a key. |

This register covers problems established in the available source and retained evidence. It does not claim an exhaustive audit of every historical deployment or unrelated website component.

## Operating and test runbook

### 1. Establish the version and preserve evidence

Record date/time, URL, deployment ID, source commit, measured viewport and test identity label. Compare the served files with the approved commit. Keep existing cookies, pending work and business records intact. Use a separate test browser context for no-storage checks. Never record full credentials, cookie values, raw IP or precise coordinates in evidence.

### 2. First-time sign-in

Use the existing Recognize UI and an authorized existing account. Confirm no usable snapshot in the isolated context. Measure credential request start, recognition response, first visible name, launcher and full Profile completion separately. Verify no Airtable call on the recognition path. Minimum identity must appear before full Profile completes. Edit must not expose an empty editable record while hydration is pending. Record the current disabled-button-only feedback honestly.

### 3. Returning user and logout

Reload without deleting cache. Confirm matching retained name/launcher can render before network confirmation, then actual data refreshes quietly. Logout: private display disappears immediately, only Recognize remains, and a reload must not silently restore the signed-out user. Sign back in and confirm the appropriate retained snapshot is reused. Switch between the authorized Owner and Rider test identities and verify no cross-account display or pending submission. GeoIP-only recognition must not unlock Profile.

### 4. Background lanes and priority

Observe the existing browser requests without manually calling a substitute pipeline. Open a prepared list: it must use device data. Open a not-yet-prepared permitted list while background work remains: that lane takes priority, then background loading resumes. Record actual request order, user ID and duplicate requests. Do not substitute a direct endpoint call for UI-path proof or alter business records to force a result.

### 5. Eight-minute refresh

Keep the recognized page open and observe the real timer. Verify a no-change response preserves state and that loaded/requested lanes are refreshed. For changed-data proof, use only an owner-authorized test edit through the established save/workflow; wait for the actual refresh and verify the changed record appears without replacing newer pending edits. Report browser throttling and elapsed time. Do not manually invoke refresh and label it cadence proof.

### 6. Save and retry

Only with an authorized test edit, use the existing inline editor. Confirm the UI retains work immediately, labels it pending until confirmed, and the actual saved record agrees. Test reload/navigation retention and retry in an approved isolated test, retaining the same operation identity. Confirm PIN is absent from persisted snapshots/pending objects, and old responses do not overwrite newer edits. Never repair production records or repeatedly submit a failed operation as a diagnostic substitute.

### 7. GeoIP and coordinates

Inspect the recognized response's `identity_only` distinction without logging raw identifiers. Observe the optional Profile coordinate request after hydration. Verify returned user scope, coordinate validity, source and observation time, then the matching local metadata. Confirm no browser location prompt and no coordinate wait before minimum Profile. Record absent/failed coordinates as unavailable, not zero coordinates. The coordinate fallback is not authorization and does not prove precise location.

### 8. Name mismatch recovery

Read the existing Recognize people source and Profile identity for the same person. If source fields are correct but the fast identity snapshot is stale, the existing owner-authorized IAM refresh is the established path: authenticated POST `/test/rs-inputs/native-recognition?operation=refresh_iam`. This is a state-changing administrative action, not a routine page-load step. Use it only with applicable authorization. On failure, stop that execution path; do not directly patch D1, invent a new sync or repeatedly retry. Verify actual Recognize response and Profile heading for both accounts after success. Normal Profile save handling already queues its existing IAM refresh.

### 9. Styling and release protection

Preserve approved components, stack, right-aligned desktop sizing, mobile behavior and launcher semantics. No redesign accompanies a data correction. Test desktop/mobile using measured CSS viewport dimensions; browser zoom can make tool dimensions misleading. Inspect the actual deployed website embed, not just the prototype. Test Profile toggle, active icon, horizontal icon overflow, logout and existing Schedule handoff. Schedule remains a separate application, not a replacement Profile landing page.

Deploy only with explicit applicable authorization through the existing route. Code changes require relevant tests and review. For the recorded release, sizing-only rollback is revert `16291a95`; first-load-only rollback is revert `291c6454`, preserving intervening owners' changes. Review current history before any revert. This README does not authorize rollback or deployment.

## Evidence and implementation pointers

Paths below identify the reviewed source, relative to `webflow-cloud-test` at the recorded commit:

- `src/lib/rs-recognition-iam.js`: cookie/D1 recognition, GeoIP hint, logout and audit creation.
- `src/lib/rs-recognition-iam-refresh.js`: existing IAM source refresh.
- `src/assets/rs-recognition/widget.js` and `widget-view.js`: active adapter, sign-in feedback and minimum identity handoff.
- `src/assets/rs-recognition/native-client.js`: shared recognition/session/history helpers. Its legacy standalone UI must not be mistaken for active v3 widget behavior.
- `src/assets/rs-profile/service.js`: local snapshots, pending edits, per-user checks, lane loading, refresh and coordinate ping.
- `src/lib/rs-profile-geoip.js`: Cloudflare coordinates and GeoJS fallback.
- `public/rs-profile-v3/refresh.js`: eight-minute browser timer.
- `public/rs-profile-v3/app.js`, `styles.css`, `optins/model.js`: active UI, desktop change and local preference saves.

Retained acceptance evidence was read from `C:/Users/gombc/AppData/Local/Temp/profile-first-load-20261111/live-acceptance.md` and `name-correction.md`. These are local evidence files, not public links. Their results and limitations are preserved above so this README remains useful if temporary files are removed. No fresh deployment or live re-test was performed merely to create this document.
