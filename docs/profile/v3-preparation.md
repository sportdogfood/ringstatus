# Profile v3 replacement preparation — 4 November 2026

Status: local preparation verified. No live switch, push, publish or production writes performed. Live end-to-end acceptance remains unverified until an approved deployment.

## Historical checkpoint — 20:01 UTC (superseded by final verification below)

- Remote deployment branch verified at `c1a58836a1f477a542f3e9f34c65fe0c50f3b2fc`; preparation branch fast-forwarded to it. Its additional Schedule assets are untouched. Recognize/Profile sources are unchanged since the initial baseline.
- Added preparation assets and an adapter using the existing Profile service. No existing tracked source modified.
- 28 existing Profile/service tests and seven adapter tests passed; Astro build passed.
- Local synthetic browser proof: own Profile save closes immediately with pending state; reload retains confirmed change; invitation create and explicit recipient acceptance; sign-out immediately removes former private content; recipient sees no owner controls.
- Independent read-only reviewer `/root/verify_v3` verified locked CSS/font/icon hashes. Its three findings (scope retry, pending status across concurrent saves, unsupported name requirement) were corrected; verification of those corrections remains.
- Next: responsive/screens verification, regression checks, final artifact/rollback record. Do not deploy. Local fixture server is process session 63572 at port 8796; browser tab 6. Fixture requests never leave localhost.
- Context checkpoint triggered on input size; account usage shows 44% remaining, not task-specific token consumption. Exact task tokens are not separately measured.

## Baselines and exact delta

- Existing Astro/Recognize/Profile implementation: `64090739bc60ab96e9f3e6a763d7c14d36996cd0` (local `origin/work/inputs-crm-20261006` at inspection).
- Locked v3 presentation: `a44f8aa7a613a8a7d5e699ddd3c0b24cbe93f82f` in the separate Profile v3 Sites source.
- Isolated branch: `codex/profile-v3-prepare-20261104`.
- Add a side-by-side v3 preparation entry and adapt its demo identity/storage to the existing same-origin Recognize adapter and Profile service. Preserve existing server authorization, Airtable contracts, queued-save/revision behavior, and browser credential boundary.
- Do not change v2, Recognize, Profile service/backend, CSS/fonts/icons, dependencies, schema, Schedule, or another owner's dirty checkout.
- Preparation testing uses local synthetic fixtures. These do not prove a production Airtable write or a live deployment.

## Connections and unresolved data boundaries

Recognize resolves identity; existing `/test/rs-profile/data` supplies Profile, roster and Team views and performs permitted saves. V3 must run on the same origin; the Sites prototype cannot consume its private cookie across origins. Team membership is not an account-switch credential. Only one's own personal Profile can be edited; Team Owners retain invitations and member-status controls.

Horses, Opt-ins, placeholder Shows/Waze/Logs and Agent are not new backend integrations in this replacement. Do not invent a People trainer reference or use `39138` as authorization. Existing prototype-only behavior must not claim Airtable persistence. Schedule remains outside scope.

## Cutover boundary

Do not push the existing Webflow Cloud auto-deployment branch. Do not change `/profile`, the existing v2 entry or any Recognize redirect. An owner-approved later deployment/cutover must first revalidate the current live revision and verify the staged v3 route with real recognition and permitted data. Keep v2 available for immediate loader rollback; preserve the existing per-person pending/snapshot storage keys and do not clear retained work.

## Delivered preparation

- `webflow-cloud-test/public/rs-profile-v3/`: locked v3 presentation and new `connection.js` adapter. Own Profile saves, Team creation/invitations/acceptance/decline/status, personal hide/show and credential/sign-out use existing v2 contracts. Another person's personal Profile is read-only. Blank names remain supported. PINs are not copied into presentation storage.
- Initial Profile load precedes silent roster and Team preparation; missing views can be retried from navigation or a visible Retry control. This is not a claim that all future list packages or the requested eight-minute synchronization cycle are implemented.
- `embed.js`: candidate loader for the existing `/profile` page. A same-origin frame contains the exact v3 document so Webflow styles cannot override it. Existing Recognize redirects remain `/profile`; the frame shares the same-origin cookie and service. Schedule links exit the frame and remain external.
- `test-support/profile-v3-preview.mjs`: localhost-only synthetic fixture server. It uses the real existing Profile client service; identity/network replies are fixtures. The `/profile` fixture loads the captured host stylesheet with its scripts removed and the candidate loader substituted. No fixture data is sent to Airtable.
- Existing v2, Recognize, service/backend and package versions are unchanged. Schedule files inherited from the current remote branch are untouched.

## Verified evidence

| Check | Result |
| --- | --- |
| Existing Profile/backend and service tests | PASS: 28 |
| V3 adapter, permission, blank-name and identity-fencing tests | PASS: 8 |
| Astro production build | PASS; existing dynamic-route warning remains |
| Locked CSS, fonts, icons and assets | PASS: 11 byte-for-byte matches; `v3-evidence/verification.json` |
| Local browser save/reload | PASS: editor closes immediately, pending state appears, confirmed value survives reload |
| Horse drafts | PASS: existing add/edit retained in per-person browser storage; reload retains draft; another account sees none and has no owner add control |
| Local identity isolation | PASS: Not you clears prior private content before sign-out reply; credential entry establishes the next fixture identity; no local account impersonation |
| Team invitation | PASS: owner invites existing candidate; recipient explicitly accepts; non-owner has no owner controls |
| Team search and member details | PASS: matching roster result; another person's contact data absent and edit disabled |
| Failed Team request | PASS: explicit error and Retry; retry restores controls |
| Navigation | PASS: direct Shows and Team links, Profile, Team, Shows, Waze, Logs, Riders, Simple list, Agent shortcut and component library |
| Opt-ins | PASS: Classes/Rider/Groom forms render; conditional minute and time inputs and browser-only save retained; Save preferences stays in section, not footer |
| Responsive shell | PASS: 375x812 and 1440x900, dark/light; no compact horizontal page overflow; Not you footer present; shared top-row heights unchanged across groups |
| Actual v3 live recognition, Airtable save and deployment | UNVERIFIED: deliberately not deployed |

The independent read-only review found scope-retry, aggregate save-status, blank-name and direct-link initialization issues. They were corrected. The direct links and failed-request recovery were then verified in the browser. Screenshots in `v3-evidence/` show synthetic data, not live records.

The public `/profile` HTML was read successfully and confirms page `6ae3b5347af0dd407ea96502` and the current module loader `/test/rs-profile/client.js`. Public script-byte retrieval returned HTTP 403; no authentication workaround was attempted. The source rollback archive comes from the verified Git baseline, not a claimed current served-script byte match.

## Exact later cutover and rollback — owner approval required

Target: existing RingStatus Webflow site `6982268b7543ac3c80151266`, page `6ae3b5347af0dd407ea96502` (`/profile`). Existing Cloud app `d7d97751-20e1-4148-a5cf-ee58671c128a`, environment `110f06dd-c1ea-4839-98af-d829cbe77941`, mount `/test`, auto-deploy branch `work/inputs-crm-20261006`.

1. Recheck current remote/deployed revision and preserve other owners' newer changes. Apply only this preparation's added files through the existing Cloud route after explicit deployment approval. Do not reset that branch to this baseline.
2. Verify `/test/rs-profile-v3/` on the existing origin with real Recognize and permitted test records, including pending save/reload, invitations, account change and desktop/compact rendering. Do not call local fixture success live proof.
3. Only after cutover approval, replace the existing page's single Profile loader tag with the candidate below; do not add both. Publish only the approved page/site operation using the established route. Leave Recognize and the standalone v2 entry unchanged.

Candidate:

```html
<script type="module" src="/test/rs-profile-v3/embed.js"></script>
```

Rollback:

```html
<script type="module" src="/test/rs-profile/client.js"></script>
```

Restore only that tag and publish the authorized correction. Verify `/profile` uses v2 again and recognition and pending saves still work. Do not restore the whole captured HTML, roll back the entire Cloud app, clear browser storage, or reverse Airtable records. The old v2 code/service remain deployed and share the original storage contracts. `v3-evidence/v2-source-backup.zip` preserves the known source baseline for investigation if needed; it is not a command to replace newer work.

Existing horse add/edit remains browser-only, isolated per person. Airtable horse synchronization still requires the approved People-to-trainer mapping; `39138` is not treated as a permission grant. Opt-ins remain per-person browser choices with SMS off, not Airtable consent. Generic Shows/Waze/Logs tabs and Agent remain their existing prototype behavior. GeoIP/location and the future full-package refresh are not newly wired by this replacement preparation.
