# Recognize and Profile widget specialists — 2 November 2026

Accountable coordinator: rs-recognize, chat `01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5`. Owner explicitly requested three specific subagents. These are subordinate specialists in this chat, not replacement chats or autonomous publishers. Initial assignments are read-only identification of source, responsibilities and acceptance. Any implementation needs an assigned approved delta; shared adapters and releases stay coordinated by the primary.

## Exact widgets

| Widget | Visible controls and entry | Served module | Source |
| --- | --- | --- | --- |
| Recognize | Current dark fingerprint launcher, one-input sign-in panel, Open profile. Used on `/`, `/ring-status`, `/about-ringstatus`, `/recognize`. Successful credential entry or Open profile opens `/profile`; silent recognition stays on the current public page. | `/test/rs-recognition/widget.js` | `webflow-cloud-test/src/assets/rs-recognition/widget.js`, `widget-view.js`, `widget.css`; route `src/pages/rs-recognition/widget.js.ts` |
| Profile | Current Profile toolbar and dark Profile/Team panels on `/profile`; standalone app entry `/test/rs-profile/`. Uses embedded Recognize before loading private data. | `/test/rs-profile/client.js`; API `/test/rs-profile/data` | `src/assets/rs-profile/client.js`, `service.js`, `profile.css`, `recognize.css`; `src/lib/rs-profile.js`; routes under `src/pages/rs-profile/` |

Legacy `/rs-recognize`, unrelated `lp-profile` and WEF serving implementations are outside these widget assignments.

## Actual dispatched specialists

| Subagent | Responsibility | Owned inspection surface | Exclusions |
| --- | --- | --- | --- |
| `/root/widget_ui` | Both widgets' design, controls, interaction states and UI acceptance | Recognize `widget-view.js`, `widget.css`; Profile `client.js`, `profile.css`, `recognize.css` | No identity/permission changes, invented data, redesign or releases |
| `/root/widget_data_staging` | Identity handoff, existing D1 index, canonical records, mapping and save/readback contracts | Profile `service.js`, `rs-profile.js`; relevant existing recognition adapter/IAM and existing User/Team links | No new staging store/schema, record repair, workflow runs or UI changes |
| `/root/widget_astro_publish` | Astro routes, module packaging, page-loader integration and release acceptance | Recognize/Profile route wrappers, standalone entry, existing Cloud target/configuration | No independent push/deploy/website publish, dependency upgrades or unrelated routes |

The shared recognition adapter, native client and existing Tenant REST boundary require primary-coordinated changes. UI consumes the data contract; Astro packages approved assets without changing their semantics. Identity does not grant Team/WEF access; those checks stay outside Recognize. Data staging means the existing canonical-to-index/application boundary, not authorization for another database or copied demo model.

## Baseline and acceptance

Deployed source `89abf609f816b6c9d10f05d301027c991a33280a`; local documentation-only baseline `6c31bdf28fb68a21e96576acafcf542248d0f00f`. Existing Cloud app `d7d97751-20e1-4148-a5cf-ee58671c128a`, environment `110f06dd-c1ea-4839-98af-d829cbe77941`, mount `/test`. Branch `work/inputs-crm-20261006` auto-deploys on push. Website publication remains separate and was not performed by Codex.

Existing-cookie Recognize -> real existing User/Profile -> active Team/six-member roster and reload passed live on 2 November. Exact User/Team/membership IDs matched independent Airtable reads. Prior 16 scoped tests/build/source review are separate preparation evidence. Fresh credential entry, live saves/retries, Owner writes, GeoIP confirmation and mobile acceptance were not verified in that release. User levels, Team creation/invitations, inline uploads and modified-date sorting remain unimplemented, without schema approval.

Evidence: main checkout `.git/ringstatus-control/recognize-model-correction-20261101/live-acceptance-20261102.json`; [current implementation record](profile-real-records-20261101.md). Agents owning chat row `reclrHKIRCyDsiKXd` records actual worker names, progress and next action. No new standing monitors or business workflows are introduced.
