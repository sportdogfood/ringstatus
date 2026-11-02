# Recognize-only widget specialists — 2 November 2026

Accountable coordinator: **rs-recognize-coordinator**, chat `01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5`. Owner explicitly cut Profile ownership from this chat on 2 November. This current scope supersedes the earlier dual-widget assignments. These subordinate specialists now cover Recognize only; they are not Profile coordinators or autonomous publishers. Any implementation needs an approved bounded assignment.

## Exact widgets

| Widget | Visible controls and entry | Served module | Source |
| --- | --- | --- | --- |
| Recognize | Current dark fingerprint launcher, one-input sign-in panel, Open profile. Used on `/`, `/ring-status`, `/about-ringstatus`, `/recognize`. Successful credential entry or Open profile opens `/profile`; silent recognition stays on the current public page. | `/test/rs-recognition/widget.js` | `webflow-cloud-test/src/assets/rs-recognition/widget.js`, `widget-view.js`, `widget.css`; route `src/pages/rs-recognition/widget.js.ts` |

Profile's toolbar, panels, User/Team data routes and permissions are assigned through the [rs-profile-coordinator handoff](rs-profile-coordinator-handoff-20261102.md). Legacy `/rs-recognize`, unrelated `lp-profile` and WEF serving implementations are outside Recognize assignments.

## Actual dispatched specialists

| Subagent | Responsibility | Owned inspection surface | Exclusions |
| --- | --- | --- | --- |
| `/root/widget_ui` | Recognize launcher, single-input panel and recognition states | Recognize `widget-view.js`, `widget.css` | No Profile UI, redesign, data changes or releases |
| `/root/widget_data_staging` | Recognize identity/index, device/session/history and proposed maintenance | Existing recognition adapter/IAM/refresh/history boundary | No Profile User/Team ownership, new schema, record repair or workflow execution |
| `/root/widget_astro_publish` | Recognize module/routes and coordinated packaging | Recognize route wrappers and agreed shared release boundary | No Profile route ownership, independent push/deploy/publish, upgrades or unrelated routes |

The shared recognition adapter, native client, credential helpers, refresh integration, D1 claims helpers and shared Cloud app require agreement between affected owners before changes. Profile owns its User/Team permissions and existing Tenant REST consumer boundary; Recognize owns identity. Data staging here means the existing Recognize canonical-to-index boundary, not another database or copied demo model. Internal Recognize subagents are not transferred to Profile; its coordinator may request bounded specialist work separately.

## Baseline and acceptance

Deployed source `89abf609f816b6c9d10f05d301027c991a33280a`; local documentation-only baseline `6c31bdf28fb68a21e96576acafcf542248d0f00f`. Existing Cloud app `d7d97751-20e1-4148-a5cf-ee58671c128a`, environment `110f06dd-c1ea-4839-98af-d829cbe77941`, mount `/test`. Branch `work/inputs-crm-20261006` auto-deploys on push. Website publication remains separate and was not performed by Codex.

Existing-cookie recognition and navigation to `/profile` passed live on 2 November. The full downstream User/Team read/reload evidence is retained in the Profile handoff; it is no longer an ongoing Recognize deliverable. Prior scoped tests/build/source review remain dated evidence. Fresh credentials, live IP-based behavior and mobile Recognize acceptance were not all reverified in that release.

Evidence: main checkout `.git/ringstatus-control/recognize-model-correction-20261101/live-acceptance-20261102.json`; [current implementation record](profile-real-records-20261101.md). Agents owning chat row `reclrHKIRCyDsiKXd` records actual worker names, progress and next action. No new standing monitors or business workflows are introduced.
