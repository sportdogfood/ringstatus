# Profile V3 and Schedule approval staging proposal

Status: documented only, 5 November 2026. Implementation is deferred. This document does not authorize creating a branch or environment, editing runtime code, configuring credentials or bindings, changing data, pushing deployment branches, or deploying.

Documentation-only commit and synchronization were separately authorized after the proposal was recorded. That authorization does not permit implementing or deploying staging.

## Existing setup inspected

- Existing Astro app: `ringstatus`, project `d7d97751-20e1-4148-a5cf-ee58671c128a`, repository `sportdogfood/ringstatus`, application directory `webflow-cloud-test`.
- One Cloud environment was returned by inspection: `110f06dd-c1ea-4839-98af-d829cbe77941`, mount `/test`, tracking `work/inputs-crm-20261006`. The live website uses this environment. `/profile`, `/wef` and the existing Webflow preview hostname are not independent approval deployments.
- Latest successful deployment observed during that inspection: source `c0d5ba274c6f0063d15eaf3996bb8ea9abb6102b`, deployment `3882c892-4f3f-49fe-a815-da196b385466`. This is a dated observation, not a promise that it remains the newest release.
- Existing recognition storage binding: `RS_RECOGNITION_CONTROL_DB`. Environment variable metadata includes protected Airtable credentials. No secret values were retrieved for this proposal.

## Intended approach

Use a separate review branch and branch-based environment in the existing Astro project. Use the same application source, approved shared V3 components, data connections and owning engines. Do not create a separate ChatGPT Sites implementation or a second Profile/Schedule loader.

No implementation/review branch name, mount path, staging URL, environment, binding or dataset has been selected or created. Webflow requires each environment's mount to be unique within the app.

Approval applies to the exact tested source revision. After authorization, promote that source through the existing live route and verify the affected live journey. Environments build separately: this means the same approved source, not an identical prebuilt artifact or guaranteed identical runtime behavior.

## Compatibility findings

Profile hard-codes `/test/` in public HTML assets, imports, service requests, embedding and navigation. Merely deploying under a different mount could still load live resources. A narrow shared environment-aware path configuration is needed before an independent approval deployment can be trusted, while preserving live `/test` behavior.

Schedule inspection also found:

- `public/wef-schedule-v2/schedule.html` and `day.html` explicitly set `data-schedule-endpoint="/test/wef-schedule/data"`.
- Schedule's standalone Back explicitly targets `/profile`; Profile owns the corresponding website-overlay override and forward links.
- `schedule-data.js` uses the browser-held scope key `rs-schedule-current-scope-v1`; `package-store.js` uses IndexedDB `rs-schedule-packages-v1`. A different path on the same origin does not by itself separate that browser-held data.
- Existing relative asset and scope routes must be assessed against the selected mount; they must not all be rewritten indiscriminately.

These findings authorize no path edits, schema changes, cache replacement, loader replacement or timer changes. Schedule retains its existing engine and ownership.

## Decisions required before provisioning

- Review origin and mount, including how standalone Profile and the real website overlay will both be reviewed.
- Recognition binding/settings and separation of cookies and device-held state from live use.
- Which permitted data the review uses and the explicit Airtable write policy. Separate hosting does not make existing save endpoints read-only or isolate Airtable business records.
- Appropriate environment configuration and access to the review URL; it must not be assumed private simply because it is called staging.

No isolation, safe write destination or staging-ready connection is claimed today.

## Later acceptance

Once the specific preparation is authorized, verify only the approved delta plus affected shared behavior: desktop and393px, dark and light, Recognize/Profile loading and permitted data, pending saves where applicable, and Profile ↔ Schedule navigation in standalone and website-overlay contexts. Keep the live version unchanged during approval review. Relevant edits after testing require the affected checks again before promotion.

The [shared enhancement contract](../profile/README.md#enhancement-rules--profile-and-wef-schedule) remains authoritative. This proposal records staging findings; it does not replace or duplicate that contract.

## Platform references

- [Webflow Cloud environments](https://developers.webflow.com/webflow-cloud/environments/): branch environments, unique mounts and access visibility.
- [Webflow Cloud configuration](https://developers.webflow.com/webflow-cloud/environment/configuration): mount-aware public assets/API calls and environment storage bindings.
- [Webflow Cloud deployments](https://developers.webflow.com/webflow-cloud/deployments): deployment workflow.
