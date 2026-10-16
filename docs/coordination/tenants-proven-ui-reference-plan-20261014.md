# Recognize / Tenants: proven UI reference plan

14 October 2026. Source review and current official-documentation research only. No application, Webflow, schema or deployment changes. No new live visual audit or performance measurement claimed.

## Recommendation
Use one responsive Recognize panel: full available width on phones, the same capped-width panel centered on desktop. Avoid a separate desktop design; retain keyboard, focus and zoom support. Dimensions should come from the selected existing shell, not an invented second visual system. This does not automatically constrain all Tenants lists to a phone width.
For Tenants, retain native Webflow presentation, current Astro endpoints, existing identity and permissions, and the current shared persistence implementation. No evidence in this review justifies a React rewrite. A targeted React island remains an option only if a measured interaction cannot be maintained cleanly using the existing implementation. Framework selection alone does not establish speed.

## Exact source map and permitted reuse
Source checkout HEAD inspected: f5e895b6d3af5b87d1d2f0d8524061a5e7f171f9; no uncommitted changes reported for the HPS/WEC client and WEC library reference files.
Owner supplied HPS asset version: 05854306ba53b0130d8f3f972fc9907554616aec. Owner supplied WEC historical proof: 647ea24f8. These historical snapshots remain baseline references; current-source observations below do not certify those deployments.

| Reference | Exact source | Observed behavior / use | Preserve boundary |
|---|---|---|---|
| HPS tenant 8778 | webflow/hps/hps.js:630 saveField; :666 saveRecordChange; webflow-cloud-test/src/pages/hps/horses.js | Field editing, tenant-scoped horse view, POST field change, returned-value merge, saved status and hp_cls logging. Text edits use 700ms debounce in inspected source. | Do not copy fixed tenant8778 as identity, permissive CORS or assume a tenantId establishes permission. Do not copy boolean rollback over the agreed retained dirty state. |
| WEC inline Save | webflow/packing-worksheet/wec-packing.js:898 postAction; webflow-cloud-test/src/pages/wec-packing/action.js; src/lib/wec-packing.js | Existing action endpoint, direct Airtable save, optimistic updates, merged returned state, preserved position, failed-action device queue. | Keep explicit save for multi-field edits and retained dirty edits. Reuse this direct save pattern; do not make an Airtable automation mandatory. Preserve Tenants identity, permissions, records and consent while adapting the boundary. |
| Manual session test | User-supplied Start session embed; webflow-cloud-test/src/pages/wec-schedule/edit.js:82 | Demonstrates explicit session POST and existing wec_sessions path. | Manual timestamp IDs and a clicked button do not prove silent recognition, stable device identity or session deduplication. Reuse current Recognize session logic instead. |
| Current Tenants | Isolated recognize-tenants-bindings checkout: src/lib/rs-tenant-barn.js; src/assets/rs-inputs/persistence.js | Recognition person maps to Tenants person; membership check. Persistence keyed by resource, tenant, actor; confirmed data refresh does not erase dirty/pending drafts. | Do not replace these tested protections or imply preference tests prove every Barn/Rider/Horse screen. |

## Intended user experience
Historical implemented path: Recognize person -> resolve permitted tenant -> display scoped cached data when available -> refresh clean data -> inline edit/add -> immediate local feedback -> pending operation -> existing Astro intake -> existing Airtable automation -> confirm saved target -> merge response without losing focus, scroll or newer edits.

OWNER CORRECTION, October14 evening: the automation boundary above is not the supplied WEC/HPS proven save method and must not be treated as mandatory. Reuse the existing direct Astro-to-Airtable save pattern rather than extend an unnecessary automation design. WEC action.js invokes actionReport and direct PATCH/create helpers; HPS horses.js updates directly. Compare the exact Tenants identity/permission/table/consent boundaries before the smallest authorized adaptation; preserve immediate feedback and retained retry state. This correction supersedes the table's earlier statement that intake/automation remains authoritative. It is not permission to disable current automations, repair/replay failed records or change unrelated source.
Cold first visit requires the network. Cache keys isolate person and tenant; account change cannot replay another person's edits. Pending is visibly distinct from Saved. Consent requires explicit intent. Server checks remain authoritative.

## Current official sources and exact applicability
- https://docs.astro.build/en/guides/client-side-scripts/ : interactive events and dynamic updates can use browser scripts without a UI framework. Supports retaining the existing approach, not a claim that our implementation is fast.
- https://docs.astro.build/en/concepts/islands/ : selectively hydrate interactive components if needed; does not require a site-wide React migration.
- https://www.w3.org/WAI/WCAG22/Understanding/target-size-minimum.html : WCAG AA target-size minimum is 24 by24 CSS pixels with specified exceptions. Recommend larger 44-48px primary controls for this phone-first use, as a design target, not the AA rule.
- https://www.w3.org/WAI/WCAG21/Understanding/reflow : test reflow at320 CSS pixels and zoom; no horizontal scrolling for ordinary forms.
- https://web.dev/articles/optimize-inp : responsiveness target INP <=200ms at75th percentile. Actual devices/field data are needed; a framework name or local build cannot prove this.

## Verification before expansion
One authorized real tenant/user, one existing record inline edit/save/reload, one add/save/reload. Capture the actual phone and desktop container flows, measure input responsiveness, check keyboard-visible controls and focus/scroll retention. Then verify pending edits survive reload, reconnect and lost response, no duplicate application, no dirty overwrite, no cross-person/tenant data exposure. Use the supplied working reference behavior as comparison at the same viewport. Expand only the demonstrated pattern; no new authentication/storage/framework infrastructure.

Skills: Tenants and shared-data-behavior-contract for business and persistence; Recognize for identity; applicable Webflow native skills for actual native changes; frontend testing and independent code review for implementation. Product-design audit was inspected for the later screenshot-based visual verification; it was not executed as a live audit today. Read equestrian-caption-app/SHELL_CONTRACT.md before any applicable shell change; its app-specific persistence is not a global cookie policy.
