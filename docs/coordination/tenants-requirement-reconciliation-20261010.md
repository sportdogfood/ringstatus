# Tenants: agreement versus actual structure

Update: structural task `recW9Cy6E6JO4eh9e` completed on 2026-10-10; [verified table/field readback](tenants-schema-completion-20261010.md). Three missing relationships now exist. The audit below records the original mismatch; runtime implementation/validation remains open.

Reviewed 10 October 2026 against live schema, rules, development and tasks. This is an implementation-gap audit, not runtime acceptance.

## Cause of the mismatch

The coordinator treated recording agreed behavior, copying legacy tables and adding generic relationship tables as delivering the requested business model. It did not demonstrate that each agreed operation had a usable place to store its state. Later documentation repeated the intended workflow while stale proposals contradicted completed changes. The missing step was requirement-by-requirement structural acceptance, not missing owner instructions.

No tool refusal or access failure prevented this review. Current connector reads succeeded. Unknown production bindings constrain live cutover, not correction of stale records or identification of missing relationships. There is no evidence that model choice or context length caused this failure.

## Requirement reconciliation

| Agreed requirement | Actual evidence | Required correction / owning task |
|---|---|---|
| Tenant is barn; shared horses/riders/trainers/rings | Tables and legacy links exist, but copied masters retain extensive engine/profile fields | rectHlHXOH2YvPAMk: review consumers and minimum retained fields; do not delete source-backed fields blindly |
| Consolidate people without duplicate identities | people, retained source links and memberships exist; uncertain identities documented | Preserve completed consolidation; reconcile remaining identities only, not reimport |
| Main trainer roster versus added unaffiliated trainer horses only | Rules exist; tenant trainer links have no clear association-specific primary/additional policy | recW9Cy6E6JO4eh9e: map exact tenant-trainer relationship and exclude added trainer riders |
| Owner can disable horses/riders within one tenant | Shared masters have global inactive/ignore flags; no clear tenant-specific exclusion state | Same task: define tenant-scoped roster state; never implement using global inactive flags |
| Personal favorites separate from tenant roster | favorites links membership and targets with status | Preserve; verify option meanings and exact tenant isolation before runtime |
| Staff may also ride; grooms see assigned items only | people.Rider and memberships.Roles exist; people.Associated horses is not tenant-scoped assignment history | Same task: explicit tenant/person/horse assignment representation required; no name or global-horse-link access inference |
| Membership survives week/day absence | Recorded rule; memberships exist | Verify consumers do not derive permission from current entries |
| Owner-managed groom options and personal opt-ins | optins and optin-options exist; linking/permissions not verified | Same task: map membership/manager/scope; preserve revoked consent |
| Venue places versus tenant show-week locations | locations and tenant-locations exist | Correct obsolete proposal saying not created; validate coordinates/IDs separately |
| Familiar horses first, broader show search second | Discussion accepted; no clear implemented suggestion/history contract | Record accepted rule; mapping belongs in role/search task, not a speculative new engine |
| Missing horse/rider red-line and full-list audit-lane | Mentioned in overview but not specifically mapped to input-submissions | recjNCFtq6dJB7sk3: preserve submitted names and full horse/class rows; admin review, no guessed identity or count-only comparison |
| Input log then confirmed targets; persistent optimistic UI | Prepared input-submissions; processor and device queue unverified | Existing submission and persistence tasks; table creation is not delivery |
| Reopen and understand actual state | Empty tenants-index; stale development proposals; legacy operational descriptions still say Entities | Correct records and describe incomplete structure explicitly; never call it a validated template |

## Execution order

Complete structural representation using existing relationships where suitable, before assigning application implementation. Reuse source and inspect current consumers before any destructive field cleanup or live cutover. Bindings discovery can proceed independently; it is not an excuse to defer agreed structural requirements. No new business data, roles, permissions or infrastructure should be invented to fill blanks.

The owner has already decided the listed rules. Do not ask them to reconfirm those rules. Only genuinely unresolved implementation or identity decisions remain open. The application is not accepted, and this audit does not change production behavior.
