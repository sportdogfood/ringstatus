# Temporary completion sequence — recommendation and retained obligations

Date: October 9, 2026. Source: assistant recommendation at revision316; owner request at revision317 to document it.

Owner statement: "document you suggests even though i am deeply disapointed and may eventially regret allowing this".

This is a documented sequencing recommendation made because the work has exceeded the owner's allotted time. The owner expressed deep disappointment and concern about later regret. Do not represent this as satisfaction with delivery, acceptance of incomplete behavior, abandonment of the intended architecture, or blanket implementation approval.

## Recommendation

1. Temporarily retain the current Recognize base app9kOZdIaGyKk5uG, including the profile and opt-in records currently stored there. Avoid moving live dependencies while finishing the essential user flow.
2. Finish and verify recognition -> load existing profile -> inline edit/add -> save -> reload, preserving the existing native Webflow presentation and Astro data mapping. Retain the verified SMS opt-in save/reload/opt-out behavior.
3. Correct only failures blocking this flow. Reuse the supplied working examples after checking their relevance and current compatibility. Preserve working code; no redesign, unrelated refactoring, invented replacement methods or layered workarounds.
4. Defer implementation of the separate Profiles base and shared device/offline layer until this bounded flow is proven. Deferral does not remove these requirements or mark them complete.
5. Handle Schedule against show-engine endpoints as a separate retained task. Schedule and CSS audit remain open; this recommendation does not close them.

## Target ownership remains

| Base | Intended responsibility |
| --- | --- |
| recognize | IAMs, devices, sessions; expose authoritative profile opt-ins |
| profiles | Barns, people, horses, roles, relationships, onboarding, location references and authoritative opt-ins |
| wef-engine | Show-specific participants, rings, groups, classes, entries, trips, results and states |
| sms-engine | Alerts, outbound messages, delivery status, responses and notifications-feed |

This table describes intended ownership, not a verified inventory of existing bases/tables. Profiles is not yet established per the owner's current report. People may hold multiple roles; show participants must be matched to profile identities without overwriting master profiles. Opt-ins have one authoritative home; their current storage remains in Recognize until a verified migration. Notifications-feed may include notifications that do not send SMS.

## Costs and limits of the temporary choice

- Recognize continues mixing identity and profile responsibilities; future base separation remains real migration work.
- Later migration must preserve identifiers, relationships, consent history and working endpoint/form behavior. Do not create a second competing source of truth in the meantime.
- Without the shared persistence layer, device fallback, durable pending edits and offline reopening cannot be promised across the applications. Poor-service usability remains unfinished.
- Deferring work saves immediate migration/implementation effort; it does not establish a delivery-time estimate or guarantee a cheaper migration later.

## Verification and completion boundary

The bounded user flow requires actual browser interaction plus matching saved-record readback and reload behavior, including add and inline edit. Prior component tests or isolated successful saves do not prove the entire flow. Report individual verified and unverified steps honestly. Completing this milestone does not mean the full Recognize scope, base architecture, offline contract, Schedule or CSS audit is complete.

Existing SMS proof: [SMS opt-in connection](sms-optin-connection-20261009.md), revision306. That proof covers preferences save/reload and opt-out only, not message delivery or universal data persistence.

The [locked shared data behavior contract](shared-data-behavior-contract.md) remains unchanged: device cache, persistent optimistic UI, dirty-state protection, controlled save/refresh cadence, retry/conflict handling and per-person/show isolation are still required. Revisit the deferred work after the bounded flow is proven; do not silently convert this temporary sequencing choice into permanent scope reduction.

Documentation only: no base migration, application edit, deployment, publication or change to the retained task's acceptance was performed by this record.
