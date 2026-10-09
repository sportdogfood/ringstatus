# Shared data behavior — locked plan

Status: owner-approved plan; implementation and cross-application verification pending.
Locked October 9, 2026, by owner instruction revision311: "lock this" following the practical device-cache and persistent optimistic UI plan (revision310). Changes to this behavior require owner direction. This record does not authorize deployment or declare existing applications compliant.

## Architecture and reuse

Keep native Webflow presentation, existing Astro endpoints and Airtable saved records. Show-engine endpoints supply current schedule/show data. Publish layout and styling independently of changing data. Reuse proven existing implementations, request identifiers and revision checks; verify their suitability per endpoint rather than inventing replacements. Add one shared browser persistence layer, not separate solutions per page. No new backend or redesign is part of this plan.

## Required behavior

1. Display the last device-cached data immediately, identify its last successful update, and refresh through the existing endpoint in the background. Never present an old show/day as current.
2. Reflect edits immediately and persist them on-device as dirty/pending. Reload must restore pending edits. Show Saved, Pending or Offline accurately; local persistence alone is not server save confirmation.
3. Send pending changes after a short editing pause, with increasing retry delays when connectivity fails. Retain the same request identifier when retrying the same operation; use revision checks to prevent duplicate saves and silent overwrites. Numeric save, refresh and retry intervals remain to be selected, not implicitly approved here.
4. Refresh unchanged fields without overwriting dirty fields. Preserve conflicting edits for review. Clear pending state only for the edit version actually confirmed by the server, preserving newer edits made while a save was in flight. Failed saves retain the user's changes and remain visibly unresolved.
5. Separate retention: cache relevant show-week/day data by show and date; load the correct day and replace obsolete show caches as context changes. Keep profile records in Airtable, with longer-lived device copies refreshed in the background. Never discard unsaved edits merely because the show changes. Isolate each person's cached data and pending edits; do not display or replay another person's state.
6. Keep Recognize separate from data caching, retaining its cookie as long as permitted with the previously agreed 365-day target. Do not shorten it to match show-week cache retention. Browser eviction and expiry remain possible; device persistence is not guaranteed permanent storage.
7. During limited service, continue displaying available cached data and accepting locally persisted pending edits. Resume synchronization when service returns while the application is available to run it; no guaranteed closed-browser background execution is claimed.

## Minimal storage and offline boundary

Use IndexedDB for structured cached records and pending edits. Reopening the entire page with no service additionally requires caching its HTML, scripts and styles using a service worker. Verify that the actual Webflow hosting/origin supports the required scope before promising offline reopening. A first visit with no cached page/data cannot be served from device cache.

The app shell's existing seven-day localStorage rule is app-specific and is not silently replaced by this plan. Safari does not universally clear all cookies and storage every seven days: WebKit documents a cap on script-writable storage after a period without site interaction. No browser retention guarantee is assumed.

## First proof before reuse throughout

Use one existing form first, without redesigning it:

- Edit while offline: updated values appear and persist as Pending.
- Reload: recover those pending edits from the same person's device storage; test page reopening separately if service-worker support is established.
- Reconnect: verify one correct save and matching Airtable readback; a lost response/retry must not duplicate the operation.
- Make a conflicting edit on another device: preserve the local pending edit and surface the conflict rather than silently overwrite either version.
- Refresh during editing and during an in-flight save: neither stale responses nor acknowledgments may erase newer pending edits.
- Change person or show/day: retain isolation and correct context, with no cross-person replay or loss of pending edits.

Only extend the proven behavior to other forms after this proof. Schedule, profile, opt-in and other endpoints still require their own applicable verification; this plan does not certify them in advance. Explicit consent actions remain explicit; a local draft must not be mistaken for confirmed consent or delivery activation.

## Existing references reviewed

- docs/wec_remaining_work_contract_2026-06-04.md
- docs/wec_p1_session_navigation_contract.md
- docs/wec_packing_comprehensive_overview_2026-06-06.md
- docs/wec_packing_current_project_overview_2026-05-31.md
- docs/wec_packing_live_app_project_contract.md
- equestrian-caption-app/SHELL_CONTRACT.md
- docs/horseshowing/wec-comments-observations/EXISTING-SYSTEM-REGISTER.md

These are historical implementation/contract references, not fresh proof of every current application. For this newly approved shared behavior, preserve dirty edits rather than adopting older affected-field rollback language as permission to discard them.

Browser references reviewed October 9, 2026:

- https://developer.mozilla.org/en-US/docs/Web/Progressive_web_apps/Guides/Offline_and_background_operation
- https://webkit.org/tracking-prevention/

Documentation-only lock: no application code, Webflow page, Airtable schema, automation, cookie policy or deployment changed by this record.
