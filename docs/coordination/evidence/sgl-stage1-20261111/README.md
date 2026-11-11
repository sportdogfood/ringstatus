# SGL endpoint inventory — Stage 1

Updated 2026-11-11. Status: saved and read back in Airtable.

## Approved task

Inventory the complete non-video endpoint set exposed by the loaded SGL application and the previously inspected legacy page scripts. Include method, parameters, scope, priority, primary/fallback assessment and existing implementation. Preserve prior browser evidence. No endpoint tests, production/credential changes or collectors in this stage.

## Result and evidence

- Airtable: [WEF Shows / sgl-opportunities](https://airtable.com/appCaSmL819KK7UFJ/tblKkbHxccs2jKCOS).
- 229 distinct routes: 38 existing records enriched and 191 added. One record per route; HTTP methods and query variants share a record.
- 12 critical data/recovery routes, 56 supporting routes, 161 account/action routes. These are evaluation priorities, not server access findings.
- 14 routes have an inspected existing implementation. `already-built` names the proxy, runner or agent source; `recommended-role` and `role-basis` distinguish current use from candidate use. Code support does not establish current deployment or scheduled success.
- All 229 records read back. All 4,590 submitted field values matched; no duplicate route. See `airtable-verification.json` for record IDs and saved-check evidence.
- No new runtime public-access claim. Existing browser results remain explicitly session-bound. A source-only record remains pending evaluation; a candidate is not an installed fallback.

## Source and coverage

`main.loaded.js` is the actual browser-loaded bundle. Its normalized length is 5,472,083 characters. The saved CRLF copy is 5,472,085 characters, SHA-256 `e301d60132ec9ac306224b464b6e3116834c686675ae46e9a2b150c56d9bb382`.

`inventory.cjs` parses the saved bundle without executing it. It follows URL getters, aliases, concatenation, conditional alternatives, rendered frame URLs, request methods and caller objects. The application scan starts at offset 3,050,000; the first SGL host/config references are later than this boundary. `requests.json` preserves 533 resolved source-request variants with exact positions and expressions. `catalog.cjs` consolidates routes, preserves the original 38 rows and attaches inspected implementation facts. Dynamic parameter objects retain their expressions; unknown runtime keys and requiredness are not invented.

The four unresolved call expressions were classified: two excluded video-asset downloads, a video subscription wrapper expanded at its literal callers, and the existing retry of an original request configuration. Other call-like matches are query/map operations, external navigation and returned PDF/asset navigation rather than additional fixed SGL data routes. The earlier video exclusion removes 53 route templates. Pusher events and parent-frame messages remain trigger/recovery evidence on their related endpoints, not fake HTTP endpoint rows.

This is complete for the inspected loaded source and retained legacy evidence within that exclusion. It is not a claim to enumerate unpublished server routes, inspect a blocked source map, or resolve runtime-generated file URLs.

## Existing implementation evidence inspected

- `workers/wef-schedules-decoded/shared-worker.mjs`, `public-live.mjs`, `fallback.mjs`, `worker.js`, `show-time-health.mjs`.
- Installed `wef-data/runtime/wef-decoder-runner/runner-relay.mjs` and `worker.js`.
- `wef-data/source-reader.js`, `wef-live/source.js`, `wef-live/lanes.js`, `lib/wef-source-read.js`, `wef-shows/browser-discovery.js`.

These files were read only. Existing production/cadence proof was not rerun. The open class-detail failure and data-response `_t` recovery difference remain visible in the catalog. No class-stage, show-time linking, proxy or fallback repair is authorized by this inventory.

## Checks and current task record

`verify-inventory.cjs` passed against the saved source/catalog: source hash and source slices, unique route keys, preservation of 38 existing identities, required metadata, method/body separation, video exclusion and absence of credential literals. See `offline-verification.json` and `review.md`.

The first oversized Airtable request was rejected with HTTP 413 and made no record changes; the unchanged 38-row table was confirmed. Bounded record batches then saved all 229 rows, followed by full readback. This was inventory administration, not a runner workflow or collection.

Stage 1 is complete. Next stage is endpoint evaluation in priority order under separately agreed test scope. No new recurring calls or automation were created.

## Owner follow-up: focus labels and open tasks

All 229 records now have `category`, `focus` and `source-pattern`. Focus is 11 Critical now, 39 Supporting and 179 Not now. Owner-deferred register/registration, billing, `/my*`, milestone, digital-signature, edit/save, supplies, `/add-entry` and class-add-drop routes are marked `not-priority` and priority 3. Ninety-two routes match those explicit rules; other account/action routes retain their source-based deferral. Existing owner checkbox selections were preserved. Source findings and access results were not deleted or changed.

Source patterns distinguish API (166), iphonev2 (39), iphone.php/esp/ (4), other iphone.php (18), /webservice (1), and SGL application (1). These are source-family labels, not access findings.

[opportunity-tasks](https://airtable.com/appCaSmL819KK7UFJ/tblq8gMKEWjAYL1SS) has 18 saved records: 2 completed inventory/label tasks and 16 open items. It records the anonymous-authentication/bootstrap/refresh discovery goal alongside class recovery, valid dual health, Fast stages/pacing/alerts, coverage comparisons and existing known operational issues. Each item has its current finding, next action, completion check, blocker, evidence and native endpoint links. A task record is not execution or edit authorization.

All labels and task fields were read back. Empty text omitted by Airtable was normalized to blank in the comparison; there were no remaining differences. See `focus-and-tasks-verification.json`. No source calls, collectors, production changes, credential changes or business-record repairs were performed for this follow-up. Further source investigation must address a specific missing piece of schedule/class/live data or fallback need; unrelated routes remain Not now.
