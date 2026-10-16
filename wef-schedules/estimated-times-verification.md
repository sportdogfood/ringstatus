# Estimated start-time capture — local only, 15 October 2026

Owner approval: add estimated times for all show days using existing code; preserve manual overrides and previous values on failure; local testing only. No deployment, task changes or business runs.

## Baseline and delta
Reused existing sgl_browser_enrichment.js browser-loading method and copied collector-core.js parseScheduleHtmlTimeOverlay. Existing scraper substring matching was not reused: offline reproductions matched 101 against 1101, accepted wrong-ring first matches, and parsed compact 1:30PM incorrectly. The structured parser already handles compact AM/PM and exposes group, ring and class identity.

Added schedule-times.js in the isolated worktree. Collects rendered HTML per nonempty show day, validates final URL scope, matches unique group within ring or exact class within ring, and rejects ambiguity. Runs before change comparison, snapshot and readable-field projection. Missing/failed/ambiguous capture retains prior times and logs estimated_times_incomplete; new rows retain available source times or remain blank. Existing manual flags (both spellings) and manual HTML override provenance take precedence. Existing empty-day warnings remain; empty days do not launch a browser.

Changed store.js, offline fixture and staging tests; added package.json declaring the existing browser dependency Playwright 1.62.1. No deployed package, shared source, Airtable schema, records or scheduled-task settings changed.

## Actual local verification
node --test --test-reporter=tap tests/schedule-times.test.js tests/schedules_staging.test.js tests/schedules_standalone.test.js tests/current-store.test.js tests/isolation.test.js
Result: 42 tests, 41 passed, 1 failed.
Time-specific tests passed: exact identity, substring collision, wrong ring, ambiguous matches, compact PM, capture failure/empty HTML, previous times, both manual flag spellings, manual HTML precedence, empty days, URL scope and browser cleanup, all three show dates, unchanged repetition and changed-time history.
Failure: isolation.test.js private dependency graph currently rejects every non-built-in module; new declared playwright dependency is rejected. Do not call this package verified or ready to deploy.

## Exact next action
Separate APPROVED TO EDIT required under owner post-failure rule. Update dependency isolation validation to verify the explicitly declared private browser dependency rather than allowing arbitrary external imports; ensure private dependency installation/lockfile before any deployment. Re-run local verification. Browser tests used injected mocks, never launched Chrome or contacted SGL; real scheduled capture remains unverified until separately authorized deployment and normal scheduled observation.


## Superseding verification — approved dependency correction
Owner separately approved correcting dependency validation and finishing local testing; no deployment or manual runs.
Installed pinned Playwright 1.62.1 within this isolated package using npm install --ignore-scripts --no-audit --no-fund. package-lock.json records the resolved dependency and integrity. No browser download, browser launch or source capture performed.
Isolation validation now permits only Node built-ins and the explicitly declared pinned Playwright dependency. It checks actual resolution inside this package node_modules, the installed version, lockfile version/integrity, and rejection of undeclared dependencies. All relative code imports remain constrained to this private package.

Full command: node --test --test-reporter=tap tests/*.test.js
Result: 49 tests passed, 0 failed. Evidence: estimated-times-test-results.txt.
Includes all-show-day capture, repeated unchanged collection, changed estimated times, unique structured matching, ring/substring collisions, ambiguous matches, compact PM parsing, manual flags and manual HTML precedence, failed/empty capture preserving previous times, private dependency isolation, detached package tests, history/current-row storage and existing collection regressions.

Local verification is complete. Remaining release action: separately authorize deploying this source package including its pinned private dependency and observing normal scheduled runs for real SGL time capture. Existing deployed package/task and legacy workflows were not modified. Live estimated-time capture has not been claimed or tested manually.


## Estimated-time release observed 15 October 2026, 14:26 Eastern
Approved tested time capture deployed at18:16:15Z; exact source and dependency hashes verified. Normal task starts14:20:01 and14:25:01 Eastern completed exit0. No manual business runs, task definition edits, shared workflow changes or test records.
Airtable connector readback after each run:473 records with identical IDs and times. Timed rows:Oct16 55/138;Oct17 41/196;Oct18 45/139. Oct15 no scheduled classes. All four day snapshots reused on second run; no duplicate growth. Unmatched332 rows include actual classes: source time availability versus matcher coverage is not yet established. Do not call complete time coverage verified. No flagged manual override rows exist in current snapshots: preservation verified by local tests only, not a live override case. No fabricated test records added.
Protected16 file hashes unchanged. All scheduled task definitions checked against deployment checkpoint. Evidence:wef-schedules/runtime/estimated-times-deployment.json and estimated-times-task-check.json.
Next authorized work would be read-only diagnosis of the332 unmatched rows using existing captured evidence; no live manual scrape or correction has been performed. Further code correction requires separate approval after failed acceptance. Existing successful collector remains on its original five-minute schedule; warnings expose missing times.
