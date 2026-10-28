# EST START inheritance — 15 October 2026

Owner-approved scope: existing parser inheritance within ring/day; today and tomorrow first; all other show days retained; zero-entry classes included. Local tests only. No deployment, scheduler edits, Airtable writes or manual business runs.

## Changes
collector-core.js: the existing structured HTML parser now remembers the last printed EST START separately for each explicit ring. Blank group times inherit it until the next printed time. Group class lists already map the same estimate to their child classes. Memory is fresh for each parser call/day. Blank first times and unknown ring identities are not guessed. No entry-count filter was introduced.
collect.js and store.js: the existing all-day loop now orders New York today, tomorrow, remaining future dates, then earlier show dates. Every show date remains included. This changes collection order only; it does not add a heartbeat/live-data dependency or alter manual focus settings.
Existing manual override and manual HTML precedence, ambiguous-match handling, prior-time preservation after capture failure, change comparison and history remain in place.

## Evidence
node --test --test-reporter=tap tests/*.test.js: 54 passed, zero failed.
Detailed output: estimated-time-inheritance-tests.txt.
Hashes: estimated-time-inheritance-verification.json.
New checks cover classes787/835/720 sharing08:00, blank subsequent groups inheriting08:00, next printed08:27 replacing it, zero entries, ring/day/capture isolation, override precedence, failed capture retaining inherited time, all-day loop ordering and New York date boundaries.

## Release state
Not deployed. The currently deployed collector still uses the preceding parser; its332 unmatched rows have not been re-evaluated with this change. Future deployment and two normal scheduled observations require owner approval. Local synthetic checks do not establish live coverage.
