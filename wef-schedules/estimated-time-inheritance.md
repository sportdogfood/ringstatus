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


## Inheritance deployment â€” failed live acceptance,15October2026
Deployed hash-verified tested package at18:51:34Z. First normal14:55:01Eastern run exited0 but failed functional acceptance. Owner screenshot classes720/787/835 onOct16 ring3 remain blank instead of08:00. Classes744/715 remain08:27/08:39. Time counts unchanged55/138,41/196,45/139 forOct16/17/18. Oct15empty. No live manual override rows present; only local override proof exists. Protectedsourcehashes unchanged. No manual runs or task changes.
Stopped second-run verification after observed failed acceptance under owner instructions. Did not disable or modify the existing scheduled task; it remains enabled. No corrective edits performed. Do not present54 passing synthetic tests as live proof. Runtime does not retain rendered HTML, so exact omitted DOM structure is not yet established; do not invent cause or another correction. Next step requires actual rendered-source evidence against the supplied screenshot before a separately approved minimal correction. Evidence:wef-schedules/runtime/inheritance-deployment.json.
