# Independent cleanupV2 launcher revision review

Reviewer /root/schedule_stage_review. COMPLETE; candidate PASS for inspected/offline scope. No unresolved release finding in the final frozen bytes. Deployment and genuine Records alert proof remain UNVERIFIED. Production, existing failed stop and business records were not changed by this reviewer.

## Approval and exact delta

The owner separately approved the cleanup-only launcher change after the prior review. Five changed files: run.ps1, cleanup.js, cleanup-airtable.js, cleanup-settings.js and cleanup-local-logs.js. Compared frozen baseline with candidate; actual hashes in manifest.json. Candidate fixture copies schema.json, logging-schema.json and cleanup-fields.js are dependencies, not deployment changes. No package upgrades, new schedule, collection/link invocation, collection gate, changed cadence, new field, record backup or logger edit. Existing failed-run stop remains. The earlier 360-second idea is absent.

run.ps1's existing not-due exit-code-3 branch now invokes only helper --service with the correct workflow switch, then exits. Existing due path is unchanged and still services after delivery through entry.js. Existing same-workflow IgnoreNew scheduling is relied on for exclusion of another delivery reader; this review used the prior task-policy evidence, not a new business run. Cleanup creates its own per-workflow request state; helper registers its PID, preserves pending families, and publishes terminal counts/error. Started/Success/Error for the same request are not reprocessed. This does not start collection to obtain a receipt.

## Findings reconciled

Prior indefinite wait caused by linker being not due is addressed by the explicitly approved existing-launcher service branch. Registered process ending without a terminal record returns Error with unknown counts; no arbitrary elapsed-time cutoff was introduced. cleanupV2 publication failures are captured while unaffected table and local actions proceed, with overall Error retained. The old table-comparison failure remains unchanged, correctly not claimed repaired.

During this revision review, inspection found completed deletion counts were reset to zero on receipt publication failure. Writer corrected catch fallback to existing out.counts before final freeze. Reviewer independently verified an actually deleted delivered family retains runs=1 and acks=1 in the fallback error record. Initial discovery was source inspection, not a claim that an earlier failing counterexample had been executed. Writer also added same-request terminal/Started guard; reviewer independently proved a later service call does not retry those outcomes.

## Independently executed evidence

Final candidate test-v2.js 4/4, test-local-preservation.js 6/6, test-local-alert.js 1/1. Reviewer edge-checks.js 5/5:
- actual candidate run.ps1 executed under PowerShell in a disposable fixture for both collection and linker switches; fixture entry rejects any business invocation and returns not-due; both cleanup receipts succeed;
- actual exited child PID produces Error/unknown counts;
- primary receipt publication failure after nonzero deletion preserves counts and same-request receipt is not retried;
- a dead Started service is not retried by a later helper;
- injected dual terminal-write failure leaves Started, and registered process death returns Error with unknown counts.

commands.json contains exact command, cwd, actual Node version, UTC start/end, exit and streams. Earlier executions retained; final guard changes justified targeted repeat. Fixture operations only; no manual collector, endpoint, production task or live deletion. The dual-failure check injects a dead-process result rather than killing production; the separate exited-child check uses a real disposable child.

## Integration and truthful limits

Read actual entry delivery/cadence branches, attempt-log payloads, cleanup-run alert consumer, hidden launcher and helper dependencies. Local ack eligibility, pending families, heartbeat sources and deletion order remain intact. Both local logging tables must be selected because cleanup removes a run and its delivered error family together. The waiter observes cleanup-owned service outcome, never a guessed idle PID. Whole-storage loss, failure before service registration, disabled scheduled tasks or a still-running process can leave no terminal evidence; this code does not promise finite detection of those states and no artificial timeout was added. They are operational proof limits, not a reason to invent further infrastructure here.

Inspected saved live-v2-schema.json: exact native table ID tblIhyIiYgvHHeChy, published field names and field types match. V2 PATCH uses typecast=true for necessary Running/Skipped/Error select values; no field addition. Parent reports current choices Success/blank; the saved schema snapshot omits choice details, so live acceptance of those values remains part of natural-alert verification. Old cleanup and cleanup-logs reads/writes removed; numeric deletion counts remain Airtable-only, local counts explicitly appended to purpose. Failed publication/error remains in existing local outcome files. No candidate deployment or stop release is authorized by this review result alone.
