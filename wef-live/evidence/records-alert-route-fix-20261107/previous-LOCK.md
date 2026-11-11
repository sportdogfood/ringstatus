# Current alert delivery — not verified

**Version 2026.11.07.7 · November7,2026 8:00p.m. Eastern.** Owner-approved notification wiring deployed: `entry.js` plus new `records-alerts.js`; all25otherJS, four direct request headers/URLs, decoded pause, collection/link/cleanup logic and task unchanged. Five offline checks and author review passed (not independent).

**Normal delivery FAILED:** the existing local Codex resume route exited1 without acknowledgement during the8:00p.m. normal invocation. Connection-check alert `65a538cc-d259-4571-aa21-2079db3205af`; target Records chat `01a169bc-e58a-7de0-94c6-5d3931fcf745`. Destination readback showed no new test message/acknowledgement. Task exit0 is not delivery success. Collection completed independently; quiet policy unchanged. No manual sender/collector or fabricated failure.

Failure notifications run after the normal invocation finishes, using its existing reports; they do not interrupt collection. Status/parser failures are included despite old ok:true/Skipped report labels; stage/logging, non-warning collection issues and cleanup errors included. Routine date warnings and linker no-match checks stay log-only. Individual delivery receipts in runtime/records-alert-*.json distinguish Started/Delivered/Failed. Legacy in-stage alert flags are not rewritten; these new end-of-invocation delivery receipts are separate. The first normal invocation makes one explicit connection check; it is never labelled a real collection failure and is not repeated automatically.

**Stop:** verification halted after the failed normal delivery. No transport retry/repair/new route or manual chat message performed. Future genuine failures remain wired to this unverified route; do not describe immediate or successful coordinator delivery as working. Owner decision required before correcting the failed route. Existing known trouble unchanged. [Evidence and acceptance](evidence/records-alerts-20261107/acceptance.json); [review](evidence/records-alerts-20261107/review.md); [installed hashes](evidence/records-alerts-20261107/deployed.json).

# Current release — four direct Wellington feeds

**Version 2026.11.07.6 · Deployed November 7, 2026 at 7:13 p.m. Eastern.** Owner-approved narrow request correction. Status verified by one normal scheduled run; groups/rings/class-details scheduled verification remains pending.

The four active public feeds now use direct Wellington URLs and exactly these explicit headers: `Accept: application/json`, `Cache-Control: no-cache`, the approved iPhone Safari User-Agent, and `Sec-Fetch-Site: cross-site`. Status uses `sglapi.wellingtoninternational.com/homepage/getLiveClassStatus`; groups, rings and live-class-details use `sgl.wellingtoninternational.com` at their existing paths/query parameters. This supersedes older all-four-proxy descriptions below. No cookie/token or extra browser headers are supplied by Live. Node may supply normal transport defaults.

Decoded `/classes/{id}` remains proxy-defined and **paused**. Only `activity.js` and `source.js` changed; the other24JS, shared source reader, registered task, cadence, status decisions/parsing, stage4–6 collection/save logic, empty-only linker, relationships and delete-only cleanup remain unchanged. No manual runs, record repairs or new observers.

Nine focused offline checks passed; author review passed (not independent). Exact baseline/candidate/diff, review receipt and installed26JS hashes are in [direct-four-headers evidence](evidence/direct-four-headers-20261107/). Normal scheduled acceptance remains separate from Decoder diagnostic proof. Existing known trouble/alert gaps remain unchanged.

**Normal-run proof:** November7 **7:15 p.m. Eastern**, status run `d602e778-0b69-45fb-b867-e41990d67d2d` returned **true**, with zero stage/logging errors. Existing date-mismatch warning: Shows priority0 November8, today November7. Existing rule kept detailed collection idle; next status due8:15p.m. Three detail feeds remain **unverified through scheduled collection** until a naturally eligible day0 window. [Acceptance](evidence/direct-four-headers-20261107/acceptance.json).

# WEF Live — locked protected baseline

**Version 2026.11.07.5 · Locked 2026-11-07T10:26:13-05:00 · America/New_York.**

Current linker refs/fields are w-shows,w-days and unchangedData schedules. Old filledshow/show-day fields retained. Existingclass.groups↔group.live-classes reciprocal pair verified and unchanged. All stages4–6/source/cadence/task/cleanup remain unchanged;decoded requests paused. Empty-only linking remains;missingmatches stayempty/logrecommendno retryuntilsourcechanges. No retrycache/newgates added.

Owner approval is required for further business code/schema/source/cadence/cleanup/deployment/resume changes. Normal scheduled operations and authorizedread-onlychecks continue. No ACL/task/process permission change. No manual collector/provider runs or record repairs.

[Protected26JS/task/docs baseline](evidence/w-shares-20261107/locked-baseline.json) · [deployed manifest](evidence/w-shares-20261107/deployed.json) · [README](README.md).

17offlinechecks/authorreviewPASS(notindependent). Normal10:15/10:21passes:1,936neww-fields on968records,then0repeatwrites;5,197populatedforwardfieldsretained;423tripbusinessrowsfrozen;decodedcalls0. NativeSourcekey/readback,class-groupbothdirections andparentlog/heartbeat checks are in [acceptance](evidence/w-shares-20261107/acceptance.json). SevenexistingDropped-onlyDataschedulelinks stayempty/logged. No fatalstage/logfailure orrevert.

## Known trouble and limits retained

- Seven unmatched Data fields: retainedgroups200028530/200028533,class200031158 andtwo trips/twoentries matchonlyDroppedData schedules. No guessing;individual logrecommendation,notneweligibilitycontrol.
- Immediate Records chat transport is not configured;local/Airtable logging is notchatdelivery.
- Cleanup conditionalP2s: unexecutedplans maystayRunning;interruptedstop mayleave stale latest/Airtable summary.
- Widget{} can silentlyyieldnoringrows;original3collectionheartbeatshow/date cells remainblank(linker includes them).
- Pauseddecoded business data/results/IDs notrefreshed;newUI-onlyentries maylackentityIDs,neverinvented.
- Checkinterval3minutes doesnotpromise3minutecompletion;IgnoreNew/PT10Munchanged. Shows clockcanlag;existingfallbackremains. Emptyread/PATCHisnotatomicCASagainstconcurrentedits;native reciprocalappends expected.

Decodedtrips remains the workflowdesign discussion. Cross-basepublication/formerstage8 andold24hretentionheld;approveddelete-onlycleanup active. Existingrecords/links/evidence/catalogs preserved;nonewcleanuparchives or schedules. [Reviewed3filecode-onlyrevert](evidence/w-shares-20261107/revert.ps1) isowner-authorizedonfailedrollout,retainsfields/links/data,andrefusesinterveningedits;notneeded. [Priorlock](evidence/w-shares-20261107/previous-LOCK.md).
