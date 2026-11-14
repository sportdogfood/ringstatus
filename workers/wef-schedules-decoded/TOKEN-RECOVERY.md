# Decoder _t failures, recovery and acceptance
Date: November 12, 2026. Documentation snapshot; no implementation changes or collectors.

## What this establishes

Token acquisition and returned-token replay are separate operations. HTTP200 alone proves neither token issuance nor useful data. Schedule success also does not prove class coverage.

The table remains [decode-change-log](https://airtable.com/appCaSmL819KK7UFJ/tblhL9n1COVPvr851). It now includes individual historical failed acquisition attempts, with deterministic change-id `token-attempt|<original refresh-id>` and a link to each original record. Their state is Historical: importing evidence is not a new fix or a claim of recovery. The original token-refresh-log remains the attempt authority. No schema or automatic logging behavior changed.

## What failed and what changed

| Date | Saved failure or change | Evidence and limits |
| --- | --- | --- |
| November10 | Config returned200 without _t before fallback schedule requests. Generic502 obscured the stage. | Existing detailed-health change documented the missing token and runner status. Adding Sec-Fetch-Site same-site and cleaned data profiles passed the existing integrated preview. [Change](https://airtable.com/appCaSmL819KK7UFJ/tblhL9n1COVPvr851/rec8DBMnkxBsljsRq). The underlying SGL decision was not established. |
| November11 | A protected data response could return a replacement _t instead of data. | Decoder now accepts only a valid, unexpired anonymous token, saves it through the existing cache callback and replays the identical GET once. [Change](https://airtable.com/appCaSmL819KK7UFJ/tblhL9n1COVPvr851/rec5Nty7WEYUVkhwV). Preview class200031333 returned two trips. |
| November11, after that release | Scheduled class200031471 still failed: upstream200, Empty source payload, decode stage. | This was a failed class save, not proof of a failed token replay. The preview used cgid=-1; the scheduled request omitted it. That difference was not established as the cause. See evidence/returned-token-20261111/scheduled-verification.json. Do not label the original release fully verified. |
| November11–12, retained acquisition log | Fifteen failed config attempts: fourteenHTTP502 and oneHTTP200 missing _t. | All fifteen are listed below and mirrored into decode-change-log. OldHTTP502 rows preserve no deeper transport cause. Counts describe retained records, not every historical event. |
| November12, 1:33p.m. Eastern preparation | November14 schedule preparation reported Source request deferred. Recovery config returned200 missing _t at18:34:03.093Z. | [Original attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recLTEF8opWB45joF). Recovery stopped before requesting the schedule; zero classes ran. The initial runner cause was discarded and cannot be reconstructed from that record. |
| November12, 3:35p.m. Eastern | Token acquisition stopped on missing _t without testing the other existing proven profiles; failure forwarding also lost detail. | [Current change](https://airtable.com/appCaSmL819KK7UFJ/tblhL9n1COVPvr851/recnUrhbZGLzR5kJZ): onlyHTTP200 missing _t advances through existing token profiles; failures retain reason, stage, upstream status, safe headers and nested cause. |

## How the current recovery works

### Acquiring an anonymous token

1. Reuse the existing in-memory token when its expiry is more than24hours away and the configured cookie is unchanged. This threshold is existing behavior, not a new retry rule.
2. When needed, use the existing direct-TLS GET to /utilities/get-config with the existing cookie, Origin, Referer, SGL request origin and Sec-Fetch-Site same-site.
3. Try the original iPhone profile first. **OnlyHTTP200 with missing _t** permits the existing Windows profile, then Android, with one-second spacing. Stop at the first valid token.
4. Require parseable claims, anon=true and an unexpired numeric exp. This is claims validation; the decoder does not independently verify SGL's signing secret.
5. HTTP errors, malformedJSON, malformed/nonanonymous/expired tokens and transport failures stop that renewal. Do not treat theHTTP502 history as covered by the missing-_t profile fix.
6. Concurrent renewals share the existing in-flight promise. Each actual config attempt uses the existing renewal log. Logging failure emits a separate console error and does not replace a usable source result.

### A data response returning a new token

Validate the returned _t, update the existing fallback cache or installed runner callback, replace the Authorization/decryption token, wait the existing one second, then replay the **sameGET** once. No URL/date/class change. A second token response, invalid token or failed replay remains failure. This is different from calling get-config.

The installed runner has its own existing Windows defaults and token callback. Its exact runtime baseline was preserved; an unadapted repository copy must not replace it.

### Data fallback and failure reporting

The existing runner-first order, saved-lane preparation and separate recovery policy remain. The five existing data profiles and renewal sweep are distinct from the three token-acquisition profiles. A missing-token failure may prevent all data profiles from reaching SGL; that is not five failed schedule requests.

Fast's public getLiveClassData class lane does not use this protected _t workflow. Schedule requests still use the decoder. Fixing _t cannot by itself resolve a malformed or mismatched public class payload.

Read the saved reason, stage, upstream/token HTTP status, lane/profile and capture pointer before the generic502. For token-before-schedule, follow the matching renewal record. For fetch, distinguish transport failure from an upstream response. For decode, distinguish an unresolved token, empty payload and invalid payload. Tokens, cookies and Authorization values must never be saved in documentation or logs.

The existing hourly health workflow submits pending decoder outage/recovery warnings once, with warning-log as notification authority. Saved Fast failures are reported separately and deduplicated by their original error/attempt IDs. Chat submission is not owner-read acknowledgement. A healthy schedule or excluded class never establishes class recovery.

## What was verified

Worker92cc485b-d86c-4d36-bb7b-1f18a865d02d deployed November12 at20:35:31Z. Existing relay restarted once; credentials, URLs, pause, task definitions and collection rules stayed unchanged.

- All three token profiles currently issued valid anonymous tokens. The historical omission was **not** reproduced or proven to be solely a User-Agent issue.
- Remote preview deliberately removed the original-profile token, then obtained a real Windows token and a real November14 schedule with nine decoded rings.
- A separate preview failure fixture verified that the shared wrapper and actual Fast reader retain a redacted socket cause, stage and upstream status. It was not a real source outage.
- Independent Code Review passed before deployment:65Node checks and one scheduled-Python reader fixture.
- Normal scheduled passes subsequently completed collection, native saving, separate linking and log delivery for November12–15. No manual collector supplied this proof.
- The20:55scheduled health returned nine rings and class200031715 with58trips through the runner; passive Cloudflare lookup showed four connections.
- No natural post-release missing-_t incident was observed. Live scheduled success through the runner does not independently prove the fallback was exercised.

### Observed scheduled timings — not universal performance targets

| Source date | Class requests | Collect seconds | Save seconds | Acknowledged writes | Collection start to link complete |
| --- | ---: | ---: | ---: | ---: | ---: |
| November12 | 7 | 7.169 | 88.731 | 1,568 | 680.054s |
| November13 | 50 | 19.693 | 194.354 | 2,744 | 487.694s |
| November14 | 0 | 4.640 | 22.379 | 285 | 670.458s |
| November15 | 4 | 5.857 | 38.091 | 234 | 664.752s |

Writes include updates and related-table writes, not unique new business records. End-to-end time includes separate-stage waiting. Do not infer per-class invocation speed by dividing end-to-end time by class count. November14 is **not a valid class-collection speed benchmark**.

## November14 coverage anomaly

The saved20:42pass contains200schedule rows and167class occurrences; it recorded67rule exclusions and zero class requests. A read-only reconstruction using its saved graph/priorities and current Airtable rules reproduces67rule exclusions, leaving100candidate occurrences. All100are blocked by existing class-detail-failed marks.

Current Airtable has102failure-marked schedule rows for that date:100say Class payload missing or mismatched ID/rows; two say fetch failed. These are schedule-row counts, not102distinct class requests. Rules currently give priority999 to Schooling pony, No entries, Add back, Mulligan and Warmup, in addition to the explicit Flat/Model/break exclusions.

This explains zero requests without establishing healthy class coverage. The exact prior_schedule input was not archived at the pass time; the reconstructed failure-mark breakdown is explicitly a current-state explanation. Do not silently clear marks, retry classes or change rules. Public-class payload/mapping evidence needs separate review before deciding a fix.

## Normalized benchmarks and anomaly checks

These are review criteria, **not implemented gates, alerts or new timing limits**.

- Compare the same show, source date, endpoint, source lane, rule revision and first/repeat-pass type. Keep Day0/live handoff separate from future days.
- Reconcile counts in order: source class occurrences -> rule-excluded -> otherwise eligible -> failure-marked -> same-date Live-covered -> requested -> successful/failed responses. Failure exclusions and valid business exclusions must be separate. Unknown/missing breakdown means unverified coverage.
- Expected requests equal eligible occurrences after the existing failure and Live exclusions. Zero with positive remaining eligibility is an execution anomaly. Zero because every candidate is failure-marked is a coverage outage, even with no new errors.
- Measure collection seconds separately from saver processing, saver wait, linker processing/wait and log-delivery time. Compare successful invocation time per actual successful request only within equivalent lanes; separately show failures and preparation time.
- Compare class identities and payload row counts, not request totals alone. Missing expected class IDs, a sudden drop in rows, or repeated mismatched-ID errors require investigation.
- Save accounting must show new/changed writes separately from unchanged rows and helper-table writes where available. Existing total acknowledged writes alone is not that breakdown.
- Require matching .saved/.linked receipts and native run-log delivery for the exact collection attempt. Started, Deferred and Skipped do not count as collection completion. New source success does not clear old class failures.
- Use these saved passes as observations, not a fabricated fixed SLA. Nine rings is the observed show result, not a global requirement; legitimate zero rings remain valid. No numeric anomaly tolerance or automatic action is introduced by this document.

## Retained failed token-acquisition attempts

Snapshot contains all15records returned by the existing token-refresh-log Failed filter on November12. FullUTC times are original; profile was not recorded in these old attempts. Older November10 omissions survive in dated release evidence and change records, not in this retained15-row set. No token values are included.

| Original UTC time | HTTP | Actual saved error | Native record |
| --- | ---: | --- | --- |
| 2026-11-12T18:34:03.093Z | 200 | Config response is missing _t | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recLTEF8opWB45joF) |
| 2026-11-11T11:58:43.193Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recCK49aUGcrU2RyZ) |
| 2026-11-11T02:33:34.861Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recrYbr5F6Lc1kCXM) |
| 2026-11-11T02:33:33.454Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recPpZZBgY8jDFZyJ) |
| 2026-11-11T02:33:31.870Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recnCk8GiYGsRf1Fo) |
| 2026-11-11T02:23:07.733Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recWrNlt5sR5SEGFZ) |
| 2026-11-11T02:22:46.290Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recDybvOEKurTo3JA) |
| 2026-11-11T02:22:37.991Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recuTZ1I9LVu6FJ1v) |
| 2026-11-11T02:10:23.971Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recR1xfhiDsZYacXd) |
| 2026-11-11T01:58:10.172Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/reclo0Q9k1nbZpoT2) |
| 2026-11-11T01:57:33.653Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/rec97Qk66siphGkN1) |
| 2026-11-11T01:47:23.529Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recOX7OmxXz2EEc13) |
| 2026-11-11T01:47:16.382Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recnKB1KBkMrTqOoG) |
| 2026-11-11T01:47:14.781Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/rec7QYTrqg48SlUi7) |
| 2026-11-11T01:47:06.431Z | 502 | Config request returned an HTTP error | [Saved attempt](https://airtable.com/appCaSmL819KK7UFJ/tbl8pJcHIaRchfDTP/recQO7CFYmVHXtJ4M) |

## Evidence locations and remaining limits

- Current release packet: C:/Users/gombc/Documents/Codex/outputs/wef-token-acquisition-error-capture-20261112/.
- Preview: candidate-preview.json and error-capture-preview.json. Review: review/. Exact deployed artifact: deployment.json and candidate-hashes.json.
- Scheduled proof: scheduled-verification.json; original per-date manifests and .saved/.linked receipts are under wef-data/runtime/fast/dayN/collections/.
- Saved failure snapshot: token-failure-history-snapshot.json. November14 inputs and calculation: nov14-rule-input-snapshot.json, nov14-failed-schedule-snapshot.json and nov14-benchmark-analysis.json.
- Existing history: README.md, FALLBACK-RUNBOOK.md, evidence/returned-token-20261111/ and linked Airtable records.
- Token-refresh-log records acquisition attempts. It does **not** provide a complete per-request ledger of returned-token replay attempts. Retained captures/tests establish the documented cases; missing historical details cannot be recreated. No automatic replay-log extension was implemented.
- Updated cleanup's natural execution remains separately unverified; token/schedule success does not establish cleanup completion.


## Approved November14 unblock — November12, 4:31p.m. Eastern

The owner explicitly approved clearing only the102existing November14class-detail-failed checkboxes. All102were cleared and read back; schedule keys and every class-detail-error value remained unchanged. This is a one-time authorized reset, not a new retry policy. No code, source URL, headers, cadence, rules, collector task or credential changed. No collector was run manually.

The latest saved schedule before reset had97eligible class occurrences from the historical100: classes200031425 and200031426now have zero entries, and200031646is no longer listed. The next normal scheduled pass remains subject to those existing business exclusions and the same-date save/link completion requirement. Collection, saving, linking and logging must each be verified for its new exact attempt.

Operational lesson: distinguish a failed source response from a permanently invalid class. Previously valid classes can acquire failure marks on a later bad response. A zero-error pass that excludes all remaining classes is not class coverage. Keep original failures, inspect the now-retained rejected body and exact validation result, and use an explicitly approved one-time checkbox reset when a scheduled recheck is intended. Do not silently clear flags, add automatic retries or declare recovery from schedule success alone.

Reset evidence: C:/Users/gombc/Documents/Codex/outputs/wef-nov14-unblock-20261112/approved-reset-before.json and approved-reset-readback.json. The next normal scheduled pass collected all97eligibleclasses and591entryrows in35.223seconds with no source errors. Separate saving completed21:47:48UTC with3003acknowledgedbusinesswrites and zero saving errors. Separate linking completed21:50:32UTC; native readback verified all97classcapturepaths, entrycounts and exactshow/day/ringlinks. See scheduled-verification.json and native-class-verification.json. No code changed or manualcollector ran. This verifies the checkbox-reset recovery; it does not claim consolidated day-wide saving, which remains unimplemented.
