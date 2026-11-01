# Timeout review - October 31, 2026

Standalone owner-requested read-only code review. No collector, provider request, record edit, stop clearing, deployment or implementation change. Evidence files only. Candidate is current independent direct-proxy release, compared with its seven-file manifest and preserved pre-release code. Reviewer chat: 01a16976-f0a6-72a1-a75f-75de7f19a815.

## Findings

1. CONFIRMED P1 - source-fetch.js codes(), lines 4-7 and retry classification lines 19-20: numeric DOMException.code takes precedence over name. Node TimeoutError has code23. Existing recovery accepts TimeoutError by name, but receives only23, skips all remaining attempts and logs an opaque error. Real failed normal run contains ten such errors (one dated inventory and nine classes). Fully offline injected DOMException reproduces exact error and single attempt. Smallest correction: retain sanitized error.name alongside code so existing bounded retries recognize timeouts; keep the current overall deadline and maximum attempts. This defect predates today's direct-count deployment: preserved before/source-fetch.js has the same extraction/classification logic.

2. CONFIRMED P2 - tests/source-fetch.test.js and tests/direct-counts.test.js: recovery cases cover network code ECONNRESET and HTTP failures, but not native DOMException TimeoutError(code23). Existing suite passes while the offline timeout reproduction fails the intended retry behavior. Add an actual TimeoutError fixture, covering eventual success and bounded exhaustion. Passing tests and two initial normal runs did not establish this failure path.

## Why yesterday differs from today

Independent direct proxy counting did not run yesterday. The preserved previous collector reads day-source.reader(api), using Data retained evidence/replay. Current approved deployment occurred October31 12:35:04.658942Z (08:35 Eastern), changed the counting input to direct-counts.countDays and expanded the request allowlist to classes. Its 18-second per-request timeout makes the existing timeout classifier relevant to every class read. No dependency/runtime upgrade is in the seven-file deployment manifest. Independent direct counting was explicitly owner-approved; no unrelated upgrade or scope expansion was identified in the inspected deployed delta. Data independence remains intact.

Yesterday was not uniformly healthy: count-resume evidence retains October30 18:07:49.560Z Count replay timed out, a different Data-backed failure. That recovery and later direct-count release are distinct events.

## Exact adjacent normal-run comparison

October31 17:39 Eastern: dd872966-dfe0-4b41-bdf5-a345ad4f78f7, 167 requests,159classes complete across fourdays, Success; completed21:39:40.436Z.
October31 17:44 Eastern: c02b2b8b-00fd-43b3-9de3-2b02655e2c3e,166requests; Oct29 inventoryfailed, Oct30 35/37classes, Oct31 66/70, Nov1 49/52; completed21:45:31.388Z, Partial. All ten errors are timeout23 after one attempt. Duration88.751seconds, below150second overall source budget and240second task limit. This was not evidence of the whole task timing out.
The publisher then wrote the existing show-days stop and retained previous totals. Later Partial runs describe that retained stop; they are not repeated source attempts.
All seven current candidate hashes match the08:35manifest, so no post-release code drift explains the adjacent success/failure.

## Scope and evidence limits

Reviewed collector -> direct-counts -> source-fetch -> show-days publication -> count stop and telemetry, prior source-fetch/collector, day-source retained path, deployed manifest/evidence, real saved normal runs and offline tests. Timeout classification is proven; the reason requests exceeded their deadline (upstream, Worker or local network delay) is NOT established by retained logs. Recognizing timeouts restores intended recovery but cannot guarantee a slow source will recover within the existing budget. No proposed increase to budgets, concurrency, framework or workflow. Persistent failures still retain accepted totals and current protection.

Review COMPLETE with findings; implementation FAIL on timeout recovery. Offline tests PASS as recorded in command log, but omit the actual failure fixture. Live fix/recovery verification NOT performed or authorized. No repairs authorized by this review. Next smallest proposal is correcting classification plus regression coverage, then separately approved stop release and two normal runs. No Data changes/dependency.
