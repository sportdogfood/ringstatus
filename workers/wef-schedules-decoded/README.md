# wef-schedules-decoded

Updated **7 November 2026, America/New_York**. README version **2026.11.07.1**. Operating lock version **2026.11.07.1**: [locked baseline and change boundary](VERIFIED-BASELINE.md).

## Current operating state

- Existing agents use the shared proxy. Data/Fast and Live were moved to the hosted shared URL; legacy tagger change was undone. This does not claim every historical script uses the proxy.
- Working route: shared Worker -> permanent tunnel -> existing runner -> SGL -> existing decoder. Local first; independent Cloudflare TLS fallback remains available on existing supported routes. Agents receive decoded data without source credentials.
- One shared queue spaces proxy operations by at least one second after the preceding response completes. Internal token renewal is not a separate agent queue item. No queue-wait maximum is configured.
- Runner and fallback obtain anonymous tokens through /utilities/get-config using existing private credentials and headers. Tokens are cached and renewed when absent or less than 24 hours remain; no token values belong here.
- Last recorded shared Worker deployment: 2b71ed7b-f3f4-44a9-89df-4854a10503c6. This documentation edit does not redeploy it. See [fallback](AUTOMATIC-FALLBACK.md), [pacing](SHARED-PACING.md), and [migration evidence](evidence/agent-proxy-migration-20261106/FINAL-SCOPE-VERIFICATION.md). Their earlier dated snapshots must not override this current summary.
- Data timeout alignment is installed: removed the default 18-second request cutoff and 28-second outer subprocess cutoff. Existing proxy failure deadlines, network/platform limits and optional explicit diagnostic timeouts remain. No invented queue deadline or delivery guarantee. A 72-second simulated Python-to-Node wait and 36 focused checks passed before installation; 36 checks passed afterward. No collectors ran. [Review](evidence/data-timeout-alignment-20261106/review-report.md), [installation](evidence/data-timeout-alignment-20261106/installation.json).

## Open work — do not resume without owner direction

1. **Class detail: PAUSED / OPEN.** Owner identifies this as the remaining paused class-detail task. Successful isolated class-detail pings (class 200031233, eight trips) do not close the paused work or prove all classes collect successfully. No broader class-detail implementation or retry is authorized by this documentation update.
2. **Show-time freshness: OPEN.** Normal scheduled saves are verified, including 07:54 and 07:57 Eastern on 7 November, updating the same show/day Airtable record recBo37q4NJZPPYCh. The saved freshness remains Unverified. The current implementation deliberately labels Date-header observations Unverified unless a positive Age header marks them Stale; it has no independently proven Fresh state. Successful save is not verified cache freshness. No freshness correction was made.

## Known trouble and acceptance limits

- **Direct Cloudflare fetch:** CF-Worker header injection was isolated as a sufficient cause of SGL returning HTTP200 with an empty object. Deleting it in Worker code did not keep it removed. The working runner path and independent TLS fallback avoid that tested path. Do not claim all403/502 failures are caused by credentials, IPs or throttling; those general causes were not proven.
- **Historical403 and502:** schedule403s and class502s were reported. Current sample success does not establish a cause or recovery for every historical failed request/class. No automatic credential change or additional retry is authorized.
- **Empty success bug:** installed runner decoder previously accepted an empty object as decoded. Hosted private-route validation rejects missing decode proof or empty/invalid top-level objects and invokes fallback. The fallback source includes the empty guard; installed runner source was not replaced as part of migration. A200 or decoded marker alone is not proof of usable domain data.
- **Runner interruption:** relay previously stopped with0xC000013A while the tunnel remained running; cause unknown. Restoration was verified, not a guarantee of unattended reliability. A tunnel alone does not prove a healthy runner.
- **Waiting:** queue wait has no configured maximum. Data's premature cutoffs are removed, but platform/network limits still apply. Other consumers retain their own existing timeouts; this change did not align every consumer. Scheduled throughput under sustained load has not been established by diagnostic pings.
- **Monitoring scope:** /health checks one configured schedule and accepts the actual source ring count. Recent checks returned200, ok=true, seven rings. It does not certify every route, each class, today's full inventory, Airtable writes or every collector. No exact-eight-rings gate is permitted.
- **Show-time:** cadence is07:00–18:57 Eastern every three minutes, then19:00,22:00,01:00,04:00. One latest observation per show/source-date is updated. A task exit0 can mean Skipped or a successfully recorded outage; inspect the recorded clock/decoder results. Freshness remains open above.
- **Airtable422:** owner reported Data classes PATCH rejection of source-json. The supplied log omitted the class/value; cause and resolution were not verified in this proxy task. Keep it as an external reported issue, not a proven proxy defect or resolved item.
- **Existing test failure:** earlier broad run had80/81 passing; the Live future-selected-date test at wef-live/test/collector.test.js:365 also failed with frozen baseline adapters. It was not repaired or hidden. The36 focused checks exclude that unrelated test. [Baseline comparison](evidence/agent-proxy-migration-20261106/baseline-test-failure.json).
- **Scope correction:** unnecessary /classsignup, /classsignup/{id}, /entries/{id} additions and legacy adapter rewrite were withdrawn and never deployed. Legacy tagger restored exactly. Do not revive old-code migrations.
- **Proof boundary:** source diagnostics prove the tested payloads and lanes only. No manual collector run was used. Timeout alignment does not itself supply normal scheduled collection acceptance.

## Preservation lock

Keep connection, runner/tunnel, URLs, one-second pause, fallback, source headers/token renewal, existing routes and schedules unchanged. No extra routes, infrastructure, credential edits, manual collectors or record repairs. Any further code/deployment change needs applicable explicit owner approval; failed verification requires fresh approval before a correction. This is a documented operating lock, not a filesystem restriction.

## Historical README — superseded where inconsistent

**Current verified setup and change boundary:** [6 November 2026 permanent runner baseline](VERIFIED-BASELINE.md). The dated description below is historical; use that baseline for the current hosted request path, token renewal, live evidence and known limits.

Current addition: [automatic schedule and class-detail fallback](AUTOMATIC-FALLBACK.md), independent of the local runner and tunnel. Normal traffic remains local-first.

Current pacing: [shared one-second queue](SHARED-PACING.md) across proxy requests, with local-first and fallback unchanged.

Separate Cloudflare Worker for decoded WEF source reads. Existing legacy routes
are not routed through this Worker. It has no scheduled trigger and performs no
Airtable writes. SGL_TOKEN and SGL_COOKIE are server-side secret bindings copied
from the existing authorized proxy configuration; values are not in this project.

Supported GET routes: /health, /ring, /schedule, /people/:id and /classes[/id].
Only explicit source query parameters are accepted; customer_id must be15.
Encrypted fields use Wellington's public AES-GCM decoding method. Schedule `_`
evidence is retained for existing source-monitor columns and composite keys.

Status 2026-10-21: redirect handling corrected under new owner approval.
Uses manual redirects and rejects3xx without forwarding credentials. Pre-switch
live ring/schedule/people tests passed; both independent collectors now use this
lane. Two normal runs of each collector passed:435 schedule class IDs and four
linked tenant entries with stable record IDs and no duplicates. Existing empty
Oct22 show-day warning remains; no failed collection days. Original helper values
and five-minute task definitions are preserved. No manual runs or record repairs.

Offline tests, cached schedule proof and normal-run receipts are in evidence/.
See docs/coordination/wef-isolated-decoder-redirect-fix-20261021.md for release
evidence. Legacy proxy routes and task definitions remain unchanged.
