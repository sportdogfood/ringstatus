# Decoder and Fast: fallback and failure plan

Date: 7 November 2026, America/New_York. Version: 2026.11.07.1.

**PLAN ONLY. Nothing activated, paused, resumed or sent by this document.** No production changes, source tests or manual collectors were run to prepare it.

## Proposed approach

**Decoder owns recovery. Fast stops adding affected source requests while Decoder tries one approved alternative header profile. If recovery is verified, Fast resumes normally. Otherwise report the outage and preserve the last good data.**

Reuse the existing proxy, runner, fallback transport, queue, task schedules and logging tables. No additional Worker, tunnel, queue, timer or collector. Do not add an arbitrary request deadline or promise a maximum queue wait.

## What happens today — inspected, not proposed

| Area | Existing behavior / evidence |
| --- | --- |
| Connection | Runner first; independent Cloudflare TLS fallback already exists. Both now omit ordinary `authority`, `method`, `scheme` headers. |
| Latest route proof | Schedule: seven rings through runner. Class detail: eight trips through automatic fallback. The sampled runner class response is still empty; that issue remains open. |
| Pacing | Existing shared queue waits at least one second after a proxy operation completes. This does not automatically space multiple internal recovery probes. |
| Fast retries | `fast_collector.py` retries schedule once after one second for 500/502/503/504 or timeout. Classes have no immediate collector retry. Fast currently does not pause admission while Decoder investigates. |
| Data logs | Existing local attempt logs mirror to `wef-data` Airtable run-logs, err-logs and heartbeat. Delivery is distinct from collecting data successfully. |
| Data notices | `collection-alerts.js` calls `collection-notice.ps1` for desktop and Records channels. It currently selects schedule-inventory failures and class `source-json` rejections; this is not complete coverage of all class source failures. |
| Records delivery | The script uses the existing Codex CLI to resume Records with an acknowledge-only notice. Latest saved `collection-notice-records.json`: `ok=false`, `Codex Records delivery exited 1`. This does not establish that every past alert failed. |
| Desktop delivery | Latest saved desktop receipt says `ok=true`, `Submitted to Windows`. Submission does not prove the owner saw the toast. |
| Other recipients | No Timing, Messages or Waze channel exists in the inspected Data notice code. Their present receipt of these failures is unverified. |
| Decoder health | Existing scheduled show-time/health code records source results in Shows health-table. Chat monitoring reports a new failure/recovery once. Health covers one schedule check, not every class or downstream save. |

The inspected Fast chat explicitly declined its proposed notification-retry correction. The inspected Records chat retains an owner stop on sending. This plan does not lift either boundary, authorize fan-out now, or claim those changes are implemented.

## Header profiles — proposed, not automatic guessing

| Profile | Contents | Status |
| --- | --- | --- |
| A — current | Existing approved headers after removing ordinary `authority`, `method`, `scheme`; existing credentials and token renewal. | Deployed. Live normal/fallback results above; primary class limitation remains. |
| B — minimal browser request | Same required authorization, cookie, User-Agent, Accept, language, Origin, Referer and SGL request-origin. Candidate omits redundant ordinary `path` and optional browser-version hints/priority. Transport keeps its own correct compression handling. | **Not tested or approved for production.** Exact omissions must be frozen and previewed before this profile becomes eligible. |

Use at most these two profiles initially. Never reintroduce the three proven failing headers, rotate random User-Agents, invent credentials or learn a new profile from an error page. Do not add a third profile without evidence and explicit approval.

## Proposed recovery sequence

1. **Recognize the actual failure.** Upstream 503 is eligible for a bounded recovery cycle. Transport 502/504 and empty/undecodable private payloads are separately identified eligible cases. `/health` 503 is a summary: inspect its schedule status rather than treating it as an upstream SGL 503. A valid empty class or zero-entry day is not an outage. Airtable 422 is a save error, not a reason to change source headers. A 401/403 enters the existing auth/access investigation; do not blindly cycle profiles or overwrite credentials.
2. **Pause affected admissions, not Windows tasks.** Fast stops launching new calls for the affected route family/show/date, lets already in-flight work finish, preserves checkpoints and successful saves, and reports the failure. No inferred empty inventory, deletion or stale-as-current result. Unrelated authorized work may continue. Existing schedules, due rules and explicit owner stops stay intact.
3. **One recovery owner.** Decoder handles one recovery cycle for that incident through its existing shared queue. Fast and other callers must not simultaneously run their own header trials or duplicate schedule retries. Reconciling Fast's current one-retry behavior is part of future approval, not already deployed behavior.
4. **Bound the attempts, not an invented elapsed deadline.** Profile A has already tried runner and existing fallback for the failed call; do not repeat those merely to start recovery. If Profile B has passed preview, try runner B once, then independent fallback B once only if needed. Stop on valid data. Thus at most four source-data attempts total for the triggering call: runner A, fallback A, runner B, fallback B. Token renewal is separate existing work, not repeated on every failed attempt. Keep at least the existing one-second minimum between internal source attempts too; honor a valid upstream Retry-After before another attempt. Use the existing next wake when waiting is required; no new polling timer or catch-up burst.
5. **Verify before resuming.** Successful HTTP alone is insufficient. Confirm expected show/date or class identity, usable decoded payload and expected schema; allow legitimate zero rows. Apply a successful profile only to the route family actually verified. A schedule probe does not restore class detail. Resume the held affected operation and normal due work through the same queue. Do not launch a manual collector or claim recovery of the whole show from one sample.
6. **If all eligible attempts fail:** end this recovery cycle, record the outage and notify the owner and named coordinators once. Hold affected new acquisitions until the next existing due opportunity. On that opportunity permit one controlled check using the last approved profile; do not repeat the full profile sweep for every waiting class. There is no permanent new owner-release lock: a verified successful check can clear this incident's hold, but cannot clear any pre-existing owner stop.

All callers must share the one recovery decision. Implementation must settle how the existing Decoder state is exposed to Fast before enabling the pause. Do not silently invent a new public endpoint, table, status field or cross-agent control service to do so. Extend the existing response/log contract only with explicit approval where needed.

## Failure and recovery communication

Decoder supplies the source/lane/profile outcome; Fast supplies the collection impact. Neither may label a failed save or missing data recovered merely because `/health` passes.

Send a **Recovering** notice when affected calls are paused, then one result notice: **Recovered** or **Outage**. Coalesce unchanged failures rather than repeating alerts for every class. A queued message, a successful CLI exit, coordinator acknowledgment and corrective action are different evidence states. Retain failed delivery as failed; never say everyone was notified without receipts. Notification failure must not block otherwise permitted collection or automatically restart anything.

Each notice contains only: incident identifier, Eastern time, affected route/show/date/class, upstream/proxy status or empty-payload reason, profiles and transports attempted, last successful observation, current pause/resume decision, affected saved-data freshness, and an evidence pointer. No bearer, cookie, raw protected payload or invented impact count.

| Requested recipient | Verified chat name and ID | Proposed responsibility on receipt |
| --- | --- | --- |
| wef-decoder | `wef-decoder` — `01a1a753-dce7-7951-a147-1b096e1538ba` | Own source diagnosis, bounded header/transport recovery and source recovery evidence. |
| wef-fast | `wef-data-fast-collector` — `01a19273-bdf8-71b2-8503-cc8079efc129` | Own affected-request admission pause, collection failure reporting and normal-cadence resume after verified recovery. |
| Records | `wef-records-coordinator` — `01a169bc-e58a-7de0-94c6-5d3931fcf745` | Record cross-agent impact and acknowledge receipt; do not start collectors or silently lift its current sending stop. |
| Time | `wef-timing-coordinator` — `01a169bc-0bd6-77d3-b7b4-283a07061e90` | Account for unavailable/stale source inputs under its existing authority. No inferred current timing from a failed acquisition. |
| Messages | `wef-messages-coordinator` — `01a169cc-d938-76f0-9ae4-bbccf7266599` | Account for freshness before user-facing messages; receipt does not authorize outbound customer messages or configuration changes. |
| Waze | `wef-waze-coordinator` — `01a169d0-8192-7851-b414-7aed7a065cea` | Account for stale/missing source inputs and existing estimates under its own authority; do not invent live facts. |

Names such as `wef-time-coordinator` and `wef-message-coordinator` resolve to the verified Timing and Messages chats above, not new chats.

**Delivery proposal:** use the existing Data notification path and existing coordinator chats, with explicit receipt tracking, rather than assuming Records will forward automatically. Actual automated fan-out and delivery retries are not installed and require approval. Until reliable delivery is approved and proven, the smallest honest handoff is for Decoder/Fast to give the owner the single notice and list which coordinator receipts are missing. No messages were sent while drafting this plan.

## Approval and acceptance before activation

1. Owner approves the exact Profile B header omissions, the one-incident recovery ownership, affected-call pause/resume behavior, and any necessary existing response/log contract change. Reconcile the Fast notification correction previously declined and the Records sending stop explicitly; do not infer that a plan lifts them.
2. Preview Profile B on schedule and class detail, including original-versus-alternative comparison, repeat useful responses and valid empty results. Keep main transport, credentials and URLs fixed. An unproven alternative stays disabled.
3. Exercise a safe simulated 503/empty-payload incident in preview: one recovery owner, no duplicate sweeps, internal one-second gaps, Retry-After handling, successful and exhausted paths, preserved data and unchanged task definitions. Do not manufacture a production incident or run collectors.
4. Verify actual notice delivery and coordinator acknowledgment separately from source recovery. A failed Records receipt must be visible. No automated messaging activation before its destination/stops and delivery route are approved.
5. After separate implementation/deployment approval, verify the next normal scheduled Fast behavior. Do not substitute source diagnostics for scheduled collection proof. Keep legacy tasks disabled throughout.

## Source references

- Current header release and remaining primary-class limitation: [release evidence](../../workers/wef-schedules-decoded/evidence/header-fix-release-20261107/review-report.md).
- [Data alert selection/state](../../wef-data/collection-alerts.js), [desktop/Records delivery](../../wef-data/collection-notice.ps1), [Fast source retries](../../wef-data/fast_collector.py), [existing log mirroring](../../wef-data/attempt-log.js).
- [Current shared source handling](../../workers/wef-schedules-decoded/shared-worker.mjs), [fallback/token handling](../../workers/wef-schedules-decoded/fallback.mjs), [queue](../../workers/wef-schedules-decoded/sgl-pacing.mjs), [show-time/health](../../workers/wef-schedules-decoded/show-time-health.mjs).
- Runtime receipt evidence inspected: `wef-data/runtime/collection-notice-records.json`, `collection-notice-desktop.json` and `collection-alerts.json`. These are current mutable files, not guaranteed historical receipts.
- Read-only chat inspection: latest Fast refusal of notification-retry correction and Records owner-stop record. No cross-chat instruction was sent by this task.
