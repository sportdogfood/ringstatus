# Shared one-second pacing review

Reviewer: wef-decoder / 01a1a753-dce7-7951-a147-1b096e1538ba. 2026-11-06T23:21:53.996Z. No independent second reviewer claimed.

## Result

PASS for shared pacing of hosted proxy requests. Owner explicitly approved the additional shared queue after its exact scope/proposal. Deployed version a43ff3d7-2838-4acf-92b2-7fe9567961da. Normal local-first and automatic fallback implementation body preserved byte-for-byte after normalization of line endings and the necessary export/import change. Source fallback, decoder, renewal, installed runner, tunnel-related files, dependency manifests and locks match protected hashes. Agent URLs unchanged; no collectors or business records changed.

One Durable Object in existing Worker selects a single globally named object. FIFO handles one request/body at a time; next-allowed timestamp persisted after every completion/error. Early timer wake rechecks the clock. Storage error fails closed. Default entry cannot silently bypass a missing queue. Existing method rejection preserved. Health recursion stays inside the current queue slot. Header timestamps describe logical proxy request processing, not each internal source call.

## Evidence and actual checks

Execution-log.json contains actual23-test command, isolated preview deploy/secrets, production deploy and full live-check process stdout/stderr/start/end/exit. The cleanup CLI command also remains logged with exit1: its KV-list permission check failed. Subsequent API404 confirmed preview Worker absent, while production settings200 retained its queue binding. A guarded cleanup script stopped at404 before issuing any DELETE; no second deletion occurred. Do not erase these failures or invent an authentication repair. Preview-cleanup.json records readback.

Preview-concurrent-checks.json: three simultaneous clients, valid local schedule and class plus forced-fallback class; all usable, measured inter-request gaps1000ms/1000ms. Preview-health.json:200/oktrue. Production live-concurrent-checks.json: three simultaneous requests across shared and broad-tooth URLs, including forced fallback, all usable with gaps1000ms/1000ms. Live-health.json:200/oktrue. Live requests inspected7rings and8trips/entry IDs; no hard-coded8ring gate. Offline tests cover concurrency, failure continuation, persisted timestamp/new-instance behavior, real one-second timer, early wake, storage failure, same-name routing, and18existing decoder/fallback cases.

Reviewed actual source and config against saved pre-change snapshots, not repositoryHEAD which includes unrelated prior owner edits. No dependency/runtime upgrades or new collector behavior. New binding, migration and class are the separately approved component. Authenticated isolated preview had its own namespace and copied existing credentials; no public credential exposure. The existing production secrets were not rewritten.

## Scope and limits

This spaces hosted agent requests after the prior body is consumed. It does not separately pace internal renewal calls within one operation or direct SGL consumers bypassing the proxy. Earlier WEF Data source-reader uses such a bypass; migrating it is outside this change. Queued bursts add latency and can exceed existing client deadlines; no deadline/collector edits or load-capacity claim. Long-term restart proof is an offline persisted-timestamp case, not a forced production outage. Underlying fallback/primary source failures are not hidden as successful payloads.

Operational reference: ../../SHARED-PACING.md. Further changes need owner approval.
