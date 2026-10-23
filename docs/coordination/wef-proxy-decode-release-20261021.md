# Tenant proxy decoding — 21 October 2026

Superseded by the owner's separate-lane/legacy-restoration instruction. The shared
proxy decoder and tenant source change described below were restored to their
originals on21 October. See wef-isolated-decoder-release-20261021.md for current
status. The following records the earlier deployment and its observed result.

Owner authorized optional proxy decoding for the tenant collector, offline tests,
deployment and verification through normal scheduled runs. No manual collector
runs, record repairs, task changes, schema changes or credential changes.

## Proven baseline and narrow change

Checkout HEAD: `6a3991ed31df187d1b9566c1a5368669b6ba85c1`. The unrelated
deleted demo video and prior evidence directories are preserved.

The live `ringstatus-proxy` forwards `/api/*` to the production service binding
`broad-tooth-b8ed`. Only `broad-tooth-b8ed` supplies upstream authentication.
The older repository Worker copy differed from production. The exact deployed
source was saved at
`wef-tenants/evidence/proxy-decode-20261021/broad-tooth-before.js` before adapting
`utils/workers/broad-tooth-b8ed/worker.js` from that source.

Prior deployment: `925d4971-68f9-4236-926d-b17194a32fb2`, version
`d8e8e91d-e6c2-456a-9184-7f6e8114c9d8` at 100 percent.

The opt-in `decode=1` is restricted to individual people GET requests. It uses
full upstream authentication and decrypts nested `_` objects as Wellington's
public browser bundle does: AES-GCM, SHA-256-derived key using the public bundle
salt, actual upstream Authorization header and embedded eight-byte minute.
Credentials remain inside the Worker. A bad blob, wrong key or token-refresh
response fails closed with a sanitized 502; there is no guessed identity or
fallback to a partly decoded response. Existing calls without opt-in keep the
same upstream request, response body, status and headers.

Public bundle inspected: `https://sglapp.showgroundslive.com/static/js/main.js`.
The public salt is pinned to the inspected bundle; an optional existing-environment
override is supported. No new binding was added. A future upstream salt/token
change that prevents decoding must remain a reported source failure.

Tenant `source.js` changes only its people request to
`https://ringstatus-proxy.gombcg.workers.dev/api/people/<id>?pid=<id>&show_id=<show>&customer_id=<customer>&decode=1`
and requires `X-RS-Decoded: 1` before the existing identity validation. The existing
transport adapter uses its Node path for this proxy host. Model, reconciliation,
archiving, roster policy, tables, trainer configuration and tasks remain unchanged.

## Offline verification and deployment

51 tenant tests PASS, including nine decoder/source tests. The feature tests were
observed failing against the saved production baseline before implementation.
They cover restored IDs, recursive objects/arrays, empty responses, wrong keys,
corrupt blobs, invalid field maps, sanitized failures, non-opt-in equivalence,
existing 401-to-full retry, missing decode receipt and wrong person rejection.
Evidence: `wef-tenants/evidence/proxy-decode-20261021/offline-tests.txt`.

Independent read-only review: no actionable findings. Reviewer ran the nine
feature tests without endpoint calls or mutations.

The exact decoder successfully recovered IDs from all four saved Wellington
50121 trips using the deployed proxy's authentication, without an upstream call.
The initial sandbox check using dynamic evaluation was unsupported; the decoder
was then executed as ordinary supplied code. No dynamic evaluation is used in
the deployed Worker.

New deployment: `ad08a0f7-2b1e-49e2-9d15-b5c67f66bffb`, version
`d4d6b81f-d91e-4e4c-8ef2-09e6d90d804f` at 100 percent, deployed
2026-10-21T23:37:21.684482Z. Source readback matches the candidate. Bindings
retained by strict inheritance and verified identical. Compatibility date/flags,
usage model, observability, tags, placement and tail consumers unchanged. Only
deployment annotations differ. `ringstatus-proxy` was not changed.

## Scheduled acceptance — decoding PASS; downstream collection FAIL

Tenant source switched after the pre-switch 19:37 run completed, while the task
was Ready. Deployed source SHA256:
`82e860646e293401d862e0c8805138425c63b618ca6321fbc7cc40dcf2b2b88b`.
Worker source SHA256:
`98c8febdd507843c1eab18eb0c95114a5d01dccba0119833d278a216931f6ff9`.
The deployed package's 51 offline tests also passed.

Normal trigger 19:42:01 Eastern, collector started 23:42:02.113Z and completed
23:43:07.641Z, task exit 1. Run ID:
`5465deb4-9a1a-4be9-9dab-ad13db030e32`.

Decoding PASS: normal-run archived `people-50121` contains all four restored
entry/class/horse/rider IDs, with no `_` left in those trip rows. Seven archive
events have matching decompressed hashes. The generic inspector initially
reported a standing-roster count mismatch because it counted its object envelope
as one; inspection against the existing archive contract confirms 1 tenant +
71 horses + 33 riders = 105, matching the recorded count. No archive corruption.

Downstream collection FAIL: `No shared schedule for entry 200241392 class
200030330`. Read-only preflight confirmed all 435 shared schedule records currently
omit `class-id`, including the four relevant class-number/day records. The
tenant's existing model requires that ID for a schedule join. No name/number
fallback was introduced and no schedule records were repaired. Entries remain
zero. 8778 remains main and returned No entries; 50121 remains additional slot2.
Heartbeat and run-logs both recorded the precise new error.

Both scheduled-task definitions remain byte-equivalent to the exported baseline.
The unchanged shared schedules task completed its normal 19:40 trigger with exit0.
This is not complete tenant collection acceptance. Verification stopped after the
first post-switch failed workflow; no second-run acceptance, retry, record repair,
manual collector invocation or further product patch was performed.

Evidence: `wef-tenants/evidence/proxy-decode-20261021/` includes baseline source,
staged/deployed source, tests, task XML, the scheduled-run receipt and read-only
preservation checks. The next required authorized work is the shared schedule
identifier/join compatibility gap, preserving current keys and workflows.
