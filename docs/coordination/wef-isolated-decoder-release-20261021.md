# Isolated decoder lane — 21 October 2026

Historical failed attempt and authorized restoration. The subsequent separately
approved correction is documented in wef-isolated-decoder-redirect-fix-20261021.md.

Owner authorized a separate wef-schedules-decoded Worker, original legacy code
restoration, testing before switching schedules, then normal scheduled-run
verification. No manual collector runs or record repairs.

## Created, tested offline; live gate failed

New Worker deployed at2026-10-21T23:53:51.129725Z:
deployment eda22cee-6ec5-45df-af6e-d91cb81c2abe,
version0000b507-69b3-44e4-a7de-501b2e6cc3f6 at100 percent.
Endpoint https://wef-schedules-decoded.gombcg.workers.dev.
No custom-domain routes or scheduled triggers were added.
Copied existing upstream authentication into secret bindings on the new Worker,
without exposing values or changing existing bindings.

181 staged/offline tests passed before deployment. Independent read-only review
of the six new Worker tests found no actionable issues. An archived Oct25 schedule
decoded139 objects with139 class IDs, retaining139 rows and all composite keys.
The initial suite failure was a test expectation hard-coded to the old source
host; staging the intended new source changed that host as expected. No production
mapping, normalization or archiving logic was changed.

The first pre-switch diagnostic GET /ring?customer_id=15 returned502:
Source fetch or protected response decoding failed. Neither collector was
switched, and no normal-run proof for the new Worker is claimed. The same ring
payload obtained read-only through the existing full-auth proxy successfully
decoded with the existing authentication. Further read-only investigation found
the new Worker sets redirect:error. Current Cloudflare Request documentation
states Workers reject this mode with TypeError before sending the request:
https://developers.cloudflare.com/workers/runtime-apis/request/?ref=blog.madbotz.com
The new Worker still needs the supported manual redirect handling and a fresh
live test. The catch currently reports a generic502, so the precise thrown
exception was not captured from the deployed invocation.

AGENTS.md prohibits product patches after failed verification without fresh
APPROVED TO EDIT. No subsequent correction, retry of the failed new lane, collector
switch or manual business run was performed.

## Authorized legacy restoration completed

broad-tooth-b8ed restored to the exact saved pre-decoding live source at
2026-10-21T23:55:31.561396Z. Readback exact match and original bindings identical.
Restoration deployment8d671fb4-bfa1-4294-8d0a-b30cf736b1e1,
version291feca0-f686-4276-b44d-3b1b21f30f38 at100 percent.
Original source: wef-tenants/evidence/proxy-decode-20261021/broad-tooth-before.js.

ringstatus-proxy was never modified and remains deployment
0263a897-f45e-4d89-8503-0d8953ea151c, version
de38179f-b130-41bb-9f78-f14ca179bf11.

Tracked utils/workers/broad-tooth-b8ed/worker.js restored to repository HEAD;
its historical repository version differs from the saved deployed baseline and
was NOT uploaded as the restoration source. wef-tenants/source.js restored exactly
to its pre-decoding original, hash
d9583779cf94479299ba9ecf2cf27b3753c45d30d06b603f5444752798e7abb7.
This removes its temporary dependence on the removed legacy decode option.
wef-schedules/settings.js and the temporary test-host edit match HEAD.
Prior new shared-proxy tests are retained as historical/staged evidence, outside
the active tenant test suite. Unrelated deleted demo video remains untouched.

166 restored collector offline tests PASS. Both Windows task XML definitions
match their baseline. Existing schedules task completed its ordinary19:55 run
with exit0. Tenant entry collection remains blocked by the original missing-ID
problem on its original source; no improvement is claimed after rollback.

## Remaining acceptance

After new approval: correct only the isolated Worker's runtime incompatibility,
test its real ring/schedule/people reads, switch independent schedules and tenant
source only after those pass, then verify normal repeated scheduled runs, filled
class IDs, unchanged schedule record IDs/keys/manual helpers, valid archive hashes,
tenant joins, heartbeat/run-logs and unchanged legacy Workers/routes/tasks.
