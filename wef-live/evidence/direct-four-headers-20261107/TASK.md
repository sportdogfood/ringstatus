# Direct public Live feeds
Owner approval: Use tested four-header set for four DIRECT Wellington feeds; keep decoded trips paused and everything else unchanged; test/deploy/verify normal runs; no manual runs/repairs.
Baseline: exact 26JS w-shares-20261107 deployed manifest verified before candidate preparation.
Delta: activity.js status host/headers; source.js public URL hosts and public-request headers. Preserve decoded endpoint/helper behavior, shared reader, retry/timeout, cadence, status parser, all three collection lanes, linker, cleanup, tables/links and task. No other runtime changes.
Diagnostic proof from Decoder supports the header choice; not scheduled acceptance. Offline tests use intercepted fetch only. Normal scheduled status/collection proof remains separate. No observer or manual collector.
