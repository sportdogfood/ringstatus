# Shared 1-second pacing — pending owner approval

Requested behavior: agents using the shared proxy run one request at a time; after one completes, wait at least1000ms before starting the next. Local remains first; automatic direct-TLS fallback and external URLs remain unchanged. No collectors run.

Existing implementation has no shared coordinator. Module-global queues in ordinary Cloudflare Workers are per instance and cannot serialize different agents globally. The fallback must not use the local runner as its coordinator because it must remain independent of that runner.

Smallest supported added component: one Cloudflare Durable Object binding SGL_PACER, class SglPacer, within the existing wef-schedules-decoded Worker. One named object receives proxy source requests, runs the existing local-first/fallback handler, consumes its response, then holds the next request for1second. It persists only the next-allowed timestamp so instance replacement cannot erase the pause. No separate Worker, new public URL, dependency upgrade, credential, collector or Airtable change.

Affected source: shared-worker.mjs (export existing handler through coordinator), new sgl-pacing.mjs (shared FIFO and class), wrangler.jsonc (binding plus migration), focused tests. Existing runner/tunnel settings and their installed files remain untouched. Token renewal stays inside the existing paths; this proposal spaces agent requests, not every internal subrequest. Existing fallback renewal already pauses1second; local renewal behavior stays unchanged.

Scope limit: direct SGL calls that bypass the hosted proxy are not paced by this queue. Earlier inspected WEF Data source-reader contains such a direct path; collector migration is outside this authorization. Queue waiting adds latency and cannot guarantee shorter client deadlines under large bursts.

Before release: offline concurrent callers/error continuation/restart timestamp tests; Cloudflare preview concurrent schedule/class probes prove>=1000ms between prior completion and next start; preview local-first/fallback acceptance; then authorized deployment and live checks without stopping local. No automatic schedule/collector runs as proof.

Approval needed before adding the new binding/resource under AGENTS.md No unapproved code upgrades. No production code or configuration is changed by this proposal.

Official design basis: https://developers.cloudflare.com/durable-objects/concepts/what-are-durable-objects/
