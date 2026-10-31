# Historical runs and collections

This implementation keeps historical evidence in the existing `sportdogfood/ringstatus-data` archive. Data, Shows and Season retain their existing keys, native identities, accepted revision chain and original source times. No Entries workflow changes are included.

The accepted native run's `summary.evidence-archive` pins the manifest and verified remote commit. The existing layout remains:

```
wef-data/shows/{customer}/{show}/retained-evidence/
  revisions/{state-sha256}.json
  runs/{compressed-sha256}.json.gz
  legacy-collections/{compressed-sha256}.json.gz
  parts/{retained-sha256}.json.gz
```

Data and Shows resolve missing historical native rows from that manifest. They validate show scope, paths, compressed hashes/lengths, original keys/native IDs and predecessor hashes. Data reconstructs the unchanged private Season handoff; Season's writer and checkpoints remain unchanged. Missing/corrupt archives fail closed and never become empty data or refreshed source timestamps.

## Scheduled cleanup

`history_cleanup.retire` is called by the existing Data runner. It is disabled without `wef-data/runtime/history-cleanup-policy.json`. The policy has format `wef-data-history-cleanup-v1`, tables `runs` and `collections`, and `reader-sha256` containing exactly the relative paths in `history_cleanup.READERS`. Hashes must match the actual deployed files, including the Shows readers and existing Season consumer. The release's policy template is preparation only; it is not installed in production.

After approved deployment and recovery, each ordinary Data wake can remove at most ten eligible records from one table. No new task or cadence is introduced. Acquisition/recovery checkpoints and production stops take precedence. An unresolved cleanup DELETE blocks source work even if its policy is absent.

Before removal, cleanup verifies the published commit/manifest/object bytes and all four Season checkpoints against accepted ancestors of the current native head. It uses the earliest accepted Season sequence as its cutoff. It refreshes native inventory and rechecks canonical context, current head, exact IDs, authored fields and forward-link consumers before saving and fsyncing the exact deletion intent. Lost responses are reconciled by their original IDs; an uncertain batch is not blindly repeated.

Current heads, failed/unaccepted owners, active/pending/recovery owners, changed rows and every native forward-link-referenced row stay in Airtable. Collections are removed before their now-unreferenced historical run can qualify. Requests, slots and other retained consumers can intentionally pin old rows. This policy does not purge those other tables, clear their links, repair records or promise every historical row is eligible.

The local durable cleanup journal is `wef-data/runtime/history-cleanup-state.json`. Readiness waits and blocked policy/validation results are included in the existing Data result's `retention-waits`; uncertain DELETE results defer acquisition through its existing run reporting path. There are no new Airtable fields or tables.

## Delivery boundary and acceptance

Reviewed source and offline tests are complete. No production deployment, policy installation, deletion, stop clearing or manual collector run was performed for this release. The separate saved-checkpoint recovery release remains frozen; its original 38 source responses and production stop must remain protected until the established deployment/recovery route is authorized.

When production activation is separately authorized, deploy the reviewed Data and Shows files together using the existing guarded route, verify exact hashes and unchanged task definitions, and install the matching policy only after reader verification. Preserve the existing Data/Season locks and original checkpoint. Do not rerun old single-use deployment scripts or invoke a collector manually.

Normal scheduled acceptance still must demonstrate: archive verification before each removal; bounded/idempotent batches; exact-ID reconciliation after interrupted removal; retained forward links and active recovery preserved; cold Data/Shows replay and four separate Season checkpoints; no recreated historical keys; unchanged original source clocks and Unknown/Stale/empty semantics. Local tests are not that production proof.
