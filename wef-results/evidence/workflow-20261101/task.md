# Results workflow — 1 November 2026

Authority: owner asked to create the simplest workflow after confirming alerts create X/Y, reconciliation polling organizes records, identifies conflicts and marks Expected or Complete. No activation remains controlling.

Baseline: repository HEAD 00f4271b8d4be7eba9639b7a711119f2104d17ac; existing wef-results runtime/package/schema retained. Both READMEs v1.4 are already modified by this chat. Other-owner Data/Live/People/Season changes are excluded.

Approved delta: native Airtable automation drafts in existing Results base appK9IX59jFwUkB2u, shared script and focused offline checks, documentation. Existing snapshot fields hold expectation and newly created X/Y records. No schema/table/dependency/runtime upgrades. Source tables are read-only. Preserve five owned tables, source clocks and exact source keys. No business/manual workflow execution, production record repair, task enabling or deployment.

X creates a new result record when results_verified=1 and at least one matching result value is nonzero. Include all matching nonzero result instances from local wef-data.results. Y creates a new result record when gone-in=1 and at least one of score1/time-one/faults-one/time-fault-one is nonzero. Both include horse/rider/entry. Only dynamic day-priority=0 scopes. Events handle source row creation and relevant updates. Polling every ten minutes reconciles expectations/status/conflicts only; it never substitutes for an alert or creates X/Y result records. This uses the prior ten-minute cadence for reconciliation only.

One expectation row per X/Y trip; a qualifying alert creates a separate result row and marks its expectation Complete after readback. Repeat delivery reuses an existing result; duplicate or inconsistent records are flagged Conflict, never deleted/repaired. Missing numeric values are not zero. No Completed/visible/full-class/freshness approval gate is added. Runtime errors stop that invocation and are logged.

Acceptance: focused mock-Airtable checks cover one nonzero field, zero-only/no-trigger, all X result phases/instances, identities, day0, repeat delivery, reconciliation cannot create results, conflicts and saved-write readback. Independent read-only review required. Save/read back all automation drafts and prove disabled. Live trigger delivery and reconciliation business execution remain unverified while inactive.
