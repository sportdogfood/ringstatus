# Airtable to Webflow Dashboard Staging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a two-table Airtable staging system and repeatable incremental sync that writes aggregate slices to the existing Stats collection and factual evidence to Classes without adding Webflow collections or recalculating unrelated scopes.

**Architecture:** Source Classes remain the fact table. A pure aggregate module produces all-time, Year, Horse, and optional Competition slices for `webflow_stats_stage`; a separate projection produces selected Class evidence rows for `webflow_classes_stage`. Stats powers summaries, Competitions and Horses power story navigation, and Classes power drill-down evidence. A server-side runner upserts Airtable staging records, compares payload hashes, and updates only affected existing Webflow CMS items as staged content.

**Tech Stack:** Node.js, built-in `node:test`, Airtable Web API/server credentials, Webflow Data API v2, existing RingStatus runner conventions.

**Spec:** `docs/superpowers/specs/2026-08-27-airtable-webflow-dashboard-staging-design.md`

## Global Constraints

- Airtable base: `appUGgVeAZFae3tEb`.
- Existing Webflow site: `6982268b7543ac3c80151266`.
- Create exactly two Airtable staging tables and no new Webflow collections.
- Never expose Airtable or Webflow credentials in browser code, Airtable cells, source control, or logs.
- Initial Webflow writes are staged/draft-only; publishing requires separate explicit approval.
- Do not remove Classes, fields, references, or bindings until full reconciliation passes.
- Laineys Webflow schema writes are disabled during the initial pilot.
- A scheduled runner, not Codex or a manual endpoint, owns recurring execution.

---

## File structure

- Create `scripts/webflow-cms-sync/config.js`: stable IDs, category dictionaries, field contracts, and feature gates.
- Create `scripts/webflow-cms-sync/aggregate.js`: pure Class normalization and aggregate calculation.
- Create `scripts/webflow-cms-sync/airtable-client.js`: paginated Airtable reads and batched staging upserts.
- Create `scripts/webflow-cms-sync/webflow-client.js`: staged Webflow list/create/update/readback operations.
- Create `scripts/webflow-cms-sync/stage-builder.js`: Stats-slice and selected-Class projections.
- Create `scripts/webflow-cms-sync/sync.js`: affected-entity orchestration, hashing, checkpoints, and reconciliation.
- Create `scripts/webflow-cms-sync/cli.js`: runner entry point with `--dry-run`, `--entity`, `--source-record-id`, and `--reconcile` modes.
- Create `tests/webflow-cms-sync/aggregate.test.js`: aggregate truth-table tests.
- Create `tests/webflow-cms-sync/stage-builder.test.js`: staging projection and exclusion tests.
- Create `tests/webflow-cms-sync/sync.test.js`: idempotency, moved-Class, failure, and no-publish tests.
- Modify `package.json`: add non-POSIX Windows-safe test and runner scripts.
- Modify `docs/ringstatus_runner_options_overview.md`: document ownership, commands, gates, and rollback.

### Task 1: Freeze live contracts and create the two Airtable staging schemas

**Interfaces:**
- Consumes: live Airtable metadata and current Webflow collection schemas.
- Produces: exact field-ID manifests saved in `scripts/webflow-cms-sync/config.js`.

- [ ] Read all source Class, Horse, Competition, Lainey, and `webflow_collections_index` field IDs from Airtable; save a read-only JSON evidence snapshot under `docs/evidence/webflow-cms-sync/`.
- [ ] Read the current Stats, Horses, Competitions, Classes, and Laineys Webflow schemas; record exact collection, template, binding, and field IDs without changing them.
- [ ] Create `webflow_stats_stage` with scope, identity, normalized content, aggregate, hash, status, error, and timestamp fields defined by the spec.
- [ ] Create `webflow_classes_stage` with source identity, dimension, selection, hash, status, error, and timestamp fields defined by the spec.
- [ ] Add both table mappings to the existing `webflow_collections_index`; keep sync disabled.
- [ ] Read both tables back and compare every field name/type to the manifest; stop with FAIL on any mismatch.
- [ ] Commit the manifest, evidence, and documentation changes without including secrets.

### Task 2: Implement and test the pure aggregate calculator

**Interfaces:**
- Consumes: `NormalizedClass[]` from `normalizeClass(record)`.
- Produces: `calculateAggregates(classes) -> AggregatePayload` using the exact keys in the spec.

- [ ] Write failing `node:test` cases covering wins, seconds, thirds, top-three, top-eight, all three years, all seven heights, all three disciplines, and all nine skills.
- [ ] Add tests proving inactive Classes and `include_in_aggregates=false` are excluded.
- [ ] Add a test proving `publish_class_to_webflow=false` does not exclude the Class from aggregates.
- [ ] Implement explicit category dictionaries; reject unknown Result, Height, Discipline, Year, or Skill values into a validation error list rather than silently inventing keys.
- [ ] Implement `calculateAggregates` as a full recomputation from input Classes, never an increment/decrement mutation.
- [ ] Run `node --test tests/webflow-cms-sync/aggregate.test.js`; require zero failures.
- [ ] Commit the calculator and tests.

### Task 3: Build deterministic Stats-slice and Class-evidence projections

**Interfaces:**
- Consumes: source entity records, normalized Classes, and `AggregatePayload`.
- Produces: `buildStatsStageRecord(scope, classes)` and `buildClassStageRecord(classRecord)`.

- [ ] Write failing tests for all-time Lainey, Year 2026, Horse Owin, and one Competition scope using the same aggregate function.
- [ ] Assert every Stats record has `scope_type`, `scope_key`, `scope_label`, and the complete canonical aggregate payload.
- [ ] Write a failing Class-selection test proving excluded Webflow Classes do not produce stage rows.
- [ ] Implement stable names, slugs, `source_record_id`, `entity_type`, and serialized payload hashing.
- [ ] Ensure hash input excludes timestamps, sync status, errors, and Webflow response metadata.
- [ ] Run `node --test tests/webflow-cms-sync/stage-builder.test.js`; require zero failures.
- [ ] Commit the projection layer and tests.

### Task 4: Implement Airtable staging upserts

**Interfaces:**
- Consumes: dashboard/Class stage payload arrays.
- Produces: created/updated/skipped/error manifests keyed by `source_record_id` and `entity_type`.

- [ ] Write tests using an injected fake transport for pagination, 10-record write batching, retries, and partial failures.
- [ ] Implement server-side Airtable pagination and exact field-ID writes.
- [ ] Upsert by the staging identity key; never match by display name.
- [ ] Skip writes when `payload_hash` is unchanged.
- [ ] Read every created or updated staging record back and compare the normalized payload exactly.
- [ ] Run the Airtable client and stage-builder test suites; require zero failures.
- [ ] Commit the Airtable adapter.

### Task 5: Implement staged-only Stats and Class synchronization

**Interfaces:**
- Consumes: verified staging records and collection routing from `webflow_collections_index`.
- Produces: staged Webflow items plus exact readback manifests; never publishes.

- [ ] Write fake-transport tests proving `rec-id` lookup, create, update, hash skip, pagination, rate-limit retry, and exact readback.
- [ ] Add a test that fails if the client invokes any publish endpoint.
- [ ] Route `webflow_stats_stage` records to the existing Stats collection and `webflow_classes_stage` records to the existing Classes collection.
- [ ] Keep Laineys, Horses, and Competitions schema writes disabled during the pilot; use their existing items only for verified navigation references.
- [ ] Store returned `webflow_item_id` and sync result back on the staging record only after Webflow readback matches.
- [ ] Stop the affected record with FAIL on schema mismatch, unknown category, duplicate `rec-id`, or readback mismatch.
- [ ] Run `node --test tests/webflow-cms-sync/sync.test.js`; require zero failures.
- [ ] Commit the Webflow adapter and orchestration.

### Task 6: Prove one Horse slice and one Year slice end to end

**Interfaces:**
- Consumes: runner `--dry-run` and bounded entity modes.
- Produces: Airtable/Webflow comparison evidence with no publishing.

- [ ] Run a dry-run for one approved Horse scope and save source Class IDs plus the calculated payload.
- [ ] Upsert only that Horse-scope Stats staging record; read it back exactly.
- [ ] Create or update only its staged Webflow Stats item; read it back and compare every mapped field.
- [ ] Repeat the same bounded proof for the Year 2026 Stats slice.
- [ ] Confirm no unrelated staging or Webflow records changed and no publish action occurred.
- [ ] Record PASS only when both pilots have zero mismatches; otherwise return FAIL and stop.
- [ ] Commit pilot evidence and any test-backed corrections.

### Task 7: Validate story drill-down and Class reduction without deleting source truth

**Interfaces:**
- Consumes: `include_in_aggregates` and `publish_class_to_webflow` controls.
- Produces: a Class keep/archive manifest and unchanged aggregate proof.

- [ ] Generate a dry-run manifest of every current Webflow Class: keep, archive candidate, or blocked, with the exact Airtable source record ID and reason.
- [ ] Verify the Competition-first path resolves Lainey -> Competition -> Classes and each Class exposes Horse, Result, Height, Discipline, Skill, and Video evidence when present.
- [ ] Verify the Horse-first path resolves Lainey -> Horse -> Classes across multiple Competitions.
- [ ] Select a bounded Class with `publish_class_to_webflow=false` and verify it disappears from the Class staging projection while remaining in Horse, Competition, and Lainey aggregate inputs.
- [ ] Recalculate affected entities and prove their aggregate totals remain correct.
- [ ] Do not archive or delete any Webflow Class during this task; obtain separate approval for the reviewed manifest.
- [ ] Commit the manifest generator and its tests.

### Task 8: Full Stats/Class reconciliation and binding-unwind plan

**Interfaces:**
- Consumes: all staging records, existing Webflow items, and current template/component bindings.
- Produces: zero-mismatch reconciliation plus an ordered binding migration ledger.

- [ ] Run full source-to-stage reconciliation for all-time, every Year, every Horse, optional Competition scopes, and every selected Class.
- [ ] Run full stage-to-Webflow draft reconciliation and report missing, duplicate, stale, and mismatched items.
- [ ] Inventory every Stats, Competition, Horse, and Class template/component binding plus every Collection List filter/sort and reference proposed for removal or redirection.
- [ ] Produce an ordered ledger: create replacement, populate, change binding, verify rendering, then remove legacy dependency.
- [ ] Require zero data mismatches before requesting approval for any binding or Class-item removal.
- [ ] Commit reconciliation evidence and the migration ledger.

### Task 9: Runner integration and operational handoff

**Interfaces:**
- Consumes: tested `runSync(options)` from `sync.js`.
- Produces: Windows-safe commands and a scheduler-owned recurring workflow.

- [ ] Add `cms-sync:dry-run`, `cms-sync:entity`, and `cms-sync:reconcile` scripts to `package.json` without POSIX inline environment assignments.
- [ ] Document required server-side environment variable names without values.
- [ ] Document incremental handling of Class create/update/delete, including recalculation of both old and new Horse/Competition scopes after a move.
- [ ] Configure the approved runner separately from Codex; do not substitute manual calls for cadence proof.
- [ ] Execute one approved scheduled run and verify its logs, Airtable staging readback, and Webflow staged readback.
- [ ] Keep publishing disabled until the user separately approves a publish workflow.
- [ ] Commit the operational documentation.

## Final verification checklist

- [ ] `node --test tests/webflow-cms-sync/*.test.js` reports zero failures.
- [ ] Airtable contains exactly the two approved new staging tables.
- [ ] No new Webflow collection exists.
- [ ] Laineys Webflow sync remains disabled unless separately approved.
- [ ] Horse-scope and Year-scope Stats pilots have exact staged readback.
- [ ] Competition-first and Horse-first Class drill paths resolve to the same underlying evidence records.
- [ ] Class exclusion does not alter aggregate inclusion.
- [ ] Full reconciliation reports zero mismatches.
- [ ] No Class, field, reference, binding, or CMS item was removed without a reviewed manifest and separate approval.
- [ ] No Webflow publish action occurred.
