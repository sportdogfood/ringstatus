# Tenant-independent participation matcher

Version 1.0 — 29 October 2026

Implemented in participation-linker.js and used by the prepared tenant-independent function at evidence/tenant-independent-20261029/staged/collector.js. It is not connected to the running collector. No task, Airtable schema, CWF workflow or production input was switched.

## Behavior

- Normalize source-confirmed entry, class, horse, rider and optional trainer identities without tenant configuration. Preserve participation keys and original observation time.
- Copy the actual local schedule key/link when show, class and source date have one active match and the ring number agrees. Mark the basis class-date; this does not claim group eligibility.
- Separately validate observed group membership using exact own-class trip ID/UUID and entity identity, same accepted revision, complete parent-group inventory, full group-order class roster equality, distinct ring ID/number and one operational group occurrence. Duplicate rosters or occurrences stay unresolved. Copy producer keys; never invent or fan out links.
- Retain supported rows when group evidence is missing, partial or contradictory. The prepared function reports per-entry linkIssues separately from invalid source scopes. A schedule-sync failure withholds schedule links but retains validated source facts.
- A complete empty class succeeds. Missing or malformed source data is not an empty success. Optional messages, results and waze are not called.

## Input boundary

linkClass(payload, show, localSchedules, observedAt, options) is pure: no network, file writes, collector execution or database writes.

The caller validates retained hashes, source freshness, canonical context and source/local schedule parity before allowing schedule links. options.sourceEvidence.revision and options.inventory.revision must identify the same accepted revision for group assertions. inventory.complete must be true; inventory carries customerId, showId and groups with explicit groupId, date, ringNumber, ringId, classIds, plus occurrences when available. Completeness cannot be inferred from an empty or partial group-order list.

These assertions are an adapter contract, not a replacement for retained-input validation. Synthetic revision labels in offline fixtures are explicitly marked fixture-only. The production accepted-inventory reader is not activated by this change.

## Output boundary

Each supported row keeps source entity IDs and separate schedule-link-status/basis and group-link-status/reason. Actual trainer identities use source-trainer-ids, never the old trainer-ids feed-membership field. No tenant ownership or private-follow fields are generated.

An unresolved schedule link is omitted, not cleared. Future persistence must preserve prior verified links as historical evidence and must not treat them as fresh eligibility. These in-memory status fields are not newly created Airtable fields.

## Saved evidence tests

- Historical class347: exact trip and complete group roster yield the saved active schedule key; the old dropped key is excluded.
- Same-run class245: matching trip IDs do not override the contradictory parent-group roster. Core facts remain available.
- Current saved class671: partial group roster stays unresolved; the unique class/date schedule association remains available.
- Additional tests cover duplicate groups/occurrences, source occurrence conflicts, ring ID versus number, identity mismatch, missing evidence, empty classes, stable replay and continuation across affected rows/scopes.

Reduced saved fixtures and hashes of their originals are in tests/fixtures/narrow-matcher/provenance.json. Schedule snapshots used by these tests are not asserted to prove same-revision live acceptance. Mutated counterfixtures are offline checks only.

## Verification and release boundary

46 Node offline checks pass, including 15 matcher checks and five prepared-function checks; four retained-input Python checks also pass. Existing IO tests use mocks. No source endpoints, manual collectors or record repairs were invoked.

This approval implemented and tested the matcher. Deployment, production persistence/input activation, export decisions and downstream reader changes were not included. CWF remains outside the new matcher and prepared function. The existing recurring collector remains unchanged.


29 October Conflict refinement: multiple active class/date schedule matches now return exact schedule-link-status Conflict when accepted group evidence cannot disambiguate. Missing matches remain unresolved. Conflict preserves entity facts and prior links as history; unrelated entries continue. No source freshness or group eligibility rule changed.
