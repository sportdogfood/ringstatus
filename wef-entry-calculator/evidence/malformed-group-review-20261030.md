# Malformed-group correction — writer review

Skill: `C:/Users/gombc/.agents/skills/code-review/SKILL.md`, explicitly requested by human in coordinator approval turn `01a18e50-5cba-7db2-b4cd-04d6b2e7a51d`. This is writer review, not independent acceptance.

CONFIRMED baseline defect: shared runner retained original raw groups for ambiguity; entry adapter passed a `rows:[null]` group into the class adapter, throwing and persisting entry_failed. The earlier shared tests stubbed entry evaluation, hiding the cross-module consequence. Baseline source is the previous frozen staging copy and `review-fixes-hashes-20261030.json`.

Correction scope: only worker.js and calculator.test.js. Before filtering, the worker counts exact raw class, group and stage-key occurrences. It then adapts only caller-accepted, structurally safe groups; duplicate groups exclude all instances regardless of order. Malformed/excluded groups produce scoped issues. Known occurrences in excluded groups still prevent false unique attribution. No input mutation, fallback identity or swallowed IO/hash failure was introduced. Stage/checkpoint/publication code is unchanged.

PASS:23 Node tests, raw `node-malformed-group-20261030.txt`. Real entry stage/preflight/control readbacks exercised with null group, null row and nonarray rows, both orders and repeats; duplicate good/bad group with valid unrelated group; excluded raw duplicate occurrence. Valid unrelated output persists, repeat writes zero answers, scoped status stays partial, entry_failed remains absent. Existing uncertain answer readback still persists intent and blocks. Existing phase/hash/source/date/retention tests remain passing. Python source unchanged; prior project-runtime4/4 evidence remains applicable.

Writer inspection traced raw inventory -> ambiguity maps -> safe subset -> unchanged class adapter -> entry scoped issues -> stage partial logs/checkpoint -> next-run preflight. No additional actionable defect identified within this narrow delta. Full shared run with real entry stage is Ring-owned; independent Class review and deployment/cadence remain unverified at this receipt. Source semantics still cannot release phase-dependent go-time predictions. No deployed or manual run claimed.

Frozen candidate: `malformed-group-hashes-20261030.json`. Next: Ring full real-stage regressions, independent combined review, coordinator-controlled deployment and scheduled proof. Stop affected path on a new verification failure.

Independent follow-up: Class-owner turn `01a18e51-e73e-7b00-aa82-9653d1d46e76` reports PASS for this entry-worker delta, no new actionable finding, all 11 hashes matched, saved23/23 tests reviewed including real stage/logging, both orders and repeat runs. This is separate from the writer review above; combined shared runner acceptance remains pending.
