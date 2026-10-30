# Independent publication-fixes review — 30 October 2026

Scope: actual staged publication.js and the supplemental collector summary line against this evidence directory's before copies. Read-only code review; only this review document was written. No provider calls, manual collectors, record repairs or runtime code edits.

## Findings

No actionable blocking regression found in the inspected delta. The three approved P2 corrections are implemented:

- `observations()` reads case-sensitive `has_JSON`, preserving true, false and missing. The saved source fixture's referenced archive hash was independently verified and the source contains `has_JSON: true`.
- `publish()` distinguishes planned/attempted work, deferred work and pending uncertain outcomes. A pending-write early exit counts untouched plans; pre-intent failures count as deferred. Pending intents remain durable and later readback precedes retry. The collector includes deferred and uncertain counts separately in its persisted summary.
- Destination validation compares stable identity, matching evidence, native links and overlay state before and after PATCH. Unrelated acquisition metadata may change without falsely stopping publication. Trip source identity and timestamps supporting identity differences remain protected. Full before/readback evidence remains archived.

The external PATCH transport, freshness and TODAY guards, lane budgets, archive/retention behavior and collection execution are not changed by this delta. Multi-group and ambiguous-match withholding remain intentional safeguards.

## Independently executed offline checks

- Publication tests: 26 passed.
- Collector tests: 45 passed, including separate deferred/uncertain counts in run-logs and heartbeat summaries.
- Saved source archive SHA256 matches fixture provenance; actual source spelling verified.

These checks do not prove deployment or two populated normal scheduled runs. Budget-limited coverage, concurrent acquisition in normal operation, and populated publication acceptance remain operational verification requirements. No overall production PASS is asserted.

## Reviewed SHA256

- publication.js: `D7DCE9FC11C11738A658769CE28463AC09DD047EAF40A59530329200EA2F22A3`
- collector.js: `8CD0247371F17A9C6552E104329FC00F9197612D0FEEE6AB74BD2BBD731915AE`
- test/publication.test.js: `6C6DB9A555E337B7D7054E46CF868C28232587581AE4BA88DF6C8D84FECDDEA5`
- test/collector.test.js: `E4122AC1AC07398E609B33CF0034A9A067ECBC982E3B47F123DB1DBF27377346`
- test/publication-source-evidence.json: `485A396ED37E2542CC0713D55D0F27FC8DA15C7AFB19D3761758C87C1E612B31`
