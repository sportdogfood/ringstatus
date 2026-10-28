# Enclosing ring identity — local correction, 15 October 2026
Approved local-only correction after the failed scheduled inheritance release. No deployment, task changes or business runs.

Root cause reproduced from actual rendered Wellington Oct16/show200000074 DOM: the grouped787/835/720 row prints08:00 but contains no ring/cgid parameters; its child rows also have no ring links. The enclosing VanKampen table contains other detail links consistently identifying ring3. Previous matcher rejected the correctly parsed time because row ring was null.

Minimal delta in private collector-core.js: preserve table boundaries and derive table ring only when its detail links give one unambiguous ring ID. Rows lacking ring parameters inherit that table identity. Existing explicit row identities remain. Inheritance state resets for every table and every parser call/day. Unknown/conflicting table identity remains unresolved; no header-name guessing or table-index assignment. Existing group/class matching and override logic unchanged.

Fixture tests/fixtures/rendered-vankampen-20261016.html contains the actual group, child and744 row HTML plus observed enclosing table header, extracted from the read-only DOM inspection. It is a relevant-row excerpt, not a complete saved page. Current rendered744 time was08:30 (the earlier screenshot showed08:27); the test uses the captured08:30.

Full offline suite:58 passed,0 failed. Output table-ring-tests.txt. Actual720/787/835 each resolve08:00 within ring3. Tests cover ambiguous/unknown table rings, missing-link inheritance, independent table/day resets, manual overrides and failure preservation. Prior all-day, zero-entry, history and isolation checks pass.

Not deployed. Next release requires explicit owner approval and normal scheduled verification. The existing deployed parser remains unchanged. Local rendered-fixture proof is not a live recurring-run claim.
