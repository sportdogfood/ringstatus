# Pending corrections — no implementation authorization assumed

Independent reviewer reproduced four candidate failures. Existing code is frozen; do not deploy, run production cleanup, or rerun the failed affected checks. Root failed-verification rule requires separate APPROVED TO EDIT before correcting code.

Smallest proposed delta, limited to class-status.js, class-status-cleanup.js, focused offline fixtures and contract:

1. Propagate unknown preceding-group finish through upcoming timing. Keep supported schedule times visibly separate; do not present a past planned start as a supported current forecast. A round/phase without known remaining workload cannot yield a finish used by subsequent classes.
2. Contain a bad class observation clock to that class. Keep its snapshot with unavailable reasons and no dependent timing; continue other classes. Do not invent or refresh an observation time.
3. Validate old-show answer/checkpoint identity and date before deletion eligibility. Missing or ambiguous dates/identity remain protected even if an ID is declared unreferenced/retired. Current/future records remain protected for every show.
4. Enforce the existing 24-hour minimum and full verified archive/hash/catalog/readback requirement for timing-answers before deletion, reusing existing proven retention evidence. If no suitable evidence is supplied, withhold deletion and report the exact missing proof. An unreferenced-ID list alone is insufficient. Do not introduce a parallel archive system or weaken previous obligations.

Then run focused offline checks using the same saved November 6 corpus and reviewer counterexamples, and obtain independent re-review. No dependency upgrade, source schema change, runner/task change, deployment, Records alert or manual production run is part of this proposal. Production writer/cleanup transport remains a separate unverified integration gate.
