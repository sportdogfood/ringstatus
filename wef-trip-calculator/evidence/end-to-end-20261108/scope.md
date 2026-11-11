# Trip candidate scope

Owner approval on November 8, 2026 covers the Trip linker, cleaner and calculator adjustments needed for the end-to-end Trip path. Candidate execution follows saved inputs, calculation, exact links, snapshot save and readback, then the five local calculated fields. Cleanup supports the three checked owned log tables only. The candidate uses saved November 8 source rows with their original clocks.

The existing shared `wef-timing-data` route remains the release target. `candidate/entry-data.js` stages one Trip call after Ring and Class. `schema-changes.json` lists the five local calculated fields and one snapshot context field. Live schema application, deployment and normal scheduled proof are separate release gates. Ring, Class, collectors and their existing schedules are protected.
