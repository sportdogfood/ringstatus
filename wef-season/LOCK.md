# wef-season — owner-locked scope and deployed baseline

Locked by the owner November 4, 2026. Version 2026.11.04.1.

1. Add, upsert and deduplicate existing Data horses, riders, trainers and rings into their four Season bases using established keys.
2. Maintain supported show/day links and horse rider/trainer links from trips. Allow multiples; preserve all records, keys and history.
3. Run through the existing hourly Season task and accurately report results and genuine errors.

Further code, schema, scope, cadence or workflow changes require explicit owner approval. Read-only verification and the existing hourly workflow continue. This is a code/scope lock, not a runtime stop or an added processing gate. No Data or sibling workflow changes, entry-based horse assignments, new readiness gates, adapters, archive work or cleanup are authorized.

The November 4 trip-only correction is deployed; 37 tests and independent review passed. Hourly acceptance remains pending the normal 3:51 p.m. Eastern run. Locking does not claim acceptance. The already-approved read-only hourly verification remains authorized. Historical conflict records are preserved; existing log-delivery timeout evidence remains unresolved by this change.

Deployed SHA256:

- season.py: `8da713fe2239869edad3bdfc1a845f013ac640e90dff32783b63b48e6399f15e`
- horse_associations.py: `22ee075396ed2ce90a1af696150be5314ca1beffb22267fd7ad78f256699678f`
- schema.json: `1b52e9cee74867311f367c7177f14f74ce58e87b1da8c651d7d7d8e3ee29716a`
- entry.js: `5e6df8a489e7ce71b98636987997815ca2ad83d0d2a4b12518629ad9e14b69f8`
- run.ps1: `9b57660513d75d8c1e514eb09d46d08d7bd77e1065c2511fd388bdee4697f043`
