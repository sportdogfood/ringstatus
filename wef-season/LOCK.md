# Controlling approved repair — November 7, 2026

Season remains owner-locked to four entity transfers, supported trip/show/day/person links, hourly operation and accurate silent error logging. The approved three-file repair is deployed.57 tests and independent review passed; normal hourly recovery was verified after the November7 8:51a.m.Eastern run; the7:51p.m.repeat also succeeded with6record-specific Ring skips. Further code/schema/task/scope changes still require explicit approval; the existing approved read-only hourly verification continues. No business-error persistent stops or added readiness gates; uncertain native writes retain their existing reconciliation protection. Historical lock details follow and are superseded where different.

Current SHA256:
- season.py: `6f91f80f150a5e1b3fa89bec606862228d0ec98f9aa198fff865838d7c55f404`
- horse_associations.py: `50b93880ef96f9f5476904e0ce618692c6d862e90567230c69a34c1500e5fd7a`
- entry.js: `f7dacc1ed911eaafc7f5ceb8ba8d59031702909ed1817a8c766d040968a956e9`

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
