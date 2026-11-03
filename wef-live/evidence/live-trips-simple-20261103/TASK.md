# Simplify live-trips — November3,2026

Latest owner correction says live-trip-results/messages/comments are unnecessary, following the approved trips build. Remove those three newly created empty tables and their new reciprocal fields/control rows. Each was GET-verified empty before preparation. Keep results directly on live-trips (placing,position,results-verified,class-status,results-json), with full source facts preserved. Downstream messages/comments reference the trip key; no local child tables or sending workflow.

Approved correction changes only live-trips.js, lanes.js, schema.js and cleanup.js plus new trip result fields and removal of the three empty tables/configuration rows. Preserve original trip key,existing class/ring/group/schedule/legacy links,source mapping,day0behavior,legacy live-entries,logging,active/current protection,delete-only cleanup and existing task/cadence. No manual collector/source calls,backfill,record repairs,new schedule,gates,dependencies or other-base changes. General lock remains outside this correction.

Baseline25JS/task captured before edit. Test/review before deploy. Remove obsolete schema only after collector no longer references it; GET-readback all changes. Natural inactive scheduled runs are not populated trip proof.
