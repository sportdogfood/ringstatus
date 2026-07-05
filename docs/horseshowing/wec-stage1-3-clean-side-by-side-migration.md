# WEC Stage 1-3 Clean Side-by-Side Migration

## Purpose

This document locks the migration from the current WEC Stage 1-3 workflow to the clean isolated Stage 1-3 path.

The current workflow stays alive until the clean path proves itself under the approved gate. The old path is deprecated only after success. Nothing is deleted as part of this migration.

## Current Proven Clean Path

| Item | Status |
|---|---|
| Clean function | `wec_stage1_3_clean_proof` |
| Location | `ringstatus-data/catalyst-workspaces/horseshowing/functions/wec_stage1_3_clean_proof` |
| Environment | Catalyst Development |
| Existing workflow used | no |
| Existing workflow mutated | no |
| Latest proof | PASS |
| Missing contract fields | none |

## Migration Rule

| Rule | Contract |
|---|---|
| Current workflow | keep running until replacement is proven |
| Clean workflow | runs side-by-side first |
| Old Stage 1-3 | deprecated only after clean cadence proof |
| Delete old code | no |
| Patch old code | no, unless separately approved |
| Manual wrangling | not accepted as replacement proof |

## Clean Stage Contract

| Stage | Source | Destination | Required Result |
|---|---|---|---|
| 0 | `focus_show` | active control | one active show/day, not paused |
| 1 | `focus_show` | `hs_heartbeat` + `hs_get_ring_days` | heartbeat proof and current focus-day ring days |
| 2 | `hs_get_ring_days` | `hs_update_schedule` | current focus-day schedule, preflight classified |
| 3A | non-preflight `hs_update_schedule` | `hs_class_oog_raw` + source row probe fields | one native class request, charcount/certainty/progress marked |
| 3B | `hs_class_oog_raw` | `hs_class_oog` | stored raw parsed; scoped entries materialized |

## Key Contract

| Key | Shape |
|---|---|
| `show_const_key` | `show_no` |
| `focus_day_const_key` | `show_no + focus_day_key` |
| `ring_day_const_key` | `show_no + focus_day_key + ring_day_no` |
| `ring_const_key` | `show_no + focus_day_key + ring_day_no + ring_no` |
| `class_const_key` | `show_no + focus_day_key + ring_day_no + ring_no + class_no` |
| `entry_const_key` | `show_no + focus_day_key + ring_day_no + ring_no + class_no + entry_no` |
| `ring_visual_key` | locked visual key only |
| `class_visual_key` | locked visual key only |
| `entry_visual_key` | locked visual key only |

## Deprecation Gate

| Gate | PASS Requirement |
|---|---|
| Clean bounded proof | Stage 1 through 3B passes |
| Schema proof | `missing_contract_fields = []` |
| Cadence proof | approved cadence-owned clean run passes |
| Output | `hs_class_oog` receives scoped rows |
| Isolation | no Step 4, live, alerts, results, mobile, print, or PDF run |

## Switch Plan

| Phase | Action |
|---|---|
| 1 | Keep current workflow active |
| 2 | Run clean Stage 1-3 side-by-side |
| 3 | Prove clean Stage 1-3 under approved cadence path |
| 4 | Compare expected handoff counts and key fields |
| 5 | Point Stage 1-3 cadence ownership to clean path |
| 6 | Mark old Stage 1-3 as deprecated |
| 7 | Leave old code in place until separately approved for removal |

## Explicit Exclusions

| Excluded |
|---|
| Step 4 runtime prep |
| Step 5 live enrichment |
| Step 6 results |
| alerts/messages |
| mobile |
| mobile-pro |
| print |
| PDF |
| two-way |
| production deploy |
| Webflow publish |

