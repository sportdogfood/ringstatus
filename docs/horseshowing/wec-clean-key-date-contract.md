# WEC Clean Key/Date Contract

## Status

Active contract for the clean WEC workflow. Older workflow/key docs are reference only unless this contract is explicitly superseded.

## Purpose

Lock date fields, control ownership, and canonical keys so Catalyst and Airtable mirrors do not drift or duplicate rows.

## Control Source

| Item | Contract |
| --- | --- |
| Workflow control | Airtable `focus_show` |
| Active day source | Airtable `focus_show.focus_day` |
| Catalyst `hs_focus_show` | Mirror/reference only |
| Airtable `hs_focus_show` | Mirror/review only |
| Workflow trigger | `focus_show`, not `hs_focus_show` |

## Required Date Fields

| Field | Format | Example | Rule |
| --- | --- | --- | --- |
| `focus_day` | ISO date | `2026-07-05` | Required on every core row |
| `iso_date` | ISO date | `2026-07-05` | Same value as `focus_day` |
| `focus_day_key` | compact date | `20260705` | Used in canonical keys |

## Core Tables

These tables must carry `focus_day`, `iso_date`, and `focus_day_key`.

| Table | Role |
| --- | --- |
| `hs_heartbeat` | cadence proof |
| `hs_focus_show` | focus mirror/reference |
| `hs_get_ring_days` | ring-day source |
| `hs_update_schedule` | schedule source |
| `hs_class_oog_raw` | class_oog raw probe payload |
| `hs_class_oog` | parsed class_oog entries |
| `hs_ring_status` | runtime ringwise state |
| `hs_class_start_times` | runtime classwise state |
| `hs_entry_go_times` | runtime entrywise state |

## Canonical Const Keys

| Level | Key |
| --- | --- |
| showwise | `show_no` |
| focus-day | `show_no|focus_day_key` |
| ring-day | `show_no|focus_day_key|ring_day_no` |
| ringwise | `show_no|focus_day_key|ring_day_no|ring_no` |
| classwise | `show_no|focus_day_key|ring_day_no|ring_no|class_no` |
| entrywise | `show_no|focus_day_key|ring_day_no|ring_no|class_no|entry_no` |

## Stage Key Ownership

| Table | Required Key Level |
| --- | --- |
| `hs_get_ring_days` | ringwise |
| `hs_update_schedule` | classwise |
| `hs_class_oog_raw` | classwise raw document key |
| `hs_class_oog` | entrywise |
| `hs_ring_status` | ringwise |
| `hs_class_start_times` | classwise |
| `hs_entry_go_times` | entrywise |

## Deprecated Workflow Identity

Do not use these fields for dedupe, handoff, or canonical workflow identity:

| Field | Status |
| --- | --- |
| `ring_visual_key` | deprecated for identity |
| `class_visual_key` | deprecated for identity |
| `entry_visual_key` | deprecated for identity |
| `ring_name_slugified` | deprecated for identity |

They may remain temporarily for legacy display/reference while old rows are retired.

## Mirror Rule

| Item | Contract |
| --- | --- |
| Canonical runtime data | Catalyst `hs_*` tables |
| Airtable `hs_*` tables | mirrors for visibility/review |
| Catalyst row id | keep `ROWID` where applicable |
| Airtable row id | keep `rec_id` where applicable |
| Mirror fields | must preserve the same date and canonical key fields |

## Verification Evidence

| Evidence | Result |
| --- | --- |
| Verified on | `2026-07-06` |
| Active focus day | `2026-07-05` |
| Catalyst core tables have `focus_day=2026-07-05` and `iso_date=2026-07-05` | PASS |
| Airtable core mirrors have `focus_day=2026-07-05` and `iso_date=2026-07-05` | PASS |
| `focus_show` remains workflow control source | PASS |
| `hs_focus_show` is mirror/reference only | PASS |
| Live/results/alerts/output lanes ran during verification | NO |

## Drift Rules

- No old-day fallback.
- No visual-key dedupe or handoff.
- No `hs_focus_show` control replacement for Airtable `focus_show`.
- No downstream stage should run without the required upstream canonical key.
- Airtable mirrors must not invent alternate key shapes.
