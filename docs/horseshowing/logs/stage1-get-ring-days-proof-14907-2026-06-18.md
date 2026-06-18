# Stage 1 Proof - get_ring_days

Status: PASS
Show: 14907
Focus day: 2026-06-18

## Source
- Endpoint: get_ring_days.php
- Required session: PHPSESSID + HscomShowNo
- Source rows: 10 rings, 56 ring-day rows, 10 focus-day rows

## Catalyst
- sync-ring-days first pass: ok=true, upstream_status=200, parsed_rows=56
- sync-ring-days repeat pass: ok=true, upstream_status=200, parsed_rows=56

## Airtable Staging Selection
- total_get_ring_days: 56
- eligible_focus_day_rows: 10
- selected_count: 10
- ring_day_no: 3937, 3943, 3949, 3955, 3961, 4220, 4225, 4233, 4301, 4306

## Cleaned Errors
- direct Invalid parameter: fixed by proving required PHPSESSID + HscomShowNo request shape.
- Catalyst timeout: repeatable deployed sync-ring-days now passes twice.
