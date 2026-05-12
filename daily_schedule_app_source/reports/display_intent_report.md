# Daily Schedule App Display Intent

Generated: 2026-05-12T22:11:04.874Z

This is not a render specification. It prevents raw source JSON from being mistaken for display shape.

## heartbeat

Records: 1

Display candidates: app_dow_raw, app_sql_date, mode

Timing candidates: none

Not for primary display:
- keys: id, record_id
- evidence: none
- calculator state: none
- operational: none

Other data candidates: app_show_id, hb_at, heartbeat_id, show_id, time

## show

Records: 2

Display candidates: none

Timing candidates: none

Not for primary display:
- keys: id
- evidence: none
- calculator state: none
- operational: none

Other data candidates: app_dow_rawv2, app_show_idv2, app_sql_datev2, show_id, sid, sql_date

## rings

Records: 6

Display candidates: ringName, ring_nickname, ring_number, status

Timing candidates: none

Not for primary display:
- keys: id, schedule_key
- evidence: none
- calculator state: none
- operational: none

Other data candidates: sid, sql_date

## groups

Records: 105

Display candidates: completed_trips, group_name, group_name_tags, status, total_trips

Timing candidates: none

Not for primary display:
- keys: id, schedule_instance_key, schedule_key, schedule_record_id
- evidence: schedule_tie_breaker_source
- calculator state: schedule_tie_breaker
- operational: none

Other data candidates: class_group_id, class_group_sequence, class_groupxclasses_id, schedule_short

## class_start

Records: 105

Display candidates: none

Timing candidates: estimated_start_time

Not for primary display:
- keys: id, schedule_instance_key, schedule_key, schedule_record_id
- evidence: latest_schedule_log, latest_schedule_log_record_id, schedule_tie_breaker_source
- calculator state: schedule_tie_breaker
- operational: manual_time_override

Other data candidates: none

## classes

Records: 105

Display candidates: class_name, class_number, class_type, schedule_sequencetype

Timing candidates: none

Not for primary display:
- keys: id, schedule_instance_key, schedule_key, schedule_record_id
- evidence: schedule_tie_breaker_source
- calculator state: schedule_tie_breaker
- operational: none

Other data candidates: class_id, class_sequence

## entries

Records: 2

Display candidates: entry_number, pid

Timing candidates: none

Not for primary display:
- keys: id, schedule_key, trip_instance_key, trips_key, trips_short_key
- evidence: trip_tie_breaker_source
- calculator state: trip_tie_breaker
- operational: none

Other data candidates: entry_sequence, h_eid

## trip_go

Records: 2

Display candidates: entry_number, pid

Timing candidates: estimated_start_time

Not for primary display:
- keys: full_nesting_key, id, schedule_key, schedule_record_id, trip_instance_key, trip_record_id, trips_key
- evidence: latest_trip_log_record_id, rs_diff, rs_latest_log, trip_tie_breaker_source
- calculator state: rs_current, trip_tie_breaker
- operational: none

Other data candidates: entry_sequence

## trips

Records: 2

Display candidates: entry_number, pid

Timing candidates: none

Not for primary display:
- keys: id, schedule_key, schedule_record_id, trip_instance_key, trip_record_id, trips_key
- evidence: trip_tie_breaker_source
- calculator state: trip_tie_breaker
- operational: is_target, last_seen_at, latest_ingested_at

Other data candidates: entry_sequence

## horses

Records: 2

Display candidates: horse, horse_name

Timing candidates: none

Not for primary display:
- keys: id, trips_key
- evidence: none
- calculator state: none
- operational: none

Other data candidates: h_eid

## riders

Records: 1

Display candidates: pid, rider_name

Timing candidates: none

Not for primary display:
- keys: id, trips_key
- evidence: none
- calculator state: none
- operational: none

Other data candidates: none
