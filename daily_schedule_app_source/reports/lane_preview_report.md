# Daily Schedule App Lane Preview

Generated: 2026-05-12T22:05:24.634Z
Source generated: 2026-05-12T20:25:11.456Z

This report does not make render, nesting, or flyup decisions. It only shows what the current source payload contains.

## Primary Lanes

### heartbeat

Records: 1
Fields: 10

| Field | Populated | Blank |
| --- | ---: | ---: |
| app_dow_raw | 1 | 0 |
| app_show_id | 1 | 0 |
| app_sql_date | 1 | 0 |
| hb_at | 1 | 0 |
| heartbeat_id | 1 | 0 |
| id | 1 | 0 |
| mode | 1 | 0 |
| record_id | 1 | 0 |
| show_id | 1 | 0 |
| time | 1 | 0 |

Sample:

```json
{
  "id": "recLGsOxY3F2T6ZxT",
  "record_id": "recLGsOxY3F2T6ZxT",
  "heartbeat_id": "200000062-2026-05-12-1778617443",
  "hb_at": "2026-05-12T20:24:03.675Z",
  "show_id": 200000062,
  "app_show_id": 200000062,
  "app_sql_date": "2026-05-28",
  "app_dow_raw": "Thu",
  "mode": "DAY",
  "time": "04:24 PM"
}
```

### show

Records: 2
Fields: 7

| Field | Populated | Blank |
| --- | ---: | ---: |
| app_dow_rawv2 | 2 | 0 |
| app_show_idv2 | 2 | 0 |
| app_sql_datev2 | 2 | 0 |
| id | 2 | 0 |
| show_id | 2 | 0 |
| sid | 2 | 0 |
| sql_date | 2 | 0 |

Sample:

```json
{
  "id": "show:200000061:2026-05-10",
  "sid": 200000061,
  "sql_date": "2026-05-10",
  "show_id": 200000061,
  "app_show_idv2": 200000061,
  "app_sql_datev2": "2026-05-10",
  "app_dow_rawv2": "Sun"
}
```

### rings

Records: 6
Fields: 8

| Field | Populated | Blank |
| --- | ---: | ---: |
| id | 6 | 0 |
| ring_nickname | 5 | 0 |
| ring_number | 6 | 0 |
| ringName | 5 | 0 |
| schedule_key | 6 | 0 |
| sid | 6 | 0 |
| sql_date | 6 | 0 |
| status | 5 | 0 |

Sample:

```json
{
  "id": "ring:200000061:2026-05-10:3",
  "schedule_key": "200000061|2026-05-10|3|723|2",
  "sid": 200000061,
  "sql_date": "2026-05-10",
  "ring_number": 3,
  "ringName": "Covered",
  "ring_nickname": "Covered",
  "status": "Completed"
}
```

### groups

Records: 105
Fields: 15

| Field | Populated | Blank |
| --- | ---: | ---: |
| class_group_id | 105 | 0 |
| class_group_sequence | 105 | 0 |
| class_groupxclasses_id | 105 | 0 |
| completed_trips | 76 | 0 |
| group_name | 105 | 0 |
| group_name_tags | 86 | 0 |
| id | 105 | 0 |
| schedule_instance_key | 105 | 0 |
| schedule_key | 105 | 0 |
| schedule_record_id | 105 | 0 |
| schedule_short | 105 | 0 |
| schedule_tie_breaker | 105 | 0 |
| schedule_tie_breaker_source | 105 | 0 |
| status | 76 | 0 |
| total_trips | 76 | 0 |

Sample:

```json
{
  "id": "group:rec4x2rSLXbPpwqCR",
  "schedule_key": "200000061|2026-05-10|3|723|2",
  "schedule_instance_key": "200000061|2026-05-10|3|723|2|cgid:200023880",
  "schedule_short": "3|723|2",
  "schedule_tie_breaker": 1,
  "schedule_tie_breaker_source": "entry_sequence",
  "schedule_record_id": "rec4x2rSLXbPpwqCR",
  "class_group_id": 200023880,
  "class_group_sequence": 2,
  "class_groupxclasses_id": 200035732,
  "group_name": "$2,640 NAL 1.20m Junior/Amateur Jumper Classic II2b",
  "group_name_tags": "isFirstUp, target, 1.20m, amateur, junior, jumpers, Over Fences",
  "total_trips": 15,
  "completed_trips": 15,
  "status": "Completed"
}
```

### class_start

Records: 105
Fields: 10

| Field | Populated | Blank |
| --- | ---: | ---: |
| estimated_start_time | 87 | 0 |
| id | 105 | 0 |
| latest_schedule_log | 105 | 0 |
| latest_schedule_log_record_id | 15 | 0 |
| manual_time_override | 6 | 0 |
| schedule_instance_key | 105 | 0 |
| schedule_key | 105 | 0 |
| schedule_record_id | 105 | 0 |
| schedule_tie_breaker | 105 | 0 |
| schedule_tie_breaker_source | 105 | 0 |

Sample:

```json
{
  "id": "class_start:rec4x2rSLXbPpwqCR",
  "schedule_record_id": "rec4x2rSLXbPpwqCR",
  "schedule_key": "200000061|2026-05-10|3|723|2",
  "schedule_instance_key": "200000061|2026-05-10|3|723|2|cgid:200023880",
  "schedule_tie_breaker": 1,
  "schedule_tie_breaker_source": "entry_sequence",
  "estimated_start_time": "07:45:00",
  "latest_schedule_log_record_id": "recvyDsxxb7hYX0tY",
  "latest_schedule_log": {
    "created_at": "2026-05-10T20:54:37.982Z",
    "rs_start_time": "07:45:00",
    "calc_status": "unchanged"
  }
}
```

### classes

Records: 105
Fields: 12

| Field | Populated | Blank |
| --- | ---: | ---: |
| class_id | 76 | 0 |
| class_name | 105 | 0 |
| class_number | 105 | 0 |
| class_sequence | 105 | 0 |
| class_type | 105 | 0 |
| id | 105 | 0 |
| schedule_instance_key | 105 | 0 |
| schedule_key | 105 | 0 |
| schedule_record_id | 105 | 0 |
| schedule_sequencetype | 105 | 0 |
| schedule_tie_breaker | 105 | 0 |
| schedule_tie_breaker_source | 105 | 0 |

Sample:

```json
{
  "id": "class:rec4x2rSLXbPpwqCR",
  "schedule_record_id": "rec4x2rSLXbPpwqCR",
  "schedule_key": "200000061|2026-05-10|3|723|2",
  "schedule_instance_key": "200000061|2026-05-10|3|723|2|cgid:200023880",
  "schedule_tie_breaker": 1,
  "schedule_tie_breaker_source": "entry_sequence",
  "class_id": 200025174,
  "class_number": 723,
  "class_sequence": 2,
  "class_name": "$2,640 NAL 1.20m Junior/Amateur Jumper Classic II2b",
  "class_type": "Jumpers",
  "schedule_sequencetype": "Over Fences"
}
```

### entries

Records: 2
Fields: 11

| Field | Populated | Blank |
| --- | ---: | ---: |
| entry_number | 2 | 0 |
| entry_sequence | 2 | 0 |
| h_eid | 2 | 0 |
| id | 2 | 0 |
| pid | 2 | 0 |
| schedule_key | 2 | 0 |
| trip_instance_key | 2 | 0 |
| trip_tie_breaker | 2 | 0 |
| trip_tie_breaker_source | 2 | 0 |
| trips_key | 2 | 0 |
| trips_short_key | 2 | 0 |

Sample:

```json
{
  "id": "entry:200000061|2026-05-10|6|411|2|8778|2815",
  "trips_key": "200000061|2026-05-10|6|411|2|8778|2815",
  "trip_instance_key": "200000061|2026-05-10|6|411|2|8778|2815|entry_sequence:5",
  "trips_short_key": "411|2|8778|2815",
  "schedule_key": "200000061|2026-05-10|6|411|2",
  "pid": 8778,
  "entry_number": 2815,
  "entry_sequence": 5,
  "trip_tie_breaker": "entry_sequence:5",
  "trip_tie_breaker_source": "entry_sequence",
  "h_eid": 2815
}
```

### trip_go

Records: 2
Fields: 17

| Field | Populated | Blank |
| --- | ---: | ---: |
| entry_number | 2 | 0 |
| entry_sequence | 2 | 0 |
| estimated_start_time | 2 | 0 |
| full_nesting_key | 2 | 0 |
| id | 2 | 0 |
| latest_trip_log_record_id | 2 | 0 |
| pid | 2 | 0 |
| rs_current | 2 | 0 |
| rs_diff | 2 | 0 |
| rs_latest_log | 2 | 0 |
| schedule_key | 2 | 0 |
| schedule_record_id | 2 | 0 |
| trip_instance_key | 2 | 0 |
| trip_record_id | 2 | 0 |
| trip_tie_breaker | 2 | 0 |
| trip_tie_breaker_source | 2 | 0 |
| trips_key | 2 | 0 |

Sample:

```json
{
  "id": "trip_go:200000061|2026-05-10|6|411|2|8778|2815",
  "trip_record_id": "recze5aOT8o3Eewos",
  "schedule_record_id": "rec4bmPTHYMu294Ca",
  "trips_key": "200000061|2026-05-10|6|411|2|8778|2815",
  "trip_instance_key": "200000061|2026-05-10|6|411|2|8778|2815|entry_sequence:5",
  "schedule_key": "200000061|2026-05-10|6|411|2",
  "full_nesting_key": "200000061|2026-05-10|6|08:40:00|200023861|411|2|8778|2815",
  "pid": 8778,
  "entry_number": 2815,
  "entry_sequence": 5,
  "trip_tie_breaker": "entry_sequence:5",
  "trip_tie_breaker_source": "entry_sequence",
  "estimated_start_time": "08:40:00",
  "rs_current": {
    "rs_trip_time2": 180,
    "rs_mins_since_start": 915,
    "rs_trip_default": 180,
    "rs_start_time": "08:40:00",
    "rs_start_time (from last_log)": [
      "08:40:00"
    ],
    "rs_trip_time": 180,
    "rs_mins_till_start": -915
  },
  "latest_trip_log_record_id": "rec8gg6reWlqlDWNW",
  "rs_latest_log": {
    "rs_mins_since_start": 915,
    "rs_mins_till_start": -915,
    "rs_trip_time": 180,
    "rs_start_time": "08:40:00",
    "rs_trip_default": 180,
    "rs_trip_time2": 180,
    "rs_run_id": "2026-05-12T20:21:23.187Z"
  },
  "rs_diff": {}
}
```

### trips

Records: 2
Fields: 14

| Field | Populated | Blank |
| --- | ---: | ---: |
| entry_number | 2 | 0 |
| entry_sequence | 2 | 0 |
| id | 2 | 0 |
| is_target | 2 | 0 |
| last_seen_at | 2 | 0 |
| latest_ingested_at | 2 | 0 |
| pid | 2 | 0 |
| schedule_key | 2 | 0 |
| schedule_record_id | 2 | 0 |
| trip_instance_key | 2 | 0 |
| trip_record_id | 2 | 0 |
| trip_tie_breaker | 2 | 0 |
| trip_tie_breaker_source | 2 | 0 |
| trips_key | 2 | 0 |

Sample:

```json
{
  "id": "trip:200000061|2026-05-10|6|411|2|8778|2815",
  "trip_record_id": "recze5aOT8o3Eewos",
  "schedule_record_id": "rec4bmPTHYMu294Ca",
  "trips_key": "200000061|2026-05-10|6|411|2|8778|2815",
  "trip_instance_key": "200000061|2026-05-10|6|411|2|8778|2815|entry_sequence:5",
  "schedule_key": "200000061|2026-05-10|6|411|2",
  "pid": 8778,
  "entry_number": 2815,
  "entry_sequence": 5,
  "trip_tie_breaker": "entry_sequence:5",
  "trip_tie_breaker_source": "entry_sequence",
  "is_target": true,
  "last_seen_at": "2026-05-10",
  "latest_ingested_at": "2026-05-10T03:10:45.368Z"
}
```

### horses

Records: 2
Fields: 5

| Field | Populated | Blank |
| --- | ---: | ---: |
| h_eid | 2 | 0 |
| horse | 2 | 0 |
| horse_name | 2 | 0 |
| id | 2 | 0 |
| trips_key | 2 | 0 |

Sample:

```json
{
  "id": "horse:FORT KNOX",
  "trips_key": "200000061|2026-05-10|6|411|2|8778|2815",
  "horse": "FORT KNOX",
  "horse_name": "Knox",
  "h_eid": 2815
}
```

### riders

Records: 1
Fields: 4

| Field | Populated | Blank |
| --- | ---: | ---: |
| id | 1 | 0 |
| pid | 1 | 0 |
| rider_name | 1 | 0 |
| trips_key | 1 | 0 |

Sample:

```json
{
  "id": "rider:8778",
  "trips_key": "200000061|2026-05-10|6|411|2|8778|2815",
  "pid": 8778,
  "rider_name": "JESSICA HEAP"
}
```

## Side Lanes

### results
Records: 0

### alerts
Records: 0

### logs.schedule_logs
Records: 15
Fields: 9

### logs.trip_logs
Records: 2
Fields: 14
