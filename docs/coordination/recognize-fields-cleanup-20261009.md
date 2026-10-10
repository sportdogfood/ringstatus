# Recognize field cleanup — 2026-10-09

User instruction: delete fields not used in latest testing. Applied to 33 prior audit candidates after live population, deployed source, automation configuration and native field-dependency checks. No tables or records deleted. No application code, business workflow, automation or deployment changed.

## Verified result

33 unused empty fields deleted; 121 fields reduced to 88 across seven reviewed tables. The owner confirmed deletion of the final duplicate relationship after Airtable's irreversible-dependency warning. All 33 prior audit candidates are now removed.

|Table|Before|Deleted|Remaining|Records after|
|---|---:|---:|---:|---:|
|rs_devices_test|10|2|8|11|
|rs_recognition_sessions_test|48|14|34|52|
|rs_people_test|29|11|18|8|
|rs_phone_aliases_test|7|1|6|1|
|rs_sms_requests|13|4|9|9|
|rs_sms_events|8|1|7|24|
|active_iams|6|0|6|0|

## Removed fields

### rs_devices_test

- `legacy_person` — `fldFYHsLRFvKq5rt6`
- `rs_recognition_sessions_test` — `fld03EwkzUwkjehYq`

### rs_recognition_sessions_test

- `legacy_event_type` — `fldiSz2Vqg4Jsbw2R`
- `legacy_event_result` — `fld8OaNoaFGN8B6b9`
- `legacy_person` — `fldqdrwjIgY7gnEnr`
- `legacy_device` — `fld2xS3lA1iwRf2Pr`
- `legacy_phone_alias` — `fldh7Idwrj09NMsWD`
- `legacy_matched_by` — `fld0LvwNfS2enfuGC`
- `legacy_automation_status` — `fldvPgGAEQcTOBWqS`
- `legacy_browser_family` — `fldLsdmDeuJfhPlbI`
- `legacy_os_family` — `fld32VXREEaEqSSqj`
- `legacy_device_class` — `fldjDqvEJf07HHhvm`
- `legacy_client_timezone` — `fldmecauFZmTwCunb`
- `legacy_viewport_bucket` — `fldCFYLERwncewTbQ`
- `legacy_page_path` — `fldDzuKREEfEcQqYG`
- `legacy_referrer_host` — `fldSUiBuYG6rlKeQ2`

### rs_people_test

- `legacy_primary_phone_e164` — `fld8O6WDODS1oxWd4`
- `Phone` — `fldooa5uhCCCjaXUj`
- `rs_devices_test` — `fldTECDQ2uwkkJPWI`
- `rs_recognition_sessions_test` — `fldGfb9OBoh1xUN7X`
- `profile_name (from ww_profiles)` — `fldN49cusB6l3mnvW`
- `ww_profiles` — `fldMieCYCDW59s8KD`
- `sms (from ww_profiles)` — `fldxbxHxA86VExqw6`
- `active_iams` — `fldHep8DVOYr1nXCf`
- `ww_iams (from ww_profiles)` — `fldRe4aOw0TRGF1PM`
- `profile_rid (from ww_profiles)` — `fld4wzKCiRmjWH2Vt`
- `rs_phone_aliases_test` — `fld5ZWj94cOthMdzj`

### rs_phone_aliases_test

- `rs_people_test` — `flduISSCBkHWFo0Np`

### rs_sms_requests

- `source_event_uid` — `fldJKQ2l3zASEvbt4`
- `to_e164` — `fldvv4iFdneSZyS8C`
- `template_key` — `fldlVDGtvzUJLXBXV`
- `expires_at` — `fldwrsdXxywUh283e`

### rs_sms_events

- `detail` — `fldxrmZxGacqxScQY`

## Duplicate relationship removed

- People `rs_phone_aliases_test` (`fld5ZWj94cOthMdzj`) <-> aliases `rs_people_test` (`flduISSCBkHWFo0Np`). Both were empty; the dependency panel identified only the symmetric relationship. The owner confirmed deletion. Airtable converted the inverse to empty text after the first side was deleted; that residual text field was then deleted.
- Canonical aliases `person` <-> People `rs_phone_aliases_tes` retained.

## Verification and rollback

Live connector schema readback confirmed precisely the listed 33 field IDs disappeared and no other reviewed field IDs disappeared. All surviving reviewed field names and types were unchanged. Detailed schema readback confirmed the canonical alias-person inverse link still exists. Record counts remained 11/52/8/1/9/24/0. Runtime-required fields are retained even if unexercised in the latest test. Prepared active_iams remains six fields, empty and not runtime-wired. The seven recognize-index rows were updated with actual cleanup counts and relationships.

Pre-deletion complete field metadata/configuration: `docs/coordination/recognize-fields-before-cleanup-20261009.json`. Prior audit: `docs/coordination/recognize-fields-links-audit-20261009.md`. Prefer Airtable's recovery flow for deleted fields; manual recreation from metadata changes IDs. Deleted fields held no values at preflight. No end-to-end application retest is claimed.

Evidence screenshots: `docs/coordination/recognize-field-cleanup-complete-20261009.png` (final aliases schema) and `docs/coordination/recognize-field-cleanup-remaining-link-20261009.png` (prior dependency warning).
