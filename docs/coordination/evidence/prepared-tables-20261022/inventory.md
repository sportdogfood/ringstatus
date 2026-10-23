# Prepared Airtable tables and links

Observed 2026-10-22T22:21:55.103Z. Metadata only; record contents, live workflow health and access enforcement were not tested. Names are exact. Table/field identities are scoped by base.

## Coverage

| Base | Base ID | Tables inspected |
| --- | --- | --- |
| domains | unresolved | Base match count 2 |
| Colleges | appvTFBUEfQJZmMuh | 10 |
| RingStatus | apptdhhNzduxm5gjn | 271 |
| rscom | appDN3R51ZPmwgMib | 216 |
| WEC | app6XS1RvsPNRT6os | 165 |
| lainey | appUGgVeAZFae3tEb | 193 |
| agents | appZahVgD156cMAe3 | 29 |
| gpt | appyJd3n37gB9iV0C | 164 |
| recognize | app9kOZdIaGyKk5uG | 29 |
| messages | apphwDLkVCp8ut2Hd | 12 |
| rsco | appdHvjeVVL82Nf5U | 30 |
| webflow | appara5767XpIaJIf | 9 |
| engine | appnOAk2pW8JawiuP | 8 |
| tenants | app7BXcw801n19QKF | 39 |
| horses | apprSZEfQsjUPrPNk | 1 |
| riders | appfabN9KQwDHSqbG | 1 |
| ringwaze | appPBk3FRrATwpHwd | 12 |
| publish | appZY1hcqGvUvxIuV | 9 |
| wef-schedules | appUq5Qz9kARqFija | 10 |
| wef-entries | appSxF4pYMBaxpTZd | 1 |
| wef-people | appeqxxJyPGS1iqdm | 1 |
| wef-live | appgzosUcVqA4vZdf | 1 |
| wef-timing | appYOykKIy1kAquhP | 1 |
| wef-results | appK9IX59jFwUkB2u | 1 |
| wef-alerts | apph3wT9cBNY4PSAT | 1 |
| wef-waze | appVgOTKPydk114wq | 1 |
| wef-horses | appP6FNRYoK1lHcqu | 1 |
| wef-rings | app7ROZrlaFMgpeqv | 1 |
| wef-shows | appCaSmL819KK7UFJ | 1 |
| wef-messages | appXjWPaCthAYs0l7 | 1 |
| wef-publish | appPLyqAX4IctCOeq | 1 |
| wef-tenant-cwf | appxKPBtdl2Rdq7CS | 17 |
| wef-engine | appam4opFT4NeL4mn | 1 |

## Relevant prepared fields and native links

### agents.bases

Base: appZahVgD156cMAe3; table: tbltaHwEyBfcuOHIm; primary field: fld5Pspa3MLylRM8k.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fld5Pspa3MLylRM8k | singleLineText |  |
| stack | fldkijVYACgHjFUqG | number |  |
| projects | flde55CgZyKg3gD6r | multipleRecordLinks | projects (tblKjqNiCiqVFSdDL) |
| lock | fldrXTUJcP1V3BGly | checkbox |  |
| priority | fldTS2gKgBVZkepaK | number |  |
| Count (base-lanes) | fldTmd465aVz9yGVT | count |  |
| base-lanes | fldW8IbhnYVCV3a1p | multipleRecordLinks | base-lanes (tbljEUrIJ9Hv5MNVj) |
| base-id | fldUqAg485WX5RFvn | singleLineText |  |
| base-link | fld0iZylhFrGwzOOl | formula |  |
| base | fldDidgxTWp0mVzNI | formula |  |
| base-btn | fldXfOEzkRDeM4Fqh | button |  |
| threads | fldjf1TSKMqmb8eFR | multipleRecordLinks | threads (tblvIft5zuUKuw3Si) |
| Resume here | fld5rmIOTrbqVYG1w | url |  |
| activity | fld0pdbatLKhQBg1W | multipleRecordLinks | activity (tblITeJfxlIhlDvGk) |
| handoffs | fldXKSPsOXcIxTJgP | multipleRecordLinks | ___handoffs (tbl5GR2kL4BlUbfJx) |
| active | fldeN53RxKakCCipa | checkbox |  |
| Still-open | fldsmMHYrGUuuF6et | multipleRecordLinks | still-open (tblqHv4l4Vy1cbvng) |
| still-open copy | fld7kE8XquNR4lRQ1 | multipleRecordLinks | pending-workflows (tbl5Fnvk3JroMRgZO) |
| wef | fldVvp4kys7HWVc5U | checkbox |  |
| gpt-access | fldjy4l2YfymE0VT9 | multipleRecordLinks | gpt-access (tbl7Ea5DppRZJEtCc) |

### agents.base-lanes

Base: appZahVgD156cMAe3; table: tbljEUrIJ9Hv5MNVj; primary field: fldc7IaJddkGxWNa8.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fldc7IaJddkGxWNa8 | singleLineText |  |
| agents-bases | fldYesEnweHi4Inc8 | multipleRecordLinks | bases (tbltaHwEyBfcuOHIm) |
| purpose | fldPlfL0CLd8Rvi4X | multilineText |  |
| Status | fldPf1G8xAOdBxnSz | singleSelect |  |

### tenants.people

Base: app7BXcw801n19QKF; table: tblyTBgrgdhT3Tcg7; primary field: fldAjH79oj8iEjjR2.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| entity_uid | fldAjH79oj8iEjjR2 | singleLineText |  |
| barn_uid | fldpJ3Ux1hIKBhDow | singleLineText |  |
| name | fldtu2ML1VODWsmSM | singleLineText |  |
| email | fldKYi1RH9EZFxHjx | singleLineText |  |
| recognition_person_uid | fld05BBGATah021FA | singleLineText |  |
| owner_uid | fldoyF786AWGgARs3 | singleLineText |  |
| revision | fldI9J0E32x7gZu29 | number |  |
| request_uid | fld8KLofbXXSFSEsX | singleLineText |  |
| request_hash | flda9xGTd4lcgBYyA | singleLineText |  |
| status | fldXr6HtuQOPyymxR | singleSelect |  |
| record_mode | fldlN43CNRV42QKuj | singleSelect |  |
| phone_e164 | fldznhiJMa1DoVHm6 | singleLineText |  |
| aliases | fldxmguvpHPrhy4Pl | multilineText |  |
| tenant-memberships | fldkgHuthZYr2oEgn | multipleRecordLinks | tenant-memberships (tblFf4aWQYA2RRsWo) |
| optins | fldknSatMtDUrlYQo | multipleRecordLinks | optins (tblCX61u6pjSLrP5S) |
| Source contacts | flduP4RFp0LdHZjmc | multipleRecordLinks | ___contacts (tblI6cwAqxAcKwgTR) |
| Source grooms | fldtoVHxzR1LxYakJ | multipleRecordLinks | ___grooms (tblRxTPS9vuy4SSZT) |
| Source exercisers | fldPjvIWIwNGFO1wK | multipleRecordLinks | ___exercisers (tblIBIowqW36AGKAt) |
| Roles | fldLr5ZeFFxOwxwGd | multipleRecordLinks | roles (tblRqRbjmqmNfJtIP) |
| Associated horses | fld0XExxfk3WZ4P88 | multipleRecordLinks | horses (tbljPvAk5EezdCQME) |
| Rider | fldE9lE0W4oAE0ufb | multipleRecordLinks | riders (tbllUlTlcEMgZiW3h) |
| Review note | fldKSFMGdnJEE2oAj | multilineText |  |
| display-name | fldMVQzpXUfyJ5hOo | singleLineText |  |
| profile-img | fldBWGq9WplW0F4cU | url |  |
| tags | fld4pcenmJhzp1Tzf | singleLineText |  |

### tenants.sources

Base: app7BXcw801n19QKF; table: tblRswgKjA9dt3PED; primary field: fldG92rtLja7jj2lT.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| mapping_uid | fldG92rtLja7jj2lT | singleLineText |  |
| entity_type | fld9XE1pzpknYZo1W | singleSelect |  |
| entity_uid | fldB7tAR2FklNHa8N | singleLineText |  |
| source_system | fldQPssUFIK9Tb9zr | singleLineText |  |
| source_entity_type | fld5UG3fFZHKHTcFL | singleLineText |  |
| source_scope | fldx2fBX5fEyS5wuR | singleLineText |  |
| external_id | fldbqTC7hbsy4NYPY | singleLineText |  |
| record_mode | fldp738XZI1ePImUw | singleSelect |  |

### tenants.optins

Base: app7BXcw801n19QKF; table: tblCX61u6pjSLrP5S; primary field: fldctfF9xSV2eA1Ka.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| entity_uid | fldctfF9xSV2eA1Ka | singleLineText |  |
| barn_uid | fldd17W6rSqvatl1s | singleLineText |  |
| user_uid | fldSn89EbY7el5smb | singleLineText |  |
| engine_scope | fld86VSiXkFct0TR0 | singleSelect |  |
| target_type | fldIed4dqqACkWh3a | singleSelect |  |
| target_uid | fldMAlV5QY8KSjgz9 | singleLineText |  |
| alert_types | fldc3Yk6crZM5NfNy | multilineText |  |
| phone_e164 | fld1Yt2APgvT39nsI | singleLineText |  |
| status | fldRu44EIT87EpCxO | singleSelect |  |
| consent_state | fldBX5pMx4y0WCcYA | singleSelect |  |
| consent_at | fldlMLgzuFRBQLrvD | dateTime |  |
| consent_source | fldYEHNEc2LkOr2ki | singleLineText |  |
| notice_version | fldflkB5U586NPcqq | singleLineText |  |
| revoked_at | fldG5Rw9HKvJUzCx8 | dateTime |  |
| owner_uid | fld3EWCrEr0Mz3rBE | singleLineText |  |
| revision | fldbIyZhgTI21BJMq | number |  |
| request_uid | fldOuxgRP5tPhFaj4 | singleLineText |  |
| request_hash | fldhpUmJW3oxE5Gkv | singleLineText |  |
| record_mode | fldnOjSnFzu8bL7z2 | singleSelect |  |
| preferences_json | fldDxqMx5Kd7SCZ3Q | multilineText |  |
| optin-options | fld561BsyKjDWinIn | multipleRecordLinks | optin-options (tblFHcfIFjGuoE2hp) |
| Membership | fldLTONZPqPRUytXs | multipleRecordLinks | tenant-memberships (tblFf4aWQYA2RRsWo) |
| Managed by | fldN7BSB1X1oqmKC2 | multipleRecordLinks | people (tblyTBgrgdhT3Tcg7) |
| Notification scope | fldhYgtWe5yNAD9Oy | singleSelect |  |
| Selected favorites | fld0URBJmfebDV5ld | multipleRecordLinks | favorites (tblyp3d7BZtZmqAVM) |

### tenants.riders

Base: app7BXcw801n19QKF; table: tbllUlTlcEMgZiW3h; primary field: fldecuXzophpkdDHE.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| rider_id | fldecuXzophpkdDHE | number |  |
| detail | fldEsbbXaJDCkgTxd | checkbox |  |
| people_id | fldOiBSoHquf0doZq | number |  |
| rider_ids | fldZ6isyGHBihhTcR | multilineText |  |
| rider_names | fldKBaCyUGEVjCn04 | multilineText |  |
| name | flda2pi9uaaZdvoh0 | singleLineText |  |
| team_name | fldl3I2lR8UYrtAch | multilineText |  |
| team_names | fld9aHSKDoo8e4X3x | multilineText |  |
| q_users | fld1jSmrgWJSPGRn5 | multilineText |  |
| lf_name | fldu3dZ51G4VsWYiQ | singleLineText |  |
| state | fld55zsruroG2gfoF | singleLineText |  |
| pid | fldO8pD7ArBXCqSQZ | number |  |
| active_subscribers | fldPzzPX8sxvndHQq | multilineText |  |
| active_tenants | fldlJtgI8JnTLxhim | multilineText |  |
| rider_number | fldZ14N6qDPO5ZKF2 | number |  |
| people_uri | fld5FumcqPsqbx1IO | multilineText |  |
| watch_trips | fldRWSjNAonxRJc3Q | multilineText |  |
| watch_entries | fld4Ch1NFMcwNrsPH | multilineText |  |
| rider_names_list | fldPziYOXQq9VrUei | multilineText |  |
| horse_ids_list | fld59J8aWOtZ67CBG | multilineText |  |
| app_sid | fldeH2tGHh6uWSKUK | number |  |
| app_sql_date | fld2zIIvQ1e7xo8iF | singleLineText |  |
| pid_mismatch | fldT9V1cYmMXa4t7O | checkbox |  |
| team_name_fallback | fldlK8apWSf9Y4U3W | multilineText |  |
| riderName2 | fldqTYp48SlUrFD1Y | multilineText |  |
| braider_name | fldH9oN6mYNZsIJLo | multilineText |  |
| usef_no | fldpW32udApmzKqRP | number |  |
| city | fldLGPmQobhHeSMXa | singleLineText |  |
| fei_id | fldIj9sHfx0DtLIBt | singleLineText |  |
| created_time | flds3nV3gbU4u5XSJ | dateTime |  |
| last_modified_time | fldvjGbNjUAawsF0n | dateTime |  |
| riderName | flduUVPUfAPNQKeAN | multilineText |  |
| properFirst | fld7ljwydNeE9ioVy | multilineText |  |
| properLast | fld2nQuSgGkacHnzF | multilineText |  |
| entry_oog | fld96rdLXGuqzQBdO | multilineText |  |
| horses_list | fldx2GPc89Ya3ciY6 | multilineText |  |
| riders_ids_list | fldmFZhgz5dS5eZYJ | multilineText |  |
| inactive | fldVzhQx04lyosDG4 | checkbox |  |
| rider_name | fldTPQ1PwEpAVcxtW | singleLineText |  |
| run_id | fldHZnOf3rQK9u3Mp | number |  |
| last_run | fldhsqDIR8hRCSJLS | singleLineText |  |
| new_rider | fld2DklN6ZbzdKkl2 | checkbox |  |
| shows | fldTboyk9xLnqmYDb | multilineText |  |
| empty_trips_show_id | fldyrSdfcNNRlme18 | number |  |
| empty_trips_at | fldQam1YQvIelWjOM | date |  |
| trips_lists_show_id | fldXOLqzmdH6XfG1N | number |  |
| trips_lists_at | flddPTvXnufHK2wmy | date |  |
| source_record_id | fld6km1w5AIJwSS7d | singleLineText |  |
| ww_horses | fldQDPGLxBy2gA9aw | multipleRecordLinks | horses (tbljPvAk5EezdCQME) |
| ww_profiles | fldXmesw6JU7qusmd | multipleRecordLinks | ___profiles (tblVN8u14UHAXsqc8) |
| ww_tenants | fldVf3SYQLgNlQ1B0 | multipleRecordLinks | tenants (tbled1saw01Thffs1) |
| trainers | fldkFGR2UhekrU60L | multipleRecordLinks | trainers (tblirnrb2OwRB4RB7) |
| ww_trainers | fldMTWYAdIyDBbcAx | multipleRecordLinks | trainers (tblirnrb2OwRB4RB7) |
| ww_exercisers | fldb2rOtf0uRCGRWA | multipleRecordLinks | ___exercisers (tblIBIowqW36AGKAt) |
| favorites | fldRi7fGRle2LYEi6 | multipleRecordLinks | favorites (tblyp3d7BZtZmqAVM) |
| people | fldP3Gk1PAbDsDPyt | multipleRecordLinks | people (tblyTBgrgdhT3Tcg7) |
| tenant-roster | fldcgkA3tnMuUnwAM | multipleRecordLinks | tenant-roster (tblwcczj8lFhVcbyt) |

### tenants.trainers

Base: app7BXcw801n19QKF; table: tblirnrb2OwRB4RB7; primary field: fldmjAtchbDl0q4HG.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| trainer_id | fldmjAtchbDl0q4HG | number |  |
| detail | fldJFGj5QSZ7XMoGE | checkbox |  |
| active | fldBK4SBIiyS5h16g | checkbox |  |
| pull_pid_allowed | fldvcYhXoQmtK2Jjc | checkbox |  |
| Calculation | fldlQuLDW1cp2E8oK | multilineText |  |
| pip | flddeyJZrk8UyMtqV | number |  |
| people_id | fldf6GLVFDNz8YAcG | number |  |
| usef | fld9vTXKnZJo9Ofx3 | singleLineText |  |
| lf_name | fldYIlv6R2Gtz3n4A | singleLineText |  |
| name | fldVTtcdSPBIgVAKJ | singleLineText |  |
| fei_id | fldqwzFr0j75Ge5Zq | singleLineText |  |
| last_modified_time | fldAgHAddWKUbt6T8 | dateTime |  |
| created_time | fld5UVyBinz3dYVLw | dateTime |  |
| people_uri | fld3URjynTN43uHW6 | multilineText |  |
| people_ids | fld6YVyjoPu6vUS22 | multilineText |  |
| riders_list | fldE9lE0KPtLFUlhI | multilineText |  |
| horses_list | fld9AICoi39g1RLcw | multilineText |  |
| watch_trips | fldnWHRnBiHfUhK0n | multilineText |  |
| active_subscribers | fldg3A2ASh1xVIilr | multilineText |  |
| eid_ignores | fldWst3CEv8JFk3lj | multilineText |  |
| eid_loans | fld2WuthRe3G8Ip0P | multilineText |  |
| active_classes | fld6Kf0wj54MZGkPA | multilineText |  |
| pid | fld2OMv3d9mT6eCpa | number |  |
| publish_scaler | fldQiZn6JfHQ1g3XZ | multilineText |  |
| trainer_label | fldCcwQmhv7Vbqtg1 | singleLineText |  |
| aka | fldpcdOd5472AFQk6 | multilineText |  |
| publish_queue | fldB2r2043n6X0AKe | multilineText |  |
| entry_oog | fldPfBLx5za5IKtC0 | multilineText |  |
| tack_lists | fldakqy9OpOOJn6ae | multilineText |  |
| trips_count | fldCXUgRmn6mvnaKu | number |  |
| watch_trips_view | fld8VU5nYXEwltS5Z | multilineText |  |
| watch_trips_alerts | fld65dqbz3ZZ0Wdax | multilineText |  |
| class_alerts | fldDhmpHhY3WVJuCn | multilineText |  |
| active_tenants | fldnvAXdi14gVSirn | multilineText |  |
| watch_classes | fldHj7YKCgiAFrWTS | multilineText |  |
| scheduler | fldF4BQzYIMuozqd2 | multilineText |  |
| priority | fldhHslpMeXY4ZBoK | number |  |
| watch_trips_targets | fldh2WH0BxfvzFyxX | multilineText |  |
| watch_entries | fldwAt94HLp0lJbm3 | multilineText |  |
| ___horses_id | fldakmV8HTq9ydlI4 | singleLineText |  |
| ___riders_id | flduBVVCEwpNzfQia | singleLineText |  |
| ___horses_count | fldpJzYi96fmxrrPD | number |  |
| ___rider_count | fldqcl7VxmZHkKRH9 | number |  |
| ___scheduler_8778 | fldyLXtPD0IUzE38W | multilineText |  |
| ___scheduler_19676 | fld31bveh6aGYsu5q | multilineText |  |
| ____scheduler3 | fldGDA6Vnu6UDw6y7 | multilineText |  |
| active_alerts copy | fldi0DFSimrujYvDx | singleLineText |  |
| alerts_template copy | fldssaEdrVWByI9CL | multilineText |  |
| trainer_recordId | fldEXLBVZcgdRSh7s | multilineText |  |
| active_tenants copy | fldJsWMxmNDfdeJdX | singleLineText |  |
| show_name | fldVpJkA7rsxxFtaG | singleLineText |  |
| active_iams copy | fld0T7lEvu2hN9Bnq | singleLineText |  |
| ww_profiles copy | fldjZnlOfWvC5vrEH | singleLineText |  |
| active_classes copy | fldZ2ZNsEB44Y8tk1 | multilineText |  |
| watch_entries copy | fldVqOo03EGRbp0tk | singleLineText |  |
| tack_lists copy | fldXDku5MTQFSgw7k | multilineText |  |
| entry_oog copy | fld8u87csIkVJJYkb | singleLineText |  |
| source_record_id | fldfpkr6Jwp3N3Wer | singleLineText |  |
| ww_horses | fldPVKGaz3OIoeTlX | multipleRecordLinks | horses (tbljPvAk5EezdCQME) |
| ww_profiles | fldznDFx6Xk550ADU | multipleRecordLinks | ___profiles (tblVN8u14UHAXsqc8) |
| active_tenants copy 2 | fldr0k5bfbo6O8rDW | multipleRecordLinks | tenants (tbled1saw01Thffs1) |
| master_trainer | fldLdxcehsBnjySyl | multipleRecordLinks | trainers (tblirnrb2OwRB4RB7) |
| From field: re;ated | fldqNMMw1GCetgEvM | multipleRecordLinks | trainers (tblirnrb2OwRB4RB7) |
| ww_riders | fldaKio6k7X38z3CW | multipleRecordLinks | riders (tbllUlTlcEMgZiW3h) |
| ww_riders 2 | fld6psN4i0Wvta9SE | multipleRecordLinks | riders (tbllUlTlcEMgZiW3h) |
| favorites | fld3lRBO9tdQsu9wp | multipleRecordLinks | favorites (tblyp3d7BZtZmqAVM) |
| tenant-trainers | fldFpLp6S6RhRs2Xa | multipleRecordLinks | tenant-trainers (tblnjrsLihxbtMkb5) |

### tenants.tenants

Base: app7BXcw801n19QKF; table: tbled1saw01Thffs1; primary field: fld5xRXWu6bNXeNpj.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| tenant_id | fld5xRXWu6bNXeNpj | number |  |
| tenant_active | fld0l2uj0QaKQ47QJ | checkbox |  |
| publish_queues | fldSx7mKPg7HSQZt7 | multilineText |  |
| ww_subscribers | fldRjhns8YOYbnCZB | multilineText |  |
| tenant_skin1 | fldAPjKQpwpvU4Nk3 | singleLineText |  |
| tenant_skin2 | fldku52quKLA2VVPY | singleLineText |  |
| tenant_logo | fldlQs0y5e8B6tFyI | multipleAttachments |  |
| view_watch_schedule | fldJBhg3XWFnMRR3f | multilineText |  |
| first_print | fldOmRf5IpnJVS8yZ | checkbox |  |
| tenant_official_name | fld7zvwWWrXs4U1sd | singleLineText |  |
| tenant_accronym | fldwrvaoYXJBaWvih | singleLineText |  |
| pid | fldnsBx6mx8UiwE9A | number |  |
| tack_lists | fldEhBu1ahhPfCCUS | multilineText |  |
| alert_milestone1 | fldTK4T7Yown6CLCs | number |  |
| alert_milestone2 | flds6LoP80gvp2Bdh | number |  |
| tenant_logo_url | fldZZlJg7EXDDVFoA | url |  |
| tenant_landing | fldUfOnGIoN7KHH6n | url |  |
| watch_trips_view | fldquA5lfNoljxw6S | multilineText |  |
| ww_horses_view | fld4vOf8L795gBZOj | multilineText |  |
| ww_horses_tack_ready_view | fldzFUQDL7ziw5Ktn | multilineText |  |
| tack_ready_view | fldyV6Wtymv10A1KS | multilineText |  |
| watch_trips | fld68sDAadivwsHRY | multilineText |  |
| class_alerts | fldDTCPR6Xznc0HXc | multilineText |  |
| tenants (from heartbeat) | fldqQTdnowUTFEDHD | multilineText |  |
| active_alerts | fld9xpwqPIeCvDMxT | multilineText |  |
| app_dow_raw (from heartbeat) | fldVoy75VR2EsV1iV | multilineText |  |
| mode (from heartbeat) | fldM5iHocifMHjGgi | multilineText |  |
| app_sql_date (from heartbeat) | fldoMhdZXuBYMF0H0 | multilineText |  |
| app_show_id (from heartbeat) | fldZjPCGRCzgiTmQ1 | multilineText |  |
| time (from heartbeat) | fldiR8BZLwFOaffjG | multilineText |  |
| epoch (from heartbeat) | fldqi0o9p0obHLxwb | multilineText |  |
| is_pro | fldbayktVMoGyw3tm | checkbox |  |
| webflow_pages | fldnYvYM10J9zV6pe | singleLineText |  |
| alerts_threads | fld4zib6EsLBt6bCY | singleLineText |  |
| tenantRID | fld46VVw54drHuqkg | multilineText |  |
| publish_scaler | fldVrW62TU8iXSZs9 | multilineText |  |
| lists | fldYEMPfwvDNBvdRu | multilineText |  |
| thread_logs | fldjZJwuPZ506KicV | multilineText |  |
| source_record_id | fldcAWEVFW9gVzode | singleLineText |  |
| ww_horses | fldIp1UryQeLmGGvo | multipleRecordLinks | horses (tbljPvAk5EezdCQME) |
| ww_profiles | fld5O7qj3yi2mUptF | multipleRecordLinks | ___profiles (tblVN8u14UHAXsqc8) |
| ww_trainers | fld5IgThec9c9SR9s | multipleRecordLinks | trainers (tblirnrb2OwRB4RB7) |
| ww_grooms | fld0D9aYOMRCDqYzy | multipleRecordLinks | ___grooms (tblRxTPS9vuy4SSZT) |
| ww_exercisers | fldZmiyyl6qo4nJWk | multipleRecordLinks | ___exercisers (tblIBIowqW36AGKAt) |
| ww_riders | fldf1GudO4RW9k29A | multipleRecordLinks | riders (tbllUlTlcEMgZiW3h) |
| tenant-memberships | fldoRstucSeW2WeQz | multipleRecordLinks | tenant-memberships (tblFf4aWQYA2RRsWo) |
| tenant-locations | fldczKZNNxtZsfw5R | multipleRecordLinks | tenant-locations (tblR6qzSlBTZV53C3) |
| Source contacts | fld1v6Xgh2OZLfxws | multipleRecordLinks | ___contacts (tblI6cwAqxAcKwgTR) |
| onboarding-stages | fld4kJRvbRgKkx837 | multipleRecordLinks | onboarding-stages (tblICyxXvZDmUxZtv) |
| Tenant level | fldgaZJ1463OyRWwj | multipleRecordLinks | tenant-level (tbleGDeOz4XDIMAGd) |
| tenant-trainers | fldrJ7qgV8ujWjrUT | multipleRecordLinks | tenant-trainers (tblnjrsLihxbtMkb5) |
| tenant-roster | fldhX098JmZ7O1bXH | multipleRecordLinks | tenant-roster (tblwcczj8lFhVcbyt) |

### tenants.roles

Base: app7BXcw801n19QKF; table: tblRqRbjmqmNfJtIP; primary field: fldTVO9xZVhQi4eEy.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| rec_name | fldTVO9xZVhQi4eEy | singleLineText |  |
| rec_label | fldSHlxykiaB54yNV | singleLineText |  |
| rec_class | fldbnOsMfBDXAZiTf | multilineText |  |
| rec_tags | fldYWS1QXPA1R6oMd | multilineText |  |
| rec_id | fld6v224zZ1r6I0Lf | number |  |
| rec_rid | fldFPq27O1y2SYlkn | multilineText |  |
| master_linking_from | fldYQLT02wu0i63uW | multilineText |  |
| master_link_base | fldDH4T80chIV4VQH | multilineText |  |
| master_linking_to | fldkROocGBQxgSjKK | multilineText |  |
| q_users | fldTTRjQBsoUOXV4T | multilineText |  |
| source_record_id | fld88IhkybTsbrvLj | singleLineText |  |
| ww_profiles | fldZpvzyyhFyHRovo | multipleRecordLinks | ___profiles (tblVN8u14UHAXsqc8) |
| tenant-memberships | flddAVl0VruYe15d5 | multipleRecordLinks | tenant-memberships (tblFf4aWQYA2RRsWo) |
| people | fldvM0hcj0VSNikQt | multipleRecordLinks | people (tblyTBgrgdhT3Tcg7) |

### tenants.rings

Base: app7BXcw801n19QKF; table: tblB0k2I6qQJpNYwc; primary field: fldZR2uIVYbSm3FJT.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ring_number | fldZR2uIVYbSm3FJT | number |  |
| ring_id | fldN7nR3e3gTabExJ | number |  |
| ring_name | fldb59JacVoO86kgE | singleLineText |  |
| ring_names | fldrH0DP79q7qUU94 | multilineText |  |
| Done | fldWr57u8Vyp0jJWw | checkbox |  |
| customer_id | fldY6TldQ5LiFBimT | multilineText |  |
| ajax_token (from customer_id) | flde53kc69NhnOWYB | multilineText |  |
| RINGS_ring_number | fldXWmsj5Njbukgfp | multilineText |  |
| RINGS_ring_id | fldXQzl0s7EMashZU | multilineText |  |
| RINGS_aliases | fldk8pnvIkP9Z5mC9 | multilineText |  |
| aliases | fld78ZuUjshsrzxBs | multilineText |  |
| ring_nickname | fldJI9OrsLUuhjApc | singleLineText |  |
| active | fldGV1NqQGNuMClas | checkbox |  |
| ring_priority | fldsX3QxFPSwMOYCM | number |  |
| ringName | fldE9CB24iBR6OL0a | multilineText |  |
| coordinates | fldA6Xt9TxcPWUqx5 | singleLineText |  |
| defaultLat | fldJYM2SMDShcXBj5 | singleLineText |  |
| defaultLong | fldOYXf3ZjbRYSha8 | singleLineText |  |
| distanceInFeet | fld1kOxPgsJzzbZas | number |  |
| estimatedTime | fldPmLR3RjLpy7Nee | duration |  |
| groups_live | fld3EIYC0HjQJkpAg | multilineText |  |
| watch_schedule | fld35O6sFBdFerHnO | multilineText |  |
| watch_rings | flduEAvpBKt5qVSdU | multilineText |  |
| watch_trips | flddXvB95UImzfEzf | multilineText |  |
| latestStart Rollup | fldWfvQKPwpW6TorD | multilineText |  |
| latestStatus Rollup | fldS4Mkq2njBKXxrF | multilineText |  |
| dt | fldZuOWyJ0tRMPd5K | multilineText |  |
| sid | fldXOUWavmZsE6hO6 | multilineText |  |
| ring_uri | fld9BDYggfogXrisP | multilineText |  |
| ring_status_endpoint | fldlhBmYVIVbezPge | multilineText |  |
| dt (from watch_trips) | fldWmMykNsKP8GfTb | multilineText |  |
| sid (from watch_trips) | fld1AgztcKqaSfPJf | multilineText |  |
| detail | fldpNdNtO2G4J1jXV | checkbox |  |
| ring_nickname_esp | fldBljdakQEfogBBS | singleLineText |  |
| feet_from_home | fldaqe4FAdZRC0Nwj | number |  |
| coordV2 | fldvW5sSk55g5CAGp | multilineText |  |
| distancetoMiles | fldh0W00AaqctCeE4 | number |  |
| estimatedTimeTo | fld1BikcsPhKCwuVc | number |  |
| distanceInFeet_legacy | fldgZx1d9dXn5szri | number |  |
| wef_map | fldoN1Ylik3eBg0WQ | multilineText |  |
| wefMap | fld8glDROjoLrlzPl | multipleAttachments |  |
| default_time_to | fld2QgrJsnez1BQg6 | number |  |
| wef_myMap | fldQwBr9Ih9U92Slq | multilineText |  |
| wef_ringPing | fld1TnCfDNVzYx0KA | url |  |
| twoway_logs | fldcxJQrQY4L1ujxW | multilineText |  |
| Created | fldjpOZA1e9Ft9nYZ | dateTime |  |
| winter | fldtD9VMZPWySapNl | checkbox |  |
| spring | fldzkDQrwDLAIDABP | checkbox |  |
| RINGS_row | fldW1WBjBNopt4B43 | multilineText |  |
| sms_ring_number | fldW6zTJ5naIxNern | number |  |
| RINGS_sms_ring_number | fldMu6lb8oDmTkTax | multilineText |  |
| RINGS_sms_label | fld5gPvTI92Ns4eOF | multilineText |  |
| ring_label | fldGve6vbzvmYyDCH | multilineText |  |
| use_label | fldRbZyzM2xlXPHeP | checkbox |  |
| ESP | fldpbviT8rOfKlRMc | checkbox |  |
| winter-supplemental-cloudflare | fldvQlFZhJx1eCjdR | multilineText |  |
| esp-supplemental-cloudflare | fldKFTW0DBKnRaq2C | multilineText |  |
| winter-cloudflare | fldlhAwk1w8Xjk0mw | multilineText |  |
| winter-supplemental | fld0fPKJhWXoFYZtY | checkbox |  |
| ring_accro | fldeqSP29CmWh5hE0 | singleLineText |  |
| defaultTimeTo copy | fldrRO1jyZwbbUHRI | number |  |
| ring_collection_key | fldMGojCQNaijr1z0 | multilineText |  |
| ring_collections | fldhT5CyUnQFrfmSn | singleSelect |  |
| groups_live copy | fldYApAjxiTIHzBPK | multilineText |  |
| source_record_id | fldBrZkRoDfryapNi | singleLineText |  |

### tenants.horses

Base: app7BXcw801n19QKF; table: tbljPvAk5EezdCQME; primary field: fld45lBoIPsEbzKr5.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| horse | fld45lBoIPsEbzKr5 | singleLineText |  |
| show_names | fldX8Cr4CLDRWTktN | multilineText |  |
| ignore | fldhc2OukdpEppNUB | checkbox |  |
| show_name | fldAUOeuL7OnhuLtJ | singleLineText |  |
| barn_name | fldtHKALYmTJGRgfu | singleLineText |  |
| barn_names | fldXNLUtrvu6y4pOD | multilineText |  |
| detail | fldo8AVfPTjHRhOZm | checkbox |  |
| horseId | fldo9hdjksCKXHMSr | number |  |
| horse_id | fld45qwQuscRVdcxR | number |  |
| ___horses | fldfF5FQyfQ4eHdBz | singleLineText |  |
| horse_ids | fldvRIHk8wEDRT9p0 | multilineText |  |
| rider_list | fldOVJ9pEIEmJv1x2 | multilineText |  |
| trainer_id | fldZQtnY4YAWIcSAT | number |  |
| pid | fldgMSROvvuueBZWN | number |  |
| active_tenants | fld9gbDZPqYMu84hM | multilineText |  |
| stall_card_input_print | fld5um2SOwPETepxR | multilineText |  |
| inactive | fldwb1RYHWPTVLkeO | checkbox |  |
| new | fldzyPrdIzLsVcV3x | checkbox |  |
| Done | fldt5kMEHhpgKNa5N | checkbox |  |
| entry_id | fldYejwLpQkaRuINi | number |  |
| new_horse | fldQSNexHyvEPqeSi | checkbox |  |
| pid_mismatch | fldJnBXUpZte8eXLl | checkbox |  |
| search_by | fld6mnKttILEjifGo | singleSelect |  |
| search_uri | fldFyEnhG2amyCtZQ | multilineText |  |
| search_by_horse | fldED5jImX7zNnuOx | multilineText |  |
| search_by_entry_no | flddEqHLhRtH0UnCZ | multilineText |  |
| search_by_entry_id | fldWKwFGQpubxvnMC | multilineText |  |
| last_modified_time | fldOi6e0qyjSPwUqm | dateTime |  |
| ___app_sid | fldtOPNdla466W6iN | number |  |
| color | fldHAWKEqF1zXKlTA | singleSelect |  |
| gender | fldUBO1XhU93qsWkx | singleSelect |  |
| shows | fldWMFN53VN7T5QUA | multilineText |  |
| color+gender | fldiWbED5oLI6sWHU | multilineText |  |
| discipline | flduZGvVODuVacNNb | singleSelect |  |
| btk | fldn4v8sGlHggFqHJ | checkbox |  |
| horse_note | fldAzWZcjmyaFExtA | multilineText |  |
| entry_uri | fldFtmf1vNhlS9BgC | multilineText |  |
| encode | fldGO5vWl1Bz0xh7k | multilineText |  |
| lower | fldYp1hG42v31qcJU | multilineText |  |
| path | fldqbYglcZO7L0qu5 | multilineText |  |
| entry_uri_team | fldIBf8jYpu7m7nSL | multilineText |  |
| horse_uuid | fld5QB69OimNABgBx | multilineText |  |
| watch_trips | fldUiGbYTbvObjlNK | multilineText |  |
| Barn Name | fldQHfLSrNRq11W6W | multilineText |  |
| Horse Id | fldy9CA1NoujtlI5g | number |  |
| rider_names | fldE77IxUYmGQCLqu | singleLineText |  |
| barnName | fldg7PvHDhkvjQqa5 | multilineText |  |
| tags | fldyqzBQAdmoOZiE6 | multipleSelects |  |
| emergency_no | fldaRwBsshAjXZ2wX | multilineText |  |
| emergency_contact | flduTWJQ3m8iNs97x | multilineText |  |
| airtable_id | fldRxc16mhOdzFPO7 | multilineText |  |
| sort | fldhu07xibvJnLFTe | number |  |
| profile_img | fldTqouJ1Qnmfhcwq | url |  |
| tack | fld7KsW8093NBOWeT | checkbox |  |
| barnActive | fld0ihFlJO7U8QZtk | multilineText |  |
| horseName | fldYsG7U4Y2YLefwD | multilineText |  |
| loaner | fldcI05poKrONFdMm | checkbox |  |
| names_rider | fldXSFZrox6XPtxq8 | multilineText |  |
| ___app_sql_date | fldKkX3deOS7QSVUe | singleLineText |  |
| inactive2 | fldp4UJskLp8TPKvJ | checkbox |  |
| run_id | fld4YUHAKTOVX8f63 | number |  |
| last_run | fld9hALAAi4DVVjGz | date |  |
| active_entries | fldlliXoTYuYSfGxl | multilineText |  |
| entry_number | fldmYDmOgtYVf3vSX | number |  |
| active_subscribers | fldFJ86jvpJujLt7F | multilineText |  |
| horse_genders_link | fldzZXjimSBoEumLV | multilineText |  |
| horse_colors_link | fldZOJ43umMuY3Cmh | multilineText |  |
| wec_horses_link | fldp4F1h80CUCRvIg | multilineText |  |
| horse_types | flde1rspevSrE15l4 | singleSelect |  |
| horse_disciplines | fldBfv0g6WRkexowX | multipleSelects |  |
| horse_age | fld48ZWyez6BaqL3C | number |  |
| tenant_id | fldSUQ5Kmowf0Qoj7 | number |  |
| horse_disciplines_link | fldjL8XFzzBpH17br | multilineText |  |
| horse_colors | fldb60LT9BQeIjhqv | singleSelect |  |
| horse_genders | fld0pg2DxUhl41hzF | singleSelect |  |
| horse_profile_tabs | fldJivjw7TB8ZArTA | multipleSelects |  |
| emergency_phone | fldJlnrFeWMFFdG9v | phoneNumber |  |
| emergency_contacts | fldT4SmCIqlCwOwPY | singleLineText |  |
| tenant_img | fldYKbxRSCSQZW68c | multilineText |  |
| active | fldzJ7JXLJIYmhp0o | checkbox |  |
| priority | fldckHTo86D3IHUVj | number |  |
| horse_profile_tabs_link | fldvSm5WOUI8lJ64a | multilineText |  |
| tack_lists | fldXpM326AcMJLIpd | multilineText |  |
| record_active | fldobx4pgSasdELH1 | checkbox |  |
| record_inactive | fldQvP0aw2YelrFqR | checkbox |  |
| app_active | fldsghp2felU5hocl | checkbox |  |
| app_inactive | fldBSFJQulGyGjvdO | checkbox |  |
| app_ignore | fld2FAclhTc5UJwRG | checkbox |  |
| app_include | fldA4bF4y9yRgimaK | checkbox |  |
| tenant_logo_link | fldnxAYzNaxAi97QE | url |  |
| tenant_logo_img | fldH3zJV75mEY9K0e | multipleAttachments |  |
| questri-horses | fldWGuLimXoTanErw | multilineText |  |
| Q50 (from questri-horses) | fld8rCNu6Gm97oXQJ | multilineText |  |
| Check (from questri-horses) | fld7mrjyAjS1hkk5j | multilineText |  |
| Created | flde84PlnmvrltOg3 | dateTime |  |
| picture (from questri-horses) | fldOBevqFEUimefWY | multilineText |  |
| sex (from questri-horses) | fldfzaON77plYfvcV | multilineText |  |
| horseId (from questri-horses) | fldLCbPhvAPxsCsHk | multilineText |  |
| color (from questri-horses) | fld2cuXZNHEQJiKEu | multilineText |  |
| hands | fld7FcG3r0nVsUq3J | number |  |
| q_rs | fldO6mK1BphLrU0b6 | multilineText |  |
| horse_rid | fldsYx2DhJ5NPnfa5 | multilineText |  |
| print_batch | fldCi5zdWUoUEol9w | checkbox |  |
| feed_items | fldsmFcYR2fHoOupB | multilineText |  |
| q_feed_pivot | fldZne98vUk9CCkQ2 | multilineText |  |
| hp_feed_plan | fld4SXFSWoZIXIiM0 | multilineText |  |
| tack_grocery_items | fldpTqbI89aVFwTne | singleLineText |  |
| wec_wave_1 | fldGwJPq3jAhaTctF | checkbox |  |
| wec_wave_2 | fldH6gmj6LEEUOedc | checkbox |  |
| wec_not_going | fldBD5zrRqYEC9E6J | checkbox |  |
| wave_1 (from wec_horses_link) | fldhBpKL0GrQiPjcO | multilineText |  |
| wec_wave_1 (from wec_horses_link) | fldyLPt9CeB3RYlId | multilineText |  |
| wec_horses copy | fldWhLG33MzfARgUp | singleLineText |  |
| wec_horses copy 2 | fldC9bpLnxSqaxOKP | multilineText |  |
| source_record_id | fldGJjSGSsnoTzNYg | singleLineText |  |
| ww_tenants | flde273VWfilm2nqE | multipleRecordLinks | tenants (tbled1saw01Thffs1) |
| ww_trainers | fldSGtEkioA4S983J | multipleRecordLinks | trainers (tblirnrb2OwRB4RB7) |
| ww_grooms | fld9Vrh3R7PIlBI6H | multipleRecordLinks | ___grooms (tblRxTPS9vuy4SSZT) |
| ww_exercisers | fldepiXVBfQ4o6ifb | multipleRecordLinks | ___exercisers (tblIBIowqW36AGKAt) |
| ww_profiles | fldk9wBh8g2U3Y3GR | multipleRecordLinks | ___profiles (tblVN8u14UHAXsqc8) |
| contacts | fldYTKR9jspjfDtMN | multipleRecordLinks | ___contacts (tblI6cwAqxAcKwgTR) |
| ww_riders | fldSeYq8EelBVJfw6 | multipleRecordLinks | riders (tbllUlTlcEMgZiW3h) |
| favorites | fld5HwAIixeyLOQcW | multipleRecordLinks | favorites (tblyp3d7BZtZmqAVM) |
| people | fldHtZN3ryX0wZ7ec | multipleRecordLinks | people (tblyTBgrgdhT3Tcg7) |
| tenant-roster | fld29XdJkHhWfNSBc | multipleRecordLinks | tenant-roster (tblwcczj8lFhVcbyt) |
| tenant-assignments | fldevoesCKiSbdu2l | multipleRecordLinks | tenant-assignments (tblnFOT8lGxCXD4Xh) |

### tenants.locations

Base: app7BXcw801n19QKF; table: tblN6gEy40kUlvCgf; primary field: fldf09BQoKXDWU1z4.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| location_id | fldf09BQoKXDWU1z4 | singleLineText |  |
| short_name | fldoYQeEkL1L38rdu | singleLineText |  |
| location_description | fld1OyDSguvuPycv0 | singleLineText |  |
| city | fldpPgZYzCwsKYRdg | singleLineText |  |
| state | fldlYgWn6WCuwDbWw | singleLineText |  |
| placeid | fld7ienvw24ghP8py | singleLineText |  |
| lng+lat | fldSe9yR8gPQJ0w13 | singleLineText |  |
| location_type | fldoJtrimznegXXMV | singleSelect |  |
| identifier | fld1wYMwESqD97AXx | multilineText |  |
| display_name | fld6qmz1fOKTtXiPO | singleLineText |  |
| airtable_id | fldClDzVURyHuKRCs | multilineText |  |
| profile_img | fldwRX1JkDc0Ifea4 | url |  |
| sort | fldHadDCONpNhaUjl | number |  |
| area_type | fldmeTH1uYKReXR1v | singleSelect |  |
| active | fldyVh01KaUPHiN8F | checkbox |  |
| location_parent | fldDopzgJ8UIux1gZ | singleLineText |  |
| location_uid | fldzkXoo1ecdUE8sX | multilineText |  |
| source_record_id | fldrpUrUsOk6QAcaD | singleLineText |  |
| tenant-locations | fldivVw2dpkhL3852 | multipleRecordLinks | tenant-locations (tblR6qzSlBTZV53C3) |

### tenants.tenant-memberships

Base: app7BXcw801n19QKF; table: tblFf4aWQYA2RRsWo; primary field: fld7iIp4iQUeezEsm.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fld7iIp4iQUeezEsm | singleLineText |  |
| Person | fldZOjotMqjbHVBPh | multipleRecordLinks | people (tblyTBgrgdhT3Tcg7) |
| Tenant | fld4jRXVyOHeOJ1B0 | multipleRecordLinks | tenants (tbled1saw01Thffs1) |
| Roles | fldfqtZy0L8LslqH0 | multipleRecordLinks | roles (tblRqRbjmqmNfJtIP) |
| Status | fldEwxctIOQq4dETY | singleSelect |  |
| favorites | fldPfOaf6HytfNJUT | multipleRecordLinks | favorites (tblyp3d7BZtZmqAVM) |
| input-submissions | fldNV7AO16jQWn1eM | multipleRecordLinks | input-submissions (tblldMTDEC697hPwE) |
| optins | fldT41kNc4gcgCeGK | multipleRecordLinks | optins (tblCX61u6pjSLrP5S) |
| tenant-assignments | fldpczBj0SEPZVElf | multipleRecordLinks | tenant-assignments (tblnFOT8lGxCXD4Xh) |

### tenants.tenant-locations

Base: app7BXcw801n19QKF; table: tblR6qzSlBTZV53C3; primary field: fldpIhuENX5xwtViG.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fldpIhuENX5xwtViG | singleLineText |  |
| Tenant | fldHmrK1fPa5RRoLR | multipleRecordLinks | tenants (tbled1saw01Thffs1) |
| Location | fldDVe2J9z8Xc4Fdv | multipleRecordLinks | locations (tblN6gEy40kUlvCgf) |
| Show-week reference | fldiQ0MCXKAkRgRDm | singleLineText |  |
| Main point | fldSI3ye4z03nd4sJ | checkbox |  |
| Status | fldrSA9hwc65SuqYV | singleSelect |  |

### tenants.tenant-roster

Base: app7BXcw801n19QKF; table: tblwcczj8lFhVcbyt; primary field: fldFXXwBxYqIdEQJ2.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fldFXXwBxYqIdEQJ2 | singleLineText |  |
| Tenant | fld2cVDu9Vx5yT7l9 | multipleRecordLinks | tenants (tbled1saw01Thffs1) |
| Horse | fldjg1nLULNprFKAg | multipleRecordLinks | horses (tbljPvAk5EezdCQME) |
| Rider | fldRK86Yw3OtFgTk4 | multipleRecordLinks | riders (tbllUlTlcEMgZiW3h) |
| Status | fldhbBQM8zxYdsILv | singleSelect |  |
| Trainer association | fldek4DnPqjDVzHmw | multipleRecordLinks | tenant-trainers (tblnjrsLihxbtMkb5) |

### tenants.tenant-trainers

Base: app7BXcw801n19QKF; table: tblnjrsLihxbtMkb5; primary field: fldbuPdOAIM3BgCqF.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fldbuPdOAIM3BgCqF | singleLineText |  |
| Tenant | fld1L3vGroQcvSOBY | multipleRecordLinks | tenants (tbled1saw01Thffs1) |
| Trainer | fldL1k6SjBabQKzvH | multipleRecordLinks | trainers (tblirnrb2OwRB4RB7) |
| Association | fld9R9PJ6gdWKtHBd | singleSelect |  |
| Status | fld6yzI0lV3FL7R17 | singleSelect |  |
| tenant-roster | fldxByYCWLY3s2vjx | multipleRecordLinks | tenant-roster (tblwcczj8lFhVcbyt) |

### tenants.tenant-assignments

Base: app7BXcw801n19QKF; table: tblnFOT8lGxCXD4Xh; primary field: fldzfSOgOOsPhAIRS.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fldzfSOgOOsPhAIRS | singleLineText |  |
| Membership | fldfvSRJqGkXp0SMi | multipleRecordLinks | tenant-memberships (tblFf4aWQYA2RRsWo) |
| Horse | fldMf44bM61AOKvyT | multipleRecordLinks | horses (tbljPvAk5EezdCQME) |
| Status | flduUszU3bH1PSMjz | singleSelect |  |

### tenants.optin-options

Base: app7BXcw801n19QKF; table: tblFHcfIFjGuoE2hp; primary field: fldw8aGJzGPaGF2rY.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Option | fldw8aGJzGPaGF2rY | singleLineText |  |
| Opt-in | fldjTyeplSOcvzQ78 | multipleRecordLinks | optins (tblCX61u6pjSLrP5S) |
| Choice | fld9ShkYLyGBnI0rY | singleSelect |  |
| Value | fldpIWmcBbt406mcJ | singleLineText |  |

### tenants.favorites

Base: app7BXcw801n19QKF; table: tblyp3d7BZtZmqAVM; primary field: fldrKb9ZtNEFhBZw0.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fldrKb9ZtNEFhBZw0 | singleLineText |  |
| Membership | fldJizpqRwKrnVXie | multipleRecordLinks | tenant-memberships (tblFf4aWQYA2RRsWo) |
| Target type | fld4GqLMQUtABjMeu | singleSelect |  |
| Horse | fldLzkuOtyYsvJTjS | multipleRecordLinks | horses (tbljPvAk5EezdCQME) |
| Rider | fld1TAvAqKz3hWb4l | multipleRecordLinks | riders (tbllUlTlcEMgZiW3h) |
| Trainer | fldYiGFvw2FuTehGJ | multipleRecordLinks | trainers (tblirnrb2OwRB4RB7) |
| Status | fldM0G2uMuHzOrwxg | singleSelect |  |
| optins | fldpqliFjrA4xGPOu | multipleRecordLinks | optins (tblCX61u6pjSLrP5S) |

### recognize.people

Base: app9kOZdIaGyKk5uG; table: tbly1PM5iFYqVzKSm; primary field: fldDoGdGnbowgTYL9.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| person_uid | fldDoGdGnbowgTYL9 | multilineText |  |
| person_name | fldg1ADzOyL9rnPa3 | multilineText |  |
| status | fldVUWbBdphw1iBjW | multilineText |  |
| access_level | fldaSRNiTxO1AayAv | multilineText |  |
| first_name | fld9Ea1lkKGrgWVca | multilineText |  |
| last_name | fld6HLOo0Xx8Ztg8Y | multilineText |  |
| email | fldKFLvI3Htgb7sDx | multilineText |  |
| member_pin | fldvGu8VTdXheMP4f | multilineText |  |
| rs_phone_aliases_test | fldPGT9t8kdlNaXXu | multipleRecordLinks | rs-phone-aliases-test (tblgDWKi0Bb6OcoqS) |
| primary_phone_e164 | fld7AgPPM7RwZC7JH | singleLineText |  |
| rs_devices_test | fldec8XscAVD5TDMc | multipleRecordLinks | recognition-devices (tblfkRSJAEMzuzApR) |
| rs_recognition_sessions_test | fldrTN37vW4W11NDm | multipleRecordLinks | footprints (tblWjbASVMIjFLyW8) |
| input_access | fldOQqVnTJVitnRb4 | singleLineText |  |
| input_invite_hash | fldsysJel4UnjuacT | singleLineText |  |
| input_invite_expires_at | fldGqomZ9KpIjwnIx | dateTime |  |
| input_session_version | fldE57fezfk8W4Ku1 | singleLineText |  |
| rs_sms_requests | fld1IBUis44Mf2fRv | multipleRecordLinks | sms-verification-requests (tblxC4SYtzYhJNcJS) |
| active_iams | fldld19QotrsOvAv3 | multipleRecordLinks | ___active-iams (tbl5Kng9jk0lhE2f9) |
| Sessions | fldn20kJUpjv11fvb | multipleRecordLinks | sessions (tblBgvP7pYSv8o20j) |

### horses.horses

Base: apprSZEfQsjUPrPNk; table: tblEdWUvCqi7VRB41; primary field: fldTDfP75yYUPUVOW.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ﻿horse | fldTDfP75yYUPUVOW | multilineText |  |
| horse_id | fldLpwQtNiO6pfgv6 | number |  |
| Created | fldPxppWUsoaeR87i | singleSelect |  |
| show_name | fldxRlbEZXD1JhFnh | multilineText |  |
| wec_horses_link | fldEK81Zth5mRAvs3 | multilineText |  |
| barn_name | fld14YBE6UCCc5WWd | multilineText |  |
| trainer_id | fldldbvw8AN8xG7iz | number |  |
| tenant_id | fldvv5B8SbQzpwsAF | number |  |
| pid | fldAjsYBFawhKqHvR | number |  |
| horse_profile_tabs | fldljHSv4KOJWs1ji | multipleSelects |  |
| horse_age | fldaNedRCBS0l7MbB | multilineText |  |
| horse_disciplines | fldDNC7zTJBemuH1E | multipleSelects |  |
| horse_types | fldmG5x9Mum5uqR9b | multilineText |  |
| horse_genders | fldKGKFJV2rYl8x3D | singleSelect |  |
| horse_colors | fld2JFAtkfIWlF24N | singleSelect |  |
| emergency_phone | fldYx0TNIbGOjJVJt | singleSelect |  |
| stall_card_input_print | fld4ef9zIbwEhxw6J | multilineText |  |
| rider_list | fldY0ai5BcV8eQxpB | multilineText |  |
| rider_names | fldkOFcSy9vVQWFlR | multilineText |  |
| horse_note | fld1briI2sVBjEqx4 | multilineText |  |
| emergency_contacts | fldcPwquq5nRQojGq | singleSelect |  |
| airtable_id | fldhlHlhh3xOGgMTZ | multilineText |  |
| priority | fldV0qfuhvfR55LSp | number |  |
| profile_img | fldL0Pcz7L5YZn2jJ | multilineText |  |
| tenant_img | fldqv7zgaQvZudDph | multilineText |  |
| inactive | fldVSbxhk5GX9O6KS | singleSelect |  |
| active | fldAq9CFK4NS7Qx5v | multilineText |  |
| ignore | fld2OXKhmYSW2qRb4 | singleSelect |  |
| btk | fld7MtyXCqd5qVOEw | singleSelect |  |
| new | fldn3ejVB8zttB2p6 | multilineText |  |
| last_modified_time | fld7rgUbqW6eT8MTI | dateTime |  |
| questri-horses | fldHEJsbbK8WUdxSy | multilineText |  |
| feed_items | fldKvV6cRIf7fqH8w | multipleSelects |  |
| wec_wave_1 | fldWYeoQ71fufPwlk | singleSelect |  |

### riders.riders

Base: appfabN9KQwDHSqbG; table: tbl3Kie66Ys8DoTBv; primary field: fldfTmLwXbKfAphPn.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ﻿rider_id | fldfTmLwXbKfAphPn | number |  |
| people_id | fldpjeUq2mxFlTxaW | number |  |
| rider_number | fldI6N9JzJWzgOP0i | number |  |
| pid | fldsb29xSnpRTHdSn | number |  |
| trainers | fldbufn3safuJfRQC | number |  |
| ww_trainers | fldaig6vof7XPX9FX | number |  |
| active_tenants | fld0STKYSm97m7KcO | number |  |
| detail | fld3pfefnzaqaINpL | multilineText |  |
| name | fldBBM3Jy9Krx3Mpd | multilineText |  |
| rider_names | fldnP9ry3q8dHBUoJ | multilineText |  |
| pid_mismatch | fldbqLbglxkG8DL3y | multilineText |  |
| new_rider | fldOIHn18A73Oj9yJ | multilineText |  |
| inactive | fld6UpKrzIP33ggOQ | multilineText |  |
| team_name | fldCwgDNoEDBuFr0b | multilineText |  |
| team_names | fld3zJMGYY7vd9WTK | multilineText |  |
| lf_name | fldkDRiLJBFN4uEQY | multilineText |  |
| team_name_fallback | fldFjGWIZrNDTaKOM | multilineText |  |
| riderName2 | fldyFhhvO4U0nWu4h | multilineText |  |
| rider_name | fldeTI4PIiyaNvlbL | multilineText |  |
| run_id | fldHpSOi6m9dyy7q6 | number |  |
| last_run | fldOvtC0m8AjFh7OY | dateTime |  |
| usef_no | fldbqp0vPPRm0PNG8 | number |  |
| state | fldFb8ZKfyKvmvgE6 | singleSelect |  |
| fei_id | fldDUcTJ8Iz5UpiMx | number |  |
| ww_profiles | fld2Ub28rNC8LEZSN | multilineText |  |
| entry_oog | fldDewHeOKo2kouaB | multilineText |  |
| empty_trips_show_id | fldQ3qpiUCo6pUcno | number |  |
| empty_trips_at | fldOw4yjdcgQqVxE6 | date |  |
| trips_lists_show_id | fldxgcApUINwiCxlN | number |  |
| trips_lists_at | fld5Cgs5NY6lrGuro | date |  |

### wef-schedules.show-collection

Base: appUq5Qz9kARqFija; table: tblvGpwxGjXXNLKAU; primary field: fldPBs8TjNRB2eCgP.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| collection-id | fldPBs8TjNRB2eCgP | singleLineText |  |
| show-id | fldEqCbPYq3O5m4f5 | number |  |
| date | fldoX8kb9qLCN7se4 | singleLineText |  |
| status | fld0K6l2KqL5GS4qr | singleLineText |  |
| started | fldQV5e4rQGCcIDeL | dateTime |  |
| completed | fldN0XABymoeR1YiJ | dateTime |  |
| dataset-id | fldTiqiG4c9VqKop7 | singleLineText |  |
| row-count | fldnhGRlGlYxaB3ny | number |  |
| fingerprint | fldYBl0Us20RGGAso | singleLineText |  |
| message | fldt1Fc93yNuA34fr | multilineText |  |
| context-json | fld4r0iSpUuD1fwdt | multilineText |  |
| shows | fld5z7Ih15qleDROV | multipleRecordLinks | shows (tblBq3gXC0TYBCmMZ) |

### wef-schedules.shows

Base: appUq5Qz9kARqFija; table: tblBq3gXC0TYBCmMZ; primary field: fldDdEVjkxcSRLhnD.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| show-key | fldDdEVjkxcSRLhnD | singleLineText |  |
| show_id | fldsoT1Mu8C1j738D | number |  |
| customer_id | fldRpyGiHV9B68zzq | number |  |
| facility_id | fldD1arrRqz4mfKqT | number |  |
| show_name | fld31B1bNcaG8ucc8 | singleLineText |  |
| start_date | fldkVmohIwCRzoL5t | date |  |
| end_date | flde28lvXx7MTQV67 | date |  |
| selected_date | fldv5eN3VcviAPC77 | date |  |
| available_dates | fldqGma7WIjRZk0n9 | multilineText |  |
| is_default | fld7pXTIMUxEGftbn | checkbox |  |
| last_checked | fldRxJaakvag7S29e | dateTime |  |
| rings | fldioAYptwpTX311f | multipleRecordLinks | rings (tbliUwvXYdMhvJ3Fm) |
| schedules | fld5EepxPEJhHmZNV | multipleRecordLinks | schedules (tblqiTFqqO3zk2KJC) |
| wef-tenant-index | fldsf1b07TzrOKhtX | multipleRecordLinks | wef-tenant-index (tblrbItiPaMu1I1AE) |
| show-status | fld6su6tkDQnQrj2f | singleLineText |  |
| archive-status | fld8I4Q7DpTugyVij | singleLineText |  |
| archive-files | fldYVEpcrkVZw4qLL | multilineText |  |
| archive-notes | fldqJHSw4tJ10iMr4 | multilineText |  |
| archived-schedule-count | fldOdTz7LiliaLhHe | number |  |
| archived-collection-count | fldF9w4NCA2J1al5p | number |  |
| archive-verified-at | fldhah3Tfgn6V7xS6 | dateTime |  |
| show-collection | fldjT4NzSaKPvKx6x | multipleRecordLinks | show-collection (tblvGpwxGjXXNLKAU) |
| run-logs | fldz0P7ddAospTx8C | multipleRecordLinks | run-logs (tbluqwuXxOvgDRUBq) |
| heartbeat | fldDWA1WmfVZY8ePT | multipleRecordLinks | heartbeat (tblj077efYbhlznOJ) |

### wef-schedules.schedules

Base: appUq5Qz9kARqFija; table: tblqiTFqqO3zk2KJC; primary field: fldD7fj71uhmJRtJb.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| stage-key | fldD7fj71uhmJRtJb | singleLineText |  |
| dataset-id | fldE5wgYVFIsAs0Py | singleLineText |  |
| row-json | fld9vFXCASV0Mq9PY | multilineText |  |
| show-id | fldgcYteLS4L595fd | number |  |
| date | fldICcTPiFGPnsiX1 | date |  |
| ring | fldIfUw1tLdJOZQJC | number |  |
| group-id | fldxMLjANNTBTzb3a | number |  |
| group | flddPYKB8hT8M5zwO | singleLineText |  |
| sequence | fldtGySwmG3jOdZcX | number |  |
| class-id | fldUXr7d6y7KW5yAL | number |  |
| class-number | flddtMJ5YuHpYPiTQ | number |  |
| class-name | fldyZURKelcc86pgr | singleLineText |  |
| type | fldi29tNZ1Sswdy7J | singleLineText |  |
| sequence-type | fldSgME8gSDfst1nl | singleLineText |  |
| start-time | fldwsACVjWz4OKocy | singleLineText |  |
| end-time | fldFSz0TgZKFHESe9 | singleLineText |  |
| total-trips | fldcKjG7x2aUp3Pzr | number |  |
| completed-trips | fldDxzqAcl2EdqO7N | number |  |
| status | fld2szFI7EG2uIL0j | singleLineText |  |
| changed-at | fld8r3FWQvefm2uXs | dateTime |  |
| show-details | fldMlfZSyFJYz4ZJx | multipleRecordLinks | shows (tblBq3gXC0TYBCmMZ) |
| rings | fldu2oVtz4wW9TVbu | multipleRecordLinks | rings (tbliUwvXYdMhvJ3Fm) |

### wef-schedules.rings

Base: appUq5Qz9kARqFija; table: tbliUwvXYdMhvJ3Fm; primary field: fldqCwYlfwc9xlpJZ.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ring-key | fldqCwYlfwc9xlpJZ | singleLineText |  |
| show_id | fldLxi0gXREQ2kc3a | number |  |
| customer_id | fldz4wpCdSBL1HL1s | number |  |
| facility_id | fldvkJXctDPR1LGjE | number |  |
| ring_date | fldE4nC2hpfvSYhUY | date |  |
| ring_id | fldl1jcCTDB7XLJfi | number |  |
| ring_number | fld6D7LGCgI5dw86w | number |  |
| ring_name | fldu8w7Tfzgfn6AAG | singleLineText |  |
| is_selected_date | fldMuaSxJ8zdiSZLV | checkbox |  |
| show-details | fldgBiCNKwvYpJzr3 | multipleRecordLinks | shows (tblBq3gXC0TYBCmMZ) |
| rings-helper | fldAVJz5Dwfg43jmw | multipleRecordLinks | rings-helper (tblgFqVUAx7QlbnK8) |
| schedules | fldyVOSEugObNtQJP | multipleRecordLinks | schedules (tblqiTFqqO3zk2KJC) |

### wef-schedules.tenants

Base: appUq5Qz9kARqFija; table: tblWxfcCx2FKR7AEU; primary field: fldGHHIa8VTGOPTBg.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| tenant-name | fldGHHIa8VTGOPTBg | singleLineText |  |
| tenant-key | fldRpChQdx1KwRfxe | singleLineText |  |
| notes | fldlqO0dhVJXw3JiI | multilineText |  |
| rings-helper | flddeuPOQHvvXl9xW | multipleRecordLinks | rings-helper (tblgFqVUAx7QlbnK8) |
| barn-name | fld6cIs5N8Q2dX3D6 | singleLineText |  |
| barn-lat | fld3wdNj7mRDO19tV | number |  |
| barn-lon | fldL6rj0tb2e8iTyK | number |  |
| barn-map | fldzpvB3LlmrNE3i4 | url |  |

### wef-schedules.wef-tenant-index

Base: appUq5Qz9kARqFija; table: tblrbItiPaMu1I1AE; primary field: fldPlLldfpZYHqWyN.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| index-key | fldPlLldfpZYHqWyN | singleLineText |  |
| tenant-key | fld8FFobuTcgpxnR6 | singleLineText |  |
| tenant-name | fldEiDr5A9zBCYxb9 | singleLineText |  |
| tenant-id | fldWA0cwgRk35qGCY | number |  |
| tenant-base-id | fldYnlYYCBaCUN3HH | singleLineText |  |
| tenant-base-url | fldnuZLt3kPt5IoDL | url |  |
| show-id | fldLuPvfzBNdJd7sZ | number |  |
| active | fld9ovgqmmzHjCSCM | checkbox |  |
| barn-name | fldbLWrsc5IoaWZ1d | singleLineText |  |
| barn-lat | flda1SKgT6iEzZW43 | number |  |
| barn-lon | fldIq0ZAtEqEQgNFz | number |  |
| last-checked | fldWQ7RC3ZoVURQaS | singleLineText |  |
| last-result | fld9vBH2OmIHShFQQ | singleLineText |  |
| notes | flduNBLVp3EKRdNGg | multilineText |  |
| show-details | fldwIOH2sNsqUbAzu | multipleRecordLinks | shows (tblBq3gXC0TYBCmMZ) |

### wef-schedules.rings-helper

Base: appUq5Qz9kARqFija; table: tblgFqVUAx7QlbnK8; primary field: fldJtkosxmKdja7mR.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ring-key | fldJtkosxmKdja7mR | singleLineText |  |
| customer_id | fldxQ05YxZECrAHEu | number |  |
| facility_id | fldoJ6hgC1Dwm6Nmy | number |  |
| ring_id | fldhFNrgNwaLnXTie | number |  |
| ring_number | fldfTc3kTMGknooCR | number |  |
| ring_name | fldwlDMkMq3GxkNFV | singleLineText |  |
| notes | fldDAmjZTHau0xLE4 | multilineText |  |
| rings | fldmYHf2swhLXCHcn | multipleRecordLinks | rings (tbliUwvXYdMhvJ3Fm) |
| ring-walk | fldkxkXacWWjxLbSO | number |  |
| walk-tenant | fldY4en4NKUnQ7ypw | multipleRecordLinks | tenants (tblWxfcCx2FKR7AEU) |
| ring-walk-is-example | fldsN6cK9ag5u5vsn | checkbox |  |
| ring-nickname | fldCmV8JXTfdOoZjx | singleLineText |  |
| ring-lat | fldMkMQ3kaG14zshU | number |  |
| ring-lon | fldSHOxUD5wVCvo3y | number |  |

### wef-schedules.run-logs

Base: appUq5Qz9kARqFija; table: tbluqwuXxOvgDRUBq; primary field: fldLQ4JxQ4VfEkJ4T.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| run-id | fldLQ4JxQ4VfEkJ4T | singleLineText |  |
| started | fldpcUwJihrRCGI8L | dateTime |  |
| completed | fldBj4fXHppqPC0l6 | dateTime |  |
| result | fldLgD4WpLwoYJ6wM | singleLineText |  |
| show-id | fldJE03GxY1msdDzM | number |  |
| rows | fldBQtW0AerMdlxM3 | number |  |
| days-complete | fld9W5qqtGlsSpVhk | number |  |
| days-failed | fldzcDHTM2D86oZWL | number |  |
| summary | fld6tkmUFT5ee9ffZ | multilineText |  |
| errors | fldUF1Ca7rLKHKhal | multilineText |  |
| archive-count | fldcyFEiFL4WNJFP3 | number |  |
| archives | fld6n4FDQq3ctZtho | multilineText |  |
| shows | fldHuNEb4MeA4S9ig | multipleRecordLinks | shows (tblBq3gXC0TYBCmMZ) |

### wef-schedules.heartbeat

Base: appUq5Qz9kARqFija; table: tblj077efYbhlznOJ; primary field: fld77dsNiekciLCi2.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| heartbeat-key | fld77dsNiekciLCi2 | singleLineText |  |
| checked-at | fldRXizjW3RPC8E29 | dateTime |  |
| started | fldlhy4zm9RyLF4G9 | dateTime |  |
| selected-date | fldcdQyH3XzMNJvG7 | date |  |
| show-id | fldDYjljzdDKJeFju | number |  |
| rows | fldBmLi7X4PsqwuC6 | number |  |
| days-complete | fldB4QHqePkUFZQ8T | number |  |
| days-failed | fldTpbcWmz4GfhOmy | number |  |
| archive-count | fldvg1cXm8hre8NWO | number |  |
| run-id | fld8osWumgA7jAiX2 | singleLineText |  |
| result | fld3uQwAlgFtvRBDo | singleLineText |  |
| summary | fldkcoh2i6isaEdX2 | multilineText |  |
| errors | flduI0N0CcbCAP6TV | multilineText |  |
| shows | fldxz6ljemeBez5fR | multipleRecordLinks | shows (tblBq3gXC0TYBCmMZ) |

### wef-schedules.code-reviews

Base: appUq5Qz9kARqFija; table: tblaLOJqekCeyvKgV; primary field: fld8Jh8ZLnTTo8AiP.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| routine | fld8Jh8ZLnTTo8AiP | singleLineText |  |
| last-review | fldY8dooZDH3cKwj8 | date |  |
| last-result | fldBV0x6Lw8KtNedV | singleLineText |  |
| next-review | fldsHYcecj5hJj7Ce | date |  |
| cadence | fldwqykNIJk0gapun | singleLineText |  |
| notes | fldGBKLXOz5sXLTui | multilineText |  |

### wef-tenant-cwf.meta

Base: appxKPBtdl2Rdq7CS; table: tblOsAWFAqPJbE0Oc; primary field: fldUntAgu6AfVk7QY.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fldUntAgu6AfVk7QY | singleLineText |  |
| Notes | fldNEldeQybT3tZrQ | multilineText |  |
| Assignee | fldGwyN0B8QfJmrmH | singleCollaborator |  |
| Status | fldXELxBGQ4YX6iRN | singleSelect |  |
| Attachments | fldE0bZEw1YHAHdab | multipleAttachments |  |
| Attachment Summary | fldQ6EWtNuAhBFeue | aiText |  |

### wef-tenant-cwf.tenant-details

Base: appxKPBtdl2Rdq7CS; table: tblUoIrtdQ2Wg7DjB; primary field: fldm6P3r2BL7Atfu5.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| tenant-key | fldm6P3r2BL7Atfu5 | singleLineText |  |
| tenant-name | fld9CB6kf6eZJNi0H | singleLineText |  |
| barn-name | fldDID2LSfsG1N4s3 | singleLineText |  |
| barn-map | fldWDrXRzqMJnTMuk | singleLineText |  |
| notes | fldiAgzYZ6HoMIdgt | multilineText |  |
| tenant-id | fld3PlhKQwqvifZq1 | number |  |
| barn-lat | fldJDTCW8ktTsgYI1 | number |  |
| barn-lon | fld5BOpsyAloIv84a | number |  |
| active | fld7knf80qxsebmgS | checkbox |  |

### wef-tenant-cwf.trainers

Base: appxKPBtdl2Rdq7CS; table: tbljBHTXEJn71RRIl; primary field: fldVGHL9sFpspy76L.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| trainer-key | fldVGHL9sFpspy76L | singleLineText |  |
| trainer-name | flduPZMPpYYDpg97J | singleLineText |  |
| role | fldTSDSZXn1tEhWYs | singleLineText |  |
| last-result | fldiOJCAUfWdEwvEU | singleLineText |  |
| last-checked | fld3884oady5IwPza | singleLineText |  |
| last-error | fldTY7B4hkhUOsDMf | multilineText |  |
| trainer-id | fld2MWxq6K5HnIz9r | number |  |
| slot | fldxSvlVtAEtVOWIJ | number |  |
| active | fldxEjEX2rD5bwWYV | checkbox |  |
| entries | fldtgGRy1LiSkak3L | multipleRecordLinks | entries (tblDhAutHYlDbvzNd) |
| collections | fld39u1CSm3bt54Ws | multipleRecordLinks | collections (tblbac3wwWJAmUthL) |
| horses | fldWUTs0xfrEpODK3 | multipleRecordLinks | horses (tblxMpG75KL3Nmt1v) |
| riders | fldoIT9gXVWoOuX6B | multipleRecordLinks | riders (tblll5l7eG5wc27uP) |

### wef-tenant-cwf.retired-show-details

Base: appxKPBtdl2Rdq7CS; table: tblxutSCBQzmkJLLO; primary field: fldHSYL0YdtV4akmJ.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| show-key | fldHSYL0YdtV4akmJ | singleLineText |  |
| show_name | fldtHTaWcTNCCOjS5 | singleLineText |  |
| start_date | fldyzfhN2HHVnOCB8 | singleLineText |  |
| end_date | fldVsZGFcjg2WuMHZ | singleLineText |  |
| selected_date | fldVldg9Wt6xtH1WQ | singleLineText |  |
| available_dates | fldyK3IdBF92etXGn | singleLineText |  |
| last_checked | fldwRYEXyizeLiJQK | singleLineText |  |
| source-record | fldlsGD0xOWiH6ZwP | singleLineText |  |
| show_id | fld0yjlHFVfskM0OT | number |  |
| customer_id | fldovht7HmCZlKHqZ | number |  |
| facility_id | fldR6DC0ifBsyucQP | number |  |
| is_default | fld6zr0z6nXILwAaE | checkbox |  |
| rings | fldxYF3H7UELgYt2T | multipleRecordLinks | retired-rings (tblsVT0Poao9wKhs4) |
| entries | flds5Ok2I176egRmm | multipleRecordLinks | entries (tblDhAutHYlDbvzNd) |
| collections | fldkYTY4DCbkWtdRN | multipleRecordLinks | collections (tblbac3wwWJAmUthL) |

### wef-tenant-cwf.collections

Base: appxKPBtdl2Rdq7CS; table: tblbac3wwWJAmUthL; primary field: fldzEg3rAofdzwNV7.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| collection-id | fldzEg3rAofdzwNV7 | singleLineText |  |
| date | fld8q3pkaYklhxIZF | singleLineText |  |
| status | fldl6F36Z8jUG2aRF | singleLineText |  |
| started | fldP6NZ9jTMNem45N | singleLineText |  |
| completed | fldVDdCFr0B5PiG7h | singleLineText |  |
| dataset-id | fld9b4U7lhT7KTN6O | singleLineText |  |
| fingerprint | fld2CnXu3zrPoZGFa | singleLineText |  |
| message | fldVO4mYQfmiGamVA | multilineText |  |
| archive-file | fldFSqLhxH1TNuLON | singleLineText |  |
| show-id | fldCHbmPXiR2w5yvn | number |  |
| trainer-id | fldIdQLqrgsZ8HtUa | number |  |
| row-count | fldrvbE7cBw26nECU | number |  |
| retired-show-details | fldg0EayAJAV97hAk | multipleRecordLinks | retired-show-details (tblxutSCBQzmkJLLO) |
| trainers | fld2reJ8ELL6vPz0A | multipleRecordLinks | trainers (tbljBHTXEJn71RRIl) |
| shows | fld35eBb4fJDavkRH | multipleRecordLinks | wef-schedules.shows (tblNABa50eiRKfZS4) |

### wef-tenant-cwf.retired-rings

Base: appxKPBtdl2Rdq7CS; table: tblsVT0Poao9wKhs4; primary field: fldsBS3uyzFtry4rO.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ring-key | fldsBS3uyzFtry4rO | singleLineText |  |
| ring_date | fldRsge7UhDmCzlxk | singleLineText |  |
| ring_name | fldPyJ7hucQHOfPFm | singleLineText |  |
| source-record | fldWkeJFY8VJ0rvc9 | singleLineText |  |
| show_id | fldNsQaesQyyjPPbM | number |  |
| customer_id | fldgGevAFpkeeMEiG | number |  |
| facility_id | fldd5WFOsT45gXilY | number |  |
| ring_id | flduvh3Zm8Hl2TQLa | number |  |
| ring_number | fldVd3fJH43GIxwsS | number |  |
| is_selected_date | fld7kyo2fDj1z2qBC | checkbox |  |
| show-details | fldqXyWBcmsMg9ZBi | multipleRecordLinks | retired-show-details (tblxutSCBQzmkJLLO) |
| rings-helper | fldVyY2K7shBEK1Ux | multipleRecordLinks | rings-helper (tblu9SBWyir2z4CCt) |
| entries | fldVi7xB61Z2bTKPC | multipleRecordLinks | entries (tblDhAutHYlDbvzNd) |

### wef-tenant-cwf.entries

Base: appxKPBtdl2Rdq7CS; table: tblDhAutHYlDbvzNd; primary field: fld9osZXFFISdDHtE.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| entry-key | fld9osZXFFISdDHtE | singleLineText |  |
| date | fldWFKA0olb6KgcVe | singleLineText |  |
| class-name | fldYtzfkoJZUSTTSj | singleLineText |  |
| horse-name | fldlOSY4iy0Y7Kzd7 | singleLineText |  |
| rider-name | fldBfporWjgVfs15n | singleLineText |  |
| trainer-ids | flddNanaWoM2kMkug | singleLineText |  |
| status | fldmDisrtuGkb8zm9 | singleLineText |  |
| source-json | fldKZ8zX2nQKuAi8a | multilineText |  |
| schedule-key | fldCouqIrWvayaWTS | singleLineText |  |
| schedule-link | fldGjFWUH4UrH7XP5 | singleLineText |  |
| last-seen | fldSSRBfZuhKZ3Rin | singleLineText |  |
| show-id | fldzH429BdAEBaDru | number |  |
| entry-id | fldsttsAGkPtKyQBK | number |  |
| horse-id | fldzaNReRMFm5Ad8p | number |  |
| rider-id | fldc5u7RvXfehvZgm | number |  |
| class-id | fldk6FO81ht2SXt7W | number |  |
| ring-number | fldkmKmva0S4i7eGo | number |  |
| retired-show-details | fldGhqNtI5phVMvPv | multipleRecordLinks | retired-show-details (tblxutSCBQzmkJLLO) |
| retired-rings | fldeHNP1diNFbGtbs | multipleRecordLinks | retired-rings (tblsVT0Poao9wKhs4) |
| horses | fldnMh8lS6vhQqHZF | multipleRecordLinks | horses (tblxMpG75KL3Nmt1v) |
| riders | fldRYhtYiRgiuXN8r | multipleRecordLinks | riders (tblll5l7eG5wc27uP) |
| trainers | fld569ZjJTyE7wtLP | multipleRecordLinks | trainers (tbljBHTXEJn71RRIl) |
| shows | fldFOuJgGPlHv6M1H | multipleRecordLinks | wef-schedules.shows (tblNABa50eiRKfZS4) |
| synced-rings | fldj7GhJaBvXusEOR | multipleRecordLinks | wef-schedules.rings (tblK8EqyEVUktiLOI) |
| schedules | fld1Y4OW66hX0eBxb | multipleRecordLinks | wef-schedules.schedules (tbl3QiMIXd6Spi336) |

### wef-tenant-cwf.rings-helper

Base: appxKPBtdl2Rdq7CS; table: tblu9SBWyir2z4CCt; primary field: fldvYPGqgOdzxrF2z.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ring-key | fldvYPGqgOdzxrF2z | singleLineText |  |
| ring_name | fldcTY1e6jM9R34p2 | singleLineText |  |
| ring-nickname | fldfeMWMDE6TwUVUK | singleLineText |  |
| notes | fldpln8BAPpEGAjPw | multilineText |  |
| source-record | fldMnFEXtBfeDWGG0 | singleLineText |  |
| source-base | fldoVplBIrvh4kLAk | singleLineText |  |
| customer_id | fldSXKDyOpIeeErJ2 | number |  |
| facility_id | fldSB2PoLFXaNYxpi | number |  |
| ring_id | fld9moLXbSWov79Lp | number |  |
| ring_number | fldPzQxvwoteYY6uK | number |  |
| ring-lat | fldveyHK74IQzANB0 | number |  |
| ring-lon | fldsG7XZbRCZUWQeM | number |  |
| retired-rings | fld8wGxdSdAqk7cRS | multipleRecordLinks | retired-rings (tblsVT0Poao9wKhs4) |
| ring-walks | fldIaq0qafoDzboIt | multipleRecordLinks | ring-walks (tbl6Gk0HzDYrjxADE) |
| synced-rings | fld9HdiYBnN3AdnRF | multipleRecordLinks | wef-schedules.rings (tblK8EqyEVUktiLOI) |

### wef-tenant-cwf.ring-walks

Base: appxKPBtdl2Rdq7CS; table: tbl6Gk0HzDYrjxADE; primary field: fld4rA0fjJVMoqGPz.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| walk-key | fld4rA0fjJVMoqGPz | singleLineText |  |
| tenant-key | fldruO21V17eTPGD3 | singleLineText |  |
| ring-key | fldv4OwnNCLIpQC64 | singleLineText |  |
| barn-name | fldkBuUP7RmZTgT9U | singleLineText |  |
| confirmed-on | fldUVVmX7zPH55kDK | singleLineText |  |
| notes | fldzOVyadkVpb9Avj | multilineText |  |
| minutes | fldHY2VtLfi7sdwzP | number |  |
| is-example | flddyZhOqxte1eg7U | checkbox |  |
| rings-helper | fldcm2TdHGgGVINxf | multipleRecordLinks | rings-helper (tblu9SBWyir2z4CCt) |

### wef-tenant-cwf.horses

Base: appxKPBtdl2Rdq7CS; table: tblxMpG75KL3Nmt1v; primary field: fldbVbMe1iYOqPzBb.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| horse-key | fldbVbMe1iYOqPzBb | singleLineText |  |
| horse-name | fldpBZEyAYofFqLgC | singleLineText |  |
| trainer-ids | fldwRCycaSIS7APAp | singleLineText |  |
| first-seen | fldahCCdflOeVOSJa | singleLineText |  |
| last-seen | fldqSxHljhSxfV05C | singleLineText |  |
| source | fldT5ig0Arc3b1idQ | singleLineText |  |
| horse-id | fldxjULm2yWAlrpLQ | number |  |
| disabled | fldiIqkeZk67iOSey | checkbox |  |
| entries | fldPVhJF1rYMDvzQZ | multipleRecordLinks | entries (tblDhAutHYlDbvzNd) |
| trainers | fldFIY4EdFhCe6iA1 | multipleRecordLinks | trainers (tbljBHTXEJn71RRIl) |
| name | fldLQPowtCKtbG50g | singleLineText |  |
| nick-name | fld7fqFPpOoR5JOWZ | singleLineText |  |
| show-name | fldlM2BEPWhUCFmzL | singleLineText |  |
| aliases | fldnLX0zTsgoZqd5m | multilineText |  |
| notes | fldZNfkULxyrv081m | multilineText |  |
| trainer-name | fldty8YGzxOneqpxF | singleLineText |  |
| source-base | fldQ7F6H6hsZWkG8k | singleLineText |  |
| source-table | fldNfjl2X3BPntCMf | singleLineText |  |
| source-record | fldbtC1Fz7VlOhBcL | singleLineText |  |
| source-link | fldDXUzgSSg4C9sWR | url |  |
| source-name | fldB2QO051SMbNok9 | singleLineText |  |
| helper-refreshed | fld7BPotwta2axzxe | singleLineText |  |
| tenant-id | fldMZONCiJamVFvEi | number |  |
| trainer-id | fldGwwKjNT4yY0P18 | number |  |
| source-inactive | fldUnbWoBNaNDvpzm | checkbox |  |
| source-id-missing | fldPzj5YmELHHuuNB | checkbox |  |
| lookup-text | fldtYpQQCJDwfMLHP | formula |  |
| riders | fld1NS33LT88kykq1 | multipleRecordLinks | riders (tblll5l7eG5wc27uP) |

### wef-tenant-cwf.riders

Base: appxKPBtdl2Rdq7CS; table: tblll5l7eG5wc27uP; primary field: fldekTBs6wsQFVFNx.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| rider-key | fldekTBs6wsQFVFNx | singleLineText |  |
| rider-name | fldBgPmPMfkG6CF0Z | singleLineText |  |
| trainer-ids | fldHwxum1XvrrIsWH | singleLineText |  |
| first-seen | fld63CBywkzAPIwuS | singleLineText |  |
| last-seen | fldY1bWd8J79YYc7R | singleLineText |  |
| source | fldOT2fGp8YyxU0Q4 | singleLineText |  |
| rider-id | fldI4L48fp0hJ2cUJ | number |  |
| disabled | fldPrnMCimCdW0PSF | checkbox |  |
| entries | fldEANCeAdn4KgbDN | multipleRecordLinks | entries (tblDhAutHYlDbvzNd) |
| trainers | flddwrR89IEn8B1gy | multipleRecordLinks | trainers (tbljBHTXEJn71RRIl) |
| name | fld5PjHm96bi6QoMs | singleLineText |  |
| nick-name | fldHYyU2h5onlECBY | singleLineText |  |
| legacy-display-name | fldjjthpKFDPZB6Ee | singleLineText |  |
| aliases | fldxD6yANHFnzMLAv | multilineText |  |
| notes | fldV7emObMm6BGTs3 | multilineText |  |
| trainer-name | fldMqATzLBqo07u2w | singleLineText |  |
| source-base | fldrf3lv6L0Cp1wRz | singleLineText |  |
| source-table | fldc5LmAA6YuG0jMA | singleLineText |  |
| source-record | fldLQlfz3k3CasBK4 | singleLineText |  |
| source-link | flduqjXDlSxHVbpOy | url |  |
| source-name | fldrHQnFbK89RMLh6 | singleLineText |  |
| helper-refreshed | flda9EM4C4bsj5bvU | singleLineText |  |
| tenant-id | fldDCCOZrkcRb3Flk | number |  |
| trainer-id | fldo07kJQBpBmF35l | number |  |
| source-inactive | fldnirmsWCzA1o1LP | checkbox |  |
| source-id-missing | fldWvylseROSaYmRA | checkbox |  |
| first-name | fldU13LSuwxuYKw1c | singleLineText |  |
| last-name | fldVTPEEespbKrs5B | singleLineText |  |
| lookup-text | fldMT2MGh4Da4TtuZ | formula |  |
| horses | fldKt8o5QoLla9eYL | multipleRecordLinks | horses (tblxMpG75KL3Nmt1v) |

### wef-tenant-cwf.run-logs

Base: appxKPBtdl2Rdq7CS; table: tblYj2xTQG0Fq3QhU; primary field: fldjs4VY5CqbxR4eL.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| run-id | fldjs4VY5CqbxR4eL | singleLineText |  |
| started | fldPUHFs3DUEmWEvI | singleLineText |  |
| completed | fld45KMthYz1r2PPO | singleLineText |  |
| result | fldqgh2wwwsu0R0XP | singleLineText |  |
| summary | fldWwx7EIk6ZcerBH | multilineText |  |
| errors | fldfldvR9JDl4OJfC | multilineText |  |
| archives | fld3MPzeQZTDeTKNE | multilineText |  |
| show-id | fldMHaAsp4nHvvqZH | number |  |
| rows | fldMbYCl3rbYBat1E | number |  |
| days-complete | fldKHzY6vpK7W6dh5 | number |  |
| days-failed | fldAANJuOGjZHIqww | number |  |
| archive-count | fld2Kv0dMcdJFVKrY | number |  |

### wef-tenant-cwf.heartbeat

Base: appxKPBtdl2Rdq7CS; table: tblLwQ0JFyuHvcp82; primary field: fld4pab3IHCO9TWkV.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| heartbeat-key | fld4pab3IHCO9TWkV | singleLineText |  |
| checked-at | fldCY5muTAkKeRaq1 | singleLineText |  |
| selected-date | flda5nSAUL7EKQxPF | singleLineText |  |
| result | fldt20ictVX4whAp6 | singleLineText |  |
| summary | fldN4PnNRkGjx1dBF | multilineText |  |
| errors | fldJxzzqHfNaqJATa | multilineText |  |
| show-id | fldjy9eF54ZJp46pm | number |  |

### wef-tenant-cwf.code-reviews

Base: appxKPBtdl2Rdq7CS; table: tblXKj2knWLvJ80mQ; primary field: fldEGZTUJoyUVkl42.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| routine | fldEGZTUJoyUVkl42 | singleLineText |  |
| last-review | fldC3j8ujTmMRImi1 | singleLineText |  |
| last-result | fldL5dePZNAH93VAF | singleLineText |  |
| next-review | fldgME0wnbtjB3Afb | singleLineText |  |
| cadence | fldJ5KRFlwDqpyZ2a | singleLineText |  |
| notes | fldhQdMpeMTMijjhT | multilineText |  |

### wef-tenant-cwf.wef-schedules.shows

Base: appxKPBtdl2Rdq7CS; table: tblNABa50eiRKfZS4; primary field: flde5Hyc5CiWaJe7A.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| show-key | flde5Hyc5CiWaJe7A | singleLineText |  |
| show_id | fldosGQgLdl07R1xM | number |  |
| customer_id | fldzcTmmyiIsAkJtp | number |  |
| facility_id | fldvYEbZXmHOMvv7b | number |  |
| show_name | fldEM4RYcZx9KPS6E | singleLineText |  |
| start_date | fldqtOGUyMx9VxUyZ | date |  |
| end_date | fldP7eykTNVb6ag1g | date |  |
| selected_date | fldiy0bacMirR5gVk | date |  |
| available_dates | fldaL6zUpOELwJJqp | multilineText |  |
| is_default | fld5XNnNZvCLUyfvR | checkbox |  |
| last_checked | fld4jj02Obvy7Ar4q | dateTime |  |
| rings | fldFMKpc7zPaKRarX | multipleRecordLinks | wef-schedules.rings (tblK8EqyEVUktiLOI) |
| schedules | fldtzT26iUZfm4rsr | multipleRecordLinks | wef-schedules.schedules (tbl3QiMIXd6Spi336) |
| wef-tenant-index | fldjWqiLE6giOkPOu | singleLineText |  |
| show-status | fld1vugdxUn6tVEcc | singleLineText |  |
| archive-status | fldx5W0hfPpGwT53G | singleLineText |  |
| archive-files | fld0DEh4W86BE9FCI | multilineText |  |
| archive-notes | fldnP6Lgp6y6tTnWu | multilineText |  |
| archived-schedule-count | fld5F9NDpgzxFDmLa | number |  |
| archived-collection-count | fld2VKtSXibBh9gXK | number |  |
| archive-verified-at | fldPu6n1fs70dfeDp | dateTime |  |
| show-collection | fldDpVaimdlGpNr5v | singleLineText |  |
| run-logs | fldLxXO71m9QcyUIi | singleLineText |  |
| heartbeat | fldeM93Sr25shvMLo | singleLineText |  |
| entries | fldjuWWW8YiFGvpP7 | multipleRecordLinks | entries (tblDhAutHYlDbvzNd) |
| collections | fldNLEFhUJaM0YMuf | multipleRecordLinks | collections (tblbac3wwWJAmUthL) |

### wef-tenant-cwf.wef-schedules.rings

Base: appxKPBtdl2Rdq7CS; table: tblK8EqyEVUktiLOI; primary field: fldK4aRWkmt0pSH6f.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ring-key | fldK4aRWkmt0pSH6f | singleLineText |  |
| show_id | fld6SPvZC7aEZya8H | number |  |
| customer_id | fldmtqrY1JBNwedNO | number |  |
| facility_id | fldOb4gP2waIqsnqj | number |  |
| ring_date | flduWcZM9ZQU34jEp | date |  |
| ring_id | fldZ4aw4wEEytBqVu | number |  |
| ring_number | fldmVimcKzYkPFX8M | number |  |
| ring_name | fldyfHg0RYC21teg5 | singleLineText |  |
| is_selected_date | fldBMDuyt49oLSloI | checkbox |  |
| show-details | fldryAyhYTdSydreW | multipleRecordLinks | wef-schedules.shows (tblNABa50eiRKfZS4) |
| rings-helper | fldEt8ftwPSpGQA1j | singleLineText |  |
| schedules | fldx3CGfYOVTKk548 | multipleRecordLinks | wef-schedules.schedules (tbl3QiMIXd6Spi336) |
| entries | fld5gXh69afMnbFvY | multipleRecordLinks | entries (tblDhAutHYlDbvzNd) |
| rings-helper 2 | fld31569MgpmeZJ3a | multipleRecordLinks | rings-helper (tblu9SBWyir2z4CCt) |

### wef-tenant-cwf.wef-schedules.schedules

Base: appxKPBtdl2Rdq7CS; table: tbl3QiMIXd6Spi336; primary field: fldpHS4mKokyOINOs.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| stage-key | fldpHS4mKokyOINOs | singleLineText |  |
| dataset-id | fldfnPKiflX9rkZU4 | singleLineText |  |
| row-json | fld7NHnuPYQbq160W | multilineText |  |
| show-id | fldwg9LvU4OW5zMKj | number |  |
| date | fldt5WTUSQwDwEhrs | date |  |
| ring | fldAU0QQO7QQaBl89 | number |  |
| group-id | fldcNHQw0Ob0Iip7H | number |  |
| group | fldiIRzU1Y23VQs0G | singleLineText |  |
| sequence | fldJQLhzkZl9HK8ZZ | number |  |
| class-id | fldXv9MAOOMq5Dt4w | number |  |
| class-number | fld0LNOrVRjFslRjI | number |  |
| class-name | fldtjLPUOncytYUqC | singleLineText |  |
| type | fldSVWDcDgu1Z7gIS | singleLineText |  |
| sequence-type | fldY9N9AU6j5Idciv | singleLineText |  |
| start-time | fldmsMu86fSEifRig | singleLineText |  |
| end-time | fld2m5Q265hkSlQUQ | singleLineText |  |
| total-trips | fldd8WTUvWQKnPcGF | number |  |
| completed-trips | fldGPXbqYKv6Qxec2 | number |  |
| status | fldhHX8t8favPrgmH | singleLineText |  |
| changed-at | fldvogrqvHouyHkXr | dateTime |  |
| show-details | fldbrC20GLs516ceT | multipleRecordLinks | wef-schedules.shows (tblNABa50eiRKfZS4) |
| rings | fldc3ExLA6wMQVRen | multipleRecordLinks | wef-schedules.rings (tblK8EqyEVUktiLOI) |
| entries | fldPrvoErIn1ee7Ck | multipleRecordLinks | entries (tblDhAutHYlDbvzNd) |

### RingStatus.live_groups

Base: apptdhhNzduxm5gjn; table: tblJeVfA8Q1Te6N5k; primary field: fldDzmk5OdqVX8MZa.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| live_groups_key | fldDzmk5OdqVX8MZa | singleLineText |  |
| class_group_id | fldi6VKnTPuBKdAox | number |  |
| show_id | fldFNeSDuiY1amNEo | number |  |
| show | fldEs8jPHPE4jKniH | multipleRecordLinks | show (tbl72LDQ2xD9xpRAC) |
| is_cuurent_scope | fldbZWxTAwQqLD0uT | checkbox |  |
| live_focus_day | fldvoOSNS6wREqdIZ | date |  |
| dropped_at | fldR32yNdHYT9RJra | singleLineText |  |
| live_classes | fldrK1AlCjX8lbfdB | multipleRecordLinks | live_classes (tblHJ40qvD8eNWkvL) |
| watch_trips | fldp9Fxj8WJAJUPHJ | multipleRecordLinks | watch_trips (tblCtNpzJtrI19VKq) |
| customer_id (from show) | fldqSMTylaqXvrVYf | multipleLookupValues |  |
| groups_live_key | fldf6XO5rTti2SiH4 | formula |  |
| day | fldpuXI4yJTtlrcXj | singleLineText |  |
| ring_number | fldLJ7zjgs9eJ0p7T | number |  |
| ring_id | fldCrTPO2vHTswvc2 | number |  |
| group_name | fldx0K2rDMUsYu2G5 | multilineText |  |
| classes | fldqpMopCZht8ztIh | multilineText |  |
| estimated_start_time | fldX5hHFLXzurO8Ed | singleLineText |  |
| gone | fldDwdynDS70tv3xx | number |  |
| total | fld978qyPN1Esd63A | number |  |
| curr_updated_at | flduAsF1GSROaeWwp | singleLineText |  |
| ingested_at | fldDw7Km4LYPzRP1y | singleLineText |  |
| run_tag | fldbkrPxrLugAQetG | singleLineText |  |
| created_time | fldhPCuG1Y2Y7JWVv | createdTime |  |
| last_modified_time | fldZcBJe5coKSwpzT | lastModifiedTime |  |
| cg_record_id | fldJdSzG66mKoxebM | formula |  |
| is_live | fld4FMfalFWJFRjes | checkbox |  |
| has_JSON | fldCwF6DMxynCB4DG | checkbox |  |
| interval | fldsG8ZyCcWJhEASI | formula |  |
| status | fldJhbaQSSOeBhpO9 | singleLineText |  |
| class_status | fldfoaUZuqgIjosws | multipleRecordLinks | class_status (tbllRZL2YIctMS2nK) |
| watch_schedule | fldBq9XrBau7EgtcX | multipleRecordLinks | watch_schedule (tbl3LHjCmuMu2uuoZ) |
| ww_rings | fldeSSA9YMjwZ0MHu | multipleRecordLinks | ww_rings (tbl8CoOJEvQJbN7kZ) |
| trips_count | fldN4o58ADps0zysg | count |  |
| groups_live_endpoint | fld2TaC4oPHQaiwXH | formula |  |
| classNumbers | fldf2Jy6ZKnX5are7 | singleLineText |  |
| class_numbers | fldsKy8iqlSDkaTja | singleLineText |  |
| show_key | fldaP7P2T2mL3Wg8P | number |  |
| customer_id | fldciqETIkVWC8Nl2 | number |  |
| watch_rings | flddg7wNvBcuVGyZ1 | multipleRecordLinks | watch_rings (tblJUchoYECk9ptyJ) |
| record_id | fldb6ZaLQF4nJ4AbZ | formula |  |
| classNames | fld42lCxpCccuskMg | multilineText |  |
| class_ids | fld8PJbMJ8lFk6wJl | multilineText |  |
| class_names | fldZMSABi48ZsBbYR | multilineText |  |
| focus_day (from show) | fldbIGD0CSwGcrAXr | multipleLookupValues |  |
| focus | fldEzsqut87r4U58o | formula |  |
| live_group_changes | fld9o5nUadBdHWBiF | multipleRecordLinks | live_group_changes (tblQ9ldsMUbcv3nb0) |
| live_rings (live_group) | fldOGaWgyoxGHgZPP | multipleRecordLinks | live_rings (tbl1RVP68zTWu83LL) |
| live_rings (next_group) | fldZ1RwV67tFazBOy | multipleRecordLinks | live_rings (tbl1RVP68zTWu83LL) |
| live_rings (focus_group) | fldMkcRQsv400tNop | multipleRecordLinks | live_rings (tbl1RVP68zTWu83LL) |

### RingStatus.live_rings

Base: apptdhhNzduxm5gjn; table: tbl1RVP68zTWu83LL; primary field: fldPSSACAlII6cAlc.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ring_key | fldPSSACAlII6cAlc | singleLineText |  |
| response_ready | fldOYZoch5OkQCm36 | checkbox |  |
| is_latest | fldOGhrbhkbQEQpCr | checkbox |  |
| ring_id | flduQ1lErvAYM7zus | number |  |
| show | fld8HDbdERiZY2Yys | multipleRecordLinks | show (tbl72LDQ2xD9xpRAC) |
| show_id | fld56daduCgovLMiJ | number |  |
| focus_day | fldxFBZPhVSe2Kzrp | date |  |
| ring_number | fldgnQru0LWPM3ezU | number |  |
| ring_name | fldXGufIaCbTRVeH5 | singleLineText |  |
| is_current_scope | fldxax1G9skGt4BDK | checkbox |  |
| dropped_at | fldoLSYUNgkN7tHno | date |  |
| as_of | fldkyfwKhMiLipBPF | dateTime |  |
| last_seen_at | fldhtWLz8f4eZbTZD | dateTime |  |
| payload_hash | fldsIDz6wdUsAMjNb | singleLineText |  |
| ring_query_key | fldqN6iZvg91YOL4k | singleLineText |  |
| live_group | fldijdDl4mDziFb4q | multipleRecordLinks | live_groups (tblJeVfA8Q1Te6N5k) |
| live_class_group_id | fldCJcvviJ5XFxdR6 | number |  |
| live_status | fld7iKEA3pcqiwuoH | singleLineText |  |
| live_start_time | flddwYz8uXNtCAVc5 | singleLineText |  |
| live_gone | fldhlw1B2woy6k7CZ | number |  |
| live_total | fldAXHvFZ84EUq98C | number |  |
| live_progress | fldKmKOXIAWc84662 | singleLineText |  |
| live_estimated_end_time | fldZdrSBg74WiyAMX | singleLineText |  |
| live_minutes_to_end | fldeyAc0J9fccumSG | number |  |
| next_group | fld6cVl43oEklUStG | multipleRecordLinks | live_groups (tblJeVfA8Q1Te6N5k) |
| next_class_group_id | fldxhdZtXPekMXp5I | number |  |
| next_start_time | fldlRFYV3vBVY8MQ1 | singleLineText |  |
| next_minutes_to_start | fldRVfZT9O3OO2na9 | number |  |
| next_late_minutes | fldWHlQOY7nRBSNVN | number |  |
| next_late_status | fldZQFwenpIUq6pck | singleLineText |  |
| focus_group | fldum9DHK66Q8nZT1 | multipleRecordLinks | live_groups (tblJeVfA8Q1Te6N5k) |
| focus_class_group_id | fldL3qK1f9dIJZzPn | number |  |
| focus_class_number | fldCfbBmUxoBPqfTQ | singleLineText |  |
| focus_start_time | fldevhu2kQKlTeYL5 | singleLineText |  |
| focus_minutes_to_start | fldVkmlWsO6Dw5ezV | number |  |
| focus_late_minutes | fld4aEf5bi48eb8ih | number |  |
| focus_status | fldLir6jOQqQvrbrJ | singleLineText |  |
| ring_state | fldL3CtU8adfSIevY | singleLineText |  |
| ring_running_late_minutes | fld2PfbuJm9uHbpHr | number |  |
| ring_delay_status | fldSw1tdybHWtjbL2 | singleLineText |  |
| ring_summary | fldGqez7zWvYba7wZ | multilineText |  |
| sms_ring_status | fldcHklHgqyVf0TOg | multilineText |  |
| snapshot_json | fldaMcZ8sUZMV11sk | multilineText |  |
| snapshot_hash | fld53zrZS9tAiymxi | singleLineText |  |
| as_of_time | fldOfH7kpaNsHczaL | singleLineText |  |
| created_time | fldtee1xA9zm7bwKD | createdTime |  |

### RingStatus.live_classes

Base: apptdhhNzduxm5gjn; table: tblHJ40qvD8eNWkvL; primary field: fldbLdQfDubkOTO7U.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| log_key | fldbLdQfDubkOTO7U | singleLineText |  |
| source_view | fldKEFtVTqkvX6RkL | singleLineText |  |
| live_groups | fldrt8Uf83ad0Mb0L | multipleRecordLinks | live_groups (tblJeVfA8Q1Te6N5k) |
| watch_trips | fldw7bZHPbuJTP37O | multipleRecordLinks | watch_trips (tblCtNpzJtrI19VKq) |
| show_id | fldds5Ts9vYpjmY8p | number |  |
| is_current_scope | fld5Nfv1g4ZXHsg10 | checkbox |  |
| focus_day | fldCEu0B8TLxNq7Wf | date |  |
| class_group_id | fld0pz0YAIOmZz9YY | number |  |
| class_id | fld45XwMapCocN72E | number |  |
| entry_number | fldmR6xVeR8PEbpEm | number |  |
| rider_name | fldGOck6rysfH9jTA | singleLineText |  |
| horse | fldGS9MTcNuOeBGpF | singleLineText |  |
| payload_row_id | fldz2E8RKRlUnVp8O | singleLineText |  |
| oog | fldEn3hj4PYIEYOWr | number |  |
| actual_oog | fldAGE8gcIllkWTSb | number |  |
| gone | fldSWtQqbB5IV7Kk2 | number |  |
| scr | fldAYFwvGqd1cBQe4 | number |  |
| pos | fldkuJVOMpRjEDb8V | number |  |
| field_changed | fldUFJP3bhyT5v9ED | singleLineText |  |
| old_value | fldftS1jzUF3KPlEa | singleLineText |  |
| new_value | fldkhufHXhJDu8E1T | singleLineText |  |
| detail_fetched_at | fldSyG3TTWbpscDIx | dateTime |  |
| run_tag | fld4itV5akCUOn9mG | singleLineText |  |
| class_detail_endpoint | fldzgevf2co3yKfuw | singleLineText |  |

### RingStatus.watch_schedule

Base: apptdhhNzduxm5gjn; table: tbl3LHjCmuMu2uuoZ; primary field: fldn3bZ9Uhdf1lk6M.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| schedule_key | fldn3bZ9Uhdf1lk6M | singleLineText |  |
| ignore | fldqBvyu1R1ux6sJ1 | checkbox |  |
| dropped_at | fldOwufamYFvlvJqO | date |  |
| show_id | fldFuKv9v7X7XqqLw | number |  |
| scheduled_date | fldgYI2jdP2W3GcWP | singleLineText |  |
| conflict | fldG8IU9Iw8WxgXtu | checkbox |  |
| is_target | fldYVItS0aJebUH7y | checkbox |  |
| shifted_to_next_dayv2 | fldtLXCWgQ2oSPPnn | checkbox |  |
| eval_key | fldHXXv3LjvjXUflq | formula |  |
| evaluate_key_date | fldiQLM1ZE5uvf1eT | formula |  |
| inactive | fldZ1u2LRttYI6vRq | checkbox |  |
| shifted_to_next_day | fldW3L4anINTJOnc4 | formula |  |
| new_schedule_key | fldvGIVNzxztIU1LQ | formula |  |
| heartbeat | fldp1qJhWITiLueqo | multipleRecordLinks | heartbeat (tblJsYPLOBiaIkcId) |
| show (from heartbeat) | fld0qOWXFru0eUS1l | multipleLookupValues |  |
| customer_id | fldXtEIfzGLXosHyQ | number |  |
| customers | fld6FZs3AU31gD894 | multipleRecordLinks | customers (tbl91NqVHM3gk4gTJ) |
| class_number | fldqCQmpheiSzv85v | number |  |
| class_numbers | fldN4F3BgTTwcJB5V | multipleRecordLinks | class_numbers (tblRDVAWkVMJjIlJA) |
| class_group_id | fld1cc1gxno1hTTNu | number |  |
| class_id (from class_id_lookup) | fldnd7i7VnIsN0IFc | multipleLookupValues |  |
| class_id | fldtRgws0T6PSbvFd | number |  |
| YYYYMMDD | fldCNbDXCw4lPf6mw | formula |  |
| app_show_idv2 | fldOsppTThTFklwP0 | number |  |
| app_sql_datev2 | fldD4WY2MfodJKGge | singleLineText |  |
| app_dow_rawv2 | fldOZrk3Va3jZhL0c | singleLineText |  |
| record_state | fld00VATolZrYddRf | singleLineText |  |
| ringName | fldP17wWtizmofz6U | formula |  |
| classes_endpoint | fldl0BJhh8JrwwxGX | formula |  |
| show_date | fldd75bslCdZZdghT | singleLineText |  |
| class_groupxclasses_id | fldvFwyusNAzBpURC | number |  |
| groups_live | fldytLC3prQSYPF3a | multipleRecordLinks | groups_live (tbl1CKMI3t2fA6B2c) |
| class_search | fldE7FTBw4KiWeWYo | formula |  |
| start_display | fldz6ft8u7datsUEj | formula |  |
| class_group_sequence | fldqgrnmwyNukaE1C | number |  |
| group_name | fldzBoNvfhyHUPzie | singleLineText |  |
| class_name | fldr2LjgnNrPrPVmu | multilineText |  |
| class_names | fld5Mrb6p0JYMP2wa | multipleRecordLinks | class_names (tbl0lziEm2r3AMK29) |
| estimated_start_time (from watch_trips) 2 | fld5lCxWskllQPEO5 | multipleLookupValues |  |
| estimated_start_time (from class_names) | fldnDP6CDkMBrzL8r | multipleLookupValues |  |
| estimated_start_time | fldRP7CIUhx9sj1dJ | singleLineText |  |
| start_display (from watch_trips) | fldgv1104cVn7fglX | multipleLookupValues |  |
| Count (watch_trips) | fldqrfIgL402b0XR7 | count |  |
| watch_trips | fldROZUwR1k9F6LW7 | multipleRecordLinks | watch_trips (tblCtNpzJtrI19VKq) |
| rider_name (from watch_trips) | fldFfKmMVqPr2bw0Z | multipleLookupValues |  |
| record_id_rollups | fldrT95HGDpYQkXuW | rollup |  |
| total_trips (from watch_trips) | fldMSWHf747dzQI1s | multipleLookupValues |  |
| estimated_end_time | fld9tmkqVzk2y4gdS | singleLineText |  |
| ring_number | fldnqXMJAS1McxTZO | number |  |
| ww_rings | fldzfYXK8KZK8UcJo | multipleRecordLinks | ww_rings (tbl8CoOJEvQJbN7kZ) |
| ring_nickname | fldkS2C9lNfqS69BP | formula |  |
| group_display | fldYkV4ZHIndBAlPK | rollup |  |
| show_days_report_title | fldrSfvfkSR43jusq | formula |  |
| show_days_display_date | fldkCer8IRbnKFQQg | formula |  |
| dt | fldYyrGQtEhKPnTiZ | formula |  |
| sid | fldIhkHBhD4bfb3IM | formula |  |
| record_id | fldXhGbViswrIus6r | formula |  |
| status | fldNUOtoM7RvIeaAi | singleLineText |  |
| class_status | fldnNslm5vmS7GVxL | multipleRecordLinks | class_status (tbllRZL2YIctMS2nK) |
| latest_ingested_at | fld7VVtd6mqzuNZT3 | singleLineText |  |
| manual_time_override | fldJbVKifKVRCkt1C | checkbox |  |
| time-check | fldDVmDyxWWUmcmvz | formula |  |
| schedule_sequencetype | fldXOpfxo0b0oi5n0 | singleLineText |  |
| is_usf | fldVfpHE2TfXQ5JNE | formula |  |
| group | fldjUizn4Ue2V904Z | rollup |  |
| prior_class_group_idv2 | fldRsdoFABvq12gsT | number |  |
| rollup_entries | fldb6FfissLzxaMCl | rollup |  |
| rollup_trips | fld2MxgtkwTXQ1sSb | rollup |  |
| rollup_horses | fldXr0jRyRefCOrdb | rollup |  |
| firstup | fld1YxwSdATwCPorT | checkbox |  |
| expected_first | fld8wtzF9jtEOW6A7 | formula |  |
| expected_second | fldExtEsqa4wphJIi | formula |  |
| shows | fldcFcbCSO6pS2pQp | multipleRecordLinks | shows (tblqetAgwMf3TkLyN) |
| mode (from heartbeat) | fldFeQoFKA0yTwyBV | multipleLookupValues |  |
| expectFirst | fldjR9uapCnzem4NR | formula |  |
| expectSecond | fldgBQXYsWv6EOpdl | formula |  |
| full_schedule_endpoint | fldSsb776mkKCYhXA | formula |  |
| isFirstUp | fld0nNT6yJ4g1rRE5 | formula |  |
| time_sort | fldYt5r7uiSv6Eg8R | formula |  |
| class_type | fldxZrmEeUa1mMxRd | singleLineText |  |
| oog_endpoint | fldBweI3f9jGY2KRp | formula |  |
| watch_rings_uid | fld6l45Dmdd4p9kmF | formula |  |
| class_alerts | fld7Mk79GKZoC800o | multipleRecordLinks | class_alerts (tbl1CWj8m6Egd4XTe) |
| created_time | fld6mEVJU7cLQCO0F | createdTime |  |
| app_sql_date | fldS9Sw0GxLO06hXH | formula |  |
| app_dow_raw | fldIB2rUUGJZb9nMW | formula |  |
| class_types | fldvkz1Hc5ZlwUonO | multipleRecordLinks | class_types (tblb0ajwYCxUqB1ow) |
| watch_rings | fldUZAWZHQf1NSZ2W | multipleRecordLinks | watch_rings (tblJUchoYECk9ptyJ) |
| run_time (from watch_rings) | fldayJ5c3OwR1zij0 | multipleLookupValues |  |
| next_group_id (from watch_rings) | fldnCJ324I4kK9pSe | multipleLookupValues |  |
| focusTargetGroupId | fldeQmHQokKCAMNWs | number |  |
| immediatePriorGroupId | fld7h9HEb0vAUuH9z | number |  |
| diif | flddIxFDYI8ULcJCU | checkbox |  |
| 8778_sched_display | fldKa0XjhHeyepGo8 | rollup |  |
| 8778_group_display | flduQStzelygxkoUa | rollup |  |
| 8778_group_alert | fldjzoWwlnD3U8KPG | rollup |  |
| 40362_group_display | fld0E25cC84T3HvjA | rollup |  |
| active_alerts | fldB8fGVPnSpW4JAy | multipleRecordLinks | active_alerts (tblgkCC4vcXIjSEn7) |
| trip_logs | fldpIttRHce0GLspl | multipleRecordLinks | trip_logs (tblwneYKbZDCWBGST) |
| class_sequences | fldLkyHw9R4JhgxX9 | multipleRecordLinks | class_sequences (tbl4obvgqOkPexmVX) |
| is_handy | flddw3gaT2T1yo9Vj | formula |  |
| is_warmup | fldDHIq2nkmH3rZz9 | formula |  |
| is_mulligan | fldySknRFZPpjIUGY | formula |  |
| is_schooling_pony | fldSftr3BvqKqIjmm | formula |  |
| is_add_back | fld4ilWb33v7KFnsd | formula |  |
| is_focus_trip | fldtH6a467yi3aaiE | checkbox |  |
| entry_sequence | fldOFDlMQ3FIZBMhC | number |  |
| targets_last_at | fld4uymnfnO8Y9EdK | singleLineText |  |
| group_name_tags | fldNBreH0T5EKlzGt | multilineText |  |
| group_tags_links | fldWSEQUpxZi6Ecq3 | multipleRecordLinks | group_tags (tbl1AEkqF5WEkepRU) |
| is_current_scope | fldzzhwcGA6AqHsHL | checkbox |  |
| day_number | fldz4TPLMyFv4mes8 | number |  |
| schedule_endpoint | fldEOHTxLlFSzlIdp | formula |  |
| schedule_empty_endpoint | fldY699Ch4rAsgqFX | formula |  |
| classes_endpointv2 | fldbNqscYFVpsVTE5 | formula |  |
| enrich_priors_endpoint | fldVs5qJywBfkHIm0 | formula |  |
| schedule_show_datev2 | fldeRkrEU1se0DaYs | singleLineText |  |
| schedule_show_display_datev2 | fldkeSjHZmSj00jcG | singleLineText |  |
| not_target | fldpE8RKSRl4P9d74 | formula |  |
| focus_day | fldVeNkrDCZ3bNnIs | date |  |
| show | fldsFteXIoeRY7lsO | multipleRecordLinks | show (tbl72LDQ2xD9xpRAC) |
| shifted_to_next_day (from heartbeat) | fldEn4j5eoAf7I2Mv | multipleLookupValues |  |
| run_tag | fld916kUIYymytgTH | singleLineText |  |
| ringName_esp | fld5DeZT1mIsZrkKY | formula |  |
| estimated_start_time (from watch_trips) | fldbbXau88TfHUtHR | multipleLookupValues |  |
| total_trips | fldXCJsgKvKCLJ70X | number |  |
| ring_nickname_esp | fldhRaN980ZcM3Qv5 | multipleLookupValues |  |
| ring_nickname2 | fldMpqWTb5upBlLIx | multipleLookupValues |  |
| large_class_tag | fldC8CHCUsKLX6pO6 | formula |  |
| now | fldIu3BnsAIhq9vzS | formula |  |
| latestStatus | fldjUV2455u42vVlW | formula |  |
| status2 | fldvXZXScgCYbmh5B | formula |  |
| latest_status | fld5hyJXH8iaiYvVP | singleLineText |  |
| ___latest_estimated_start_time | fld6fP2dsukwUcR8B | singleLineText |  |
| last_modified_time | fldhWaXgKhO4KCxJu | lastModifiedTime |  |
| start_display2 | fldFHmiHkjfRzWPLe | formula |  |
| last_ingested_check | fldBVOUC1bBwoEHDM | formula |  |
| last_updated_at | fldQRtMidsRwSuZkv | singleLineText |  |
| is_gotcha | fldQ3AEGmGa4xdCHv | checkbox |  |
| startKey | fldi98Rtyb1WKftjN | formula |  |
| startKeyDT | fldXyOllR4EjNR3qD | formula |  |
| startKeyDTT | fldkHb66b1x0f9NsM | formula |  |
| nowKeyDTT | fld1QSmj0baiDr6Fv | formula |  |
| secondsTill | fldIH2XW4rrhwwuHG | formula |  |
| 30Till | fldYQrsfzNLRjewcV | formula |  |
| classTrigger | fldhCQBqEdFLu33HN | singleLineText |  |
| classActual | fldotmxxrhjHuQT3a | number |  |
| latestStart | fldzGhUx9bHnY3w68 | formula |  |
| group_alert | fldZTSaUbHlHIq0nt | rollup |  |
| nowTime | fldGArT3of5i58TXr | formula |  |
| latestStart24 | fldz6rFagzSdXa3SJ | formula |  |
| estimatedStart24 | fldHl6xRZYISRmeS9 | formula |  |
| minsTill | fldN3nWLcQQHlh254 | formula |  |
| entry_id (from watch_trips) | fld53pgNMHuWt5hsu | multipleLookupValues |  |
| entryID | fldEtS3WnC5T09mOp | formula |  |
| clear | fldnw04g67eaOQJI4 | checkbox |  |
| epoch | fld6iedy3agbXS4n8 | formula |  |
| next_due_epoch | flduqJ0jQM0W4AxrR | number |  |
| latestStatus_fallback | fldbguNzvR4TAxhoD | multipleLookupValues |  |
| showsMode | fldXHi32rE4NqwEs6 | formula |  |
| defaultTimeTo (from ww_rings) | fld9VJvAsEWyM38eS | multipleLookupValues |  |
| ringWalk | fldYocQ3eQaUuruhx | formula |  |
| latestStart (from watch_trips) | fldF5PZJ9wN4f6TD0 | multipleLookupValues |  |
| prev_estimated_start_time | fldMprg80HpbujDON | singleLineText |  |
| ___expected_sort | fldZueP1C2ruNPRUt | formula |  |
| oog_endpoint copy | fldpNheF0boSFuCrZ | formula |  |
| order_of_go_endpoint | fldOD89I2iAMsJExi | formula |  |
| group_alert2 | fldXadwiF32P2aQrY | rollup |  |
| latest_estimated_start_time | fld2KVutbYqlEwqfe | singleLineText |  |
| active_classes | fldDgxp0Fnv8TAFG9 | multipleRecordLinks | active_classes (tblkaonJjHXU0MVoZ) |
| entry_oog | fldHkbJ1aGXNVJlng | multipleRecordLinks | entry_oog (tblXJ9ps3al0u9SOn) |
| is_target (from watch_trips) | fldWs8WCAsOWT69fb | multipleLookupValues |  |
| is_classic | fldPFaOthVrqUwZtO | formula |  |
| mode | fldMMc1rzwinFfMS4 | formula |  |
| app_show_id | fldaSppo7DZbjyTfI | formula |  |
| epoch (from heartbeat) | fldlQrq6YQYLP3TBa | multipleLookupValues |  |
| time (from heartbeat) | fldt7kKdvfCdD4iZg | multipleLookupValues |  |
| app_dow_raw (from heartbeat) | fldzUpitz0oFSu1vF | multipleLookupValues |  |
| dow_raw | fld4WFYdseUr38TmZ | multilineText |  |
| heldover_from_sunday | fldttmMMu1SvK6dWR | checkbox |  |
| tripsCount | fld1rBqdRGE0ZOgHj | count |  |
| status (from groups_live) | fldgRETwtRhHlEmZA | multipleLookupValues |  |
| estimated_start_time (from groups_live) | fldUKQ1y6dzA5BCJ7 | multipleLookupValues |  |
| since_lastModified | fldLLeTmNPKPOWxIF | formula |  |
| isA | fldWETB2cieNiOFAM | checkbox |  |
| publish_diffs | fldqNledipaVYkqoI | multipleRecordLinks | publish_diffs (tblUqWq0HCtcDQJi3) |
| tripTarget | fldAqSqwLOk0DFPTP | formula |  |
| nextTargetClassId | fldoNCk4w978uBDsP | number |  |
| focusTargetClassId | fldp4gyzxj9QCU1Dp | number |  |
| tripTargetClassId | fldw8V7NOmukfzWOV | formula |  |
| mins_per_trip (from watch_rings) | fldUBXVS2DZ8xT2OD | multipleLookupValues |  |
| immediatePriorDelayMinutes | fldUCNgfTlmZGYg9y | number |  |
| perTrip | fldfgS338A8bwnXwX | formula |  |
| app_sql_date (from heartbeat) | fldHZ7ZpjD885gSkg | multipleLookupValues |  |
| app_show_id (from heartbeat) | fldoctIYTsqfLrdVX | multipleLookupValues |  |
| 40362_group_alert | fldEgjJaoq9PdekbU | rollup |  |
| is_classic2 | fldTf84Qw1YbkTSlH | formula |  |
| class_NOT_both | fldl0M33OTYaxyaF4 | formula |  |
| thread_logs | fldMK3FMBVlCKIo2Q | multipleRecordLinks | thread_logs (tblGAFRLhh0ulElVG) |
| app_dows | fldcjrP3U4uoKTwBH | multipleRecordLinks | show_days_list (tblFmLg7sgZnPLzza) |
| is_target (from watch_trips) 2 | flduAbP1DskZu09o9 | multipleLookupValues |  |
| class_state | fldoNzZ4tBOm6kmlR | formula |  |
| start_display_short | fldiSHS3q1CcGHwP4 | formula |  |
| table_name | fldd9P5tejexkCAri | formula |  |
| table_view | fldXR0ylBs7tTGCWo | formula |  |
| scope_key | fldTIGQU7Eiz4jmCu | formula |  |
| scope_run_id | fld5fbm6YOKNYHq3i | singleLineText |  |
| scope_status | fldlab7glgK7JkqRP | singleSelect |  |
| last_seen_at | fldyPeNZmHOeG76gu | date |  |
| record_ID Rollup (from watch_trips) | fldP7neg9QMXLC5EZ | rollup |  |
| schedule_logs2 | fldru9d75yCiv8lRY | multipleRecordLinks | schedule_logs (tbly9UI0zl1ULYzkw) |
| schedule_logs | fldb14RQQD3Me9Ytd | multipleRecordLinks | schedule_logs (tbly9UI0zl1ULYzkw) |
| schedule_logs_rid | fldD2ZqQhnDQ0YZtt | multipleLookupValues |  |
| heartbeat_rid | fldr5uKmuvHwotxPX | multipleLookupValues |  |
| record_id (from watch_trips) | fldtxmsIDXUvjh8Jq | multipleLookupValues |  |
| watch_trips_rid_rollups | fldFUiRlLEjvLEisK | rollup |  |
| schedule_show_display_date_dayv2 | flduTsBbpK0QHu3AS | singleLineText |  |
| shows_rid | fld7FXZQG2DbH7RyI | multipleLookupValues |  |
| completed_trips | fldTuQ52DUrkPOUrZ | number |  |
| group_tags copy | fldvixBHkrb5ijMfF | multipleRecordLinks | trigger_tags (tblA0x5dAZ8rwTZGw) |
| is_usef | fldDp9NqagVIybedG | formula |  |
| class_completed10 | fldKGTdjwKzqHi2UJ | formula |  |
| prior_class_group_status | fldIQwpKAjQxGxYGg | singleLineText |  |
| prior_class_group_trips_text | fld7ZfW5n5xePXskq | singleLineText |  |
| prior_class_group_updated_at | fldyvulapVrb5rANV | number |  |
| alert-title | fld7O79SuNQUwgsTd | formula |  |
| ring_name | fldih5c6peOoDnIVJ | formula |  |
| is_firstUp | fldJ1PfWnMYtwQ2B2 | formula |  |
| test-key | fld3Yt94lwzGICfst | formula |  |
| schedule_short | flddye2Turo2pFXQ4 | singleLineText |  |
| archive | fldD3IMHIXrrReVg7 | checkbox |  |
| ring_collection | fld9UKfwT0m2PJ47P | singleLineText |  |
| show_scope_key | fldr7nEhPbnwW4JnW | singleLineText |  |
| is_tomorrow | fldZpTzAMud4zsFqq | formula |  |
| is_2nd_trip (from watch_trips) | fldblw6Xx6QvSfjfN | multipleLookupValues |  |
| class_est_time | fldWMjLji0yQtN0ou | singleLineText |  |
| mj | fldxkbwUV2oB1jFp2 | multipleRecordLinks | mj (tbl34Pl22bKjoVBJY) |
| sgl_token_raw | fldqEpkQERVPoYCYk | multilineText |  |
| sgl_token_prefix | fldaAA2iO1kvFc3Yq | singleLineText |  |
| sgl_token_length | fld99Bjat0o9YlNYQ | number |  |
| sgl_token_hash | fldsU9nlLFqEtmTQD | singleLineText |  |
| class_id (from class_numbers) | fldNB3OoxUQtG9dlG | multipleLookupValues |  |
| groups_live copy | fldg5W5VuOPwCPR6i | multipleRecordLinks | live_groups (tblJeVfA8Q1Te6N5k) |
| gets | fldwuJkwfDVL9HLWs | multipleRecordLinks | gets (tblFak1Mr2o3Iq49J) |
| Table 271 | fldl2J4qkfcnEvIN6 | multipleRecordLinks | Table 271 (tblNd8XY4fHvhixVr) |

### RingStatus.watch_trips

Base: apptdhhNzduxm5gjn; table: tblCtNpzJtrI19VKq; primary field: fldEh0ytctE8eQimo.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| trips_key | fldEh0ytctE8eQimo | singleLineText |  |
| new_trips_key | fldgA2M2PehQ4puK1 | formula |  |
| show_scope_key | fldct0UTEDjBCfiIE | singleLineText |  |
| is_current_scope | fld1HK3ieoLoucxpt | checkbox |  |
| new_schedule_key | fldlxejcGEWw2XFei | formula |  |
| run_id | fldnmvjbq19dp1di6 | singleLineText |  |
| customer_id | fldoMS1EDDtwfB8bC | number |  |
| customers | fldanqBxVC9XRRVBq | multipleRecordLinks | customers (tbl91NqVHM3gk4gTJ) |
| ajax_token (from customers) | fldO3sWxEikRTFtlZ | multipleLookupValues |  |
| dropped_at | fldHFAesdsZDzj37A | date |  |
| focus_day | fldBN3Bw2nHFNXOZz | date |  |
| getLiveClassData | fldeKIIcliIw4Zplj | formula |  |
| class_id (from watch_schedule) | fld7FnZxLoICyvYH6 | multipleLookupValues |  |
| new_key | fldDYxOukUHn6sdwe | formula |  |
| watch_ring_key | fldUhgUEgCEQpFy4l | formula |  |
| watch_rings | fldJCDDgBAqcib7BO | multipleRecordLinks | watch_rings (tblJUchoYECk9ptyJ) |
| entryxclasses_uuid | fld0GOCW20teKISUz | singleLineText |  |
| record_id | fld84pO0xzVoADd5z | formula |  |
| ring_number | fldS3wBbYUgxtzn8S | number |  |
| ww_rings | fldUcAWE5QIVbvrXf | multipleRecordLinks | ww_rings (tbl8CoOJEvQJbN7kZ) |
| ring_name (from ww_rings) | fldkRtmdftyPcO8Gh | multipleLookupValues |  |
| class_group_id | fldNDumIHyfBfvDjK | number |  |
| class_group_sequence | fldzkBjnTKfiuR4Tu | number |  |
| group_name | fldmneino3A9hUu5j | singleLineText |  |
| manual_time_override | fldezS95sntXoF7nv | checkbox |  |
| estimated_start_time | fldEqCIhNmv9b39zR | multilineText |  |
| schedule_starttime | fldbfvYf7aKu3BuBE | singleLineText |  |
| estimated_start_time (from watch_schedule) 3 | fldhnXSpubAPwK0qD | multipleLookupValues |  |
| archive | fldToPl68394xREmH | checkbox |  |
| rs_order_of_go | fldzQ6XrqHMGdjvE4 | number |  |
| order_of_go | fldTEkedpq9sU0LTW | number |  |
| not_target_override | fldMUAgCv1HB8Y4CH | checkbox |  |
| not_target | fldd2USsuT1MFLrzg | formula |  |
| entry_sequence | fldn9Fnqoujfe5u0P | number |  |
| class_number | fldAmh1XTNHI6i1AW | number |  |
| help | fldM1qF1wLbR80Ym9 | checkbox |  |
| help2 | fldJGOmmfWkLP0u9m | checkbox |  |
| is_2nd_trip | fldXodLGrDGJVz70G | formula |  |
| is_target | fldPHmzfInTtrMySj | checkbox |  |
| class_alerts_8778 | fldkBhFzYtJI7NStp | multipleRecordLinks | class_alerts (tbl1CWj8m6Egd4XTe) |
| time_sort | fldXa1EMBKJSJI5zf | formula |  |
| class_name | fldSq063ZaeifXKfr | singleLineText |  |
| horse | fldYCoDSssk2hYcOr | singleLineText |  |
| ww_horses | fldihQM3g2w97M4aR | multipleRecordLinks | ww_horses (tblliyUZ1ZS88Kfvl) |
| barn_name (from ww_horses) | fldNIkDOywQOhLcXN | multipleLookupValues |  |
| schedule_sequencetype | fld0XvKX6liYvqd20 | singleLineText |  |
| watch_schedule | fldyJj6mCXBWSM7At | multipleRecordLinks | watch_schedule (tbl3LHjCmuMu2uuoZ) |
| dropped_at (from watch_schedule) | flduuLO5bfLVoTuWf | multipleLookupValues |  |
| conflict (from watch_schedule) | fld8n3dDljiCafRXe | multipleLookupValues |  |
| sched_display | fldixS19jF3repoWW | formula |  |
| rider_name | fld44HmVAyaRFxT0I | singleLineText |  |
| rider_names | fldQCJzdUSi93vHiw | multipleRecordLinks | rider_names (tblTRxi244zCDptqt) |
| rider_id (from rider_names) | fldFlGpMSzeGfmuaI | multipleLookupValues |  |
| rider_id | fldmf1xJdLkG7vMI1 | number |  |
| ww_riders | fldtM7WtAWWhT7Pcj | multipleRecordLinks | ww_riders (tblI6yrqv0Y4QTvyi) |
| class_type | fldEfHC4zNBYbmsa9 | singleLineText |  |
| pid | fldakySoyaKXiZiZk | number |  |
| ww_tenants | fldz6Rj7pZIkdjYUf | multipleRecordLinks | ww_tenants (tblKqQdTmkoMj406a) |
| active_tenants | fld0eaY9AZfDZWIo1 | multipleRecordLinks | active_tenants (tblby9SVxkV55HKAW) |
| ww_trainers | fldOhhIrqHC2kIAuF | multipleRecordLinks | ww_trainers (tblRJOVhOLuEJOkgF) |
| status | fldVuEtPTfNVwxvrs | singleLineText |  |
| class_status | fldHGD4MMLJmuCoR3 | multipleRecordLinks | class_status (tbllRZL2YIctMS2nK) |
| is_usf | fldcqupNu5Ldpa62T | formula |  |
| time_one | fldCjIIzH7PV6PGld | number |  |
| placing | fld6ZX3pQudemc2K6 | number |  |
| score1 | fldeWk0yxNCk1snv7 | number |  |
| score | fldTQltdpwjHz69E3 | number |  |
| total_trips | fldRo2EWzBZRaHRZ6 | number |  |
| completed_trips | fld3WuNnMMQht1H89 | number |  |
| app_sql_datev2 | fldIendQ0sN6xvJvg | singleLineText |  |
| show_datev2 | fldiqWP4ubcgaCkvY | formula |  |
| check_v2 | fldboRxMV8xRytUI0 | formula |  |
| getLiveClassStatus | fldBtDynOh9t0LHnt | formula |  |
| class_endpoint | fldIrma8VAfsaDPYX | formula |  |
| class_endpoint_plus_group | fldTbvBBcSvB79QpI | formula |  |
| start_display | fldV9G4RBEDGz5DMC | formula |  |
| entry_number | fldnaJ2xQrr3JLbcy | number |  |
| h_eid | fldr4pJcJEga0cWNJ | number |  |
| now_ts_local | fldvnc83E2kVTyBT5 | formula |  |
| last_order_of_go | fldJMrb8dnKWCOiWM | number |  |
| horseName | fldhOpxFUmdtQkhcD | formula |  |
| classsignup_url | fldttDXjhCNxEg0Yj | formula |  |
| dow_raw | fld4CF7nJH5mdfDgE | singleLineText |  |
| app_sid | fldQCiJHWBGNgpdah | number |  |
| app_show_mismatch | fldTEsw4mb3xBAyyO | checkbox |  |
| app_dt | fldfpNvYe8c2mq2vA | singleLineText |  |
| hb_second_pass_at | fldr2zf1lZNHzHYcr | dateTime |  |
| hb_second_pass_reason | fldT2xdm2J0BOgqua | singleLineText |  |
| hb_second_pass_done | fldt2Byhyv7dITrOW | checkbox |  |
| entry_oog_uid | fldYx8z3GJBSnGb1O | formula |  |
| order_of_go (from entry_oog_log) 2 | fldtaX1na6Op9R301 | multipleLookupValues |  |
| app_dows | fldjtrdranAFzcNKI | multipleRecordLinks | show_days_list (tblFmLg7sgZnPLzza) |
| class_types | fldjDE8Jpjj3nRkGH | multipleRecordLinks | class_types (tblb0ajwYCxUqB1ow) |
| actual_order | fldHPtxeCXju10AVI | number |  |
| order_of_go (from entry_oog_log) | fldHIn3QwhEGshW2Z | multipleLookupValues |  |
| app_dow_raw (from heartbeat) | fld0OBGDp5114Jdgc | multipleLookupValues |  |
| shifted_to_next_day | fldQlg4NzkmmMlCqD | checkbox |  |
| rs_running_order_of_go | fldTxSt62ioUThQmR | number |  |
| rs_start_time | flddpYo7XyAmbUbGz | singleLineText |  |
| rs_status | fldNxHDe76W5znF7b | singleLineText |  |
| rs_go_time | fldFvMkX3fEl9n59b | singleLineText |  |
| rs_order_of_go (from last_log) | fldesPrd1rDRJwKWQ | multipleLookupValues |  |
| rs_completed_trips | fldRl6jnyU7wJ68ex | number |  |
| rs_trip_default | fldEqpmJJLP8snBwY | duration |  |
| rs_trip_time | fldsSX1fH5ExSJTDL | duration |  |
| rs_trip_time2 | fld6KTYTv5Xd2Eu7X | duration |  |
| rs_gone_in | fldSQqvlSdlnJfw1n | number |  |
| rs_mins_till_start | fldxoa4Ne4t1gf0pt | number |  |
| rs_mins_since_start | fld7Z08vNdU7YDxtt | number |  |
| rs_length | fldOmC08uQBzxe6i1 | singleLineText |  |
| rs_end_time | fldCA59RH2EUMIpHo | singleLineText |  |
| rs_go_mins_from_start | fldxb5GvamCInU9ov | number |  |
| rs_go_time_from_start | fldDFWACjiBABlViA | singleLineText |  |
| rs_min_till_go | fld4wZhBlSIaRNXRe | number |  |
| inactive | fld5SebMnszMmuiNH | checkbox |  |
| last_score | fldjUm7wb9E3ospsV | number |  |
| people_endpointv2 | fldxMG1l3ah21Qy4k | formula |  |
| is_current | fldq3UCEQVD9tqIP2 | checkbox |  |
| show_id | fld9ytoBlbRPmHfGL | number |  |
| shows | fldJNUpCc1tG5f1Gj | multipleRecordLinks | shows (tblqetAgwMf3TkLyN) |
| heartbeat | fldvqBSuRj4RPrXaH | multipleRecordLinks | heartbeat (tblJsYPLOBiaIkcId) |
| app_show_id | fldXng5wKC3NlsVUA | number |  |
| mode | fldn3DGgU9gJ3BYz3 | singleLineText |  |
| not_current | fld2yIflL05B7Nn72 | checkbox |  |
| sid | fld20h9O88OiMh9sV | formula |  |
| date | fldy2tE6OhzeyM3Jv | singleLineText |  |
| dt | fldjUT3CpoyrsGLfr | formula |  |
| schedule_show_datev2 | fldfWWRVHf0ytIEx3 | date |  |
| app_sql_date_mismatch | fldQkxzAX1XMBoJVS | checkbox |  |
| ringWalk | fldCr9FTD9A9ECUKO | formula |  |
| defaultTimeTo (from ww_rings) | fldgajvJX7nhfqIkb | multipleLookupValues |  |
| grouped_class | fldWYvD1UoZpSlqCH | checkbox |  |
| groups_live | fld1rcsX0M6Kkb74V | multipleRecordLinks | groups_live (tbl1CKMI3t2fA6B2c) |
| watch_groups | fldfedi3RLpD4jP1J | multipleRecordLinks | watch_groups (tbleNmV4DYAQ8Vru2) |
| class_groupxclasses_id | fldO6N67AdMbwF3zu | number |  |
| class_id | fldC9mbVlcDpMKrVq | number |  |
| timeDiffMin | fldtvtZVYJLMaVMYe | formula |  |
| is_classic | flduTt3NxO58UGIID | formula |  |
| lastPosition | fldqkVeWtBRklv9Ci | number |  |
| lastGoneIn | fld0xvwVOKDxbtMsd | number |  |
| lastPlace | fld7Wp2Ngq44lUBBQ | number |  |
| ___sqlDate | fldeFVMqqtU4aT9k6 | formula |  |
| scheduleApproved | fldBQeLWDWnj4d6zM | formula |  |
| watch_classes | fldLktuB3Pfij6Fez | multipleRecordLinks | watch_classes (tblnLfzP3HYiFcsLv) |
| schedule_show_datev2 (from watch_schedule) | fldZ05pbvHAipSbBy | multipleLookupValues |  |
| entry_id | fld21CYLxjQRN15Lu | number |  |
| watch_entries | fld7TeF7GdN6KS5UV | multipleRecordLinks | watch_entries (tbl6KiyJtLRJmw4p1) |
| ignore (from ww_horses) | fldlQDODAp5cAuvpd | multipleLookupValues |  |
| run_time | fldMD7nPJYyzocbYZ | dateTime |  |
| trip_id | fldBvnlc4uTjygK1C | number |  |
| ___secondsTill2 | fldkqPIcFlKWGGs7X | formula |  |
| time_per_trip | fld5mn2PfA3yrD9eo | singleLineText |  |
| estimated_go_time | fldVZ7KRr3nw0vSFO | singleLineText |  |
| score2 | fld4KxUY4R1wkWstm | number |  |
| unscratched_count | fld1el9OoiAleslRp | number |  |
| remaining_trips | fldmMFDXE6uBt1Kyt | number |  |
| LiveClassData | fldia3uBMGTwAKB33 | formula |  |
| gKey | fldBMZejlLeB0c6Kn | formula |  |
| YYYYMMDD | fldKj1TFzi4ZKOMDn | formula |  |
| last_run_at | fldmmBKKtMMl3mrah | dateTime |  |
| barn_name_lookup | fldwo1PpkN3K4mhug | multipleLookupValues |  |
| created_time | fldeWG3BXKyp1pSXT | createdTime |  |
| latest_estimated_start_time | fldE6MNlOk0rb7Cll | singleLineText |  |
| latest_status | fldM79pMdmfaPExMR | singleLineText |  |
| latest_ingested_at | fldpvNRX7zItRaXvL | singleLineText |  |
| ___completed_done | fldUUXjgp5DUhW7t3 | checkbox |  |
| startTimeChange | fldJV5kly0NkU54Dt | formula |  |
| start_display2 | fldUfJoDfaNxHWFrY | formula |  |
| ring_nickname_lookup | fldjZC3hifAKLje9M | multipleLookupValues |  |
| show_days_report_title | fldWV4ov2pTvYvt1f | formula |  |
| barnName | fldjbTPVJQCik78vz | formula |  |
| team_name_lookup | fld6VUmHvQmraPcl6 | multipleLookupValues |  |
| teamName | fld3Cph3u8BeoLOzI | formula |  |
| is_missing | flduWYYgvrcU941Vf | checkbox |  |
| ringName | fldpWsf1Dmuwma87F | formula |  |
| estimated_start_time_display | fld4Pt2AJKgBzIFD5 | formula |  |
| latest_estimated_start_time_display | fldhcjDsdmkxxPjZN | formula |  |
| latest_estimated_go_time | fldtyOhhfEpjCpamc | singleLineText |  |
| latest_estimated_go_time_display | fldyRdeT6GmMmxx3a | formula |  |
| ___path_gpt | fld9PFBB9XVijpzCo | formula |  |
| oog_rule | fldqBJzNR6S96Yidg | formula |  |
| latest_oog_display | fldjLyKd038Ibcg13 | formula |  |
| est_start_change | fld68El8l4NFAkMn1 | checkbox |  |
| est_start_change_done | fldByfGLkd1tn4Seg | checkbox |  |
| timeDiff | fld91iaal12v3DuFp | formula |  |
| backNumber | fld4B78j8MKfzGYTu | formula |  |
| entry_number (from watch_entries) | fldnlF6SvMvJiEALJ | multipleLookupValues |  |
| triggerStartAlert | fldi18dG1qnZK8SP8 | formula |  |
| timeDiff2 | fldWbC0bsAr6CviIE | formula |  |
| startKey | fld7h2KEVeKSaakJa | formula |  |
| startKeyDT | fldVNRrlrWzD6TFgr | formula |  |
| nowKeyDT | fldjN7Bu8z2MKMlQL | formula |  |
| startKeyDTT | fldT3DlEPBxESMBh4 | formula |  |
| secondsTill | fldWWJ9UAq3RwITjS | formula |  |
| ___latest_status_display | fldVfyFzkgvqo7yti | formula |  |
| go_time_rule | fldqjWT2AJnmjY7YZ | formula |  |
| latest_calendaStart | fldj693tBIzBOKmpv | formula |  |
| latest_calendarEnd | fld8uCPUe6EyMja30 | formula |  |
| latestStart | fldkeNgfhO9Bd1pe6 | formula |  |
| latestGO | fld5tj5tzYIxn48Q7 | formula |  |
| latestStatus | fldOoxRJVFzGJvJFc | formula |  |
| latest_calendarBucket | flddvkpSdNVvoUDjn | formula |  |
| last_status | fldW6eemuaV0A0pMZ | singleLineText |  |
| last_estimated_time | fldVLQVjmrThAOJhl | singleLineText |  |
| last_actual_time | fldCZUapjsbzcYd6N | singleLineText |  |
| last_estimated_go_time | fldp8ynlZqKbl7mYW | singleLineText |  |
| last_placing | fldmFRAXgxn0MfY04 | number |  |
| lastOOG | fldDNOBZA0nCo6E4c | formula |  |
| lastPlacing | fldiVgTWKqT7mzPYG | formula |  |
| lastScore | fld72Pk6XD6ujOFBl | formula |  |
| riderName | fldFSKEKcRV5UUQZU | formula |  |
| entryNumber | fld2DMHhACrHzH0zK | formula |  |
| status (from groups_live) | fldKwpAE2SoaJ1Zrd | multipleLookupValues |  |
| estimated_end_time | fldH1nVBP6P2ehRib | singleLineText |  |
| latestPlacing | fldE4pGisEzoSOzOU | formula |  |
| ___is_gotcha | fld0rg51SxEtCOPuW | checkbox |  |
| ___gotcha_reason | fldYfarW8oe1VkYE4 | singleLineText |  |
| ___gotcha_date | fldwxteLm7O2pMb2S | formula |  |
| watch_results | fldSR9FnsMmFyEdDv | multipleRecordLinks | watch_results (tbl2xWLY9WpuxJoEP) |
| lastTime | fld7eLHK5jHZwyKu2 | formula |  |
| time_two | fldbPjDPYP0mtWwOc | number |  |
| time_three | fldvm0DmQogWquVy6 | number |  |
| score3 | fldj2HmeOvppm3xgg | number |  |
| last_modified_time | fld5lDDMfD0awTY3B | lastModifiedTime |  |
| ring_offset_min | fldm7xS7TZsPYgmVZ | number |  |
| adj_secondsTill | fld4Ywg2NcjdEP2U0 | number |  |
| adj_go_dt | fldOC4UnfK9yhD4dw | dateTime |  |
| should_alert | fldwqMz5oC99xry88 | checkbox |  |
| alert_reason | fldlT1AlPMwt5bhNG | multilineText |  |
| last_alerted_at | fldij83OdKcfpmWOc | dateTime |  |
| runningOOG | fld3IWOSaA59YsZ2u | formula |  |
| runningOOG_alert | fldBgEIJUC5PjYjlm | formula |  |
| watch_buckets | fldKTmnLgjaa7SB82 | singleLineText |  |
| results_verified | fldmENdwXSn1Csdav | checkbox |  |
| epoch | fldeU6UZ0LzMdlbtT | formula |  |
| conflict | fldghMAcejNGSHUkY | checkbox |  |
| watch_conflicts | fldwbYYoVz3mfXmIj | multipleRecordLinks | watch_conflicts (tblHplYKPbFbVq6yx) |
| estimated_start_time (from watch_schedule) | fldssTEPjT6e2rr4f | multipleLookupValues |  |
| groom_walk_lead_min | fldtDZ5knFzOZT3UM | formula |  |
| groomName | fldfaTCtTu1eYDhMv | formula |  |
| rider_running_oog | fldOk9hdNwaHDZtb1 | number |  |
| scratch_trip | fld6COMsCh0Z5RglR | number |  |
| time_alerts | flderKTKUYdjnfrkk | singleLineText |  |
| classsignup_oog | fldLiBH7fa42Rp3YT | number |  |
| classsignup_at | fldkV7ngv0hp5zq51 | dateTime |  |
| entry_oog | fldLAfKUJls6C2Jod | singleLineText |  |
| eid_order_of_go | fldUWgPtvFsIxw3Nu | number |  |
| check_dns | fldBzCMjFIhQJMny2 | formula |  |
| show_date | fld7q2ulWFuO6aKOy | date |  |
| lastGone | fldnE80nCz98BhPbQ | checkbox |  |
| leadMin | fldA5jtpYKxNPLAZn | number |  |
| path_tenant | fld4Oolljq3FfSHvc | formula |  |
| targets_last_at | flddKicbcVYFwQCGh | date |  |
| group_alert | fldkK6fJPm9zwiFM8 | formula |  |
| app_show_id_hb | fldAKE9Nx7n0PcCrl | formula |  |
| dt (from watch_schedule) | fldu6IBaRjAj5OUww | multipleLookupValues |  |
| app_sql_date_hb | fldd00Fe1YHt8eMyW | formula |  |
| app_sql_date | fld57ReeK2HTOYryH | singleLineText |  |
| app_dow_raw | fldDOA5uFObSxCMzK | formula |  |
| shifted_to_next_day_hb | fldNHQAWlAautYWWZ | formula |  |
| entry_oog_log | fldeBaHAEEUXRrAD4 | multipleRecordLinks | entry_oog (tblXJ9ps3al0u9SOn) |
| time_zone_offset | fld5hf6f2m0gPoODY | formula |  |
| hb_modes | fld6LRWqx7PS3KeSB | multipleRecordLinks | hb_modes (tbl29nEaYdbsiLJZl) |
| success | fldXNGGK3qnRk4n2i | singleLineText |  |
| ___estimated_end_time (from watch_schedule) | fldvm7oBF1HEwCViB | multipleLookupValues |  |
| ___estimated_endTime | fldSpTTaNvtqYEumI | formula |  |
| estimated_startTime | fldvdCpuLsY6gUq5M | formula |  |
| startTime | fldgfzkR0GL0rrGDH | formula |  |
| 60_before_EGO | fldL5Bui4tiDQeHNN | formula |  |
| WALKP_before_EGO | fldy9ZuPBOlxaCwbK | formula |  |
| 60_before_EST | fldMOlXEypLhz1fgK | formula |  |
| 40_before_EST | fldXJv8eajXgKYaBh | formula |  |
| workingTIME | fld6630LUmBSbh67h | formula |  |
| till_startTime | fldNNtmll1zK4Pz3H | formula |  |
| 40_before_EGO | fldSd38TeXXzjJJXj | formula |  |
| lastOOG_display | fldi6wISWNt0haUmh | formula |  |
| since_lastModified | fldGoTcexbiqQiEAk | formula |  |
| publish_diffs | fldDX40m5NwBiVnzy | multipleRecordLinks | publish_diffs (tblUqWq0HCtcDQJi3) |
| firstUp | fldXsjjfK68eZqEq3 | formula |  |
| sched_display+runn | fldfh0XYHUGaHqM77 | formula |  |
| mins_per_trip (from watch_rings) | fldZaW199LiwuV87e | multipleLookupValues |  |
| status_reason (from watch_rings) | fldOmWFFtTRjLPNRE | multipleLookupValues |  |
| status (from watch_rings) | fld2z1aYBZvjzTmck | multipleLookupValues |  |
| run_time (from watch_rings) | flddk88yMK7osSYut | multipleLookupValues |  |
| 8778_sched_display | fldmS8XU7vhkEDyNB | formula |  |
| 40362_sched_display | fldxcclqbg8giEvpQ | formula |  |
| 8778_group_alert | fldfm6hb0cfmE6wUH | formula |  |
| 40362_group_alert | fldXApcNazIWMl65i | formula |  |
| actual_time | fldDHYXrjRh1GWZRH | singleLineText |  |
| estimated_time | fldwp1lVoopILPZAC | singleLineText |  |
| total_entry_trips | fldgvrwmsvVEsU4PJ | number |  |
| time_fault_one | fldLbWNyywzabZuC2 | number |  |
| faults_one | fldA8mIuebqRKyS61 | number |  |
| time_faults_two | fldQDURV7DmmAA1PS | number |  |
| faults_two | fldxBtlYh2roQCbVS | number |  |
| gone_in | fld3NTdhLSxcLuTLC | number |  |
| next_due_epoch | fldB0RYURtp228jHw | number |  |
| mode_hb | fldzJ12l0cc81dtWu | formula |  |
| app_sql_date (from heartbeat) | fldYFdrFTXumMPFao | multipleLookupValues |  |
| app_show_id (from heartbeat) | fldmJ2TSXUEws340b | multipleLookupValues |  |
| epoch (from heartbeat) | fldCkt6Ds4OLcl6Tw | multipleLookupValues |  |
| time (from heartbeat) | fldRAlNmbgjNbcjgn | multipleLookupValues |  |
| shifted_to_next_day (from heartbeat) | fldUiR8CkV2SRy5WD | multipleLookupValues |  |
| mode (from heartbeat) | fld4ssaKzFlP3eMwY | multipleLookupValues |  |
| ww_grooms_lookup | fld5JvT8x16USc62T | multipleLookupValues |  |
| status (from groups_live) 2 | fldgdpJm4muiN1xUM | multipleLookupValues |  |
| estimated_start_time (from groups_live) | fldHTk2VmaJEYI7eL | multipleLookupValues |  |
| class_is_NOT_both | fld3SArQIQMMa3zII | formula |  |
| is_handy | fld18wBwtGr2e40r2 | formula |  |
| group_is_NOT_both | fldIKeDWHMGFUFNeV | formula |  |
| perTrip | fldoiAFjbOqd49tBu | formula |  |
| estimatedGO | flduX60MdnV7OXvKk | formula |  |
| minsAfter1 | fldqsApuPsSBX0ayD | formula |  |
| estimatedGO00 | fldsU3OqbOtB3pMLD | formula |  |
| completedis10 | flddS0Mp6ZycKsjnR | formula |  |
| completedis10Away | fldyRTqKiedVWxOVN | formula |  |
| go15s | fldItN7U3NsYA2jly | formula |  |
| class_alerts_40362 | fldG0R3MVgKR9yOh5 | multipleRecordLinks | class_alerts (tbl1CWj8m6Egd4XTe) |
| 7589_sched_display | fld0gRjzEQin2gBxe | formula |  |
| 34933_sched_display | fld1Dfun8W16LbyWi | formula |  |
| class_alerts_34933 | fldayhnXWKJK0M59e | multipleRecordLinks | class_alerts (tbl1CWj8m6Egd4XTe) |
| alerts_threads | fldxsZPwn0XTE8R6s | multipleRecordLinks | thread_logs (tblGAFRLhh0ulElVG) |
| ww_alerts | fld4wIym9VmhMR1xH | multipleRecordLinks | active_alerts (tblgkCC4vcXIjSEn7) |
| app_time | fldYGs8woVUWDzlME | singleLineText |  |
| trip_logs | fld3kXOAp8eH1Idqt | multipleRecordLinks | trip_logs (tblwneYKbZDCWBGST) |
| remaining_trips (from trip_logs) | fld0Y0of43oJRQF1f | multipleLookupValues |  |
| alert_templates | fldx9mBdo14woQPVy | multipleRecordLinks | alert_templates (tblsyHZpnYlpYBaQ6) |
| last_log | fldmwUhSgfsMUEDox | multipleRecordLinks | trip_logs (tblwneYKbZDCWBGST) |
| estimated_start_time (from watch_schedule) 2 | fld73rCTaPYmMWO0G | multipleLookupValues |  |
| rs_start_time (from last_log) | fldfyvaJf6M0NtifL | multipleLookupValues |  |
| rs_go_time (from last_log) | fldnFFBuvklptbx6j | multipleLookupValues |  |
| total_trips (from last_log) | fldxHWELc3sIaonuD | multipleLookupValues |  |
| rs_completed_trips (from last_log) | fldH0ZaAsVCpzDkDy | multipleLookupValues |  |
| rs_min_to_actual_go | fldtGbK1ZmNtmqhS4 | number |  |
| is_warmup | fldljyFcYrRJ9q2O0 | formula |  |
| class_sequences | fldHi8dAT2Hxh2q6R | multipleRecordLinks | class_sequences (tbl4obvgqOkPexmVX) |
| flag_done | fldcGht9uaDUZSZsj | checkbox |  |
| last_alerted_epoch_ms | fldM712f43oOEWpbv | singleLineText |  |
| last_alerted_offset_min | fldjBAPy1CoeJcFsd | singleLineText |  |
| last_alerted_adj_secondsTill | fld7T0Ii21Kt7r2XN | singleLineText |  |
| mins_per_trip | fldciux1GWXYfddKP | formula |  |
| current_group_mins_per_trip | fldSkxmHIuJc6Eg6z | number |  |
| is_group_mins_per_trip | fldVnye6aruixqLKz | checkbox |  |
| active_entries | fldL9goVdo4HODRsw | multipleRecordLinks | active_entries (tbl05SvRJmoG9xaik) |
| active_classes | fldqXp9CXkdpSXfn9 | multipleRecordLinks | active_classes (tblkaonJjHXU0MVoZ) |
| barn_name | fldD8znIqfWJfvKKe | formula |  |
| ring_nickname | fldaDRP69s2ftqPcH | formula |  |
| team_name | fldoBiEsrUhX77b8u | formula |  |
| groom_name | fldjHvtJ4EvtyRHtk | formula |  |
| is_mulligan | fldg2KPjjm1gGohJ5 | formula |  |
| is_add_back | fld5SbiJHbpmwGqpK | formula |  |
| is_schooling_pony | fld1suHEXpM7gD2zF | formula |  |
| group_name_tags | flduZjd1FESJry8nh | multilineText |  |
| is_usef | fldxEbVfk4xBRkAKv | formula |  |
| group_tags_links | fldUI30U1843kloGX | multipleRecordLinks | group_tags (tbl1AEkqF5WEkepRU) |
| record_id (from watch_schedule) | fldvPpOATr12g3eEy | multipleLookupValues |  |
| schedule_rid | fldxeYrvJRwdkq45r | formula |  |
| schedule_logs | fld56DyQNuCZQ56S6 | multipleRecordLinks | schedule_logs (tbly9UI0zl1ULYzkw) |
| heartbeat_rid | fld21w1qfkDpWWGI3 | multipleLookupValues |  |
| watch_schedule_rid | fldXAqHP9lsvbewcw | multipleLookupValues |  |
| trip_logs_rid | flduasIc97fDJVCbb | multipleLookupValues |  |
| app_show_idv2 | fld3wbfEeZiDUjSXw | number |  |
| app_dow_rawv2 | fldiiKVfnis6Pv3Vs | singleLineText |  |
| shifted_to_next_dayv2 | fldhLU1QPKz76aTam | checkbox |  |
| last_seen_at | fld5jNrC9XzvNMHDT | date |  |
| table_name | fld1jSKXcL24EXLmq | formula |  |
| table_view | fld1hekRTJvkZFMRd | formula |  |
| scope_key | fldkEXiA3oYIV2gqS | formula |  |
| scope_status | fldKEWIWM5k30r3Fe | singleSelect |  |
| scope_run_id | fldzLUV264ECqAl12 | singleLineText |  |
| watch_entries_rid | fld5DGVq1RUh2FAEW | multipleLookupValues |  |
| watch_groups_rid | fldhFoE9b3l3ddT76 | multipleLookupValues |  |
| watch_classes_rid | fldzl5bCd7HNLOp8c | multipleLookupValues |  |
| shows_rid | fldjPDHKhkKpO73Ad | multipleLookupValues |  |
| scheduled_date | fldpqIecOImi6y4bx | singleLineText |  |
| trigger_tags | fldt8WLHW2gQw0Yvz | multipleRecordLinks | trigger_tags (tblA0x5dAZ8rwTZGw) |
| horse_name | fld1UR8MTjyZu670K | formula |  |
| estimated_start_time (from groups_live) 2 | fldpNYu3werKwPWWg | multipleLookupValues |  |
| 8778_FIRST_sent_at (from class_alerts_8778) | fldFnHO9a96vISRyh | multipleLookupValues |  |
| key_test | fldTBCMXb7ApmGLZH | formula |  |
| ring_collection | fld72cr1kp4t0N9Lz | singleLineText |  |
| show | flddKRQz8tdoKRkjr | multipleRecordLinks | show (tbl72LDQ2xD9xpRAC) |
| path_base (from customers) | fldlT7AEogF4Zzg2H | multipleLookupValues |  |
| mj | fld4hr9nwLUscKPWb | multipleRecordLinks | mj (tbl34Pl22bKjoVBJY) |
| sgl_token_raw | fldoybVykWI6E29aH | multilineText |  |
| sgl_token_prefix | fld0IM477A0O39wNN | singleLineText |  |
| sgl_token_length | fldU3aZJhyUajXo4Q | number |  |
| sgl_token_hash | fldNlB6yywmTR0wiq | singleLineText |  |
| live_groups | fldJ3nVP595oYbj73 | multipleRecordLinks | live_groups (tblJeVfA8Q1Te6N5k) |
| live_classes | fldkDBjcI7nuG9YZ5 | multipleRecordLinks | live_classes (tblHJ40qvD8eNWkvL) |
| is_focus_trip | fldV0pGjfJIRmG0f3 | checkbox |  |
| is_second_target_trip | fld1CU44NFJQY7DFv | checkbox |  |
| classsignup_url_viewsetorder | fld41QqTTMMZL5dvG | formula |  |
| classsignup_url_viewsetorder_scrape | fldmf4ysowNvGvjif | formula |  |
| LiveClassDataEndpoint | fldnaN5kSsaBld95Z | formula |  |
| order_set | fld1Q6CJ1tKSMDdtO | singleLineText |  |
| order_set_last_updated | fldB2P5my1aYL0tuW | lastModifiedTime |  |
| show_name (from shows) | flddkbyZjdasoMweA | multipleLookupValues |  |

### RingStatus.groups_live

Base: apptdhhNzduxm5gjn; table: tbl1CKMI3t2fA6B2c; primary field: fldAuKhvOsvX6dolp.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| class_group_id | fldAuKhvOsvX6dolp | number |  |
| show_id | fldXb3pLpVZnwmBBg | number |  |
| show | fldWQXQXCsFqFKbfz | multipleRecordLinks | show (tbl72LDQ2xD9xpRAC) |
| customer_id (from show) | fldIgBqGgNrjRrJV7 | multipleLookupValues |  |
| groups_live_key | fldxuMldmwuEoS6EW | formula |  |
| day | fldHSMfctmUPHr0Ub | singleLineText |  |
| ring_number | fld37W6rb5aA50d4L | number |  |
| ring_id | fldUPImWX8IfOwj9U | number |  |
| group_name | fldPozzzypVOkuQDX | multilineText |  |
| classes | fldINBVxxCiPuzhF9 | multilineText |  |
| estimated_start_time | fldft6eNGAAQNOWB5 | singleLineText |  |
| gone | fldVU25vyv8mPvRup | number |  |
| total | fldrvXXGKq20OdU0s | number |  |
| curr_updated_at | fldMYhc9BvSaweKth | singleLineText |  |
| ingested_at | fldVUWhuZoZbVRDYq | singleLineText |  |
| run_tag | fldtIgmFmovCWQ2qy | singleLineText |  |
| created_time | fldzdr1OWB3ktJKSn | createdTime |  |
| last_modified_time | fldhAqgm0Pp6ewdwL | lastModifiedTime |  |
| cg_record_id | fld1BH6O1Jn6Kx28E | formula |  |
| is_live | fldm3BMigiX51R7bk | singleLineText |  |
| has_JSON | fldUUuDLHazJYBSAy | singleLineText |  |
| interval | fldK4XwGxPX5DEoPA | formula |  |
| status | fld1F0HYNvPAXhdL1 | singleLineText |  |
| class_status | fldxMZr7p3h4Fogtk | multipleRecordLinks | class_status (tbllRZL2YIctMS2nK) |
| watch_schedule | fldTOYuzwNvt0gh9P | multipleRecordLinks | watch_schedule (tbl3LHjCmuMu2uuoZ) |
| ww_rings | fldwgH7hTpkSl0AEm | multipleRecordLinks | ww_rings (tbl8CoOJEvQJbN7kZ) |
| watch_groups | fldpQBhrTG5e5NsPq | multipleRecordLinks | watch_groups (tbleNmV4DYAQ8Vru2) |
| trips_count | fld5sdCgvgqOmzmp8 | count |  |
| watch_trips | fldHxu4r3zKW5UDEB | multipleRecordLinks | watch_trips (tblCtNpzJtrI19VKq) |
| groups_live_endpoint | fldkhZ9cjsIcwikUz | formula |  |
| classNumbers | fldxqy5eUnojrafbZ | singleLineText |  |
| class_numbers | fldK8nFqlYTZGaHg2 | singleLineText |  |
| show_key | fldsdWmaOFn7pW45H | number |  |
| customer_id | flduGfb1DXWiY8BiU | number |  |
| watch_rings | fldvEW3VqedQhGmWT | multipleRecordLinks | watch_rings (tblJUchoYECk9ptyJ) |
| record_id | fldtuOHTLi5J54o8R | formula |  |

### RingStatus.watch_results

Base: apptdhhNzduxm5gjn; table: tbl2xWLY9WpuxJoEP; primary field: fldqZRcUJaM8k6LtC.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| trip_id | fldqZRcUJaM8k6LtC | number |  |
| entryxclasses_uuid | fld3U0hv8jRpmX0NY | singleLineText |  |
| watch_trips | fldeo8fJTfQgWNtPM | multipleRecordLinks | watch_trips (tblCtNpzJtrI19VKq) |
| points_earned | fldhiShuKHgmqI2Zu | number |  |
| time_one | fldscukQB84vwx0dC | number |  |
| time_fault_one | fld9Iw9cFgBKPwNz1 | number |  |
| faults_one | fldtKO32QSgQSXf6b | number |  |
| time_two | fldWHviMavlk44axv | number |  |
| time_fault_two | fldQFZXvKGs3kZZ9K | number |  |
| faults_two | fldULT6aFS2iNI91Y | number |  |
| scratch_trip | fldKrYhzMLwC5G8OW | number |  |
| placing | fld38ftKqtaGP7FgH | number |  |
| position | flduRDhdeGNZRkA6v | number |  |
| score1 | fldpT6tTgjY1l1DA1 | number |  |
| score2 | fldbR3I92Sd0iVcCt | number |  |
| score3 | fldc8VvycTwtrdk9N | number |  |
| disqualify_status_one | fldH9PdnmGvATlh6e | singleLineText |  |
| disqualify_status_two | fldN4iF6owmgBvdH2 | singleLineText |  |
| score | fldYHc4GOhXBWHKft | number |  |
| class_types | fldNaClV3hLFx4NU3 | multipleRecordLinks | class_types (tblb0ajwYCxUqB1ow) |

### RingStatus.active_entries

Base: apptdhhNzduxm5gjn; table: tbl05SvRJmoG9xaik; primary field: fldoNLzrgQyoUcQVR.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| entry_id | fldoNLzrgQyoUcQVR | number |  |
| key | fldBTWis5OmsBwkKl | singleLineText |  |
| shows | fldsfdqfu3JP6rBhh | multipleRecordLinks | shows (tblqetAgwMf3TkLyN) |
| watch_entries | fldOf5DGUVfRChNcc | multipleRecordLinks | watch_entries (tbl6KiyJtLRJmw4p1) |
| entry_number | fldP1I4p5rr94XTGs | number |  |
| run_id | fldKwjwqvVzvzddI5 | number |  |
| last_run | fldDDWECdlQJLBmYu | date |  |
| app_sid | fld75K8DjLWNoza4n | number |  |
| app_sql_date | fldw5kN0ULMxBk9PX | singleLineText |  |
| app_dt | flditP1UiYVOg3zD9 | formula |  |
| isCurrent | fldpwgiHEQ4uw6fKQ | formula |  |
| isLatest (from shows) | fldFO19xIkEQ5XGfg | multipleLookupValues |  |
| pid | fldNmNd4D5mqb0dAy | number |  |
| ww_trainers | fldpHnBYI9l52z592 | multipleRecordLinks | ww_trainers (tblRJOVhOLuEJOkgF) |
| pid_mismatch | fldzGEIMzUKf2kv06 | checkbox |  |
| new_entry | fldY6fxbv6eH7GnXC | checkbox |  |
| inactive | fldpueAdKA3XgI6nK | checkbox |  |
| ww_horses | fldVE16E08H2iK6bb | multipleRecordLinks | ww_horses (tblliyUZ1ZS88Kfvl) |
| watch_trips | fld0tnw4c7cJkaSv9 | multipleRecordLinks | watch_trips (tblCtNpzJtrI19VKq) |
| horse_id | fld8U7eAAsv3HKyvA | number |  |
| horse | flddz8Q1JqadogTPT | singleLineText |  |
| rider_id | fld4ovr3l3ueE0ED8 | number |  |
| rider_name | fldHed5vpIj9wZ29X | singleLineText |  |
| record_id | fldne8mNdXYUdsLvz | formula |  |
| active_classes | fldTGnpsSUY8wwK3j | multipleRecordLinks | active_classes (tblkaonJjHXU0MVoZ) |
| active_classes_rid | fldTrZJSJDTJnVmrk | multipleLookupValues |  |

### WEC.class_results

Base: app6XS1RvsPNRT6os; table: tblnyxgTAf6QVFLEO; primary field: fldJuoSLvBPoFL9cE.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| class_result_key | fldJuoSLvBPoFL9cE | singleLineText |  |
| show_no | fldyLsGHjfNZvRAPe | number |  |
| shows | fldbFwbNsrcOOiJRU | multipleRecordLinks | shows (tblyjlXwdf0zg0mhn) |
| focus_show | fld5Agqo3IZdTgWm8 | multipleRecordLinks | focus_show (tblQldkP8wwIRxd4z) |
| focus_day | fldSSgz8KMvHspQET | date |  |
| class_no | fldXsUa3r4aLCpVai | number |  |
| classes | fldFhcj8XVbx0y1xV | multipleRecordLinks | classes (tblhxn7Jhkcnetaq5) |
| sect_no | fldPfpLVyGBagvf4B | number |  |
| class_number | fldClaYj165BhptIm | number |  |
| class_name | fld6IWx0E2V0dV7JK | singleLineText |  |
| place | fldNeZomWqxKnzvbm | number |  |
| entry_no | fldicrK4XddzWZFgU | number |  |
| entries | fldmJs5oHMhTy2nbW | multipleRecordLinks | entries (tblrRnqH6utOdyhSk) |
| horse | fldKvY7IWQIqk3VCN | singleLineText |  |
| horses | fldiGHvzlEJKXBWEH | multipleRecordLinks | horses (tblgWogH7B6Cvusvm) |
| ww_horses (from horses) | fldXhhT7h09VEpQWj | multipleLookupValues |  |
| barn_name (from horses) | fld4xD9S73LR0GxEf | multipleLookupValues |  |
| rider | fldT0fGpl3WRw7yga | singleLineText |  |
| riders | fld0SBBKT8ARWmfEe | multipleRecordLinks | riders (tbl75W08G7nB4MYAl) |
| owner | fldKFG31AHVMBlvno | singleLineText |  |
| owners | fldPcWJF3NukAGLuc | multipleRecordLinks | owners (tblOJW13HPILJHepN) |
| score | fldjAWPghLrvRJxzf | singleLineText |  |
| prize | fldpjChiTQejMcINU | singleLineText |  |
| wec-alerts | fldknwUxLSUXAr941 | multipleRecordLinks | wec-alerts (tblqkxLPy9zZ2FI6z) |
| entry_go_times | fldGIJrHryJBDlLqG | multipleRecordLinks | entry_go_times (tblj1qWXAUS79jijF) |
| update_schedule_staging | fldeZevWMOoJr5pdC | multipleRecordLinks | update_schedule_staging (tblzsoU59zmYxhPah) |
| class_start_times | fld4UnMoAfChmIkPT | multipleRecordLinks | class_start_times (tblgOxoLf6r3xGWxB) |
| class_oog_staging | fldfpyPCwM1NW7omB | multipleRecordLinks | class_oog_staging (tbljJQRgFvf1oL0Kf) |
| completed_at | fldJsfY7Lkq8hkpiz | dateTime |  |
| source | fldQrPbHS3fZKpvTf | singleLineText |  |
| raw_json | fldMgGlzJvgRmNkpj | multilineText |  |
| show_days | fldzNNtTEXZEh6rIb | multipleRecordLinks | show_days (tblqAdx2mA9qDw3KU) |
| ring_days | fldqvByyGxzriJIqo | multipleRecordLinks | ring_days (tblMw8DPVzlt3H8M7) |
| rings | fld0bbtjl1Wlq3Hi4 | multipleRecordLinks | rings (tbl5WKTbwL6IVrjyI) |
| ring_names | fldIrnRDddBAf0SOx | multipleRecordLinks | ring_names (tblcHfnJzCYLoBhjf) |
| trainers | fld0xZuCHqNSKB8sb | multipleRecordLinks | trainers (tblB72MubQbWfEqdf) |

### WEC.entries

Base: app6XS1RvsPNRT6os; table: tblrRnqH6utOdyhSk; primary field: fldBsb34haUaEecGT.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| entry_no | fldBsb34haUaEecGT | number |  |
| horse | fldYRHg1TIDaD3TR4 | singleLineText |  |
| horses | fldd7oilafZKuiNto | multipleRecordLinks | horses (tblgWogH7B6Cvusvm) |
| rider | fldgwP2U5NAJscelx | singleLineText |  |
| riders | fldkZ4vit8pN90qFK | multipleRecordLinks | riders (tbl75W08G7nB4MYAl) |
| trainer | fldYlxGcb2OGM3YSU | singleLineText |  |
| trainers | fldvd0vfFsSSS4gnR | multipleRecordLinks | trainers (tblB72MubQbWfEqdf) |
| source | fldfuHuHMyKvkp4Tr | singleSelect |  |
| get_orders | fldDbntUTCpgCjmx6 | multipleRecordLinks | get_orders (tblxaVS0dtetxjiGZ) |
| get_rings | fldrDhY4J6wawdch0 | multipleRecordLinks | get_rings (tblPWRF978F9YV3qW) |
| class_oog | fldTjUpNHRhraI06R | multipleRecordLinks | class_oog (tblgUbX5n8GIuiqUI) |
| barn_name | fldFGhOhqIewONeoa | multipleLookupValues |  |
| rider_display (from riders) | fldld0MfiNnI6C9Nz | multipleLookupValues |  |
| trainer_display (from trainers) | fldCZSyWvWdIjx3hA | multipleLookupValues |  |
| horse_display (from horses) | fldc2hAtGKaE72iNE | multipleLookupValues |  |
| profiles | fldd8hHVzJ129A2cf | multipleRecordLinks | profiles (tbl0Rpv3zuZQwaoLI) |
| Created | fldPBdCkEROArHQfu | createdTime |  |
| entry_go_times | fldKGSGo5BwXSWQOI | multipleRecordLinks | entry_go_times (tblj1qWXAUS79jijF) |
| assigns | fldqthhErorKEHU2Y | multipleRecordLinks | assigns (tbldcp5Mr9py1hgBr) |
| rec_id | fldJ8jB6OahFTqBAB | formula |  |
| wec_comments | fldmHxWSxNrM7yvTo | multipleRecordLinks | wec_comments (tblxazLk2DEAyPswp) |
| wec_observations | fldtU3uLljscGW49C | multipleRecordLinks | wec_observations (tblejM6A7Abm22thw) |
| wec_ring_comments | fldwA2AS64Qca6AT1 | multipleRecordLinks | wec_ring_comments (tblpFiUIuONVWgwYj) |
| wec_class_comments | fldOtRTyf8AFCCZvj | multipleRecordLinks | wec_class_comments (tblLaIliwlbgPF5V6) |
| wec_entry_comments | fldNJEsQyrGvyDiwp | multipleRecordLinks | wec_entry_comments (tblmsnmZZiWFiOBaM) |
| wec_sessions | fldi8iwaPuYWhV3dI | multipleRecordLinks | wec_sessions (tblChGuM5HgNLEJ2w) |
| wec_ring_checkins | fldmtbdKRzT8m0FSr | multipleRecordLinks | wec_ring_checkins (tblnUZOHJmoLXf6TN) |
| wec_comment_presets | fldRfdOkiRsmYGxOr | multipleRecordLinks | wec_comment_presets (tbl640XNbdRNEWlqm) |
| wec_question_templates | fldfibkhmVO41583f | multipleRecordLinks | wec_question_templates (tblEq008dZ4ZmZcRQ) |
| waze_users | fldnF6y2IIyInhlGN | multipleRecordLinks | waze_users (tblVeUEa8n7QLHjRk) |
| waze_session_footprints | fldwCh9KryrFEak8l | multipleRecordLinks | waze_session_footprints (tblyJ7CPubgbBYEai) |
| class_results | fldwNzivh3jZucZsy | multipleRecordLinks | class_results (tblnyxgTAf6QVFLEO) |
| class_start_times | fldXVjkTHTrWNNBmN | multipleRecordLinks | class_start_times (tblgOxoLf6r3xGWxB) |
| wec-alerts | fldN6x1wItyNModgy | multipleRecordLinks | wec-alerts (tblqkxLPy9zZ2FI6z) |
| class_oog_staging | fldW8zjYZeQK4bAWo | multipleRecordLinks | class_oog_staging (tbljJQRgFvf1oL0Kf) |
| get_rings 2 | fldAbAC1OKHQ4QbbD | multipleRecordLinks | get_rings (tblPWRF978F9YV3qW) |
| get_orders 2 | fldSAOc8IXvHs9ScL | multipleRecordLinks | get_orders (tblxaVS0dtetxjiGZ) |

### engine.lanes

Base: appnOAk2pW8JawiuP; table: tblahQaNEwKKys0qP; primary field: fldwWSfWhSaspxpIk.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| ﻿final-item | fldwWSfWhSaspxpIk | multilineText |  |
| projects | fldz7ioq27bknLPps | singleSelect |  |
| rs-bases | fldhq7cB2Geawa9a3 | multilineText |  |
| Main JS | fldOLAGZCamVRX5JR | singleSelect |  |
| wStages | fldZba8AS1hrNMkiJ | multipleRecordLinks | stages (tblALw5TndjvOxlW2) |
| Paths | fldxA9Dpt9ZDTDUNy | multilineText |  |
| Timing | fldeIEXIrc2GnrFnf | multilineText |  |
| Responsibility | fldWb5qCIaIUGPhxM | multilineText |  |
| final-list | fldcEwH049fAm8pIF | singleSelect |  |
| priority | fldiJFWUd6XXUVpxt | number |  |
| purpose | fldsD17332JoiM2cn | multilineText |  |
| agent-support | fldhdzitlqJWzm6xf | singleSelect |  |
| Stage | fldB8fj6bjQwSGhpR | singleSelect |  |
| From | fldlZHNkQQDkOf6BX | multilineText |  |
| To | fld3NAHUZVD653MhJ | multilineText |  |
| traces | fldWujbsioqlFxRHh | multipleRecordLinks | traces (tblh9zKZaWHvyecbz) |
| trace-status | fldW7aWble85he1Dm | multipleRecordLinks | trace-status (tblhnkwnrM72pWXaW) |

### engine.traces

Base: appnOAk2pW8JawiuP; table: tblh9zKZaWHvyecbz; primary field: fldU7mVkuNgApS3by.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| name | fldU7mVkuNgApS3by | singleLineText |  |
| stages | fldDXm3LGHEgMvcKD | multipleRecordLinks | stages (tblALw5TndjvOxlW2) |
| lanes | fldlc2Gxb5cD8Jwgn | multipleRecordLinks | lanes (tblahQaNEwKKys0qP) |
| date | fldDVcRLfwcU6qsFq | date |  |
| basis | fldmMuvI4ZjjNKD4L | singleSelect |  |
| ___note-status | fldFHzZaFGaHp7P2S | singleSelect |  |
| finding | fldSqGuMoWfZPOuI8 | multilineText |  |
| evidence | fld8oCHWaAe0FmuHY | multilineText |  |
| next-step | fld4hjm1fUwjcMLEh | multilineText |  |
| trace-evidence | fld5mkxUD0Uvgw5Vq | multipleRecordLinks | trace-evidence (tbln8bDBh8ZWxPhGB) |
| trace-status | fld3aYgYVKLEctZF1 | multipleRecordLinks | trace-status (tblhnkwnrM72pWXaW) |

### engine.trace-evidence

Base: appnOAk2pW8JawiuP; table: tbln8bDBh8ZWxPhGB; primary field: fldaylc52dswYJ322.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| name | fldaylc52dswYJ322 | singleLineText |  |
| traces | flde8mt8RXMMgrZ1g | multipleRecordLinks | traces (tblh9zKZaWHvyecbz) |
| date | fldQ5KV0GT9VAZy5Y | date |  |
| basis | fldl1uwveew02anug | singleSelect |  |
| finding | fld5CBnhRtOrWn7LU | multilineText |  |
| source | fldZsLXuIHZGTx16l | multilineText |  |
| limit | fldL6mZNVsUhQXCe9 | multilineText |  |
| trace-status | fldH7fH9tslTqb9IR | multipleRecordLinks | trace-status (tblhnkwnrM72pWXaW) |

### engine.trace-status

Base: appnOAk2pW8JawiuP; table: tblhnkwnrM72pWXaW; primary field: fldIjXDM6LeNhrurM.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| name | fldIjXDM6LeNhrurM | singleLineText |  |
| traces | fldB9S1NaHjepEokR | multipleRecordLinks | traces (tblh9zKZaWHvyecbz) |
| stages | fldeTZPlTn9Db7Idn | multipleRecordLinks | stages (tblALw5TndjvOxlW2) |
| lanes | fld7e2goqp9VEy0hS | multipleRecordLinks | lanes (tblahQaNEwKKys0qP) |
| trace-evidence | fldwvyNnxI3WUhohN | multipleRecordLinks | trace-evidence (tbln8bDBh8ZWxPhGB) |
| source-review | fldiCOJ8Im2JsCMO7 | singleSelect |  |
| runtime-check | fldkMmwOvnsaJ9TiM | singleSelect |  |
| end-to-end | fld3GduenNnsIEhzt | singleSelect |  |
| remaining | fldxOBu1oZtP3ks8q | multilineText |  |
| evidence-date | fld4tt0BYdEc4Z2dM | date |  |

### engine.endpoints

Base: appnOAk2pW8JawiuP; table: tblcgmHa2XJkGRvar; primary field: fldOEC4E2ZXPqmejQ.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| name | fldOEC4E2ZXPqmejQ | singleLineText |  |
| reference | flduvYJDETqFgNF04 | multipleRecordLinks | reference (tblw8QC3Bdqdc8Odd) |
| URL (from reference) | fldVH6WwJ808QlFci | multipleLookupValues |  |
| method | fldwXAUvnUdWh5WM3 | singleLineText |  |
| address | fldDxumgoRjMzMTB1 | singleLineText |  |
| purpose | fldL5RwqagKo7LtKy | multilineText |  |
| target | fldCsk1sdlVFj6CH4 | multilineText |  |
| inputs | fldYPrjoIGo8XMtcB | multilineText |  |
| status | fldJO21SvuwwPaEty | singleLineText |  |
| evidence | fld5bCgLetCAyM75F | multilineText |  |
| usage | fldqHH3AUZEJxHTc5 | singleLineText |  |
| request-example | fldnhvpISoqcpgroS | multilineText |  |
| lanes | fld4hB2unKAtzijtC | multilineText |  |
| caller-evidence | fldBIzUFBDVdxabDI | multilineText |  |
| how-to-complete | fldO5oj5JiqaGpoXp | multilineText |  |
| id-sources | fldPvh4Htz1MaAMsb | multilineText |  |
| id-links | fldLosvpEWAVxxRie | multilineText |  |
| id-resolution | fldwfP4zfOkcjjSXR | multilineText |  |

### messages.rs_sms_requests

Base: apphwDLkVCp8ut2Hd; table: tblQWqhaSVXdDxQF0; primary field: fldXeXzPHUcc98l4e.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| request_uid | fldXeXzPHUcc98l4e | singleLineText |  |
| person_uid | fldmZfnyTxaWV1THY | singleLineText |  |
| primary_phone_e164 | flduFsAyca4I63mK3 | singleLineText |  |
| status | fldZmZIcP525UivcZ | singleSelect |  |
| record_mode | fldYAoB34Z8rk9OaU | singleSelect |  |
| requested_at | fldFGzTOlqjzb76BN | dateTime |  |
| provider_message_sid | fldLRb5UTXT8VacGP | singleLineText |  |
| error_code | fldjjOMgZ5Ny0qmtg | singleLineText |  |
| rs_sms_events | fldiVER24hqvtP1Li | multipleRecordLinks | rs_sms_events (tblr7sWHgoYDgj7uV) |

### messages.rs_sms_events

Base: apphwDLkVCp8ut2Hd; table: tblr7sWHgoYDgj7uV; primary field: flduF12Ex64BAhPIh.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| event_uid | flduF12Ex64BAhPIh | singleLineText |  |
| occurred_at | fld2ySjKGnQ2OkrNx | dateTime |  |
| event_type | fld7sYlbd4VY4TnLT | singleSelect |  |
| provider_message_sid | fldUgduVdnHOSpyhy | singleLineText |  |
| source | fldQcmajj5fsEl0ng | singleLineText |  |
| error_code | fldWac7fq1TwuNAMm | singleLineText |  |
| request | fld4fsl1nf4aAdEPC | multipleRecordLinks | rs_sms_requests (tblQWqhaSVXdDxQF0) |

### publish.publish-index

Base: appZY1hcqGvUvxIuV; table: tblAzdr8GmPG1gUaW; primary field: fldB8aQCuR8IUdPEG.

| Field | Field ID | Type | Native target |
| --- | --- | --- | --- |
| Name | fldB8aQCuR8IUdPEG | singleLineText |  |
| Notes | fldRJS7Rtzpb4DcnB | multilineText |  |
| Assignee | fldQGJLIDaQYOlrRr | singleCollaborator |  |
| Status | fldfmqbfxHhHejVWK | singleSelect |  |
| Attachments | fldBpxfMvkd8xN6J4 | multipleAttachments |  |
| Attachment Summary | fldJxdDKpobyRGTVH | aiText |  |
| Table ID | fldFhi52JJeqaPGxF | singleLineText |  |
| Purpose | fldROf8bukfKf7Loc | multilineText |  |
| Table link | fld6sCMgiuSOuR42Q | url |  |
