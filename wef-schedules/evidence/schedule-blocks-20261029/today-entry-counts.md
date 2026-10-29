# October 29 source entry counts

Show: 200000076, 2026 ESP Fall Finale. Source: payload saved by the normal schedules collector.

There are zero competition classes. These 26 rows are schooling/break groups with empty class lists. Every group reports total_trips=0; the sum is 0 reported trips. This does not measure attendance at open schooling. The top-level total_trips_count=26 equals the block count in this saved payload; its meaning has not been independently verified as participant count. It is excluded from the trip total.

| Ring | Source group | Kind | Reported entries/trips |
| --- | --- | --- | --- |
| International Ring | 1.40-1.45m Fall Back # Schooling 9-10:15am | Schooling | 0 |
| International Ring | 1.30-1.35m Fall Back # Schooling 10:20-12:05pm | Schooling | 0 |
| International Ring | 1.20-1.25m Fall Back # Schooling 12:10-2:20pm | Schooling | 0 |
| International Ring | 1.10-1.15m Fall Back # Schooling 2:25-4pm | Schooling | 0 |
| Rost Arena | 3'6" Back Number Schooling From 9:00am to 10:00am | Schooling | 0 |
| Rost Arena | 3'3" Back Number Schooling From 10:05am to 11:05am | Schooling | 0 |
| Rost Arena | 3' Back Number Schooling From 11:10am 12:05pm | Schooling | 0 |
| Rost Arena | Drag Break 12:50pm to 1:00pm | Break | 0 |
| Rost Arena | 2'9" Back Number Schooling From 1:00pm to 2:25pm | Schooling | 0 |
| Rost Arena | 2'6" Open Schooling From 2:30pm to 4:00pm | Schooling | 0 |
| Mogavero | .70-.80m Fall Back # Schooling 9-10am | Schooling | 0 |
| Mogavero | .90-.95m Fall Back # Schooling 10-11am | Schooling | 0 |
| Mogavero | 1.0m Fall Back # Schooling 11:15-12:45pm | Schooling | 0 |
| Mogavero | 1.10m Fall Back # Schooling 12:50-1:50pm | Schooling | 0 |
| Mogavero | 1.15m Fall Back # Schooling 2:25-3:40pm | Schooling | 0 |
| Ring 6 | 2'9"-3' Fall Back # Schooling 9-11am | Schooling | 0 |
| Ring 6 | 2'6"-2'9" Fall Back # Schooling 11-12:30pm | Schooling | 0 |
| Ring 6 | 2'3"-2'6" Fall Back # Schooling 12:40-3pm | Schooling | 0 |
| Ring 6 | 18"-2' Fall Back # Schooling 3-4pm | Schooling | 0 |
| Ring 7 | 1.15m Jumper Schooling From 9:00am to 10:15am | Schooling | 0 |
| Ring 7 | 1.10m Jumper Schooling From 11:00am to 12:05pm | Schooling | 0 |
| Ring 7 | 1.10 Jumper Schooling From 12:00pm to 1:00pm | Schooling | 0 |
| Ring 7 | Drag Break From 1:00pm to 1:15pm | Break | 0 |
| Ring 7 | .90 to .95 Jumper Schooling From 1:15pm to 2:15pm | Schooling | 0 |
| Ring 7 | .70 to .80 Jumper Schooling From 2:25pm to 3:30pm | Schooling | 0 |
| Ring 7 | .60 to .70 Jumper Schooling From 3:30pm to 4:15pm | Schooling | 0 |

CWF normal entry collection at 08:24 Eastern also reports empty October 29 datasets for trainers 8778, 50121 and 80970, main-user and user-2. This is a tenant slice, not an all-show attendance count.

## How the zero was determined

This is a source-reported trip count, not a count of identified horses or confirmed schooling attendance.

1. Read the schedule payload saved by normal collection for show 200000076 and date 2026-10-29. No live source request or manual collector run was made.
2. Visit each group object at `rings[ring index].classes[group index]`. Despite the property name `classes`, these 26 objects are source schedule groups.
3. Check that `total_trips` exists and is explicitly numeric `0`. All 26 objects passed; none had an absent, null or blank value. No missing value was converted to zero.
4. Add those 26 explicit values: **0 reported trips**. `completed_trips=0` and `class_list=""` are context, not the basis of this calculation. The break flag identifies the block, not its trip count.
5. Compare with normal-run publication: the 08:30 and 08:35 runs completed successfully; Airtable contains 26 October 29 blocks, each with `total-trips=0`. The second run reused today's dataset without changes.

Example: `rings[0].classes[0]` is International Ring group **200028178**, "1.40-1.45m Fall Back # Schooling 9-10:15am". Its source `total_trips` is explicitly `0`; the collector preserves that value as normalized `fields.total_trips` and Airtable `schedules.total-trips`. No participant list was fetched to derive it.

Do not promote this result to "no horses entered/attending." The source may not track open-schooling participation through this field. The empty CWF datasets are only a separate tenant observation and are not used in the all-block sum.

Source snapshot: `wef-schedules/runtime/cache/schedule/schedule_2026-10-29_show_200000076_1793276109.json`.

Source SHA-256: `faca57c1363034c1b95d156e20b47ecf44c6680f589b9750e3d05fd3a6f28f18`.

Exact field paths, all 26 group IDs/values, source hash and scheduled-run IDs are retained in `today-trip-count-evidence.json` in this directory. These are dated observations; they do not assert future source values.

## Handoff to wef-shows

For show/date coverage, report **24 schooling sessions, 2 breaks, 0 competition classes; 0 source-reported trips; schooling attendance unknown**. Do not mark October 29 as an empty day, equate trips with unique horses, or use the source header's 26 as an entry total. `wef-shows` remains the show/date authority; dated schedule and per-block count evidence comes from `wef-schedules`. This handoff requests no code, Airtable, collector or cadence changes.

Handoff sent to the existing `wef-shows` chat (`01a16976-f0a6-72a1-a75f-75de7f19a815`) on 2026-10-29T08:38:59.780494-04:00. Message delivery was confirmed; acknowledgment or documentation updates in that chat have not been verified.
