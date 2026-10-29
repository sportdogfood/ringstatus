# Data, Season and Entries scheduled acceptance

Verified 29 October 2026, approximately 2:50 p.m. America/New_York. Bounded point-in-time acceptance, not a guarantee of future source availability. No manual collector/source runs, task changes, record repairs or deletions during this verification. Necessary cache, archive-path and directory-conflict corrections were separately approved directly by the owner in Season and independently reviewed; prior failures and stops remain retained.

## Data: two normal cycles after the final correction

| Normal attempt | UTC start / completion | Accepted Data head | Result |
|---|---|---|---|
| 06a13f28-d950-473c-a3de-170a38d005d2 | 18:33:15 / 18:40:15.998 | adaptive-2026-10-29T18:22:08.409536+00:00, sequence16, 75 observations | Succeeded; verified head and handoff; one held identity |
| 06b5f1ed-1c44-484c-86c2-53b02967dc28 | 18:41:14.789 / 18:46:09.353 | adaptive-2026-10-29T18:43:08.624367+00:00, sequence17, 39 observations | Succeeded; verified head and handoff; same held identity |

Sequence16 state SHA: 7a06fa09a3e3c808cb220a94e6f3419aa7459734796692846d3a67831f01b754. Sequence17 state SHA: 0d9d8405f4ac49872a86f34ab7ffae154c85ce6f1ec4b3f14980138881dbb9d5. Records independently replayed actual frozen cache evidence with exact predecessor, collection-key, record-ID and payload-hash validation. Source observation times were not renewed by replay/publication.

Directory acquisition verified866 captured entries; identity reconciliation remains PARTIAL. Entry200000076|200244210 retains original record recHmJwh8XvblkKWP, horse200005163 and source time2026-10-29T13:09:45.572Z; contradictory horse200005221 source evidence is retained separately. The entry is excluded from directory writes, not relinked. All original fields/native links were unchanged on actual Airtable readback; separate IdentityConflict errors are saved and acknowledged for both successful attempts. Unaffected records continue. Existing strict class-delta identity guards remain; this release does not promise blanket immunity to all possible source conflicts.

## All four Season transfers

Normal18:21–18:25 transferred all four domains from verified sequence15; normal18:26 verified the same checkpoints with zero adds/updates. Records independently compared actual Airtable snapshots142536 and142956: inventory hashes, native-ID/key maps, checkpoints and links unchanged.

Subsequent normal18:41–18:42:47.985 accepted sequence16, and normal18:46–18:47:16.976 accepted sequence17 in all four bases. Actual readback approximately18:49 confirms:

| Domain | Records | Show mirrors | Day mirrors |
|---|---:|---:|---:|
| Horses | 955 | 2 | 8 |
| Riders | 634 | 2 | 8 |
| Trainers | 265 | 2 | 8 |
| Dated rings | 56 | 2 | 8 |

Keys and local native links validated; checkpoints bind the verified current head plus the published retained show75 archive. Exact source-key/native-membership comparison passed7736 checks. Added records reflect new accepted source facts, not duplicate identities.

## Entries final head handoff

Normal run abd9de11-bd59-4c40-8025-006c7de19f65,18:49:02.428–18:49:48.911 UTC (2:49 p.m. Eastern), task exit0, business ok, Partial, no processing errors, consumed verified sequence17. Row source-json, run-log and heartbeat agree. All127 record IDs, keys, native/scalar/plural schedule links and source timestamps were preserved against the previous accepted baseline; no duplicates.

Entries links only its shared schedules. It does not acquire source entries or depend on CWF. Ambiguous schedule relationships remain Conflict and missing evidence remains diagnostic; neither is converted into a fabricated link or empty success. Group-linked with class-date basis establishes source-group membership only; operational group occurrence requires observed-group basis and its evidence. This verification does not claim all source identities or group occurrences are resolved, all facts are fresh, or a deployed Timing/tenant-cwf consumer has been accepted.

## Archive and preserved boundaries

Normal show75 archival accounts for all23 Data tables and25 verified files,3905 domain rows plus3 meta records. Records independently validated the first publication at commit2cc7d95fbde5a035b858f31238708df018d05d28 against every manifest hash. A subsequent verified cutoff at commit3b27b402132aa3d7cd637718dd6eccfa0325e4b1 retains shared-entity updates; previous archives remain immutable. Original divergent ringstatus-data checkout and local commits were not reset or pushed. No physical old-show deletion was authorized; this acceptance does not claim current-show-only physical storage.

Evidence in local Season workspace C:/Users/gombc/Documents/Codex/2026-10-23/find-all-docs-discussing-wef-people-4/outputs/: full-correction-scheduled-evidence/identity-isolation-{first,second}-*.json; wef-season-two-normal-cycles-proof-20261029.json; wef-season-target17-source-proof-20261029.json; wef-season-scheduled-readback-20261029T144928.json; held-entry readbacks and deployment manifests. Entries final proof: wef-entries/evidence/shared-schedules-20261029/latest-head-verification-1849.json. Prior failures remain in their original evidence files.
