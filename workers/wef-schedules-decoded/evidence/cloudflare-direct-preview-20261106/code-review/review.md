# Read-only schedule / Cloudflare review

Reviewer: wef-decoder / 01a1a753-dce7-7951-a147-1b096e1538ba. Reviewed 2026-11-06T22:04:05.752Z. No independent second reviewer is claimed.

## Findings

**P1 CONFIRMED — worker.js decodeSglPayload / fetch success path.** Raw upstream {} is accepted as an object, visits no protected fields, and returns HTTP 200 with X-RS-Decoded:1. Trigger: successful upstream HTTP response containing an empty object. Consequence: consumers can mistake empty source data for a decoded schedule. Reproduced offline with one mocked fetch, without source requests. Smallest proposed correction: validate the route-specific required schedule shape before reporting usable success. No correction implemented; failed-source review grants no production-edit authority.

**P2 CONFIRMED — prior compare-token.mjs sameCookie expression.** Optional digests compared directly yield true when both are absent. Smallest correction: require two present digest strings before comparison. Current schedule-comparison diagnostics already check presence and show equality; the old bug does not invalidate the captured schedule comparison. No old diagnostic file repaired in this review.

## Raw response comparison

One local and one actual Cloudflare remote-preview schedule request used the same already-minted local token. URL, method, all application-built headers, token and cookie matched. Secret values and fingerprints were removed from saved comparison. Local raw response: HTTP 200, 59,843 bytes, 7 rings, show 200000077/date2026-11-06. Local decoded response:61,035 bytes,7rings. Cloudflare raw response:HTTP200, exactly2bytes, literal {}, Content-Type application/json, Server cloudflare, CF-Cache-Status BYPASS, Ray a567628d2b7353f5-MIA. Its decoded response remained exactly2bytes,{}. Both response Date headers:November6 22:02:52GMT.

This localizes data absence upstream of the decoder on the Cloudflare-facing HTTP path. It does not identify Wellington/SGL's internal filtering rule. Exact on-wire platform-added headers, source IP handling and upstream server logic cannot be verified without their logs. Do not claim proven IP throttling, token expiry, JSON corruption or cache hit.

## Scope / authority

Original owner authorized schedule/class preview only, no tunnel/production/collectors; subsequent correction required locally minted token and schedule only, then stop. Latest request authorized code review first and exact outgoing/raw comparison, no classes. Only temporary schedule diagnostics were changed. One local/Cloudflare schedule comparison executed; no new token minted in that comparison, no class requests, no collectors, no production source, task or credential changes. Existing manifest/lockfiles unchanged for this delta; no dependency upgrades. Earlier working-tree relay/header/public-route edits and the separately approved clock task are protected, not absorbed into this review or changed.

## Verification disposition

Code review COMPLETE with candidate FAIL: direct Cloudflare is not a usable schedule fallback and decoder's empty-success defect remains. Offline mock checks executed in execution-log.json. Saved live comparison inspected, not rerun during offline review. Production deployment not attempted. Scheduled proof not applicable to this diagnostic. Remote preview stopped after comparison. Raw populated local body stayed in memory; saved metadata and hash are not a complete raw-body archive. Credentials excluded.

## Next boundary

Use the already proven local path as current evidence. No extra infrastructure is proposed here. Any decoder validation change requires explicit approval. Determining the upstream rule needs SGL-side evidence; another Worker name alone is not evidence of a fix.
