# Returned-token review

Reviewer: wef-decoder, this chat; 2026-11-11T18:30:57.997Z. No delegated reviewer or external review claim.

PASS: actual source delta is only valid anonymous returned-token adoption, same-GET replay once after 1000 ms, authentication-safe failure handling, and replacement of the existing fallback memory token. Invalid/account/expired tokens remain failure; repeated token response stops without a third request; redirect and HTTP failures remain failures. URL/query/header profiles and public routes are unchanged. Required return-token/cookie redaction inspected.

The source worker before this edit compiles identically to the active deployed decoder block. The release artifact replaces that block and the corresponding fallback cache callback in deployed version 1c176106-d141-487c-a762-fd3ea765f2a6. Every other deployed bundle byte matches the frozen baseline. This excludes unrelated local dual-health work. Nine protected files, including queue/pause, runtime runner, config and Fast consumer, retain baseline hashes. No dependencies/runtimes/config/credentials/route upgrades.

Executed: 48 focused worker/fallback/health/pacing checks passed. Actual isolated Cloudflare remote preview exercised the release artifact with only its first class Authorization removed in preview: a real _t was issued, the production handler cached it, replayed the identical GET after 1000 ms, and decoded the real class 200031333 with two non-null trip IDs. No preview hook is in candidate.mjs. No Airtable write or collector in preview.

Scope limits: no class-health, stage/alerts, new headers, endpoints or collector changes. Deployment and normal scheduled Fast class save remain acceptance steps after this review. The initial preview upload stalled over the local IPv6 connection; restarting only this preview with IPv4 preference reached Ready and passed. Production networking was unchanged.
