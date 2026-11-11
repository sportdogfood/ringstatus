# Confirmed request-header cause - 7 November 2026

Read-only diagnostics; no application source changes, runner/tunnel restarts or collectors. Legacy tasks remained disabled.

Browser CDP observation of its normal scheduled page request: GET /schedule returned200. Browser wire metadata contains :authority, :method, :path and :scheme as HTTP/2 pseudo-headers. It does not send authority, method or scheme as ordinary headers. No credentials were displayed or saved in this evidence.

Existing installed runner and shared/fallback decoder source constructs authority, method, path and scheme as ordinary application headers. fallback tcp-fetch serializes these ordinary header names into HTTP/1.1 lines.

Controlled actual SGL comparisons reused the same fresh anonymous token, cookie, URL and remaining headers within each test process. Original headers yielded HTTP200 with exactly2-byte {} and server awselb/2.0. Removing all four copied pseudo-header names yielded7rings (76429bytes). Removing client hints alone did not help. Removing just one of the four did not help because other blocking headers remained.

Add-one isolation, with the other pseudo names absent: authority alone ->2-byte {}; method alone ->2-byte {}; scheme alone ->2-byte {}; path alone ->7rings; none ->7rings. Thus each of authority/method/scheme is individually sufficient for this empty response. The exact SGL load-balancer rule/config is not available; causal request-field behavior is proven.

PowerShell A/B/A using the same URL, token, cookie and all remaining headers: original ->200/2bytes/{}; remove only authority,method,scheme ->200/76429bytes/7rings; original again ->200/2bytes/{}. Class detail with the same existing installed decoder and only these headers omitted returned class200031233 with8trips.

Existing hosted/fallback decoder executed locally with identity encoding (matching fallback transport) reproduced502 at decode stage with Error: Empty source payload when source returned{}. Removing only authority,method,scheme returned200/7rings; restoring them reproduced502. See fallback-decoder-ab-a.json. This establishes the same bad-header failure in the fallback decoder; this was not a new Cloudflare deployment/runtime test. Stored Cloudflare logs remain inaccessible with current authentication.

Smallest supported correction: omit authority/method/scheme ordinary headers from the decoder request construction used by runner and fallback. Also omitting redundant path would match browser semantics but its removal is not required by the isolated failure. Do not add type:module to root package.json, replace transport, rotate credentials, change queue or rebuild the runner. Any production correction requires owner approval and preview/live verification. No such correction was made in this investigation.

Provenance: command observations in this chat include token/header4-case isolation, add-one5-case isolation, PowerShell A/B/A and class decoder check. Their safe measured outputs are summarized here; raw credential-bearing requests are intentionally not retained. Node warning is nonfatal and unrelated.

## Follow-up: why success changed to failure

Scheduled observation transition: last successful slot 2026-11-07|11:39, first failed slot 2026-11-07|11:42, America/New_York. All three installed runner module hashes match the recorded6November baseline. Local private env-file modification times also predate the healthy period; values were not exposed. Node versionv22.22.2 matches previous successful test logs. These facts provide no evidence of local code/runtime drift. Hosted deployment history could not be retrieved with current app permissions; do not claim fresh Cloudflare history verification.

Direct-address tests of both currently resolved SGL ALB nodes (100.57.42.211 and100.59.103.186), with normal certificate validation/SNI/Host, sent no Authorization or Cookie. The same public get-config request returned token/config data with ordinary authority absent and exactly{} with server:awselb/2.0 when authority was added. Therefore this current filter is independent of token expiry, saved cookies and logged-in identity, and is present on both current nodes. No per-IP successful/broken node difference was observed. Earlier lookup-hook diagnostic failed locally before HTTP; this is preserved rather than treated as SGL evidence.

AWS documentation states that ALB-generated responses carry server:awselb/2.0: https://repost.aws/knowledge-center/alb-add-configure-headers . ALB access logs identify actions_executed and matched_rule_priority: https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html . Thus current evidence localizes the empty response to SGL's AWS gateway behavior before application/authentication, not decryption. An SGL-side configuration/rule change is a supported explanation for the transition, NOT a proven historical change. Only SGL's gateway/WAF access and configuration-change records for19:18–19:21UTC on7November can establish the exact historical trigger. Those are not in the owner's Cloudflare account. No claim that the current header isolation proves the earlier cause is permitted.

Chronology clarification: the first failed observation in the entire day was an earlier isolated failure, not the beginning of the sustained outage. Full successful/failed observation runs are now in transition-and-runtime-baseline.json. Keep that transient incident separate from the current sustained failure; its reason is not proven to be the same. The current sustained outage follows last healthy 2026-11-07|14:18 and starts 2026-11-07|14:21.
