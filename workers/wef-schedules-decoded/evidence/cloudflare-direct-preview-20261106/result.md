# Direct Cloudflare preview, no tunnel — November 6, 2026

Owner authorized remote preview tests only, no production changes or collectors. Used existing Wrangler 4.82.1 remote dev temporary session, account ringstatus dce50dddf8a92d48664f0089f8f5da22. Diagnostic script and existing cookie binding stayed in temporary directory; no secret values included in evidence. Existing decoder worker.js was copied unchanged into the preview. No hosted deployment was made.

First diagnostic attempt: schedule and class detail each returned 502 before any get-config response (renewals=0). Preview had copied Node's redirect:error option. Cloudflare documentation confirms that mode throws before network access. Owner separately approved changing only the preview to manual redirect mode with explicit rejection of 3xx.

Corrected remote preview:
- GET /schedule?date=2026-11-06&show_id=200000077&customer_id=15 -> HTTP 502; x-preview-execution cloudflare-direct-no-tunnel; x-preview-renewals 1; response {"message":"Direct Cloudflare preview failed","stage":"get-config","upstreamStatus":null}.
- GET /classes/200031233?show_id=200000077&customer_id=15&cgid=-1 -> HTTP 502; x-preview-execution cloudflare-direct-no-tunnel; x-preview-renewals 2; same response stage and null upstreamStatus.

Code path: renewal counter increments after get-config fetch returns. Non-2xx would set upstreamStatus. JSON parsing failure would produce stage preview. Therefore these corrected responses support a successful HTTP get-config response and parsed JSON, followed by failed token parsing/claim validation. They do NOT establish which token condition failed: missing _t, malformed JWT, nonanonymous claims or expiry. Exception detail and raw response were not exposed by the diagnostic wrapper; exact distinction remains UNVERIFIED. No SGL schedule or class fetch follows failed renewal. Zero usable schedule/class payloads in this corrected remote test. Repeated successful data acceptance is FAIL; do not call this a working fallback or infer that all Cloudflare-to-SGL requests are 403.

No further changes to preview token parsing, production decoder, runner, credentials or collectors. No repeated failed requests were issued after the one corrected pair. An attempted connection to the preview's local debugger port was refused; this did not call SGL.

Cloudflare docs: https://developers.cloudflare.com/workers/runtime-apis/request/ ; remote execution mode: https://developers.cloudflare.com/workers/local-development/ . Original corrected-pair output provenance: chat command chunk a0b964; pre-network pair: 91f88c. Both requests used the remote dev front door localhost:8791; execution of preview worker and source fetch occurred on Cloudflare, not on the local runner.

## Exact token diagnosis — owner-approved preview diagnostics only

Comparison used the same preview source and token-renewal headers in remote Cloudflare execution and local Node execution. Cookie SHA-256 digests matched; digests were removed from saved/public output. No bearer, cookie, token, sid or personal claim value is in the comparison artifact.

Cloudflare response: HTTP 200, Content-Type application/json, bodyBytes=2, parsed object with no keys, tokenPresent=false. JSON parsing establishes the actual body is {}. The preview failed at token_payload_unreadable because _t was absent. No JWT reached validation or decoding.

Local response: HTTP 200, Content-Type application/json; charset=utf-8, bodyBytes=333, keys protection/mobile_app/_t. Token is a three-part string; anon=true, numeric future exp, issued November 6 21:56:04 UTC, expires December 6 21:56:04 UTC. Validation succeeded. Token and sensitive claim values were not printed or persisted.

Confirmed boundary: the config endpoint returns an empty object to Cloudflare and a populated object to the local runner for matching application-level inputs. This is NOT evidence of a decoder bug or malformed JWT. The specific upstream policy (IP/network filtering, header normalization, other server-side rule) cannot be established from these responses alone; do not label IP throttling proven.

No production changes, manual collectors, token redistribution or direct schedule/class fetches were made in this diagnostic comparison. Remote preview stopped afterward. Safe evidence: token-comparison-safe.json. Comparison output provenance: tool chunk 2021a2.
