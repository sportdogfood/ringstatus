# Anonymous auth discovery — November 11, 2026

Status: current 26 test-now records covered, plus 14 missed leads. Findings saved and read back in [sgl-opportunities](https://airtable.com/appCaSmL819KK7UFJ/tblKkbHxccs2jKCOS), with a filterable `auth-result` field. Existing observations remain below the new dated finding; owner checkboxes, priorities and source-pattern labels were preserved.

## Useful result

Eleven source-route patterns returned `_t` claiming `anon=true` with no request cookies or bearer: `/utilities/get-config`, `/shows`, `/schedule`, `/ring`, `/classes`, `/classes/{id}`, `/classsignup`, `/classsignup/{id}`, `/results`, `/results/{id}`, and the missed `/users/getUser` route. The last returned `user=null`; an anonymous token is not a user login. These are alternative issuance points on the same API service, not proven independent transport fallbacks.

The fresh config token decoded the selected schedule's nine rings and source-derived class 200031333's two trips. Class detail's own newly issued token also decoded two trips on replay. Deliberately changing signature bytes on the anonymous token caused schedule to return a replacement anonymous `_t`; using it decoded nine rings. No real expired token was tested. Tokens were not saved in evidence or installed configuration.

The actual data response carrying `_t` was rejected by the unchanged existing decoder with `Upstream token refresh required`. Source-style token adoption (save token, remove `_t`) accepted that first class body, and the protected HTTP replay decoded two trips. This verifies the compatibility gap with main.js's save-and-retry-once flow. It does not prove the cause or resolution of every production class failure. No implementation was changed.

## Error evaluation

- `/add-entry/getShowData` and `/users/getLoggedInUser` returned 401 both without credentials and with the working anonymous token.
- Legacy GetShowData returned HTTP 200 body `DoLogin`, also with an isolated anonymous Detail-page cookie. Legacy signup Index returned `DoLogin`. Those are not successful data responses.
- AuthenticateUserViaJWT returned `success=false`, `Invalid Token` for the working anonymous token. Adding the same bearer header did not change it. Its source caller normally bridges a user access_token into a legacy session; the precise reason for rejecting the anonymous token is not proven.
- Legacy getLiveClassData returned 404 `{}` for real class 200031333. XHR header and anonymous-cookie controls did not change that class's result. Changing only the class ID to the prior browser-verified 200031716 returned 21 rows with no credentials. Public access is demonstrated for the control, while the internal reason for the first class's 404 remains unknown. This is not a complete class-detail fallback.
- GetAssocaitedHorsesAndPeople reached its diagnostic 25-second deadline without an HTTP response. Authentication cause and endpoint availability remain unverified; no unchanged retry loop ran.
- IsLoggedIn POST returned `success=false` with a new anonymous session cookie. The older GET isLoggedIn redirected to a different HTTP site; that redirect was not followed. Neither inspected response issued a token.

No login/password/OTP/reset-link input was invented. The four selected account flows were traced through their actual request and response consumers. Seven additional token/session/refresh-named routes were checked in source: bank ACH balance refresh, mobile-session clearing and request/agreement/invitation link actions are separate business operations. They were not invoked.

## Scope and acceptance

Forty route records have findings: 29 route patterns received local HTTP diagnostics and 11 have explicit source-contract/input/action findings. Forty-three sequential requests ran with at least one second between requests; one timed out. Two exploratory requests are excluded from useful-data proof: the mistaken class ID used for a division-results path and a noncanonical `div_type=all` filter. Both were followed by source-correct inputs. The valid division ID came from the actual jumper-results listing and matched the detail response.

The current default show was independently matched between `/shows` and WEF Shows: 200000078, selected date November 11. That dated schedule contained nine rings and zero classes. The class sample was selected from this show's actual `/classes` directory using entry_count>0; its class date is November 13. The valid sample does not establish today's full collector coverage.

Readback matched all 40 endpoint updates across 361 submitted fields, plus the related task updates. `OPP-003` records the completed discovery pass. Class collection, response-token implementation and class-coverage work remain open/paused in their existing tasks. No production code/configuration, stored credentials, connection, pause, URLs, schedules, collectors or business records changed.

## Evidence

- `probes-redacted.json`: actual request labels, methods, URLs, profiles, credential presence only, HTTP/body metadata, redacted errors, cookie names and token claims without values.
- `request-profiles.json`: exact nonsensitive diagnostic headers; no Cookie or Authorization values.
- `decoded-summary.json`, `token-handling-proof.json`: useful-data and token-adoption controls.
- `findings.json`: all 40 route findings and next checks.
- `airtable-verification.json`: saved readback result and scope.
- `browser-decode-baseline.mjs`: existing browser decoder, with only its CommonJS export adapted for the diagnostic tool. Decoder logic and salt remain unchanged; no installed source was edited.

Current owner direction: no implementation changes until endpoint use, authentication requirements and payload overlap are understood. Evaluate iphone feeds first and retain the proxy schedule/class routes for missing data or fallback. Documented token handling is the baseline; this is not a new token-mechanism investigation.

## Actual payload examples

The `payload-example` Airtable field and `payload-examples.md` now show captured raw/decoded responses with explicit redactions and array omissions. Forty records were saved and read back: 27 have body/HTML/error payload examples, one has binary Excel response metadata only, one records a local timeout with no server response, and 11 explain why source-contract inspection produced no server payload. No new SGL requests were made to prepare these examples. Source-only, timeout and binary cases do not have invented JSON data.

The API class sample is 200031333 and the working iphone trip sample is 200031716. The readable examples identify available field names and the difference between an entry number, FEI identifiers and the internal IDs present in the decoded API sample. They do not establish same-class value equivalence or complete replacement coverage. `payload-examples-proof.json` verifies retained JSON fields/values against the captured responses; `payload-airtable-verification.json` records saved readback.

Next decision: agree the actual iphone-first field/coverage comparison before any implementation change. Do not add proxy routes, header profiles or collector changes on the strength of these examples.
