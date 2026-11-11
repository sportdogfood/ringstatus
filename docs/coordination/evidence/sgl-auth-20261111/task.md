# Anonymous auth and refresh investigation — 2026-11-11

Goal: evaluate the current test-now endpoints and missed source-backed anonymous-auth/bootstrap/refresh leads. Tie results to useful schedule/class/live access. Save actual findings in existing Airtable endpoint records and the related opportunity task.

Authority: owner approved diagnostic endpoint checks and findings writes. No production code/configuration changes, installed credential changes, collectors, cadence changes, real-user login/password/OTP operations or business record repairs. Follow source methods/inputs. Source inspection can establish a route's client contract without invoking an account action with invented inputs.

Baseline: 229 inventory routes, existing observations preserved. Source snapshot is the saved main.loaded.js used by Stage 1. Refresh current test-now selections before tests. Fresh anonymous diagnostic state is separate from the installed runner/proxy; no existing tokens or browser login cookies will be printed or replaced.

Checks: distinguish no-auth responses, anonymous-token issuance, session checks, authenticated exchange and password/OTP flows. Inspect HTTP status, content type, body shape, token metadata and cookie names only. HTTP 200/empty data is not authentication proof. Use an actual source-derived show/class/group/entry identity; do not invent path IDs or session inputs. Space requests by at least one second, sequentially. A diagnostic result is not normal collection proof.

Current: discovery pass complete. All 26 selected records and 14 missed leads have actual findings in Airtable; readback matched 40 records / 361 submitted fields. Eleven route patterns issued anonymous tokens without request credentials. Config, class-response and replacement-schedule tokens were replayed successfully. The existing decoder's response-_t rejection was reproduced offline. Source-contract-only account/action findings and unresolved timeout/class-specific behavior remain explicit. See README.md and airtable-verification.json.

Current owner direction: no implementation changes until source use, token/login requirements and payload overlap are understood; evaluate iphone feeds first, with the proxy schedule/class routes for gaps or fallback. Existing documented token handling stays the baseline.

Payload examples are now saved in Airtable and payload-examples.md, with raw versus decoded formats, real identifiers, exact errors, omissions and redactions labeled. All 40 records / 69 submitted fields matched readback. This examples step made no additional SGL requests. Class samples differ, so same-class equivalence remains unverified.

Next action: owner review of the actual payloads and agreement on the iphone-first coverage comparison. No production edit, proxy-route addition or manual collector is authorized by these examples. Class collection and normal scheduled proof remain open in their existing tasks.
