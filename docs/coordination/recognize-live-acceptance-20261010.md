# Recognize published acceptance checklist

Existing task `recEOLQ5QiYvVKQot`; coordinator `reca7j2MgPfqxgoiv`, actual chat `01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5`. Preparation only. No scenario below has been run live in this assignment.

Before execution: owner authorizes coordinated Cloud deployment, named Webflow publication, write-producing visitor tests and any SMS sending. Use only approved recipient `16318752160`; original browser enters the code, including desktop. Record the exact deployed source/ID, published page scripts, device/browser/viewport and timestamps before testing. Check Agents reservations and exclude sibling unfinished changes. Do not treat local tests or read-only HTML GETs as this acceptance.

| Case | Required observation | Evidence to record | Current state |
|---|---|---|---|
| Known and unknown public visitor, each of `/`, `/ring-status`, `/about-ringstatus` | Same page remains; no added overlay/UI/navigation | URL/UI before and after; loaded native public loader; server result and permitted record readback | Not executed |
| Known `/rs-recognize` | Silent route to `/rs-member`; no redirect loop | Original and resulting URL, cookie response attributes, recognized person/device | Not executed |
| Unknown gated desktop and mobile | Existing native phone/code UI; code entered in original browser; same browser recognized | Device/browser identity, code-flow stages without exposing code, cookie after check | Not executed |
| Refresh/navigation/silent return | Session reused appropriately; server revalidates; activity current | Existing session UID, summary identity/status/time and correlated footprint IDs | Not executed |
| Invalid, expired and reused OTP | Rejected without binding browser; usable retry/request flow | Result code/state, cookie/identity unchanged, no duplicate mutation | Not executed |
| Revoked/retired recognition | Server rejects, cookie/browser state cleared; lifecycle reflects actual evidence | Prior device binding, server rejection, state cleanup, existing session lifecycle | Not executed |
| Account switch | Prior session cannot be rebound to another identity; stale browser context cleared | Old/new person/device/session IDs and expected separation | Not executed |
| Retries/concurrent requests/uncertain external write | No duplicate summary or footprint; no stale activity regression; unresolved writes fail closed | Request IDs, durable ownership status and exact record counts/results | Not executed |
| Existing Inputs baseline | Existing authorized setup/alerts behavior and access checks intact | Named unchanged flow, permitted before/after records; no migration inferred | Not executed |

For each executed case use PASS/FAIL/BLOCKED with the specific observed criterion. Record a failed approved workflow once and stop that execution path; no substitute commands, record repair, repeated attempts to force state change, or patches after failed verification without separate APPROVED TO EDIT. Continue independent authorized work. Redact OTPs, secrets and unnecessary private values from reports.

Current dependencies: public payload awaits required page-update confirmation; Cloud release/publication/live tests are not authorized; Sessions summary implementation/proof remains unfinished. Owner performs only the pending authorization/availability steps; coordinator owns the scoped execution/evidence once authorized. No new runner, provider, database, page gate or launcher destination is introduced.
