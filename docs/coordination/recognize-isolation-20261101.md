# Recognize isolation — 1 November 2026

Owner approved finishing the existing recognizer and actual live landing-page acceptance in chat 01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5. Baseline: e9b1c453f196f2d86fe62254577eab916ff24641; existing Cloud app d7d97751-20e1-4148-a5cf-ee58671c128a, environment 110f06dd-c1ea-4839-98af-d829cbe77941, mount /test. No dependency, framework, runtime or database-schema changes.

## Boundary

Recognize owns identity, secure recognition cookies, single-input phone/PIN/recognition Invite, session IDs and background history. Silent recognition uses a valid cookie OR a unique indexed exact edge IP. IP mappings are HMACs learned only from credential-backed identity; shared-IP ambiguity remains unknown. Geography such as city or region is not a person identifier. Existing expiring, one-use invitation and credential collision handling remain.

Recognition does not require Team membership or WEF access. Inputs policy retains the previous private-data eligibility decision outside recognition. A GeoIP-issued cookie supplies identity only and cannot authorize private Inputs. WEF, Profile and Messages must enforce their own access rules; denial must not clear recognition or retire devices.

Cookie and D1 device expiry renew together. A supplied expired/retired cookie cannot fall through to GeoIP. Explicit sign-out retires the current cookie and suppresses GeoIP until explicit sign-in. The widget serializes recognition and credential/retirement requests through the same browser lock, retains uncertain operations, and fences stale display responses.

## Landing integration

Only /rs-recognize (page 6ac459b135ad0c4c3b253d97) is selected for this release. Its obsolete prototype footer/controller is replaced with the existing dark widget presentation and D1 adapter. Silent success stays on the page; explicit sign-in/profile-button activation navigates to /profile only. No Profile implementation, sitewide rollout or change to other demo pages is included. Existing rec2/Inputs consumers and unrelated routes remain protected.

Astro and Profile confirmed no overlapping edit/release reservations. Independent read-only source review: /root/recognize_boundary_review. Four targeted backend scenarios passed, including independent identity/private denial, renewal, unique/shared IP, mapping refresh survival and retirement. The legacy native-controller suite retains the same four obsolete SMS/OTP failures as the exact released baseline; it is not used as a new passing gate. Build passed; final package/live acceptance still pending at this checkpoint.

Evidence and exact before/after page code: approved checkout .git/ringstatus-control/recognize-isolation-20261101/. No credential values belong in this record. Local checks and source review are not live completion.
