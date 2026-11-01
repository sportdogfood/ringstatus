# Recognize isolation — 1 November 2026

Owner approved finishing the existing recognizer and actual live landing-page acceptance in chat 01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5. Baseline: e9b1c453f196f2d86fe62254577eab916ff24641; existing Cloud app d7d97751-20e1-4148-a5cf-ee58671c128a, environment 110f06dd-c1ea-4839-98af-d829cbe77941, mount /test. No dependency, framework, runtime or database-schema changes.

## Boundary

Recognize owns identity, secure recognition cookies, single-input phone/PIN/recognition Invite, session IDs and background history. Silent recognition uses a valid cookie OR a unique indexed exact edge IP. IP mappings are HMACs learned only from credential-backed identity; shared-IP ambiguity remains unknown. Geography such as city or region is not a person identifier. Existing expiring, one-use invitation and credential collision handling remain.

Recognition does not require Team membership or WEF access. Inputs policy retains the previous private-data eligibility decision outside recognition. A GeoIP-issued cookie supplies identity only and cannot authorize private Inputs. WEF, Profile and Messages must enforce their own access rules; denial must not clear recognition or retire devices.

Cookie and D1 device expiry renew together. A supplied expired/retired cookie cannot fall through to GeoIP. Explicit sign-out retires the current cookie and suppresses GeoIP until explicit sign-in. The widget serializes recognition and credential/retirement requests through the same browser lock, retains uncertain operations, and fences stale display responses.

## Current owner-corrected integration

The owner rejected the legacy /rs-recognize target and confirmed the CURRENT /recognize and /profile models as the designs. Own unpublished /rs-recognize changes were restored exactly to their prior saved head/footer. Do not revive that target.

Approved current scope is removal of embedded demo payloads on /, /ring-status, /about-ringstatus, /recognize and /profile, preserving the confirmed designs. Profile must use Recognize first, then exact real User/Team records. All publishing and deployment are prohibited by the current owner instruction. Four Recognize loaders and the Profile loader are saved only; no new live-five-page acceptance is claimed.

Current local Profile connection and required remaining real-record mappings are documented in profile-real-records-20261101.md. Existing unrelated page runtime, visitor code, shared site code and other application consumers remain protected. Backups and evidence: approved checkout .git/ringstatus-control/recognize-model-correction-20261101/.
