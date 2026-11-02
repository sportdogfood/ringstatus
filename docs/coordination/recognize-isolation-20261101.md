# Recognize isolation — 1 November 2026

Owner approved finishing the existing recognizer and actual live landing-page acceptance in chat 01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5. Baseline: e9b1c453f196f2d86fe62254577eab916ff24641; existing Cloud app d7d97751-20e1-4148-a5cf-ee58671c128a, environment 110f06dd-c1ea-4839-98af-d829cbe77941, mount /test. No dependency, framework, runtime or database-schema changes.

## Boundary

Recognize owns identity, secure recognition cookies, single-input phone/PIN/recognition Invite, session IDs and background history. Silent recognition uses a valid cookie OR a unique indexed exact edge IP. IP mappings are HMACs learned only from credential-backed identity; shared-IP ambiguity remains unknown. Geography such as city or region is not a person identifier. Existing expiring, one-use invitation and credential collision handling remain.

Recognition does not require Team membership or WEF access. Inputs policy retains the previous private-data eligibility decision outside recognition. A GeoIP-issued cookie supplies identity only and cannot authorize private Inputs. WEF, Profile and Messages must enforce their own access rules; denial must not clear recognition or retire devices.

Cookie and D1 device expiry renew together. A supplied expired/retired cookie cannot fall through to GeoIP. Explicit sign-out retires the current cookie and suppresses GeoIP until explicit sign-in. The widget serializes recognition and credential/retirement requests through the same browser lock, retains uncertain operations, and fences stale display responses.

## Current ownership cut — 2 November 2026

The owner rejected the legacy /rs-recognize target and confirmed the CURRENT /recognize and /profile models as the designs. Own unpublished /rs-recognize changes were restored exactly to their prior saved head/footer. Do not revive that target.

The owner published the five-page demo cleanup and then directed completion of the prepared app-only release. Commit89abf609f was deployed on 2 November at12:11:26 UTC; Recognize cookie/handoff and downstream existing User/Team reads were verified. Codex did not publish the website. These are dated results, not new release authority.

The owner now assigns this chat to **Recognize only**, ending at stable identity and server-validated trust. Profile loading/editing, User/Team data, membership/roles and consumer permissions belong to rs-profile-coordinator. See [the Profile handoff](rs-profile-coordinator-handoff-20261102.md). This is an ownership/documentation cut; no runtime refactor, schema change or deployment is authorized by it. Existing unrelated page runtime, visitor code, shared site code and other application consumers remain protected. Evidence: approved checkout .git/ringstatus-control/recognize-model-correction-20261101/.
