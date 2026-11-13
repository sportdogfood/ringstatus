# Profile location contract

Candidate correction, 2026-11-13. Not deployment proof.

- Recognize, minimum Profile display, Schedule and the existing eight-minute data refresh are unchanged. Location work runs separately after Profile hydration, plus explicit permission changes. No continuous location tracking or new timer.
- GPS is preferred only after the person opts in and browser permission permits it. Returning opted-in users with granted browser permission can supply a fresh position without a new prompt. Otherwise the existing Cloudflare/GeoJS approximate lookup is used.
- Existing same-origin Astro POST `/test/rs-profile/data?view=geoip` validates recognition and actor scope. GET reads the last confirmed location from the log; it does not collect an unlogged position. No client-supplied target person or check-in target is accepted.
- Every submitted observation gets a stable request ID. Pending requests stay in `rs_profile_location_pending_v1:<user>` until Astro confirms the Airtable log and latest projection. On a later load/request they replay in order under that same authenticated user, including after credential-token rotation. They cannot submit as another user. The existing save journal/readback prevents duplicate logs after lost acknowledgements. No hidden background runner is introduced.
- Airtable `profiles` / `events` (`app7BXcw801n19QKF` / `tbluecPFIorOytASP`) is the authoritative append-only log. Each entry identifies actor, Profile record, request ID, observation time, location source, coordinates and available browser accuracy in `result_json`. Unavailable-location attempts are also logged. No raw IP or credential is logged.
- Last location is selected from confirmed location logs, ordered by observation time. `profiles` / `people` stores only the derived latest `location_latitude`, `location_longitude`, `location_source`, `location_observed_at`. These fields are a projection, not a second authority. Profile revision/roles/identity are unchanged.
- `rs_profile_geoip_v1:<user>` caches the confirmed last log for that device/user; it never confirms a pending save. Turning precision off removes its precise cached copy and requests approximate fallback. Historic consented log entries remain history.
- Current log input method is `automatic`, target is null. Future button and QR inputs must use the same log with their method and a validated target. They are not enabled by this correction and do not grant observation rights.

## Distance and ring qualification test

`test-support/location-walk-estimate.mjs` is a test-only helper, not deployed UI or an observation authorization route. It computes straight-line Haversine distance and divides by an explicitly adjustable walking speed (test default 1.4 metres/second). No map/routing dependency is added. Fences, actual paths and walking conditions are not represented.

Owner-supplied WEF test targets, verified in Profiles/w-rings: Ring 6 (26.634806, -80.274528) and Rost (26.635028, -80.273806). Tests use a labelled simulated device fix at Ring 6: approximately 76 metres to Rost, or 0.9 minutes at 1.4 m/s. The simulated fix qualifies at Ring 6 and not Rost. Synthetic mathematical edge cases remain unit tests; no simulated fix is submitted as a real user observation.

Qualification tests require fresh GPS with a known accuracy radius. Distance plus reported accuracy must fit inside the supplied feet threshold. GeoIP, stale GPS and uncertain boundary cases do not qualify. Owner requested a conservative test: 100 feet and a reading no older than 60 seconds. This test policy is not an enabled production observation gate. Physical presence cannot be guaranteed from browser-supplied coordinates alone. Server-side observation authorization and actual Ring 5 coordinates remain outside the enabled implementation.

References: https://turfjs.org/docs/api/distance and https://www.w3.org/TR/geolocation/ .

## Rollback

Revert only this location correction commit, preserving Schedule release 64c83aa1 and other owners' subsequent work, then publish through the existing Astro branch route after approval. Keep the four added fields and historical logs; rollback must not delete recorded observations. The current browser-only release does not populate the new fields. Pending requests must be preserved across rollback for later same-ID replay.
