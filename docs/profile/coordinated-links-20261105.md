# Coordinated Profile / Schedule links — 5 November 2026

Authorized by owner: ask wef-schedule-app and coordinate both links. No styling, identity, data-contract or Schedule behavior rewrite.

Schedule owner: wef-schedule-app, thread01a18df3-c380-7f11-afe6-057e10f86308. Release61b1f899 changed only obsolete external Profile link to /profile target=_top. Deployment b8492963-c719-4e0d-aa0e-00608c384aeb reported success; actual Back click verified by Schedule owner desktop and393.

Profile release309ac5f8 changed only three Schedule href values (launcher and Agent links), using the same conditional target already present. Live served app.js includes the correction.

- Standalone staging Profile: /wef target=_top. Actual click reached https://ringstatus.com/wef.
- Standalone staging Schedule Back: /profile target=_top. Schedule owner verified actual return to recognized V3. Their proof: .git/ringstatus-control/wef-v3-shared-components-20261105/back-navigation-verification.json in approved main checkout.
- Website overlay: /test/wef-schedule-v2/schedule.html target=_self. Actual Schedule rows appeared.
- Overlay Back: /test/rs-profile-v3/?website=1 target=_self. Actual return restored recognized launcher; Profile opened. Top-level URL remained https://ringstatus.com/ throughout.
- The obsolete ringstatus-profile-widget.sportdog.chatgpt.site is not a return destination.
- Back reloads Profile using existing device data; mounted Profile DOM is not preserved. No new message contract added.

Browser screenshot capture timed out; no live image capture claimed. Actual clicks, link targets, URLs and visible Profile controls supplied the verification.
Rollback Profile delta: revert309ac5f8 only; preserve Schedule owner's changes and unrelated work.
