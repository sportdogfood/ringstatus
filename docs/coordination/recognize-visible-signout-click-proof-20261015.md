# Visible sign-out click verification

Verification-only window: 2026-10-16 01:36:37–01:41:37 UTC (15 October, 9:36:37–9:41:37 PM America/New_York). Five minutes maximum, no extension. No code/deployment, direct navigation to the sign-out href, direct API calls, injected clicks, or browser-state manipulation.

Before: existing controllable in-app browser displayed the assigned test Barn and the separate User's existing profile `TEST — Tenants User 1 2026-10-15 1014 edited`. Visible link: `Not you? Sign out`. Restored empty display name remained visible. No profile editing or saving performed in this verification.

RESULT: FAIL at visible click/navigation gate. Used the supported normal Playwright role-locator click on the visible exact-name link, inside expectNavigation. The tool returned without an explicit interaction error. Immediate inspection and another inspection after 10 seconds both showed the unchanged `/rs-barn-onboarding-setup-v24` URL and same User profile. No sign-in form appeared. Screenshot captured the unchanged rendered page and visible link.

Stopped after this one actual click, without repeating it or substituting direct href navigation. Cleared-recognition and post-sign-out reload gates were not reached and are not passed. This observation does not establish whether any hidden server-side retirement occurred; no such claim is made. No record edits, code changes, deployment, injected actions, direct API calls, cookie/storage changes or owner troubleshooting.
