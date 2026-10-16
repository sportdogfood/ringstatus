# Visible sign-out activation correction

Owner-approved ten-minute correction window: 2026-10-16 02:01:09–02:11:09 UTC (15 October, 10:01:09–10:11:09 PM America/New_York). Scope: actual visible sign-out activation, existing retirement route, live signed-out reload. No authentication replacement, record edits, API-agent changes, static Webflow publishing, or repeated phone/profile proofs.

Baseline: ca94a11fbe839dec709b493690b6fc899770cda0. The generated sign-out anchor had its existing destination but lacked the explicit activation helper used by other native controls. The precise cause of the previously observed default-navigation failure was not established; host interception is a test scenario, not a claimed live diagnosis.

Correction: bind sign-out through the existing native capture-phase click/keyboard helper, navigating to the existing /rs-recognize?sign_out=1 route; remove the generated anchor on disposal. Existing authentication and retirement logic unchanged.

Release: e3722c2b4e2bd457112da3cebe0d036c820ba807, parent ca94a11fbe839dec709b493690b6fc899770cda0, pushed to work/inputs-crm-20261006. Exactly three files: native view, generated bundle, focused browser test. Release assembled with an alternate index from the deployed baseline, preserving unrelated working changes.

Validation: focused real Edge browser test PASS (1/1), application build PASS, independent read-only review PASS and independent execution PASS (1/1). Test uses normal locator click, synthetic bubbling default-prevention, exact existing destination, and unrelated draft retention. Recognition is mocked in this local test; it does not prove live retirement.

Deployment: c0f6c8b5-8ff5-4866-ae85-e459e63eee87. Live deployment and click/reload evidence pending at report creation.

## Live obstruction established and corrected

On e3722c2b at approximately 02:06 UTC, one normal visible role-locator click still left the Barn page unchanged, including after ten seconds. Read-only DOM geometry/hit testing identified the actual obstruction: sign-out anchor rectangle x0/y0, width847.24/height44; its center hit .rs-barn-v24-header before the anchor. The visible text location x50/y22 hit the anchor directly. Screenshot showed the text at upper left with the native header overlapping the rest of the oversized hit area. No retirement or signed-out claim was made from that failed click.

Adjusted the existing anchor width to fit-content, keeping its click area around its label. No auth behavior changed. Targeted test now also models the header obstruction starting at x120 and asserts the control stays narrower than120 before normal click. PASS 1/1; independent review/retest PASS. Test fixture strips external CSS, so actual live styled verification remains required.

Corrected release da2c3cba20a140cfe8a22c1b5ec1183a85a56ca1 (parent e3722c2b), same three files only, pushed approximately02:07:43 UTC. Original activation release deployment c0f6c8b5-8ff5-4866-ae85-e459e63eee87 succeeded02:05:45.611Z. Final width release deployment/live proof pending.

## Final live acceptance — PASS

Final deployment 8659acbd-41ee-4dd7-95bb-6fac55883370 for da2c3cba20a140cfe8a22c1b5ec1183a85a56ca1 succeeded2026-10-16T02:09:03.164Z.

Actual existing IAB test session, final release:
- Reloaded Barn page; assigned test Barn loaded normally. Visible sign-out button width108.423px, height43.998px; read-only elementFromPoint at its center returned the button itself (previously header).
- Used one supported normal getByRole(button, exact name Not you? Sign out).click(), with no force, injected click, direct href navigation, cookie manipulation, or API substitute.
- Browser navigated to /rs-recognize?sign_out=1, then existing retirement flow removed flag and displayed native Welcome back form: Enter your phone number to sign in.
- Reloaded /rs-recognize normally. After initialization, URL remained /rs-recognize; native form displayed Enter your phone number and empty 10-digit phone input. No return to assigned Barn. Screenshot captured the visible signed-out form.

PASS: actual visible click, live sign-in form, signed-out state retained after reload. This closes the sign-out gate only. Earlier phone/profile proofs preserved; no record/role/consent changes, API-agent changes or static publishing. Live PIN/invite checks remain pending as instructed. Finished within the original ten-minute deadline.
