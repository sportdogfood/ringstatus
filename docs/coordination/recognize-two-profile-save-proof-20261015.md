# Two existing profiles: edit/save/reload/restore

Live-verification-only authorization. Start 2026-10-15 23:05:34 UTC; hard deadline 23:15:34 UTC (7:15:34 PM America/New_York). No extension, implementation or deployment. Owner `rec09Fqo8EzuNXHa1` and separate test User `recfl24ktSvsf6R0q` only. Normal profile UI mutations; read-only canonical checks supplement, never replace, actual UI proof.

Original display names and per-account observed results are recorded below. No phone, PIN, role, status, membership, consent, Barn or protected User edits are authorized.

Initial read-only canonical check: owner display name `RingStatus Test`; separate User display name empty/omitted. Both Active, existing phones and recognition links unchanged at start.

Owner temporary value: `TEST Owner save proof 2026-10-15 1906`. Normal Save immediately closed the edit form and displayed the new value in the owner row while status said `Saving` (browser call completed in 1.106 seconds). This is immediate local feedback, not yet server confirmation.

Owner background status subsequently became `Saved`. Canonical readback confirmed the temporary display name while name, phone, status and identity links retained original values. Reload returned the default Horses panel; the Users tab did not switch on that attempt. Normal navigation to the existing `?profile=me` page loaded the actual owner form and displayed the temporary value in its User row. This fresh page load plus canonical readback verifies persistence; the tab issue is not concealed.

Restoration was submitted through the same Display name input and normal Save, restoring `RingStatus Test`; immediate feedback again said `Saving`. Final confirmation/readback follows below.

OWNER PASS: restoration reached `Saved`; reload of the existing own-profile URL showed the `RingStatus Test` row and owner form. Canonical readback restored `RingStatus Test` and all six observed fields exactly matched the initial record. Both temporary edit and restoration used normal UI Save; no retry or direct mutation was used.

User original Display name was empty in the loaded existing User editor, matching initial canonical readback. Temporary value `TEST User save proof 2026-10-15 1909` was entered and normal Save clicked. The first immediate snapshot still showed the editor; no immediate-close claim is made for that snapshot. Confirmation is checked separately.

User follow-up snapshot showed the editor closed, temporary display name in its row and `Saving`; the next confirmation showed `Saved`. Canonical readback contained the temporary display name with unchanged observed name, phone, Active status and identity links. After explicit reload and normal Users-tab navigation, the same temporary display name was visible in the sole User row. Edit → Save → reload is verified for the separate User; restoration follows.

USER PASS: original empty Display name was restored through the existing User editor and normal Save. The row reverted to its original name while `Saving`, then status reached `Saved`. After reload and Users-tab navigation, only the original `TEST — Tenants User 1 2026-10-15 1014 edited` row appeared. Final canonical readback omitted the display-name field again, matching its original empty value.

FINAL: both accounts passed edit → Save → confirmed save → fresh-page/reload verification → restore → confirmed save → reload. Final canonical values for all six observed fields on both records exactly equal the initial snapshot. All four writes were display-name edits through normal UI; no direct write, retry, code edit, deployment, alert/Barn-link work or protected User creation/edit was performed. The owner reload required normal navigation to its already-existing own-profile URL after one Users-tab attempt did not switch. No failure in either save/restore path was observed. This proof does not claim other V1 gates passed.
