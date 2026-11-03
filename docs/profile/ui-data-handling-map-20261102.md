# Profile widget: data and UI behavior

2 November 2026. Requested mapping and source comparison only; no application, data or deployment changes.

Use the existing Profile widget, actor-scoped device storage and `/test/rs-profile/data`. Preserve the current design, permissions, User/Team rules and working saves. No new backend, endpoint, storage service, scheduler or workflow is proposed.

## Recovered decisions

- **9 October, rs-skills:** owner distinguishes Profile retention from show-week data, requests persistent optimistic UI/dirty state, and then says "lock this" after reviewing the device-cache/retry plan. Original chat `01a11632-62fd-73b1-997e-8be4119185de`, source-history lines 19425–19489.
- **14 October, same chat:** trace WEC packing inline edit/Save to Airtable; compare HPS and session examples; extend proactive input handling to opt-ins, forms, comments and check-ins. Original messages at source-history lines 37351, 39341 and 39395.
- **15 October, same chat:** after Save/Submit, update device and screen immediately; let the person continue while Airtable updates quietly. Reuse working methods; keep this small and phone-first. Original message at source-history line 52306; [maintained handoff, lines 41–44](../coordination/recognize-tenants-sol-medium-handoff-20261015.md#2-locked-workflow-and-terminology).
- [Cause review](../coordination/tenants-cause-audit-20261015.md#2a-owner-correction-measure-user-wait-separately-from-server-completion): an immediate row update is insufficient if the next interaction is still blocked. Protect the affected operation without locking unrelated edits.
- [Shared behavior record](../coordination/shared-data-behavior-contract.md): retain pending edits, protect newer changes, separate Users, and confirm Saved only after canonical confirmation. Read it with the later corrections above.

Original rs-skills history: `C:/Users/gombc/.codex/sessions/2026/10/07/rollout-2026-10-07T07-49-23-01a11632-62fd-73b1-997e-8be4119185de.jsonl`. The 16 October LP/Cloudflare instruction concerns recognition/sign-out; it is not used here to select Profile save mechanics. WEC comments/Waze are historical comparisons, not authority to copy their cookie lifetimes into Profile.

## Data map

Source base **profiles** `app7BXcw801n19QKF`; identity source **recognize** `app9kOZdIaGyKk5uG`.

| Widget data/action | Existing source or destination |
| --- | --- |
| User identity | Recognize `people.person_uid` joins exactly one profiles `people.recognition_person_uid`. |
| First/last name; phone/PIN changes | Existing Recognize-owned fields and credential helper. PIN is set-only, not returned. |
| Email, photo, tags, User level | profiles `people.email`, `profile_photo` (`profile-img` fallback), `tags`, `user_level`. Level remains read-only. |
| Team and members | Existing `tenants`, `tenant-memberships.Person/Tenant/Roles/Status`, and `roles`. |
| Invitations | Existing `team-invitations` and its revision/request fields. |
| Hide/search/sort | Existing browser view state; hiding is not a membership change. |

## What the person should experience

Inspected source: `C:/Users/gombc/.codex/worktrees/widget-prototype-parity-20261102`, commit `46296cd97ee286d3a8f961e8afe637487d48dd3d`. Paths below are relative to its `webflow-cloud-test` directory. This is source comparison, not a new live save test.

| Interaction | Agreed behavior | Source observation / smallest correction to prepare |
| --- | --- | --- |
| Open Profile | Show available device-held Profile data immediately, then refresh. | `service.js:11–23` waits for GET before returning state; existing device storage retains pending operations and hidden preferences, not a last-known Profile response. Reuse that storage for actor-scoped confirmed display data; retain access checks. |
| Save a field | Update visible/device state and close the editor immediately; continue using the widget. | `client.js:150` closes the editor and updates state only after `await saveProfile`. Move the existing local update/editor close before the request; keep canonical confirmation separate. |
| Make another edit while saving | Unrelated work continues; protect the affected operation from duplicate or conflicting submission. | `service.js:32` rejects every different payload while one pending operation exists. Reuse the proven action/record-scoped pending pattern instead of a User-wide lock. Same-Profile saves carry multiple fields and one revision: retain newer local edits without concurrently submitting stale versions of that record. |
| Navigate/reload or lose a reply | Keep the pending edit and reuse its operation on retry. | Existing actor-scoped pending storage/request ID is useful and must be preserved. The PIN is intentionally excluded from device persistence and requires re-entry. Do not clear other drafts to get past an unresolved save. |
| Refresh or receive an older reply | Preserve newer local changes. | Prepare merging that updates only confirmed/unchanged values; do not replace the entire active state with an older response. Exact behavior needs the focused delayed-response check below. |
| Confirmation/failure | Quiet Saving/Saved/Retry; no acknowledgment required to continue. | Keep existing endpoint confirmation. Saved means confirmed, not merely stored on the device. Retain failed input and the same operation. |
| Change User or sign out | Clear the old User's private display; never show/replay it for another User. | Preserve existing actor checks and actor-scoped storage. The save checks generation before sending but only actor identity after receiving; finish the late-response check within the same service, without changing Recognize. |

Independent source review by `rs-astro` (`01a14153-29b7-7a70-a912-cd9db26540c9`) confirmed the three source gaps and that both files match the inspected commit. Its same-record revision, late-response and PIN-persistence findings are incorporated above. No new backend, endpoint, schema, dependency or infrastructure is needed. This review did not run live saves.

## Bounded next action

Prepare only the existing Profile client/service delta needed for the rows above, reusing the WEC/HPS behavior. Application edits remain subject to the owner's existing "no changes yet" boundary; this map does not authorize them.

Verification must hold the save response and show that the editor closes, the requested value remains visible, and another record can be edited. Then verify reload/retry preserves the exact operation, an older response cannot erase a newer edit, account switching stays isolated, and canonical confirmation survives reload. Keep the current widget design and User/Team behavior intact. No new testing infrastructure is required.
