# Proactive data and immediate interaction — consolidated UX draft

Version 1.0 · 3 November 2026 · Draft for wef-publish-coordinator, rs-recognize-coordinator and rs-profile-coordinator. Includes both coordinators' read-only findings received today.

This is the requested full behavior draft, not an implementation or deployment authorization. It joins the recovered owner decisions to the current simple User/Team model. It replaces neither the existing widget nor its identity, permission or save implementation. The smaller [field/source comparison](ui-data-handling-map-20261102.md) remains the implementation-gap reference.

## 1. The intended experience

The person should not wait for a server response or a success message before continuing. Prepare likely data while they are already using the site. When they open Profile or a prepared view, display the device-held data. When they Save or Submit, keep their change on the device, update the display, end the edit interaction and let them continue. Airtable confirmation happens quietly behind that interaction.

The preparation is ordinary application work using existing recognition, device storage and endpoints. There is no new service, background agent, scheduler, webhook dependency or separate synchronization platform. Keep the current small, phone-first UI, also usable on desktop.

"Zero wait" is an interaction requirement: the UI must not wait on network completion for work that can be prepared or retained locally. A saved-record confirmation remains truthful. First-ever visits with no identity or device data use the existing entry flow; the application must not invent private data to fill that gap. Tests must distinguish an unprepared first visit from a returning, prepared User.

## 2. Which discussions establish this

| Date / original discussion | Decision carried into this draft |
| --- | --- |
| 9 October, rs-skills: WEC packing, app-shell storage and WEC comments references | Separate recognition state, profile data, show-week data and unsaved input; use the working examples to establish behavior. |
| 9 October: Profile retention, optimistic UI and dirty state; subsequent "lock this" | Keep Profile details available on the device and refresh them. Show changes immediately; retain pending input through reload; protect it from refresh. The owner reviewed and locked the proposed plan. |
| 14 October: trace editable WEC packing to Airtable; compare HPS and session examples | Reuse the existing inline edit/Save/API/saved-record flow and its actual user interaction. |
| 14 October: proactive input, Cloudflare endpoints, opt-ins, forms, comments and check-ins | The immediate-input principle applies across these consumers. It does not require building every consumer now. |
| 15 October: complete proactive-save clarification | Save/Submit updates device and screen, ends editing, and lets the person continue while Airtable updates quietly. No whole-page/form/User lock or blocking handshake. |
| 29 October and 2 November: independent User and current Team model | Recognize does not require Team membership. Keep independent Users, explicit invitations, the current one-Team rule and existing access checks. Do not import the older Barn-required identity model from the October 15 handoff. |
| 3 November: current owner instruction | Be proactive: silently recognize and prepare needed data before the person requests it. Draft the known use patterns and keep the solution simple. |

Primary source chat: `rs-skills`, `01a11632-62fd-73b1-997e-8be4119185de`. Original history: `C:/Users/gombc/.codex/sessions/2026/10/07/rollout-2026-10-07T07-49-23-01a11632-62fd-73b1-997e-8be4119185de.jsonl`, relevant lines 19273–19489, 37351, 39341, 39395 and 52306. The explicit lock follows the proposed plan; it is not inferred from an assistant summary.

Supporting records: [shared data behavior](../coordination/shared-data-behavior-contract.md), [15 October interaction clarification](../coordination/recognize-tenants-sol-medium-handoff-20261015.md#2-locked-workflow-and-terminology), [blocking-behavior cause review](../coordination/tenants-cause-audit-20261015.md#2a-owner-correction-measure-user-wait-separately-from-server-completion), and [current Profile ownership/model](README.md). The October 16 LP/Cloudflare instruction relates to recognition/sign-out and does not select a new Profile save design. Historical WEC commenting cookie durations and the caption app's seven-day storage rule do not set Profile retention.

## 3. The simple responsibility split

| Responsibility | Owner and existing path |
| --- | --- |
| Identify the visitor silently; sign in/out; signal identity change | rs-recognize-coordinator, existing Recognize widget/adapter and server validation. |
| Resolve the correct User, permitted Team data and canonical saves | rs-profile-coordinator, existing `/test/rs-profile/data` and `profiles` Airtable base. |
| Prepare data early and make the widget use local state immediately | wef-publish-coordinator with wef-profile-prototype and rs-astro, within the existing widget/client/service. |
| Public schedule data | The separate public Cloudflare endpoint lane and wef-schedule-app. Public schedule preparation does not depend on signing in. |

Recognize supplies the identity. Profile decides which application records may be returned or changed. The UI uses the resulting permitted state. Neither a cached UID nor an optimistic membership change creates permission.

## 4. Prepare data before the click

On designated RingStatus pages, start the existing silent-recognition work early without delaying the public page. Reuse its result; do not introduce another recognition request for every panel, field or click. A public page remains usable for an unknown visitor. Silent preparation must not open Profile, redirect the visitor or close a Sign in form they deliberately opened.

As soon as the existing identity/access flow permits it, start loading the current User's small Profile response. Do this while the person is reading the page, not for the first time after they press Profile. Keep that permitted state available to the widget and on the device. Prepare the existing authorized Team/member and invitation data included in that response; do not load whole Airtable bases or unrelated Users.

The same prepared response should serve Profile home, the person's editor, Team summary and authorized member lists. Search, sorting, expanding a panel and moving back use these local records. They should not each start another data request. Existing photo assets may load quietly with the prepared Profile so the avatar is ready too; no new image-processing system is proposed.

The Recognize coordinator found that the current public silent refresh does not expose a success subscription to Profile; the existing `onRecognized` callback concerns credential submission. Profile then asks for identity again. Therefore this draft requires a small handoff of the already-resolved result through the existing integration boundary. It does not claim that duplicate-free silent preloading already exists. Keep that handoff inside the current widgets/services; no general event system is proposed. Persist the permitted prepared response in the existing device storage because an in-memory response alone is lost when the person navigates to `/profile`.

When the person opens a prepared panel, render local state immediately. A background refresh may reconcile unchanged information without reopening or resetting that panel. Preparation should be shared by the widget's consumers so a second control reuses the work already underway.

This is a preparation pattern, not an instruction to add polling. Use existing page, recognition, data-load and successful-save events. No numerical polling interval, cache TTL or new refresh schedule is selected by this draft.

## 5. Silent recognition and private data

Returning eligible Users should not repeatedly sign in. Use the existing remembered identity and silent-recognition behavior. An independent User can have a Profile without a Team. Team role, User level, tags and identity remain different things.

The preload starts only at the point permitted by the existing identity/access rules. Both coordinators confirm that the current Profile server requires credential confirmation for an IP-only recognition result. Use the existing confirmation control when needed; do not build a second sign-in mechanism to support preloading. A changed recognized icon is not the required Profile access confirmation.

Local data belongs to the identified User and the relevant Team scope. If the identity changes or the person signs out, immediately remove the previous person's private state from view and stop its in-flight responses from repopulating the screen. Retain unsaved work under its original User; never submit it as the next User. A sign-out or switch must not clear unrelated drafts as a shortcut.

Keep the existing access checks for server reads and writes. Proactive loading moves permitted work earlier; it does not enlarge access or renew permission because a local cache exists. The exact eligibility of private cached display during unavailable recognition must come from the current approved owner rule, not a newly invented expiry or permission envelope.

Live RingStatus pages can use their existing same-origin state. External ChatGPT/Sites prototypes have separate origins and storage. A cache prepared on ringstatus.com is not automatically available to another origin. The schedule remains public; integrating private Profile data into an external prototype requires an explicitly supported connection. Do not redesign identity or share private caches across sites as part of this UX draft.

## 6. What is kept on the device

Keep only the state needed to make the current experience immediate:

| Device-held information | Purpose |
| --- | --- |
| Last confirmed, permitted Profile response for the User | Immediate returning display and quiet refresh. |
| Current local values and pending changes, scoped to their User/record | Immediate feedback and recovery through navigation, reload or weak service. |
| Existing operation identity/revision information | Resume the same save without duplicating or overwriting it. |
| Existing small view preferences | Preserve current hide/show and interaction behavior where already supported. |

Reuse the existing browser persistence boundary and its actor isolation. Keep pending and confirmed state distinguishable without making the person manage a queue. Do not add another storage layer merely because an earlier reference used a different browser API. A storage implementation change would require evidence that the existing boundary cannot meet the requirement and separate authorization.

Profile details are retained separately from show/date data and refreshed when appropriate. A show change does not discard Profile details or unsaved input. Do not borrow another app's seven-day limit or promise permanent browser storage. No new numeric retention period is selected here.

PINs remain excluded from persistent pending storage, matching the current protected behavior. The UI must never return or cache the actual saved PIN as a normal Profile field. Retrying a PIN change after reload uses the existing re-entry requirement. Do not store cookies/tokens inside a general Profile cache or expose backend credentials to the browser.

## 7. Save means the person can continue immediately

For an ordinary field edit, follow the existing Save/Submit interaction:

1. Check the input locally using the existing field rules.
2. Preserve the valid requested value and its pending operation on the device, with the credential exception above.
3. Update the visible Profile, close the editor and return normal focus immediately.
4. Let the person open another panel, edit another value, search, sort or navigate.
5. Send the existing save request and reconcile its result quietly.

Steps 2–4 do not wait for Airtable, a webhook, history logging, a success modal or a complete reload. A spinner or disabled whole form does not satisfy the requirement merely because some text changed first.

Use the current Saving/Saved/Retry feedback quietly. The person is not required to watch or acknowledge it. A locally retained change is pending, not Airtable-confirmed. Only the existing confirmed-save result may establish Saved. If local retention itself fails, keep the value visible in memory and report the existing functional problem honestly; do not falsely promise recovery after reload.

Keep the working request IDs, revisions and retry safety. Unrelated records/actions must not be blocked by one pending operation. Multiple edits to the same Profile can remain usable locally while their server writes are reconciled in the correct order. Do not submit stale whole-Profile values concurrently or erase a newer local edit when an older request finishes. Use the proven WEC action/record pattern at the applicable boundary, not a new synchronization framework.

Once a server response arrives, merge only the data confirmed for that operation. Preserve newer or unrelated dirty values. Clear only the pending operation/version that was actually confirmed. If the request reached Airtable but its reply was lost, recover/retry that same operation using existing evidence; do not create another Team, invitation or person.

## 8. Known user patterns to serve

These patterns come from the existing widget, approved User/Team controls and recovered UX discussion. They describe expected actions, not measured usage analytics.

| Pattern | Prepare in advance | Immediate experience and background work |
| --- | --- | --- |
| Return to a public RingStatus page | Existing silent identity result; permitted own Profile data once available. | Browse normally. Profile does not make the person sign in again or open a panel unexpectedly. |
| Open Profile after browsing | Own name, photo, tags, level, current Team summary. | Home panel renders from prepared state; refresh is quiet. |
| Open Profile immediately after arriving | Reuse the earliest existing preparation already started. | Use eligible local data when available. A genuinely cold/unknown visit uses the existing entry/loading behavior; do not manufacture saved data. |
| Open, edit and Save a name/email/tag | Current field value and existing field rules. | Save changes the display and closes editing immediately; canonical persistence follows quietly. |
| Edit a second value while the first saves | Current local Profile and retained first operation. | Second editing remains usable. Ordered same-record persistence protects both changes. |
| Open Team and return to Profile | Existing permitted Team response. | Navigation uses local state, preserving pending edits. No new fetch for each panel. |
| Search or sort Team members | Current authorized roster, names and existing sort values. | Results update locally as the person types or sorts. |
| Owner enables/disables a member | Exact membership target, current status and revision. | Reflect the requested state locally and let the Owner continue. Server authority and final status remain enforced. |
| View/respond to an invitation | Current permitted invitation, Team name and revision. | Retain the choice immediately and continue. Membership/access changes become authoritative only after the existing server confirmation. |
| Preferred User creates a Team | Existing User level and one-Team eligibility. | Keep the entered name and action locally. A lost reply resumes the same creation; no duplicate Team. |
| Change/upload/remove a photo | Current photo and existing allowed-file rules. | Preview immediately; preserve the pending change while the existing upload/save runs. Confirmed attachment replaces the preview without dropping a newer change. |
| Change phone or PIN | Current own Profile, existing credential validation/helper. | End ordinary editing without waiting for unrelated work. Never use an unconfirmed credential as authentication; keep current credential authority and PIN-persistence protection. |
| Close/reopen or reload during a save | User-scoped local values and the exact pending operation. | Restore the requested values and continue; avoid duplicate submission. |
| Service becomes weak or fails | Prepared permitted data and retained input. | Available views and valid local editing remain useful. Preserve failed work for the existing safe retry path. |
| Return later / another confirmed change exists | Last eligible device state plus current server revision on refresh. | Refresh unchanged values; preserve local dirty values and surface a real conflict without silently choosing a winner. |
| Sign out or another User signs in | Existing identity-change handling and separate User storage. | Old private display disappears immediately; late responses and old pending work cannot cross into the next User. |
| Open Schedules or Opt-ins | Current destination links and each application's own permitted data preparation. | Reuse the destination's established endpoint/consent behavior. Do not treat navigation as consent or assume cross-origin caches are shared. |

Search/sort, hide/show, close/back and panel navigation keep their existing visual design and accessibility behavior. This draft adds no new panels, badges, confirmation stages or explanatory banners.

## 9. Weak service, refresh and returning use

The device is useful because it already has data and pending input. A fresh endpoint read must not be required for every action. Refresh confirmed state quietly while leaving the visible local state and active interaction intact.

If a save fails, retain the requested input and the same operation. Let unrelated actions continue. Retry through the existing application lifecycle and safe retry mechanism while the application can run. Do not promise execution while the browser is closed, and do not add a service worker or scheduled job just to make that promise.

Do not reset a panel, scroll position, open editor or new local value just because an older response returns. A permission or identity rejection is handled through the existing access path; it cannot be hidden by optimistic UI. The User's unsaved work remains retained under the original identity for an authorized return.

Changing show/date concerns the schedule cache. It does not reset Profile or Recognize. Changing User concerns private visibility and pending-operation ownership. These are separate, small existing responsibilities.

## 10. What FINISH must demonstrate

The acceptance test should deliberately hold network responses so a fast server cannot conceal blocking behavior:

- Returning prepared User: Profile opens from device state while the refresh response remains unanswered.
- Public-page silent recognition: public content remains usable; preparation causes no redirect, automatic Profile opening or interruption of an intentionally opened Sign in form.
- Save: device-held and visible data change and the editor closes before the response is released.
- Continue: another panel/record can be opened and edited while that save remains unanswered.
- Same Profile: a later local edit survives the earlier response, and both final saved values are correct after reconciliation.
- Reload/interruption: the same User recovers the exact pending operation; no duplicate Team, membership, invitation or save is created.
- Failure/retry: input remains available and Retry does not start a different operation for the same uncertain outcome.
- Photo: preview remains immediate; confirmation, unchanged attachments and removal preserve the intended image.
- Identity change: private state disappears on sign-out; stale responses and prior User edits do not cross accounts. PIN remains absent from persistent storage.
- Existing browser behavior: mobile/desktop, search/sort, close/back, User/Team permissions and current design remain intact.

Record user wait separately from background completion. Airtable may take time; the person's next permitted action must not depend on that time. Check the saved-record result separately so immediate display is not mistaken for successful persistence.

## 11. Review and implementation boundary

rs-recognize-coordinator reviews the existing recognition event, current credential/eligibility boundaries and earliest permitted preload point. rs-profile-coordinator reviews the current response, local-state/save behavior and permitted User/Team data. wef-publish-coordinator reconciles their concrete findings into this draft and owns the widget acceptance handoff.

Both coordinators returned their read-only findings on 3 November. They agree with immediate local Save/Submit, ending the edit interaction, preserving pending input and permitting the next action. Their concrete findings are:

The full v1.0 file was then sent to both existing chats. Both explicitly confirmed reading it completely and reported no concrete contradiction: Recognize turn `01a1a184-7acf-7322-b93d-6673d0443c75`; Profile turn `01a1a184-7d26-7b02-bdc0-9fde5bb44fd3`. This records completed delivery/read acknowledgment, not product implementation approval.

| Present source behavior | Work required for this UX |
| --- | --- |
| Public silent recognition has no completed Profile-preload handoff; Profile can ask for identity again. | Reuse the resolved eligible identity through the existing integration boundary and start the single permitted Profile preparation earlier. |
| Profile waits for its GET and has no confirmed-response device snapshot. | Reuse existing actor-scoped device storage for the prepared permitted response, including navigation to Profile. |
| Editor closes after the save response; one pending payload blocks every different payload for that User. | Immediate local update/close; independent record/action handling; ordered same-record writes without blocking local editing. |
| Whole returned state can replace local state; save response checks actor but lacks the repeated generation check. | Preserve newer dirty values and reject superseded responses in the existing service. |
| PIN is stripped before persistent storage. | Preserve this exception and existing re-entry behavior. |

The Profile coordinator inspected source `fce024154` and its previously verified deployment evidence; it did not recheck today's deployment. These findings are source evidence, not newly performed live saves. The draft does not use the earlier five-page visual acceptance as proof of proactive data preparation.

One access-policy point is not established by the recovered decisions: when private cached Profile data may be displayed before renewed server authorization after restart or while recognition is unavailable. This draft retains current access restrictions and does not authorize that bypass or invent a cache permission lifetime. Ordinary proactive preparation after the existing checks, immediate editing, safe retry and public Schedule preparation remain separate executable design work. If a broader offline private-display policy is wanted, the owner must settle that exact point before it is implemented.

This is one joined UX specification across the existing responsibilities. It does not reopen unrelated collectors, move canonical tables, select a new framework or authorize changes to code, data, credentials, runtime or deployment. Implementation is the smallest approved delta to existing code after the owners reconcile the current boundaries.
