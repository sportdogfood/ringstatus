# RingStatus Profiles — ownership and operating guide

Version: **1.0.0**  
Dated: **2 November 2026 — America/New_York**  
Owner: **rs-profile-coordinator**  
Chat: `01a12690-0e9d-71f0-bb85-000d02307e8d`

This README describes Profile's responsibilities, existing implementation, specialist roles and connections. It is documentation, not authorization to run workflows, change code or records, deploy, publish, or activate agents. Current owner instructions remain controlling. Profile implementation remains stopped pending a specific authorized assignment.

## 1. What Profiles does

Profiles is the application that lets an identified User view and manage their own application profile and their permitted Team information. Recognize answers **who is here**. Profile answers **which application records belong to this person, what may they see or change, and was their change saved**.

The coordinator owns the complete Profile outcome: current toolbar and panels, User loading/editing, Team screens, memberships, roles, consumer permissions, save confirmation, downstream compatibility and acceptance evidence. It coordinates the capabilities needed for that outcome without taking over other domains.

An independent User does not need a Team to have a Profile. Team membership is a separate relationship. A Team role is not the same thing as a User level, descriptive tag or recognition credential.

Profile responsibilities include:

- Match the server-validated `person_uid` to exactly one canonical User through `recognition_person_uid`.
- Load the correct User and permitted Team records, using actual record links rather than matching names or selecting the first ambiguous result.
- Apply consumer permissions before returning private data or accepting a change.
- Preserve the approved current Profile design and its visible interaction patterns.
- Save approved User changes through the existing application route and confirm the designated saved records.
- Retain uncertain changes and stable operation identifiers so a lost reply is not silently treated as success or repeated as a new operation.
- Keep another person's data and pending edits out of the current User's screen and saves.
- Maintain useful source, review and acceptance evidence, with the remaining gaps stated honestly.

## 2. What Profiles does not do

Profile does not identify a visitor, build a replacement sign-in system, issue recognition cookies, validate phone/PIN credentials itself, maintain the recognition device index, retire devices, or own recognition sessions and footprints. Those are Recognize responsibilities.

Profile does not grant Team access merely because recognition succeeded. It does not create Team membership from a recognized identity, infer permission from a photo/tag/favorite, or treat a browser-supplied UID as authority.

Profile does not own WEF collection, timing, results, RingWaze serving or Messages delivery. It may supply approved records or permission information through an agreed interface; each receiving domain retains its own rules and acceptance. Editing a Profile does not imply notification consent or activate an alert.

Profile does not automatically create missing Users, Teams, schemas, credentials, storage services or new integrations. It does not repair production records, substitute manual calls for the approved runner, or publish a website because a build passed. A plugin or specialist role does not enlarge authorization.

## 3. The Recognize → Profile handoff

The existing public entry is `https://ringstatus.com/recognize`. Successful credential entry or **Open profile** opens `https://ringstatus.com/profile`. Silent recognition stays on the current public page.

```mermaid
sequenceDiagram
    participant V as Visitor
    participant R as Recognize
    participant P as Profile application
    participant A as Profiles Airtable records
    V->>R: Existing cookie or supported credential
    R-->>V: Recognized identity / unknown / unavailable
    V->>P: Open Profile
    P->>R: Existing server identity validation helper
    R-->>P: person_uid and server-held identity_only
    P->>P: Enforce private access and actor scope
    P->>A: Resolve exact User and permitted Team records
    A-->>P: Canonical records
    P-->>V: Profile and permitted Team views
```

The browser's existing `recognitionAdapter.current()` returns a `person_uid`, null, or an unavailable error. It does **not** return a newly designed trust envelope. Profile's service sends the cookie and expected `X-RS-Actor-Scope` to `/test/rs-profile/data`.

The Profile server independently calls the existing `remembered(request, env)` helper. It rejects missing identity, a changed actor, and `identity_only` access to private Profile data. An IP-only recognized visitor can use the existing Recognize phone/PIN confirmation. Profile does not invent a second credential panel or turn an IP match into permission.

The recognition cookie is `__Host-rs_recognition_device`. Recognize owns its creation, renewal, expiry and retirement. Profile consumes the validated principal; it does not alter recognition state when a Team operation is denied. Sign-out belongs to Recognize; Profile must clear its own private display and actor state when identity changes.

## 4. Information shared with Recognize

| Information | Purpose and ownership |
| --- | --- |
| `person_uid` | Recognize-owned stable identity consumed by Profile. |
| `recognition_person_uid` on the User | Profile-owned link to that identity; exactly one matching User is required. |
| Credential-change coordination | Profile requests an approved phone/PIN change through existing Recognize-owned helpers; the existing refresh path must update the identity index. Credential authority stays with Recognize. |
| `/profile` destination | Shared navigation agreement, not a permission grant. |
| User status/phone, memberships, Team active status and roles | Existing shared refresh reads these for the separate Inputs-access projection. They are downstream permission inputs, not prerequisites for identifying a person. |

Recognize does not need Profile photos, tags, preferences or the entire Team roster to identify someone. Shared refresh dependencies still exist in the working code; an ownership document does not remove them. Interface changes require coordination with the affected owner and one designated writer.

Recognize coordinator: `01a1268e-253f-7fb1-bdc9-e6b6bd3e29a5`. Existing chat tools can read status and, with human authorization, send a scoped message. A message is coordination, not automatic acceptance or execution authority.

## 5. Records and the new Teams table

The existing base was renamed from Tenants to **profiles**. Its stable ID remains `app7BXcw801n19QKF`.

| Table | ID | Current meaning |
| --- | --- | --- |
| people | `tblyTBgrgdhT3Tcg7` | Canonical application User records, identity link and Profile fields. |
| tenants | `tbled1saw01Thffs1` | Existing Team records used by the deployed application. |
| tenant-memberships | `tblFf4aWQYA2RRsWo` | Existing User–Team relationship, status and roles. |
| roles | `tblRqRbjmqmNfJtIP` | Existing role records used by membership permissions. |
| teams | `tblaczX2otacXkA9w` | Newly created at owner request, currently with a Name field. Not connected to the deployed application. |
| code-review | `tblggsFkWZ53LtkmJ` | Review date, files/language, source version, reviewer, findings, result and evidence. Creating it does not mean a review has run. |

The new `teams` table has not replaced `tenants`. No records or links have been migrated. Moving the application to it requires an explicit, bounded mapping and compatibility assignment; do not create competing Team masters silently. The owner requested a Teams table in this base; no standalone Teams base has been created.

Recognize's own People table remains in base `app9kOZdIaGyKk5uG`, table `tbly1PM5iFYqVzKSm`. Existing first/last names and credentials are accessed through the established coordinated interface. Do not duplicate or move them merely to simplify a screen.

The base also contains preferences, shared directories, processing records and administration tables. Their retention is being reviewed. A current dependency or unsettled intended use is not evidence that a table is disposable.

### Deprecation marker

Owner policy: `___` marks a table for deprecation after 90 days. On 2 November 2026, the following 14 tables had that marker and a description recording **31 January 2027**:

`___agents`, `___fails`, `___activity`, `___development`, `___workflow`, `___changes`, `___contacts`, `___locations`, `___exercisers`, `___grooms`, `___rs-input-riders`, `___profiles`, `___rs-input-horses`, `___entities`.

Records and links are retained. No automatic deletion job exists or is authorized by the marker. Historical references must remain inspectable while dependencies and final disposition are reviewed.

## 6. User and Team rules

The agreed model distinguishes User levels **Independent / Preferred** from Team roles **Owner / Member**. These labels must not be mapped to existing unrelated access values without an approved decision. Descriptive tags such as owner are not membership permissions.

The model calls for explicit invitations and acceptance, Owner management of Member active status, and at most one membership in V1. It does not include browsing, selecting or switching Teams. Personal hide/show affects that viewer's display only. Creating a Team while already a Member remains an unresolved transition; do not silently leave the old Team or create a second membership.

Those are model requirements, not a claim that every control is implemented. Current source supports existing User/Team reads, own-User saves and Owner changes to nonowner membership status. PIN is set-only and never returned. Multiple memberships are not resolved by picking the first.

## 7. Three Profile specialist responsibilities

Profile uses three specialist capabilities under its coordinator. **These are defined responsibilities, not a claim that three Profile agents have been created or are running.** Recognize's internal specialists remain Recognize-only and were not transferred. A concrete assignment must identify the actual worker, source, approved delta and evidence required.

| Specialist | Owns within an authorized Profile assignment | Does not own |
| --- | --- | --- |
| Data-staging | Inspect canonical tables/links, map real records, prepare approved validation and data changes, trace revisions/request IDs/readback and consumer permissions, identify schema gaps and downstream effects. “Staging” does not mean an automatic new base or copied database. | Recognition identity decisions, arbitrary record repair, unapproved migrations or schemas, UI redesign, production workflow substitution. |
| Astro / publish | Profile routes, module packaging and application delivery preparation; confirm the established app/branch/environment, exact release contents and downstream callers; perform only an authorized deployment and its required proof. | Independent publication rights, shared configuration ownership by default, permission redesign, unrelated runtime/dependency upgrades. “Publish” in the role name is not authorization. |
| UI | Current Profile toolbar/panels, Team views and interactions, clear save states, actor-safe rendering, responsive and accessibility checks applicable to the assignment; preserve the approved design. | Credentials, canonical data authority, invented Team policies or fake success/sample data in production. |

The coordinator keeps requirements and later corrections together, sequences shared changes, reconciles results and owns downstream acceptance. Use one writer per shared mutable resource. Independent review remains separate where required. No specialist may authorize its own scope expansion or treat a passing local test as live completion.

## 8. Connected tools and how information moves

The coordinator can use available connectors and skills for a scoped task. A plugin is a tool connection or guidance package, not another application domain or an automatic data-sharing channel. Tool availability does not establish current account access, permission to write, a deployed integration or an approved transfer of private data.

| Connection | Profile use | Boundary |
| --- | --- | --- |
| Airtable | Inspect schemas and real records; perform specifically approved table/record changes; read back the saved result. | Airtable remains canonical for application records. No guessed identities, blind retries, record repair or duplicate stores. |
| Cloudflare | Available tools can search documentation/API capabilities and access resources within their actual connected permissions. Coordinate any required Profile runtime/binding work with the shared app and Recognize owners. | Existing D1 identity data belongs to Recognize; shared claims/helpers remain protected. No new Worker, D1, R2, scheduled process or data export is implied. No new direct Cloudflare deployment route is established here. |
| Webflow | Inspect or, when authorized, maintain the existing Profile page/custom-code loader and Cloud application metadata through the production Webflow connector. | Website publication and Cloud application deployment are separate actions. The current app route is reused; a saved page is not published and a local build is not deployment proof. |
| RingStatus Operating Skills 0.2.9 | Request Router, Instruction Loader, Rebalancer, Handoff, Code Review, Context Compact and Complaint Reporting supply their applicable operating procedures. | All seven were read in this chat. Reading does not launch every procedure, activate a workflow mode, spawn agents or prove a review/continuity check. Preserve explicit invocation requirements. |
| OpenAI Developers | Available connection supports the actual exposed dashboard/key-setup capabilities. Relevant guidance can support a separately authorized agent/API integration. | No Profile runtime OpenAI integration, AI agent, key setup or private-data transfer is established by this README. An installed plugin does not prove API access or authorize sending User/Team data to a model. |

The currently implemented runtime path is browser → existing Astro Profile route → server identity validation → canonical Airtable records → permission-filtered response. This is distinct from coordinator tool calls used to inspect or maintain the system. Sharing a README, chat message or record ID does not create an API or transfer access.

Keep credentials, PINs, device tokens and raw private chat histories out of documentation and review records. Return only the consumer data required by the authorized operation. Record the source, contract and actual result rather than assuming every connector shares a common session or database.

## 9. Source and delivery boundary

Verified implementation checkout:

`C:/Users/gombc/.codex/worktrees/recognize-isolated-20261101/ringstatus`

The last inspected application release is `89abf609f816b6c9d10f05d301027c991a33280a`, deployed on 2 November 2026 at 12:11:26 UTC. Subsequent inspected local commit `53b8c87df6e9e76efc68b10dfcd74d6ea72a65af` adds documentation/ownership material, not application changes. This dated evidence is not continuous deployment monitoring.

Profile source, relative to that checkout:

- `webflow-cloud-test/src/assets/rs-profile/client.js`, `profile.css`, `recognize.css`: consumer presentation.
- `webflow-cloud-test/src/assets/rs-profile/service.js`: browser identity/data/pending-save adapter.
- `webflow-cloud-test/src/lib/rs-profile.js`: canonical records, permissions and saves.
- `webflow-cloud-test/src/pages/rs-profile/client.js.ts`, `data.js`, `index.astro`: assets, API and standalone entry.

Public paths are `/profile`, `/test/rs-profile/client.js`, `/test/rs-profile/data` and standalone `/test/rs-profile/`.

Existing Cloud app: `d7d97751-20e1-4148-a5cf-ee58671c128a`; environment: `110f06dd-c1ea-4839-98af-d829cbe77941`; mount: `/test`. The existing GitHub branch `work/inputs-crm-20261006` auto-deploys on push. A push is therefore a release action, not a harmless way to save this README.

Recognize widget/IAM/credential/refresh code, D1 claim/save helpers, `rs-tenant-barn.js` and Cloud configuration are protected shared dependencies. Coordinate their callers, designated writer and release before changing them. Preserve unrelated owners' work.

## 10. What has been proved and what remains

Inspected 2 November evidence establishes existing secure-cookie recognition → Open profile → real preexisting test User → active linked Team and six-member roster, with correct identity after reload. Canonical User/Team/membership IDs were independently matched to Airtable by the releasing specialist. Unauthenticated Profile returned 401; app entry/assets returned 200. This chat inspected the saved evidence; it did not independently repeat that browser run.

Prior scoped tests, local fixture browser checks, build and bounded source review are separate evidence. They do not prove production writes or every model control. No live Profile/Team data was edited in that release check.

Still unverified in that release: fresh credentials, live Profile saves and retries, live Owner membership writes, IP-only private-access confirmation and mobile acceptance. The broader shared IndexedDB/cache behavior is not complete merely because this implementation retains some drafts in localStorage.

Still unimplemented: Independent/Preferred backing, Team creation/invitation persistence, inline file uploads and real modified-date sorting. Older proposed fields/tables remain proposals. The newly added `teams` table alone does not implement those features.

Evidence in the approved main checkout:

- `.git/ringstatus-control/recognize-model-correction-20261101/live-acceptance-20261102.json`
- `.git/ringstatus-control/recognize-model-correction-20261101/deployed-http-20261102.json`
- The same directory's `source-review.md`, `profile-final-tests.log` and `profile-final-build.log`.

Current intake/source references in the isolated checkout: `docs/coordination/rs-profile-coordinator-handoff-20261102.md`, `docs/recognize/README.md` and `docs/coordination/profile-real-records-20261101.md`. Current owner instructions supersede dated imperative statements in historical handoffs. Do not resurrect the resolved pre-release 404 blocker.

## 11. Review records and working standard

Use the new `code-review` table when an authorized JS/Python review is actually performed. Record exact files/source version, review date, reviewer, findings, result and inspectable evidence. A row is an index to evidence, not a substitute for the review, required independent checks or live acceptance. No historical review row was fabricated when the table was created.

Keep explanations short and human unless the owner requests detail, as for this README. Keep code small, simple and maintainable. Preserve proven source; do not layer patches, redesign working behavior, add speculative infrastructure or upgrade dependencies outside the approved delta.

## Version history

| Version | Date | Change |
| --- | --- | --- |
| 1.0.0 | 2026-11-02 | Initial owner-requested detailed Profile guide; documents domain cut, specialist capabilities, tool boundaries, new tables, deprecation policy and evidence limits. |

Future revisions should update this header and history, retain exact proof dates, and distinguish a documented decision from an implemented or verified change.
