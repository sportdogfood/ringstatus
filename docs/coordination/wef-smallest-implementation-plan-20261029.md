# Smallest next implementation — 29 October 2026

## Technical preparation addendum — exact first-release scope

This addendum narrows earlier broad field proposals. Root is `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus`. No product changes made during preparation. Shows owner inspected current files/deployment notes and accepted the protection mechanism below. Local Season accepted typed count summaries in the existing frozen chain; detailed source-specific additions remain proposed code review, not deployed count support.

### Data code and retained body

Bounded existing files: `wef-data/data_runner.py` (schedule summary observation before finish), `wef-data/data_contract.py` (pure count-envelope validation), `wef-schedules/tests/wef_data_handoff.py` (typed summary validation/replay only), appropriate `wef-data/tests/` and README. Proposed new pure module `wef-data/day_counts.py` and count tests. Do not rewrite trial builders, transport, directory capture, wrappers or tasks. No Data schema addition: reuse approved runs.state-json and collections.retained-json/retained-sha256. The source modules under wef-schedules/tests are deployed dependencies; their path is not permission to modify schedules acquisition or stage keys.

Use existing observation()/pending_candidate() reservation, **run-key + `|observation-{ordinal:03}`**, freeze/thaw cell bounds, saved pending replay and record maps. Proposed logical scope label `day-count-summary` describes the frozen unit; do not create an alternative business identity or source request. Build counts from the accepted/reconstructible scope graph plus actually accepted new units; never read later mutable rows into an older revision. Summary units must themselves carry successful immutable readback/record-map proof as required by the existing replay validator, including a safely handled empty graph; review/tests must demonstrate this before deployment.

Exact proposed body inside existing frozen unit's source property:

- `format = wef-day-count-summary-v1`, `policy-version = dated-registrations-v1` (owner-approved semantics), provider/customer-id/show-key/day-key/date.
- coverage (competition-class-participation or all-dated-activity), completeness (complete/partial/unknown), exact covered-scope-keys/expected-scope-keys/accepted-scope-keys and valid-empty/missing/failed scope outcomes.
- nullable unique-entry-count/proven-trip-count/participation-count; distinct entry/trip/participation source-key membership or hash-verified retained membership reference; scratch/drop counts, missing/conflicting identities and reasons.
- observed-from/observed-through/accepted-as-of/verified-at; no new timestamp refresh of bootstrap observations.
- exact input collection-key/record-id/retained-sha256 list and existing predecessor/run identity. Summary content digest is SHA256 of existing packed() canonical UTF-8 JSON; the digest-bearing outer envelope hashes the body without recursively including itself.

Provider value is the established Wellington source namespace, not a newly assumed global identity. Count summary must explicitly attest observation window and coverage; source directory count alone is never daily count. Present dated-class observed counts only as that subset; unknown day coverage does not become complete0. All show dates can have canonical unknown rows even when no valid summary exists. Source gaps remain technical limitations, not pretexts for new acquisition.

### Shows code and exact schema delta

Existing files: `wef-shows/configure.js`, `airtable.js`, `collector.js`, `verify.js`, tests and README. Proposed focused new `show-days.js` plus tests. Keep `entry.js`/wrappers/discovery helper unchanged unless reviewed integration requires only an import/call in collector. No new scheduler. API adapter read allowlist expands **only** to Data runs/collections evidence needed for the exact chain; Data access GET-only. Writes remain existing Shows tables plus new local show-days; no foreign writes or deletes. Missing Data evidence affects day status, not an alternate show discovery process.

One new table in existing Shows base appCaSmL819KK7UFJ, no invented table ID:

| New show-days field | Type |
|---|---|
| show-day-key | Primary single line text, customer\|show\|date |
| show | Single native link to existing shows tblJA5CUon5s1sWib; reciprocal shows.show-days |
| date | Date only |
| unique-entry-count, trip-count | Nullable integer numbers |
| count-coverage, count-rules-version | Single line text |
| count-observed-at, count-verified-at | Date/time UTC |
| data-evidence-ref | Long text containing portable run/day/collection keys, exact record/hash locators and revision digest |
| accepted-summary | Long text: canonical body/envelope containing complete accepted scope/counts/times/evidence/revision/digest |
| publication-status | Single select Unknown / Verified / Stale |
| latest-attempt-at | Date/time UTC |
| latest-attempt-status | Single line text, distinct unknown/incomplete/failed/verified attempt meaning |
| latest-attempt-reason | Long text |

Use linked canonical show's existing integer customer_id/show_id and date selected_date as authority; retain full scope inside accepted-summary. Do not require duplicated customer/show/date selector fields or participation projection for release1. Earlier extended field lists are a superset, not all required additions. Actual schema types must be read and checked immediately before any approved schema edit; only missing agreed fields are added, no existing type conversion.

**Mixed publication safeguard:** accepted-summary is the count/evidence authority; scalar columns are display projections. Validate upstream immutable chain/body first. Stage new projections with nonready state, read back, then write complete accepted-summary and eligible publication marker last, and read back again. Reader validates body digest/scope/as-of/coverage, compares accepted-summary before/after reading display columns, and requires identical revision/digest plus exact projection match and eligible current state. Status alone is never sufficient. Existing accepted body remains when validation fails; staged mismatches are nonconsumable. On ambiguous write outcome resolve by readback, never blind create/retry. Stable show-day row and native show link reused by exact key. Define observation-source status separately from discovery success so day failures do not falsely refresh show/count readiness.

### Exact existing deployment route and rollout

Data existing deployed folder `root/wef-data`, registered `wef-data-collector`: run-hidden.vbs → run.ps1 → entry.js → private .venv Python scheduled.py → data_runner.run(). Shows existing deployed folder `root/wef-shows`, registered `wef-shows-collector`: run-hidden.vbs → run.ps1 → entry.js → collector.runShows(). Both target this approved checkout; deployment updates only reviewed source files there, preserving runtime/env/private venv/archives. Pin baseline hashes and review protected changes before in-place replacement; no wholesale folder copy/cleanup. Existing deploy manifest and configured task action must be reread before approved deployment. Do not execute collector entrypoints for validation.

Current owner correction: CWF is excluded from all WEF workflows and acceptance gates. Entries is a linking module only; Data and Season collection/retention belong to local wef-season-coordinator. Historical first-release CWF checks elsewhere in this document no longer govern current work.

Order: finish independent prepared code/fixtures after build approval → review exact frozen envelope/schema contracts → apply only approved Shows schema delta and read schema back → deploy Data summary code and verify normal producer evidence → deploy Shows consumer and verify two ordinary scheduled Data→Shows pairs. Schedule task identities/cadences unchanged. Task enable/resume after an unrelated failed path is outside this scope unless separately authorized. No CLI/manual source requests or seeded production records.

### Ready scope versus remaining constraints

**Reviewable first-release approval scope is the bounded code/schema/deployment above.** Summary encoding/replay compatibility, exact live schema revalidation and preservation fixtures are mandatory pre-deployment checks within the approved build, not owner data-model discovery requests. Source-specific unproved attendance/identity coverage blocks only affected complete count/zero claims; publish unknown/narrow scope instead. Season's current directory correction is separately approved; owner reports accepted coherent directory and normal repeats, but no new count/daily attendance acceptance is inferred.

**Not part of first-release approval:** consumer suppression, Entries input switch, Season entity schemas, global identity migration, unsupported new schooling acquisition, archive migration, schedules changes or new task cadence. Live gate remains later scoped approval and owning-reader acceptance. No broad zero claim can be approved into existence without evidence.

Planning only; nothing in this document authorizes code, Airtable/schema/records, deployment, tasks, source calls or manual collector runs. Count policies are already approved; do not ask for them again. Local Season is accountable for both distinct Data and Season functions. Cloud Data is historical reference only.

## First useful release: Data evidence → canonical Shows days

Deliver canonical show-days with accepted scoped counts when available, explicit unknown otherwise, source as-of, coverage and evidence. Do not wait for unrelated global/entity migration or Entries input replacement. Unknown schooling blocks broad zero readiness, not day creation or truthful narrower counts. Canonical discovery remains unchanged.

| Step | Owner | Smallest delta | Verification / dependency |
|---|---|---|---|
| 1. Dated count evidence | Local Season coordinator, Data function | Add typed frozen day-count-summary through existing retained-json/retained-sha256 collections and runs.state-json/predecessor machinery. Explicit scope, counts/policy/coverage, source window/as-of, required child membership/identity quality and completion-last. Preserve existing Data days/keys/IDs; no parallel evidence store. | Reuse current separately approved directory work; do not conflate completed bounded run or full show-directory count with complete dated attendance. Independently support expected dated scope and retain unknown schooling/classless. Exact summary reservation/encoder/replay contract must be reviewed before deployment. |
| 2. Canonical publication | Shows coordinator | Create proposed show-days in existing Shows base appCaSmL819KK7UFJ; add reciprocal to existing shows. Reuse prepared exact field mapping/local link. Validate accepted summary, stage/read back tuple/evidence, expose accepted publication last; preserve prior tuple/source as-of on failure. | Existing configure.js schema route and entry.js scheduled collector; use established approved checkout/runtime, exact deploy commands revalidated from existing configuration/history before execution. No new runner or cadence. Missing summary produces unknown, no default0. |
| 3. Independent protection check | Entries coordinator | No Entries input, schema, task or CWF changes. Read two ordinary Entries→CWF cycles during release. | Successful completion, same input mode and correct participation identity/links/helpers/privacy; zero duplicate CWF people fetches. Legitimate source changes allowed, counts need not freeze. |
| 4. Acceptance | Records coordinator + producer/consumer owners | Consolidate exact run/revision IDs, archived proofs, canonical readback, stable keys/local links and exception outcomes. | Two normal scheduled Data→Shows pairs. A fresh completed summary and exact canonical matching dates/counts/evidence; repeat idempotency; existing Shows identity/default discovery and retained history intact. Do not manually start collectors. |

### Relevant offline gates

Dedup one entry across classes/rings/groups; participation distinct from trip; proven identity versus missing/ambiguous fallback; scratches included, verified drops excluded with history retained; undated show-only; explicit empty versus missing/partial/failed; unknown schooling; stale source-asof vs newer publish time; mixed generation/changed fence; failed publication preserves prior verified tuple; replay exact parts/hash/predecessors; source context/date mismatch; interrupted/repeated delivery preserves IDs/local links. Run affected established suites; local tests do not replace normal-run acceptance.

### Existing routes and protected behavior

- Data: existing local `wef-data/`, hidden VBS → PowerShell → entry.js → private Python, established wef-data-collector time trigger. Current approved correction/verification has its own authorization and is not expanded or retried by this plan.
- Shows: existing `wef-shows/`, configure.js for schema, entry.js for scheduled collection; existing wef-shows-collector five-minute trigger, default-discovery/archive handoff remains intact.
- Entries/CWF: existing hidden runner/entry.js, current feeds8778/50121/80970 and private follows retained. No stop of duplicate acquisition in this release.
- Live: existing three-minute wake and source/reference ownership remain unchanged in release1. Schedules continues collecting changes even when a later valid zero lease exists.

Exact deployment file manifests/checksums, task identity and source versions must be captured from existing routes before any later approved deployment. No route invention, task replacement, default rediscovery by consumers or archive migration.

## Follow-up release: Live skip/resume only after producer acceptance

Live owner implements a consumer gate, not a stopped task. At existing wakes read canonical context/readiness, require exact matching scope and all evidence covering the requests being skipped. Five-minute lease from accepted source-asof; never from verification/publication. Expiry, revision/scope changes, positive data, corrections, failure or unknown/uncovered activity resume existing eligible acquisition. Whole-Live skip requires all-dated-activity completeness; competition-only evidence may cover only an explicitly supported class lane, not schooling or other uncovered activity. No permanent day shutdown. Data must continue producer collection; it cannot gate itself off using its zero.

Live acceptance: meaningful offline complete0/unknownschooling/subset0/stale/failure/fencechange/positive/resumption fixtures; two normal scheduled observations reading real canonical evidence, preserving links and unrelated work, with exact request/skip reasons and producer revision. Natural zero→positive/drop transitions remain unverified until observed or separately accepted test evidence; do not create records/run collectors to manufacture proof. If no source-supported fresh zero is available, verify safe unknown handling and explicitly leave zero suppression operational proof open. Do not call polling reduction verified without actual evidence.

## What does NOT belong in this release

Season's twelve proposed entity/mirror tables, global entity authority/conflicts, Entries retained-input migration, trainer-feed/private-follow equivalence, participation-to-operational-schedule crosswalk, natural multi-group eligibility, populated private delivery and archive portability remain separate work. They block those respective migrations, not truthful Shows count publication. Preserve existing readers and evidence. Source gaps that affect a declared count scope still block that scope's completeness/zero claim.

## Exact approvals required

**Approval A — release1 build/deploy:** authorize Local Season to implement typed retained daily count evidence using existing Airtable revision storage, and Shows to add show-days plus reciprocal/local publication fields in the existing base, implement and deploy through established local routes, and verify two normal scheduled Data→Shows pairs. Entries performs read-only protection checks. Preserve all existing data/keys/links/discovery/feeds/cadences. No manual collector runs, repairs, unrelated schema or task changes. This approval includes necessary recurring schema/record writes by the approved implementations, not one-off record repair. Schema delta and source summary contract must be concrete/reviewed before applying it.

**Approval B — later Live gate:** after release1 acceptance, separately authorize Live code/deployment for the scope-specific skip/resume gate, using existing task cadence, followed by normal scheduled consumer acceptance. No broad suppression without proven scope. An Entries gate/input switch or Season entity schema needs its own future scoped approval and is not included in A/B.

No new policy decision is needed now. A failed approved scheduled path stops under runner rules and requires separate approval for a new fix/resume; A/B are not blanket future failure repair authority. Source coverage cannot be supplied by owner permission alone.

## Review status

- Shows accepts the minimal canonical day publication/unknown delivery, reuse of Data evidence, two normal scheduled pair gate and separate Live acceptance.
- Entries confirms no input switch/readiness reader required for first publication/Live release; two ordinary Entries→CWF protection observations suffice for its unchanged boundary.
- Local Season ACCEPTED first release: typed scoped count summaries in existing frozen collection/run chain plus canonical show-days; exact source/child membership/policy/as-of/identity completeness and terminal revision last. Unknown schooling/classless permits narrow counts but blocks broad zero-day claims. No Entries switch, readiness consumer, Season entity migration or directory-work expansion authorized. All three owners therefore agree this minimal planning boundary; detailed implementation/schema contract and normal-run proof still remain.

Related exact specifications: wef-show-days-plan-20261029.md; wef-entries-data-season-mapping-20261029.md; wef-records-workflow-ownership-20261026.md.
