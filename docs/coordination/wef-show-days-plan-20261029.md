# Canonical show-days ownership and count contract — 29 October 2026

## Final reconciliation — current ownership and existing revision storage

This section supersedes conflicting historical proposals below. **wef-season-coordinator now owns the former Data coordination scope**, including acquisition, retention, count computation, completeness and handoff, under the owner's recorded takeover and subsequent approvals. The former cloud Data chat is a source/evidence reference, not a separate supplying owner. Shows remains sole canonical show discovery and show-day/count/readiness publisher; Schedules, Entries and Live consume Shows authority independently. No execution triggers or operational reader migration are implied.

Current source baseline inspected: `wef-data/README.md`, `data_runner.py` finish(), `data_contract.py` freeze()/thaw()/directory_page(). Existing nineteen Data tables and keys are retained. Approved source now uses **runs.state-json**, **collections.retained-json**, **collections.retained-sha256** with frozen ordered observation parts, exact key/record-ID mappings and hash-pinned predecessor journals. Run terminal status is **Verified completed revision**; collection terminal status is **Verified retained observation**. These are existing values, not proposed replacements. Canonical context is checked again before completion. Existing `schema.json` owns actual field/table IDs; do not invent IDs in this plan.

**Withdraw the separate day-count-revisions table and separate parallel day-state proposals below as the default implementation.** Reuse the existing immutable retained observation mechanism: add a typed `day-count-summary` body to existing ordered collection parts, tied to existing day/run/collection keys. Exact encoder integration and summary observation-key reservation must follow the existing builder and be reviewed before build. No alternate key algorithm, new raw archive or independent acquisition is needed. This is a small authorized-build proposal, not a claim that count summaries already exist.

### Finalized count policy recommendation v1

- Scope: provider/customer/show/source date. Count unique source entry IDs across all accepted dated entry/class participation, once per day regardless of classes/groups/rings. Preserve source IDs; no name, horse, display-number or tenant-subset inference.
- Include scratched rows as observed registrations/participation, not starters or completed rounds. Exclude source-dropped rows from the current accepted count only following a complete successful owning-class revision; preserve historical accepted revisions. This avoids false zero merely because all registrations are scratched. This inclusion policy was approved in Shows chat October29; implementation and zero-gate acceptance remain separate.
- Count distinct proven trips separately, preserving the existing UUID/trip-ID identity rules and rejecting ambiguous/empty identity as a basis for complete trip totals. Count unique customer/show/date/entry/class participations separately in evidence; the Shows participation field remains optional.
- Never allocate undated/classless directory entries to a day. Retain them at show scope unless source evidence establishes dated participation. Schooling blocks with unknown attendance make whole-day entry coverage unknown, even if the competition-class subset is complete and empty. Full directory completeness cannot establish a date for those entries.
- Support two explicit coverage claims: `competition-class-participation` and `all-dated-activity`. Publish only the claim actually evidenced. The earlier `scheduled-class-participation` planning label maps to the former where only competition classes were acquired; do not silently upgrade existing historical labels. Broad Live suppression requires all-dated-activity coverage of its actual scope. A narrower class gate can skip only the named fully covered class scopes.
- Incomplete scopes may retain historical counts or publish clearly scoped positive observations, but never assert a complete whole-day zero. Zero is permitted only for a complete explicitly empty accepted scope with reconciled expected source membership, valid identities and coverage. Count0 alone has no suppression meaning.

### Exact proposed summary body and canonical projection mapping

Retain the Shows fields below, with `data-revision` meaning an integrity-verified completed run/journal identity, not a newly guessed identifier. Cross-base refs are portable keys; native record IDs are evidence locators resolved within their own base.

| Summary property inside existing frozen evidence | Meaning / Shows destination |
|---|---|
| type, policy-version | `day-count-summary`, approved rules version → count-rules-version |
| provider, customer-id, show-key, day-key, date | Exact canonical scope → existing show-day-key/customer-id/show-id/date and local Shows show link |
| coverage, covered-scope-keys | Supported coverage label and exact dated ring/class/activity membership → count-coverage; manifest carries scope keys |
| unique-entry-count, proven-trip-count, participation-count | Nullable validated counts → unique-entry-count/trip-count/optional participation-count |
| scratch-count, dropped-count, identity-quality | Retained audit evidence; not additional required public fields |
| expected-scope-keys, accepted-scope-keys, scope-outcomes | Independent schedule-source expected set, exact accepted set, explicit empty/missing/error outcomes; frozen evidence, not a field inferred from fetched classes alone |
| observed-from, observed-through, accepted-as-of, verified-at | Source window, oldest required observation as conservative as-of, and verified persistence time → count-observed-at/count-verified-at |
| run-key, observation-keys, predecessor, payload-hashes | Existing exact revision chain and count observation membership → data-run-key/data-revision/data-collection-keys/data-evidence-ref |
| completeness, errors | Explicit completeness for declared coverage and error list; errors/incomplete cannot enable a zero gate |

Reuse existing freeze()/thaw() ordering, per-part hashes and under60000-byte cell budget. The replay validator must verify predecessor journals, expected collection keys AND original IDs, hashes, complete part order, source scope, count membership and summary policy. Bootstrap historical observations retain their original times and never become fresh merely because a new run completed. Derive counts from the accepted retained graph, not later mutable business rows. Existing run state/receipts hold latest attempt; a canonical failed-attempt status must not overwrite last accepted summary/evidence pointer.

**Commit order:** reserve existing unit/observation keys → retain scope graph/count-summary parts → write/read back facts, links and counts → verify exact retained hashes/membership and canonical fence → collection status Verified retained observation → run status Verified completed revision LAST. Existing completion status still requires explicit summary coverage checks; e.g. a completed run with directory-complete=false is not whole-show/day complete. Shows verifies the run/chain/body, rereads the accepted fence, stages canonical fields and reads them back, then exposes verified publication last. No cross-table atomicity is assumed. Consumers read publication status and evidence pointer before/after the tuple, rejecting any mismatch; a concrete safe publication fence must be implemented/tested before enabling skips.

**Failure preservation:** retain previous accepted tuple, source as-of and evidence pointer. Update separate latest-attempt-at/status/reason and canonical nonready status. Never mix a newer partial count with an older verified evidence pointer, advance a freshness time on failed attempt, or delete canonical days at rollover. A new default changes current scope, not historical ownership.

### Proposed readiness and recheck defaults

For the first planned consumer, **Live**, the approved zero-readiness lease is **five minutes from count-observed-at**, bounded additionally by existing canonical show-health rules. This is approved policy, not implemented readiness, an existing cadence change or a source freshness guarantee. Read canonical authority/day readiness at Live's existing three-minute wake, with no additional direct discovery or source probe while a valid covering zero lease applies. Do not stop the task. Inventory/entry collection must continue under the existing Data producer cadence so newly added entries can appear; Data itself cannot be disabled by its published zero. The observed Data inventory interval is initially300s, but required count evidence may take longer: an expired lease must become unknown and permit normal acquisition, never silently extend the lease.

Resume existing eligible collection on changed customer/show/date, positive revised count, source correction, invalidated publication, uncovered activity, missing/partial/failed evidence or lease expiry. Scope-specific skips must preserve independent noncovered work. Invalidations are Shows-owned publication state; Data sends evidence/revision/attempt state. No permanent empty-day flag, new polling task, retry loop or manual execution is introduced. Actual skip effectiveness depends on producer timeliness and needs scheduled proof; no promised call reduction before that proof.

### Approved owner decisions and technical dependencies

1. **Business policy APPROVED October29:** observed dated registrations include scratches, exclude successfully evidenced drops, and leave undated/classless entries at show scope. Consequence: totals are observed registrations, not expected starters/attendance; unknown schooling prevents whole-day zero. Alternative active-only totals may be useful but must not suppress scratch/result/history lanes. This is settled policy, not a pending owner question.
2. **Freshness policy APPROVED October29:** five-minute zero lease with existing three-minute Live rechecks. Consequence: at most five minutes of suppression after a source observation, subject to producer detection; slower producer cycles may yield little suppression. A longer lease saves more calls but delays detection of additions. No cadence change is authorized by choosing the plan.

Owner approval source: Shows chat direct user message October29, verified by records coordinator readback. Earlier pending-policy statements below are historical and superseded. No code/Airtable approval was granted.

Routine choices settled: retain every canonical show date; source-selected indicator is a derived selector, not scope restriction; Live is the first zero-readiness consumer; display blanks/unknown for unavailable values and show retained as-of/stale status; preserve existing Data days and schedules health; use existing immutable evidence storage. These choices do not authorize table creation, a reader switch or deployment.

Technical gaps are **not owner declarations of completeness**: Season owns stable full-directory reconciliation, dated schooling/classless source support, independent expected membership and cross-date/identity fallback validation, typed count-summary implementation and replay proof. Shows owns canonical table compatibility and safe publication fence; Live owns exact lane coverage/skip/resume behavior. Missing source coverage remains unknown until demonstrated. Season is actively handling a separately owner-approved Data correction; the earlier directory drift stop is historical latest-inspected evidence, not a claim that no later work exists.

**Build acceptance after separate approval:** preserved keys/IDs and nineteen-table retention; meaningful offline dedup/scratch/drop/empty/unknown/missing-ID/mixed-revision/fence/expiry fixtures; exact table links and replay hashes; two successful normal scheduled Data→Shows→Live observations with consumer readback; positive/zero/expired evidence behavior and resumption without fabricated source records; independent proof no manual collectors or unrelated cadence changes; unknown schooling does not suppress broad Live. Natural source additions/drop/rollover stay unverified unless observed or separately accepted test evidence is available. A bounded completed run or offline tests alone do not satisfy recurring/full-day proof. **Not build-ready for verified whole-day zero while material source/coverage or approved-policy gaps remain.**

Planning only. No implementation, schema/record changes, source calls, manual runs, task changes or record repairs are authorized. Proposed fields below are not existing Airtable fields. No new table ID is assigned.

## Responsibility and existing baseline

Shows owns canonical shows AND canonical show-days publication. Data owns acquisition, count computation and evidence. Season consumes accepted retained revisions for history/reconciliation; it is not another canonical day writer. Schedules retains its current show-collection/date-health contract. Entries and Live keep their current operational contracts and keys.

Existing Shows base appCaSmL819KK7UFJ has shows tblJA5CUon5s1sWib, run-logs, heartbeat and meta; the received Shows inspection reports no show-days table. Proposed show-days is local to that base. Existing Data days tbl7WyLyshiM4KNjh in appdl16HJ5ETm8N69 retains day-key, show, date, status, expected-classes, collected-classes, trip-count, last-success, last-run and rings/classes/schedules/collections links. It has no unique-entry total. Reuse these records, links and existing key builders; do not move, rename or duplicate Data acquisition tables.

The inspected bounded trial covered show200000075 through dated schedules/classes and associated trip/entity data. It did not prove all directory entries, classless entries, source pagination, recurring operation or cross-show retention. Data current rows mutate; old collection receipts do not identify immutable past snapshots. Local checkpoint projections are not an authoritative Airtable queue. An Oct28 inventory count of zero is not a verified Oct29 daily zero.

## Proposed exact Shows schema

All physical writes to canonical show-days are by Shows. Data supplies the count/evidence fields through the portable publication contract below; Data does not write this table. Field names use existing hyphenated convention. Dates use ISO YYYY-MM-DD; timestamps are UTC ISO instants, displayed in New York time when needed. The business date is a source show date; source timezone remains unknown and no UTC-midnight derivation is permitted.

| Field | Airtable type | Meaning / value owner |
|---|---|---|
| show-day-key | Single line text, primary | Shows: customer_id\|show_id\|YYYY-MM-DD; unique logical key; uniqueness requires writer validation, not assumed database enforcement |
| show | Link, single, to local shows | Shows: native link to existing canonical show; reciprocal field shows.show-days is multiple links |
| customer-id | Single line text | Shows: canonical customer context, never inferred from Data record ID |
| show-id | Single line text | Shows: canonical source show ID |
| date | Date, no time | Shows: canonical dated context |
| source-selected-date | Lookup from linked show selected_date | Shows-owned source context; use verified existing selected-date field name at build time, no duplicate show date writer |
| is-source-selected-date | Formula checkbox/boolean | Shows: date equals nonblank source-selected-date; selection is not active-day or readiness proof |
| unique-entry-count | Number, integer >=0, nullable | Data computes; Shows publishes last verified daily distinct source entry total |
| trip-count | Number, integer >=0, nullable | Data computes; Shows publishes last verified daily distinct proven trip total |
| participation-count | Number, integer >=0, nullable; optional projection | Data computes; distinct dated entry/class participation, separate from trips; retain in Data evidence even if omitted from minimal Shows display |
| count-observed-at | Date with time | Data: source observation as-of for accepted scope; conservative oldest required observation if collected at different times |
| count-verified-at | Date with time | Data: completed persistence/readback verification of accepted revision, not a freshness substitute |
| count-coverage | Single select | Data: scheduled-class-participation; directory completeness is a separate claim and cannot be inferred |
| count-rules-version | Single line text | Data: approved count policy version, required before first publication |
| data-day-key | Single line text | Data: exact existing day-key; validate matches canonical scope |
| data-run-key | Single line text | Data: exact accepted run-key |
| data-revision | Single line text | Data: reproducible accepted completed revision identity; implementation/storage mapping remains a prerequisite |
| data-collection-keys | Long text, JSON array of exact keys | Data: accepted dated scope receipts; their child evidence must cover this date, not one class; a single receipt is still a one-item array |
| data-evidence-ref | Long text | Data: portable manifest locator using base/table and business keys with verification digest; excludes credentials/raw private payload |
| latest-attempt-status | Single select | Data: unknown, incomplete, failed, verified; distinguishes attempt from retained accepted values |
| latest-attempt-at | Date with time | Data: latest attempt completion/failure observation; never refreshes count-observed-at |
| latest-attempt-reason | Long text | Data: concise scoped error/missing coverage; no secrets/private selection data |
| publication-status | Single select | Shows: unknown, verified, stale, incomplete, failed; retained values do not imply latest attempt success |
| published-at | Date with time | Shows: canonical publication completed; does not establish source freshness |

Unknown count fields remain blank, not zero. Proposed status meanings: unknown = no accepted count; verified = accepted and meets approved freshness; stale = accepted counts exceed approved freshness; incomplete/failed = latest count attempt has that outcome, prior accepted values/evidence retained if any. A failed attempt before any accepted revision leaves counts blank with failed status. Freshness threshold is pending owner decision; no arbitrary threshold is adopted. Per-metric completeness must all pass before publishing this combined count tuple; otherwise preserve the entire previous tuple to avoid mixed revisions.

If existing canonical selected-date field cannot support a native lookup/formula, present the exact compatible change before build; do not invent its field ID or silently duplicate it.

## Count rules

1. Scope is exactly source system + customer + show + source date. Daily unique entries are the distinct nonblank source entry IDs in accepted dated participation, deduplicated across classes, groups and rings. Do not sum class entry totals, use entry numbers/names or count horses as entries. A repeat class appearance does not duplicate a participation.
2. Participation identity is customer/show/date/entry/class using source context. An entry/class association with no proven trip remains participation evidence; it never becomes a made-up trip. Multiple source trips may belong to one participation.
3. Trip identity must use the preserved proven source builder and collision checks. The trial uses class-key + entry ID + entryxclasses UUID; fallback trip_id, then empty suffix was not naturally proved. Empty or ambiguous identity blocks verified trip completeness for the affected scope; do not replace it with participation count. Cross-date class lookup and duplicate class/group appearances require validation against saved source before build.
4. Scratched, dropped and undated/classless entries require explicit policy below. Until settled, no default inclusion rule is claimed and no operational total is published. Retain source status/provenance without deleting history.
5. Zero requires a successful complete dated coverage manifest, all expected child scopes reconciled and explicit valid-empty evidence with no unresolved identities. Missing day/class, failed fetch, old inventory, no source rows yet or incomplete run is unknown/incomplete, not zero.
6. Complete scheduled-class coverage proves dated class participation only. It cannot establish total show-directory population or date allocation for classless entries. Present that limitation with count-coverage.
7. Counts are source observations, not live gone counts, timing readiness, official results, tenant membership or publication permission.

## Data → Shows publication and links

### Exact proposed Data count-evidence addition

Data owner recommends keeping existing days fields unchanged and adding a separate durable Airtable revision/count-evidence record. Proposed table label `day-count-revisions` is a planning label only, no table creation/ID or replacement key builder. Existing source/day/run/collection keys remain intact. Data is sole physical writer of every field below. Shows is read-only here. Every accepted revision's membership and payload must remain reconstructible after current acquisition rows change.

| Proposed field | Type | Contract |
|---|---|---|
| revision-key | Primary text | Data-issued stable revision identity; exact generation/dedup algorithm must be specified before build |
| day | Single native link to Data days | Local Data link only; reciprocal count-revisions |
| customer-id, show-key, day-key | Text, three fields | Exact portable scope; retain source system/provider context in evidence |
| run-key | Text | Exact owning Data run business key |
| collection-keys | Long text JSON array | Exact existing receipts supporting the dated scope |
| expected-class-keys, accepted-class-keys | Long text JSON arrays, two fields | Exact distinct dated/ring/class membership; expected set must be independently supported by schedule source evidence, not generated from only fetched details |
| expected-ring-keys, accepted-ring-keys | Long text JSON arrays, two fields | Exact dated ring coverage including non-class scopes; schooling/other blocks need declared coverage and cannot be inferred empty |
| scope-outcomes | Long text JSON | Missing/failed/valid-empty child scopes, source inventory and expected/collected/readback reconciliation |
| unique-entry-count, participation-count, proven-trip-count | Nullable integer numbers, three fields | Count tuple for the declared accepted coverage; identity gaps cannot become zero |
| count-rules-version | Text | Approved inclusion/dedup/drop rules |
| identity-quality | Long text JSON | Missing IDs, duplicates/conflicts, scratch/dropped totals and ambiguity reasons |
| source-observed-from, source-observed-through | Date with time, two fields | Actual observation window across required child scopes |
| accepted-as-of, verified-at | Date with time, two fields | Conservative count observation and persistence/readback verification respectively |
| fingerprint, fingerprint-version | Text, two fields | Membership/payload digest and explicit algorithm/canonicalization version |
| retained-payload-ref | Long text | Reconstructible Airtable payload/membership location; exact record/chunk mapping remains build prerequisite, no local durable store |
| acceptance-status | Single select | proposed pending, verified, incomplete, failed; verified is written last after all evidence/readback, no invented compatibility with existing status labels |
| acceptance-error | Long text | Scope-specific reason; no credentials/private selection data |

Latest attempt status/time/error must live in a separate Data-owned mutable day-state/attempt record keyed by existing day-key, not mutate accepted historical evidence. Minimum proposed fields there: day-key (primary text), day (local link), latest-attempt-at (timestamp), latest-attempt-status (unknown/incomplete/failed/verified), latest-attempt-reason (text), accepted-revision-key (nullable text). Exact reuse of existing work-items/collections as this mutable state requires Data's compatible field mapping before build; no new parallel queue is authorized. The separate state surface is required; the exact smallest physical placement is still an implementation prerequisite.

Data remains sole writer of its existing acquisition records and evidence. Minimum proposed accepted payload is canonical scope plus the count tuple, count-observed-at/count-verified-at/coverage/rules-version, exact data-day/run/revision/day-collection keys, manifest reference/digest and separate latest-attempt fields. No cross-base native record-ID link is permitted. Shows resolves its own show link from customer/show and validates the date against authoritative show context; invalid or conflicting context is rejected and logged. Extra/historical included dates need approved inclusion policy, not a second discovery process.

Data marks an affected scope non-consumable before mutation, verifies its retained graph/identities/expected child coverage/counts and readback, then publishes the completed revision last. It must specify how that revision can be reconstructed after later mutations in Airtable. A status/fingerprint alone is insufficient. Existing Data days/collections/runs are reuse candidates; exact new fields and retained revision mapping must be agreed before build, not assumed present.

Shows reads the accepted payload/fence, validates scope, count rules and complete child evidence, rereads the same revision/fence, then publishes the whole canonical tuple and its evidence. A changed fence or partial/error preserves previous verified tuple. Shows must expose publication-status until canonical writes/readback complete; no Airtable multi-record transaction is assumed. Repeat delivery of the same day/revision must reuse the existing row and local links. Data evidence references point to the accepted tuple; latest failed-attempt refs must not overwrite last verified evidence.

Data days.show remains a local Data show link; canonical show-days.show is a local Shows link. A portable day-key connects them logically. Schedules show-collection continues operational date health, with no renamed keys, new native cross-base links or consumer switch under this plan. Season reads retained accepted Data revisions and may reference canonical show-day-key; it never advances Data checkpoints or writes canonical days.

## Remaining decisions and smallest next implementation scope

| Decision / gap | Accountable owner | Required result before implementation |
|---|---|---|
| Scratched/dropped participation | Business owner, with Data recommendation | Decide daily accepted-registration coverage versus currently active participation and correction handling; approve count-rules-version. Avoid adding both totals unless actually needed. |
| Classless/undated directory entries | Business owner, with Data evidence | Decide whether these belong only in show totals; do not allocate a date without source evidence. |
| Included dates and freshness | Business owner; Shows/Data propose | Confirm all canonical show dates versus only source-selected date, stale threshold and collection/publication cadence; no task changes implied. |
| Reconstructible revision/count manifest | Data coordinator | Exact existing storage reuse, minimum Airtable field additions, child key coverage, identity gap behavior and publication fence; source acquisition pagination if directory completeness requested. |
| Canonical projection compatibility | Shows coordinator | Confirm native show link/reciprocal and actual selected-date field; preserve locked discovery/default-show behavior and current consumer contracts. |
| First consumer and display | Records coordinator with consumer owner | Name first reader, blank/stale display and acceptance; no automatic reader migration. |

After decisions and separate build approval: Data adapts the proven trial narrowly for authoritative Airtable checkpoint/revision and count publication; Shows adds the canonical show-days projection using that accepted handoff. Verification must include meaningful offline fixtures, two normal scheduled Data→Shows pairs, dedup across classes, valid empty versus unknown, partial/failure preservation, repeat row/link stability, retained prior-show days and downstream readback. Natural drop/rollover and currently unproved identity fallbacks must be labeled unverified until observed or separately authorized test evidence exists. No manual collector runs or record repair to manufacture acceptance.

## Owner review

Both Shows and Data ACCEPTED the ownership and planning contract October29, with conditions below. This proposal records owner direction, not a built table or verified daily count. Exact new revision table/field names above remain coordinator proposal for schema review, not independently accepted deployed fields.

- Shows: locked implementation; sole canonical show/day publication, native local show reciprocal, portable Data references, preserve previous verified counts on failure. IDs may be local show lookups where actual schema supports them. Separate participation projection is optional. Existing native selected date is `selected_date`; confirm lookup formula compatibility against schema at build time.
- Data: preserve days fields unchanged; mutable days alone are not an accepted completed revision. Data supplies separate durable count/revision evidence including source observation window, identity quality, exact expected/accepted child scope sets and reconstructible replay. Equality of expected-classes/collected-classes is insufficient: both originate from the fetched detail inventory in the trial.
- Data recommends including scratches in observed entry/participation totals, exposing scratch count in evidence, and excluding dropped rows from current counts while retaining history. These are recommendations pending business owner policy approval, not changes to published data. Do not label observed totals starters/actual rounds.
- Historical saved final trial projection: Oct22 trips0/uniqueentries0/scratches0; Oct23 211/153/9; Oct24 330/169/11; Oct25 253/130/0. These are retained Oct24 observations of the respective dated scopes, not final attendance or current counts. All794 stored trips have UUID or tripID; natural missing-ID fallback, removal and reused class ID behavior remain unproved.
- Shows reports Oct29 schedule inventory of 24 schooling sessions, two breaks, zero competition classes and zero source-reported trips. This is owner-reported schedule evidence, not independently refreshed here. Schooling attendance is unknown; no daily unique-entry or identified-trip zero is certified under this proposed contract.

Source review by Data: cloud workspace `wef-schedules/tests/build-wef-data-graph.py`, `build-wef-data-delta.py`, `decoded-day-ring-class.py`; `wef-schedules/evidence/wef-data-20261024/schema-final.json` and `latest-graph.json`. Those paths are in its separate cloud workspace, not locally transferred files. Shows review via chat01a16976-f0a6-72a1-a75f-75de7f19a815; Data review via chat6adbb542-32f8-83e9-af94-23b83d138d1a. The accountable Data owner is confirmed as that coordinator; deployment/runtime owner is not invented.
