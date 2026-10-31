# Independent day-priority automation review

Reviewer: `/root/shows_review`. Applied installed Code Review skill 0.2.2.
Target: frozen three-file automation candidate in `prepared-release`, compared
with `collector-before.js` and the current protected helper contracts. Exact
baseline/candidate/dependency hashes and UTC review time are in the receipt and
review hash manifest. No production calls, implementation edits, collector runs,
record repairs, task changes, count-stop or lock clearing were performed.

## Findings

**No unresolved actionable finding in the final frozen candidate.**

P2 CONFIRMED by initial source inspection, corrected before release: the initial
day-row selector accepted a native show-link array containing both the current
show and another show. It could therefore PATCH a shared ambiguous row. The
writer added the exact-one-parent check before writes. The final targeted test
and reviewer-owned offline reproduction both reject this row with zero PATCHes.
The reviewer made no implementation changes.

## Integration and behavior checked

Collector invokes the independent priority phase after canonical publication and
before the existing count-stop check. A stopped count publisher stays stopped;
priority errors are captured separately and cannot authorize its retry. Source
timestamps, canonical selection, task cadence, native keys/links and other data
fields are not patched by this module.

The anchor is canonical `selected_date`. Discovery triggers independently of hour;
otherwise the first eligible wake at/after 19:00 Eastern runs once for that
date/show/anchor. Midnight alone does not run or shift priorities. Offsets are
whole UTC calendar-day differences; only existing, uniquely linked rows with
valid current-show keys and dates within -3..3 receive priority-only PATCHes.
Other-show and outside-range rows remain untouched. Canonical selection is
rechecked before writes; changed rows/native keys/links are read back before
writing the durable phase checkpoint.

Empty or missing existing rows are not manufactured: an empty inventory succeeds
with updated=0 and verified=0. This is not a completeness claim. Rows absent at
the trigger, or linked later, await the next authorized priority trigger. Invalid
or duplicate linked rows fail before writes. An uncertain PATCH may leave the
already written priority in place, but records a priority-specific stop, leaves
the success checkpoint absent and prohibits automatic rewrite on later wakes.
No rollback or record repair is attempted.

## Independent execution and evidence

- Targeted suite only: **4/4 PASS**, exit 0. Includes count-stop integration,
  midnight repeat, before-19 discovery, evening checkpoint, untouched rows,
  duplicate inventory and ambiguous links.
- Reviewer-owned offline cases: **PASS** for ambiguous-parent withholding,
  response-loss-after-write durable stopping/no second PATCH, and zero-row
  successful no-op. Fixture implementation is `review-cases.cjs`.
- Exact commands, working directory, runtime, start/end times, exit codes and
  separate stdout/stderr are saved in `review-command-log.json`. Tool output
  references: exec chunks `3b2755` and `b818e2`. This is independent execution,
  not the writer's test result. No full suite or business run was invoked.

Code review: **PASS**. Authorized offline checks: **PASS**. Deployment and normal
scheduled proof: **UNVERIFIED** by this review. The existing 19:09 failure, count
stop and other lock/failure states remain separate; review completion grants no
retry permission. Deploy only this reviewed manifest at the established idle
boundary, new module before collector, subject to existing owner authorization.
Do not copy staged dependencies or change any task/lock as a shortcut.

`review-receipt.json` and its mechanical verifier output establish evidence
integrity, not production readiness or a guarantee of future operation.
