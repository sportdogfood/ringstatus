# Custom skills maintenance — November 16, 2026

## Correction and blocked bookkeeping

Corrected one paragraph in the custom rs-coordinator skill. It previously described saved Agents tasks, threads and activity table IDs as current. The live Agents schema no longer lists those IDs. Reading the existing inventory task through tasks returned HTTP 422: table tbluT1yhP1FJMb8Cm could not be found.

The guidance now requires live schema resolution, preserves the existing task ID and checkpoint, prohibits creating a duplicate assignment or guessing a replacement register, and continues independent authorized work. No table was recreated or business workflow resumed.

The existing assignment remains recxWQh2x0bMoHRpY, “Complete skill inventory reconciliation and adoption checks.” Its present Airtable location/status could not be verified or updated. The owner needs to identify its authoritative current location. No replacement assignment was created.

Before SHA256: aab67e455c0c3a6a040806da2943857a09b032dd81e4485bfd9521725223d49d.
After SHA256: 1e13832b742bf43c9e283eff033e9cd1681db79dca4939c18742ccdc4f25bc0e.

The same registry row recoSiGs6XlHpymx1 was updated and independently read back. Change [recbU6FzcewYJjD5x](https://airtable.com/appZahVgD156cMAe3/tblilqxgvWvQOS83B/recbU6FzcewYJjD5x) records this limited source/metadata correction. “Verified” describes the source diff, links, registry readback and backend discovery, not agent adherence or complaint remediation.

## Source and catalog coverage

The three local roots contain the same 46 top-level entrypoints: 15 established custom skills and 31 entries outside that custom ownership baseline. There are no new or removed entries outside the baseline. All 56 custom entrypoint/supporting-reference files were hashed. Before this run's correction, all matched the November 9 baseline. Afterward only rs-coordinator changed. Existing invocation policies and all other custom sources were preserved.

The live skills-included scan returned 129 records over two pages and reached its terminal cursor. Current table names and field types were checked before relying on references or writing metadata.

The exact running desktop executable is under OpenAI.Codex_26.1111.2228.0. Its separate read-only skills/list request reported Codex Desktop 0.169.0-alpha.11.1. The repository scope returned 68 skills, including all 15 canonical custom entrypoints enabled, with zero discovery errors. Rediscovery after the correction also passed. The home scope excludes repository-only skills as expected. This is backend discovery, not picker visibility, instruction loading or observed use.

RingStatus Operating Skills 0.2.10 cache contains 39 files matching the prior release archive, with parsed-JSON comparison for the generated manifest. This supports cached content only. The seven packaged skills were not separately exposed at packaged paths in this backend catalog. A plugin-directory search did not return an exact RingStatus match; that search is not exhaustive and does not prove removal. No plugin was released, installed, repaired or modified. rs-coordinator is not one of the seven packaged entrypoints.

## Next bounded inventory item

Checked the existing figma-use-figjam row recGWzWfayE7urDhS. Its registered Figma 15 source path is absent. Figma 18 cached source exists and has the same SHA256, 8d566444a1aaf31c122682a5fbe3d6f14c6a844a4e05164cda3de872752cea24. Plugin search reports Figma enabled/installed, but exact desktop discovery contains no figma-use-figjam entry.

The existing row's review state and check time were corrected and read back. Its ID, name, category, source path and revision were preserved. No guessed source migration or vendor edit was made. Exact exposure remains pending. Revisit after a relevant catalog/capability change; otherwise proceed to the next item in the saved missing-reference queue at the next bounded maintenance. The dated remaining-reference count has not been freshly revalidated.

## Complaint delta review

Read 217 distinct complaints over three pages to the terminal cursor. Compared six-field content fingerprints: complaint, solution, complaint status, remediation state, source and Task. There are no new or missing complaints. Seven fingerprints changed.

For all seven, the only difference is Task: the same task title is now plain text instead of a linked-record array. The live schema confirms that Task is now singleLineText. No complaint, solution, status, remediation or source content changed. The seven IDs are rec0W3tGvkL0uGMbo, rec3y5PHaTnNz9wTw, recCaXALRmfQDaVRf, recFe0BAKvwM1Glpb, recXkL3tyeyjjNbrv, reckWfmSjM4aHbzAb and recnPRWYOdym56wmw.

The source of the schema changes was not investigated or attributed. Existing incident assessments remain dated evidence. No complaint was merged, closed or marked behaviorally remediated. No external method research was needed for the observed schema/reference mismatch.

## Validation limits and next action

The narrow source diff preserves frontmatter and unrelated instructions, existing relative links resolve, and runner boundaries remain intact. The previously unavailable formal validator was not retried without a relevant dependency change. No model change, neutral adoption exercise, operational agent message or restart occurred. Recipient loading/adoption remains pending.

Next action: identify the current authoritative location of the existing inventory task before its dependent Airtable update; preserve recxWQh2x0bMoHRpY and the local checkpoint. Continue future bounded source/complaint delta checks. Do not recreate a task/table or restore linked fields without authority.

Evidence: .git/ringstatus-control/skill-maintenance-20261116/ contains schema and exact task error, registry/complaint pages, coverage, six-field fingerprints, field deltas, before/after skill evidence, source hashes, discovery, validation and registry/change readbacks. No runtime code, operational data, vendor sources, hooks or schedules were changed.
