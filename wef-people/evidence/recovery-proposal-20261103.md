# People recovery proposal — November 3, 2026

Status: preparation only; no implementation, tests, business calls or stop release performed. Owner: People chat `01a19727-9a70-7283-a865-01dd38aacb9d`. Repository root: `C:/Users/gombc/OneDrive - Sport Dog Food/github/repos/ringstatus` (all paths below relative to this root).

## Proven diagnosis and limits

Attempt `53fefa28-63e4-49e1-b2e0-f428abea01d8` failed November 2 at `2026-11-02T20:54:56.463Z`, stage Join, RuntimeError `Airtable HTTP 422`. `runtime/stopped.json` was written at `20:54:56.457Z`; subsequent four saved attempts skipped as Stopped. Last completed join at `2026-11-02T16:55:06.087Z` reported 942 unchanged people, zero added/updated and 882 coverage issues. No post-failure full readback establishes current record/link preservation.

Existing durable error: `wef-people/runtime/logging/err-logs/53fefa28-63e4-49e1-b2e0-f428abea01d8-0001.json`; delivery acknowledgment records `recSu1XRoEFhlx2o1`, run acknowledgment `rec6WqQ02osYMl6pV`. Do not create duplicate error rows. The saved error has empty operation/target and no request payload or Airtable response detail. Method/table/field and any earlier successful batches are UNKNOWN. The blanket write-outcome Unknown is not proof that the rejected request was a write. No schema repair can yet be justified.

## Exact implementation and baseline

`wef-people/join.py`: `named()` line 90 performs paginated reads; `apply()` line 105 compares existing output and calls `client.request()` line 139 for POST/PATCH batches; `run()` line 149 instantiates the shared Airtable transport. The terminal handler line 162 records `str(error)` and loses richer details. `wef-schedules/tests/wef_data_airtable.py`: `Airtable.request()` line 80 already obtains sanitized HTTP response details in `failure.log_message`; `error_text()` line 59 exposes them. Its non-transient error does not retain request context. `wef-people/entry.js`: `scheduled()` line 9 checks both historical Failed/Interrupted attempts and the stop file before spawning Python; terminal errors use the existing durable logging path. Deleting the stop file alone would recreate the stop from history.

Current SHA256, inspected for this proposal:

| Path | SHA256 |
| --- | --- |
| `wef-people/join.py` | `65f782fcafb2034620c55a63c35af03aa2b278c7dac07174ee1b36a64fd0b521` |
| `wef-people/test_join.py` | `641ecdc64672eb30363eac44bfc0c167618b2c60c990e1a0f74818df7622828e` |
| `wef-people/entry.js` | `b376443d17bffedd2300ed4e8dd426bff3aa4d29e2a997a5dc90f057bd81b90c` |
| `wef-people/logging.js` | `8b23df41138badfff3cb93e6cd083eca541a6734076bf6aa925a21aaadde636c` |
| `wef-people/run.ps1` | `6eb34fb1a602791080fed24a71368965b57aa7d343387db2f275b6af1fecbe51` |
| `wef-people/run-hidden.vbs` | `bbd1986b8e166e40b20f5528048713bddd3a03850d4581fa85f0acd95ec945f0` |
| `wef-people/register-task.ps1` | `cc120784426908327f3fe3f53b1ffb45c799b3ac8337c65daaceb1829ef2effe` |
| `wef-people/schema.json` | `2cb220560e0c8aa14e16b46d0a53b18d8c13da44c48940c1833f0884e9a4dc2a` |
| `wef-schedules/tests/wef_data_airtable.py` | `20ba396883f6fa2b0aeedd97d5d986376693ae8f72c31595de2bf72b7ce149d3` |

Shared transport differs from the November 1 reviewed hash `7fbc14e677f229be95695b81c6cecbc68fb94a5c5ac94edcd12fd897ed44f2d3`. This does not establish causation. Reuse the current helper without changing it; do not absorb sibling changes. The helper describes Data ownership; Records is the established coordination handoff to the Data specialist if a shared change later proves necessary. No gateway defect or handoff completion is established, and no sibling assignment/message is made here.

## Smallest proposed edit

Edit only `wef-people/join.py` and focused cases in `wef-people/test_join.py`. Use the existing shared `error_text()` and sanitization facilities. Add a module-local thin request wrapper/subclass around the existing `Airtable.request()`; on failure retain sanitized method/base/table, requested field identifiers, batch count, existing native record IDs and available person keys, plus the existing sanitized response error type/message. Keep context minimal; no credentials, headers, environment dumps, full source records or unbounded payload logging. Mark GET failures NotApplicable and retain conservative Unknown for uncertain writes. Send these details through the existing terminal result/durable error path, within its existing size bounds. Do not add a feed, table or observer.

This diagnoses future failures only. It cannot recover the discarded original response, prove current links intact or restore service by itself. No changes to identity, memberships, batch size, pagination, dependencies, schema, task, retention, or logger delivery behavior. Owners excluded; horses unlinked. Existing stop remains intact.

## Checks and independent review, after explicit approval

Focused offline fake-transport checks: richer 422 detail survives to the terminal receipt; method/table/field/identity context is retained and sanitized; read/write outcomes are distinguished conservatively; long details fit existing logger bounds; one failed write is never retried. Reuse unchanged role/link/idempotence evidence; run existing relevant join cases only after approval, with no production access. An independent read-only reviewer must inspect the exact candidate hashes, diff, checks, scope and stop preservation before accepting the logging change. No reviewer has been dispatched for this proposal. Preserve the author-review versus independent-review distinction.

## Established deployment route and separate release decision

Live task configuration was read: `wef-people-join`, four-hour cadence, `C:/WINDOWS/System32/wscript.exe //B //Nologo` targeting this checkout's `wef-people/run-hidden.vbs`; then `run.ps1` → `entry.js` → `wef-data/.venv/Scripts/python.exe -B wef-people/join.py`. Existing host-managed environment stays private. No build, task replacement or registration is needed. Accepted edits to this checkout are the scheduled runtime target; the stop must remain effective while preparing/reviewing them.

Capturing another BUSINESS request requires a SEPARATE explicit stop-release decision. There is no existing read-only scheduled diagnostic mode. Ordinary join execution may POST/PATCH people and reciprocal native links before a later failure; it is not a safe read-only observation. A read-only GET/projection observation could reveal a read failure and available inputs, but cannot reproduce a rejected POST/PATCH or prove write recovery; adding such a mode would itself need separate scope approval. Reading current saved evidence is the only observation performed here.

Any future recovery release must acknowledge the exact failed attempt without deleting history, resolve both historical-failure and stop-file guards through an explicitly approved mechanism, and define the allowed business writes. That mechanism is not yet authorized or implemented. Do not simply clear `stopped.json` or edit old run/error records. Before release, reconcile all current People records and links with saved evidence through authorized read-only output access to establish prior write effects and duplicates.

Normal scheduled acceptance, only after the cause-specific correction and release authorization: a distinct ordinary four-hour invocation reaches JoinReadback with useful exact output/log readback; check all person/source-role duplicates, existing native IDs and link additions/removals, original source clocks and supported memberships. A subsequent ordinary repeat verifies idempotence against its actual inputs. Coverage issues remain distinct from runtime failure. Any new Failed/Interrupted/unknown-write outcome stops the path again; no automatic retry, repair or further edit authority follows.

## Exact first approval request

**APPROVED TO EDIT `wef-people/join.py` and `wef-people/test_join.py` solely to retain sanitized Airtable error/request context through the existing logging path, with focused offline checks and independent read-only review. Keep the current stop, four-hour task, schema, shared transport, dependencies, owners-excluded and horses-unlinked scope unchanged. This approval does not authorize a production run, stop release, retry, record repair or a claim of service restoration.**
