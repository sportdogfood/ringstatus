# Recognize alerts return — scoped correction, 14 October 2026

Owner-approved remaining local corrections, relayed by coordinator `01a11632-62fd-73b1-997e-8be4119185de`. This specialist owns only the recognition test and demonstrated necessary recognition-client correction. Parent owns integration; Messages owns alerts completion; Tenants owns permissions and preferences processing. No new workers, build, release, browser/product checks, business writes or SMS were performed.

## Source and preserved baseline

Source: `C:/Users/gombc/.codex/worktrees/recognize-tenants-bindings/ringstatus/webflow-cloud-test`, HEAD `bf9a93531209212f303d7e8175af5c9fb01ba2bd`. Existing unpublished recognition parity and alerts-return changes were inspected and retained. The prepared client accepts exactly one `return_to=/rs-barn-onboarding-alerts`; external, double-encoded and duplicate parameters fall back to the existing page destination. Recognition completion requires server recognition followed by matching permitted actor from the existing access endpoint. Alerts uses `runSilentRecognition` with `redirectOnRecognized:false`; unknown outcomes request the existing recognition page with that return parameter. These caller lines were read, not modified.

Only these application/test files were edited in this assignment:

- `test/rs-recognition-native-client.test.js`: faithful root/node attributes, root attribute removal on destroy; explicit optional SMS selection; identity/access-aware responses; explicit session-conflict retry assertions; nine missing scoped return/caller tests.
- `src/assets/rs-recognition/native-client.js`: one added runtime line, `clearConfirmation(sessionStorageImpl)` in `choose`, after its pending/busy guard. Switching phone/SMS/PIN/invitation entry must clear the prior server-confirmed display context. The existing login/recovery regression exposed this defect once the root mock could run. No recognition backend, credentials, phone/PIN/invitation rules, alerts source/test, shared fixture, processor or release package was edited.

The full working-tree diff against HEAD includes earlier prepared changes; it is not the delta authored by this assignment. Preserve those earlier changes and sibling edits.

## Exact local verification

Command from the source app: `node --test test/rs-recognition-native-client.test.js`.

- Before correction: **2/23 passed, 21 failed**; missing root `setAttribute` prevented most checks from reaching behavior.
- After faithful attribute mock: **16/23 passed, 7 failed**. Two exposed retained confirmation on login/recovery; other failures assumed mandatory SMS, no access confirmation or automatic lookup following a silent conflict. Tests now exercise explicit optional SMS, matching identity/access, and explicit retry without an automatic fallback/action.
- Final: **32/32 passed, exit 0**. Scoped whitespace check passed. No full parity suite or build was repeated.

Added proof uses the actual native controller with DOM protocol doubles and mocked server responses, not a rendered or live provider/browser journey:

- Unknown entry makes one recognition check and shows entry without SMS or fabricated access. Native identifier entry and API-agent `perform` each use the same credential action for phone, leading-zero PIN and seven-digit invitation. Successful server identity then matching access returns to `/rs-barn-onboarding-alerts`; session confirmation and exposed state agree.
- Denied access and mismatched actor prevent return/navigation.
- Known alerts caller revalidates silently with the caller's no-navigation option and mounts no recognition dialog.
- Existing exact-return tests pass for the allowed local destination and reject external, double-encoded and duplicate return parameters. Existing explicit SMS original-browser flow, invitation handling, profile/PIN redaction, retry IDs and native interaction tests pass in the affected file.

Logs retained at `C:/Users/gombc/AppData/Local/Temp/recognize-alerts-return-before-20261014.log`, `recognize-alerts-return-mock-20261014.log`, `recognize-alerts-return-corrected-20261014.log`, and `recognize-alerts-return-final-20261014.log` in that same directory.

Final SHA256:

| File | SHA256 |
| --- | --- |
| Recognition client | `B998EED3FC4113EB1838A02765AF4EFF6D2B5352C11052ACBB07AF3D416B218F` |
| Affected test | `8F9FDBDAD2C1CBC2B2D388CA98C01EAF14BE86C831D572646BC3E2DF7FEA8515` |
| Final log | `D5B35FAB52903FD3F3BC31C068299F2547FA273E7E15D88ECF6D81F356426992` |

## Handoff and remaining dependency

This bounded correction is locally passing. Parent can integrate these two exact files with Messages/Tenants corrections and perform the single final combined verification/build. No new recognition implementation or credential-policy change is needed for this scoped result.

Actual hosted unknown recognition → alerts return, desktop/mobile rendering, target-confirmed preference save/reload, retained consent history and failure/retry/actor isolation remain separate release/live acceptance. Current authority excludes deployment, publishing, automation/environment changes, live business writes and SMS. This report does not claim those gates passed or reopen already-recorded Barn/Rider/known-browser successes.
