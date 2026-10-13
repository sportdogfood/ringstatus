# Recognize legacy table naming — 2026-10-10

Owner request: fix the six deprecated tables still missing ___, preserving existing Inputs behavior and records.

Existing deployment baseline: 1c8239c51a6ba7bb259157d4a236ced0856697c0, verified against Webflow Cloud environment 110f06dd-c1ea-4839-98af-d829cbe77941 and GitHub work/inputs-crm-20261006.

Minimal correction: commit 9d11db06d changes only the six Airtable table addresses in src/lib/rs-inputs-airtable.js to stable table IDs, plus affected mock-provider fixtures. No business logic, field mappings, access policy, Webflow presentation or records changed.

Source checkout: C:/Users/gombc/.codex/worktrees/recognize-deployed-delta/ringstatus.

Verification: 64/65 selected checks initially passed; remaining assertion expected old name-based URLs. Corrected that assertion to require the six exact table IDs; all six native-contract tests then passed. Six runtime subscription tests passed. Astro build passed. Initial fixture schema map also needed the same verified ID mapping; no application correction was needed after the initial identifier change.
Evidence: webflow-cloud-test/.table-id-tests.txt, .table-id-native-tests.txt, .table-id-runtime-tests.txt, .table-id-build.txt in source checkout.

Release: pushed via existing GitHub deployment branch. Physical renames are conditional on confirmed successful deployment of this exact commit. These tests are compatibility evidence, not a fresh full-system business acceptance claim.

Mappings:
rs_input_barns = tblRvTwo3HYPUkZou
rs_input_users = tblYgoeLEey05xgw9
rs_input_riders = tblnd2ToLs7dTzLAM
rs_input_horses = tblpyyaOMgjLzLvkP
rs_input_locations = tblEuwOr1rUKnj1j3
rs_input_events = tblwts3huk3w1ACjh

Rollback order: restore original physical table names before reverting the ID mapping. Do not remove records. ___ signifies intended retirement, not proof that the legacy runtime dependency has ended.

Tool issue: unsupported get_environment action rejected without writes; list_environments successfully established the exact existing environment.

Completion: exact deployment 22961c5a-b046-4bd9-8632-7d45bf5ffb01 succeeded 2026-10-10T15:11:33.176Z (11:11:33 America/New_York). All six tables subsequently renamed with ___ and schema readback confirmed original IDs retained. recognize-index updated to the same names. No records deleted or workflow executed. Naming task complete; underlying legacy data migration remains separate.
