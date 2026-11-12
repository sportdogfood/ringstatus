# Cleanup Contract

Updated November 12, 2026. Source: the owner's cleanup rules and clarification in rs-skills on November 11, 2026.

## Common rules

1. When a checked logging table contains **1,000 or more total records**, delete its oldest eligible records until **500 total records remain**. The deletion quantity is the actual starting count minus 500, not a fixed batch of 500. **1,400 records means delete 900 eligible records to leave 500.** Do not stop at an arbitrary batch size.
2. At **7 p.m. America/New_York**, clear completed eligible logs.
3. On a **new show**, clear eligible owned show records and logs. A new show-day or priority change alone does not trigger new-show cleanup.
4. Protect **active/pending records, reference tables and cleanup history**. If too few records are eligible to reach 500, preserve protected records and report the actual remaining count and reason.
5. **No archives.** Cleanup creates no archives or backups. This does not authorize deleting historical archive files or changing separately approved source-evidence collection.

## Apply the same rules in each base

Use the agent's actual existing base and cleanup table. Map its real owned tables to the common rules. Identify reference/imported tables and other owners' data; their presence in the base does not authorize deletion. Existing code cannot establish a different threshold, retained count, trigger or retention rule. Any policy exception needs explicit owner approval.

The cleanup table lists the base's tables and purposes, identifies approved cleanup targets and records the latest actual outcome. `records` means the actual table count at action start, not a retention limit. Preserve permanent cleanup action history. Use existing approved fields and history storage; do not create duplicate controls or new schema without authority. Example rows and prior execution results are reference evidence, not instructions to copy selections, run IDs or results.

## Execution and verification

Use the existing approved runner/workflow and its approved handoff. The agent prepares and verifies the work; the runner executes cleanup. Sending a reminder, saving a contract or filling a control table does not prove execution.

Record the trigger/event and run IDs, start/completion times, before count, actual deletions, remaining count, protected records, result and errors. Read back actual saved outcomes and affected records. Report unobserved execution as unverified. Keep latest outcomes and permanent history distinct.

Preserve applicable failed-workflow stops. No substitute manual runs, fake events, record repairs or automatic retries after failure. A failed runner verification requires the existing separate edit authorization before code changes.

## Approval boundary

This contract supplies the common policy. It does not transfer another agent's deployment approval or authorize changes to this agent's base, code, schema, schedule or operational data.

Before implementation, prepare the agent's exact table mapping and smallest existing-route implementation plan for the owner's approval. Reuse approval already explicitly granted for that target. Identify missing ownership or access without inventing a different policy. Verify implementation and real workflow results separately.
