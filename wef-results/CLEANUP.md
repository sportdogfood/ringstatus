# Results cleanup contract

Approved November 8, 2026, Eastern time. `cleaner.js` runs through the separate Results cleanup automation on a ten-minute schedule.

| Normal wake detects | Action |
| --- | --- |
| A checked log table reaches 1,000 rows | Delete the oldest eligible rows until 500 remain. Remember an unfinished drain for later normal wakes. |
| First normal wake at or after 7 p.m. Eastern | Clear eligible rows from checked `heartbeat`, `run-logs`, and `errs-log`, once per cutoff. |
| The default show changes | Clear every row in checked `snapshot`, including rows already collected for the new show, and clear eligible checked logs. A date change in the same show keeps snapshots. |

The cleaner reads the `checked` selection in Airtable `cleanup` and permits deletions only in `snapshot` and the three log tables. `snapshot` is Results' owned live output. Shared inputs and reference tables are outside the allowlist. The first run records the default show as its baseline.

Log eligibility retains the existing protections for active, current-day, priority-zero and unknown runs. The latest heartbeat for each key stays available. An error requires a completed matching run, and a retained error keeps its run evidence. Completed `Complete`, `Success` and `Failed` records can qualify. An uncertain delete stops the cleaner pending review. The cleaner stores its observed show, cutoff and drain state in the existing `cleanup` table.

The former three-hour draft now holds the separate workflow. The former 7 p.m. and default-show drafts are historical. Use the new workflow for all three triggers. [Results cleanup automation](https://airtable.com/appK9IX59jFwUkB2u/wflw2JEI2FLbLhB52)
