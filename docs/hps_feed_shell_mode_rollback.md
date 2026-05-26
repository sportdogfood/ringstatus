# HPS Feed Shell Mode Rollback

## Scope

This note covers the shell-list FEED mode added to HPS. It is read-only and device/session-only. It does not add Airtable fields, does not write feed data, and does not change the feed detail tab.

## Touched Files

- `webflow/hps/hps.js`
  - Adds the `Feed` shell control.
  - Adds `feedClosed` session state for per-horse collapse while FEED opens expanded by default.
  - Renders records with `feedPlan` in a read-only FEED list.
- `webflow/hps/hps.css`
  - Adds scoped `.th-feed-shell-*` list styles.

## Rollback Steps

1. In `webflow/hps/hps.js`, remove the `Feed` button from `.th-hps-controls`.
2. Remove `feedClosed`, `feedGroupRows`, `feedHorseRow`, `feedShellLines`, `feedShellLine`, `visibleFeedPlan`, `hasFeedQuantity`, `hasDisplayValue`, `toggleFeedRecord`, and the `[data-feed-toggle]` click handler.
3. Restore `filteredRecords()` to only filter by active or inactive app status.
4. In `webflow/hps/hps.css`, remove `.th-feed-horse-row`, `.th-feed-toggle`, `.th-feed-shell-lines`, `.th-feed-shell-line`, and child span rules.
5. Re-run `node --check webflow\hps\hps.js`.
6. Rebuild/push the repo and update the Webflow embed pin if the live page is pinned to a commit URL.

## No Schema Rollback Needed

No Airtable schema or Webflow Cloud backend changes are required for rollback.
