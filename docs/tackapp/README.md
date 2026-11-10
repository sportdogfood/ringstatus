# RingStatus TackLists

TackLists is the device-local packing checklist for Castlewood Farm (tenant `8778`). The public entry points are `https://tack.ringstatus.com/` and `https://ringstatus.com/tackapp`. Both use the client script at `sportdogfood/ringstatus-data/docs/tack/app.js`. The standalone page is `docs/tack/index.html` in the same repository. Its host is a custom domain on the production `ringstatus-proxy` Worker.

## Airtable connector

The server route is `webflow-cloud-test/src/pages/tackapp/[dataset].json.js` in this repository. Webflow Cloud serves the Astro app at `/test` on `ringstatus.webflow.io`.

| GET path | Airtable source | Response fields |
| --- | --- | --- |
| `/test/tackapp/horses.json` | `ww_horses` → [`8778-tacklists-full-roster`](https://airtable.com/apptdhhNzduxm5gjn/tblliyUZ1ZS88Kfvl/viwaB98wQ0MZcWkXC) | `horseId`, `horseName`, `barnName`, `barnActive` |
| `/test/tackapp/lists.json` | `tack_lists` → [`8778-tack-lists`](https://airtable.com/apptdhhNzduxm5gjn/tblRGvWvkLYV7RKqK/viw3WXE5krtboYmxr) | `list_id`, `sort`, `key`, `label`, `type`, `inNav`, `inSummary`, `inShare` |

Both responses are plain JSON arrays with the same fields as the published feeds. The server uses the Cloud application's Airtable credentials. The existing browser loaders still handle startup refresh, last-known data, and built-in fallbacks. Horse IDs and list keys identify selections in device storage, so preserve them across data-source changes. An existing packing session retains its snapshot and its 12-hour sliding expiry. New or restarted sessions use the available catalog on the device.

## Release and rollback

1. Deploy the existing Astro Cloud app. Compare the live horse IDs against the Airtable roster and compare list keys, labels, sort values, and flags against the current feed. The migration baseline is 58 unique horses.
2. Switch only `HORSES_DATA_URL` and `LISTS_DATA_URL` in `docs/tack/app.js`. Check the browser requests and horse picker on the existing hostname, then check that a saved session resumes with its original selections.
3. To roll back, restore those two constants to `https://ringstatus-proxy.gombcg.workers.dev/docs/8778/tack/horses.json` and `https://ringstatus-proxy.gombcg.workers.dev/docs/8778/tack/lists.json`.
