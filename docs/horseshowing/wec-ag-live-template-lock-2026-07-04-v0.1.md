# WEC AG Live Template Lock

Version: `v0.1`
Date: `2026-07-04`

Locked baseline:

- `catalyst/datastore-analytics-sync/wec-ag-live/wec-ag-live_LOCKED_2026-07-04_v0.1.html`

Working deploy target:

- `catalyst/datastore-analytics-sync/wec-ag-live/index.html`

Local preview copy:

- `C:\Users\gombc\Downloads\wec_ag_styled_template_live.html`

Lock intent:

- Preserve the current grouped-by-ring AG schedule template as the baseline.
- Future UI experiments should be copied into a separate file first.
- Do not experiment directly on the locked file.
- Do not change the base row/data contract unless explicitly approved.

Baseline behavior:

- Groups rows by ring.
- Ring sort uses `ring_name_prioritized`.
- Class rows sort by earliest time within ring.
- `class_related_data` owns both the rollup row and the class line.
- Rollup entries are isolated as `.rollup-item` wrappers.
- Ring anchors are always visible above the grid.
- Horse filters are hidden behind the `Filter` action.
- Horse filter uses OR behavior for multiple selections.
- Print snapshots the current viewing state.
- Drawer, hide mode, focus mode, print, and ring anchors are preserved.

Rollback:

```powershell
Copy-Item "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\catalyst\datastore-analytics-sync\wec-ag-live\wec-ag-live_LOCKED_2026-07-04_v0.1.html" "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\catalyst\datastore-analytics-sync\wec-ag-live\index.html" -Force
Copy-Item "C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\catalyst\datastore-analytics-sync\wec-ag-live\index.html" "C:\Users\gombc\Downloads\wec_ag_styled_template_live.html" -Force
```
