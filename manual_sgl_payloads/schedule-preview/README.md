# Manual Schedule Preview Payloads

Drop browser Network Preview schedule JSON here when the live `/schedule` fetch is stripped but the browser preview contains `estimated_start_time`.

Use this filename shape:

```text
schedule_YYYY-MM-DD_show_id_SHOWID_EPOCH.json
```

Example:

```text
schedule_2026-05-31_show_id_200000063_1780190000.json
```

The schedule runner scans this folder recursively through the existing manual fallback path. The fallback is used only when the live schedule payload for the same show/date has zero `estimated_start_time` values and this manual payload has populated `estimated_start_time`.
