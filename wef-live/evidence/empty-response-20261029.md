# Empty live response release — 29 October 2026

Owner approved wef-live-only fix, existing records/links preserved, three-minute cadence, two normal scheduled runs. No manual runs or record repairs.

Baseline: saved normal-run groups contained json_data: []; matching widget contained live_data: []; live status was true. model.ringState rejected the empty array. Existing empty fixture used {}.

Change: model accepts an empty widget array only when there are no planned rings. After widget validation, collector ends an empty group plan successfully before business writes or retirement. Malformed widgets and empty widgets with expected rings still fail. Other collectors and task configuration untouched. Existing scheduled launcher reads this checkout directly.

Validation: 45 offline tests passed, including repeated empty responses, full existing record/link preservation, older records protected from retirement, and malformed/nonempty-plan rejection.

Normal scheduled proof (America/New_York):
- 08:14:13–08:14:30: bd31d0cc-0ed1-49c2-956e-a0fb359cd4d1. Heartbeat recrenz7DNFUHv71J and run-log recWr109vyEvqVArk both Success; no errs-log rows or logging errors.
- 08:17:16–08:17:33: ab950b94-8928-4a23-8915-fd46c7d06704. Heartbeat recnCTujXRqgOrlrd and run-log recUAeLur00us0bNk both Success; no errs-log rows or logging errors.

Both: "Success: no live activity available; existing records and links preserved." Zero new rings/groups/classes/entries. Existing complete API record snapshots were sorted by record ID and hashed before/after; hashes matched for all five tables:

| Table | Rows | SHA256 |
|---|---:|---|
| live-rings | 5 | 39573f803ff7f1849eb8da3ef4110105753cc31b906961379fbdbc3f59d67bfa |
| live-groups | 31 | 75cfe00af59e9cb2d5502c647b79f2651962c6c0cc78fdc3f59b7b01921020fa |
| live-classes | 56 | 2421adf20a711c1e6bd41dcda6b8eeb2c0d4720e0c8f48a9dfeec01b03712499 |
| live-entries | 305 | 30bd488796effa8ebcf7c06534996b1c14eb09451aaf824a5153aca4e46e8aea |
| collections | 153 | bef1a93d6e1bf27e7f43a9e1306e928969ef6816c48b44b43d40cce5b8f03947 |

Final task readback: Ready; PT3M. This proves the empty-response path, not new-show populated live collection or previously outstanding multi-group/rollover acceptance.
