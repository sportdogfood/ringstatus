# Scoped correction authorization

Owner replied in this Results chat: "APPROVED TO EDIT — correct and recheck" to correcting the validator's incomplete trip arrays and duplicate participation UUIDs, then rerunning checks and review. The task activation prohibition remains unchanged.

The original reviewed candidate is preserved under baseline/ with baseline-manifest.json. The original failed review and40-test log remain dated evidence. New regressions reproduced missing/malformed/mismatched total_entry_trips and UUID duplicates/whitespace before the correction. Production change is confined to those two model.js checks. Fixture helper includes an explicit count; empty fixture uses count0. Sanitized retained-shape fixtures restore actual original total_entry_trips22/3/3 previously omitted by sanitization, not inferred group counts. No Data/Live source writes, business runs or task actions.
