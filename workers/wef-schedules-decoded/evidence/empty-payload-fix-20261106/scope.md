# Approved empty-payload correction

Owner: "Fix the bug that labels empty data successful. Approved to edit that only. Test in preview before changing production. Don’t run collectors or touch the working connection."

Baseline: worker-before.js is the exact installed and verified decoder before this correction (SHA256 66fc72f8e02b9ee62ada124469778a43678c0c8df5d267a3c104c5ba99a16dd4). Existing unrelated working-tree edits are preserved.

Delta: reject an empty decoded top-level object through the existing HTTP502 error path, without X-RS-Decoded. Preserve identified responses with empty arrays, all headers, routes, renewal and transport. No fixed ring count or new schema validation. Only decoder, focused regression tests and evidence/documentation may change. Production activation requires copying only the verified decoder and reloading its existing runner task; permanent tunnel, worker routing, task definitions, credentials and URLs remain unchanged.
