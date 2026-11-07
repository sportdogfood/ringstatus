# Necessary cleanup compatibility change — not implemented

Target: wef-data/attempt-log.js, files(). The new independent cleanup can delete an already-delivered completed log between the logger's directory listing and file read. The existing reader treats that normal deletion as a delivery failure and stops delivering pending records.

Smallest correction: ignore only ENOENT while reading a filename from that listing. Preserve every other filesystem/JSON error. No retry, wait, lock, gate, collection change, schema change or schedule. Completed deleted files are simply absent from the reader's result.

Delivery acknowledgment files remain preserved by the new cleanup. They prevent an already-running logger that loaded an old record before cleanup from recreating that deleted record. They contain delivery identifiers/hashes, not record backups.

This logger edit is outside the frozen cleanup-only integration files and requires the owner's exact scope approval before implementation. No logger edit or replacement activation has been performed. After approval: make the one reader correction, independent verification of both interleavings, switch existing launcher/remove old cleanup-only hooks, verify through next genuine Records alert. No manual run or fabricated/replayed event.
