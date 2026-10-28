# Separate error records and identifiers - 25 October 2026

Owner requested keeping existing errors/messages and adding err-class, err-trip and err-group, with one error per line rather than combined errors.

Added three integer fields to wef-live.errs-log (tblMzOaTvWus2pM4I): err-class fldAD04lpSQ57m8SM, err-trip fldBHUDwXOwfPxMci, err-group fldrYN1HDuZ137z4C.

New errors retain the existing readable message and get a separate Airtable record and JSONL line. Missing-identity errors carry the exact source trip ID and class ID. Participation errors carry the observed trip ID and class ID. A class error includes a group ID only when the source class belongs to exactly one group; ambiguous/unknown groups remain blank. General errors have no guessed identifiers. Multiple failed source requests for one class are separate errs-log records; the class/run summary may still summarize them together.

Existing log records were not rewritten. No collector was manually invoked, no source calls or record repairs were made, and other collectors/tasks were untouched.

42 offline tests passed, including source-ID propagation, multiple source failures as separate records, ambiguous group left blank, historical-link preservation and logging-failure isolation. Code is deployed in the existing task checkout. Normal scheduled error-row readback is pending; tests are not live proof and no error was manufactured.

Metadata note: generic configure.js stopped at an existing errs-log.date type mismatch (live field is date, old schema declaration expects text). It made no business-record writes. Only the three requested missing fields were subsequently created through the metadata API. Existing date configuration was preserved; the unrelated generic configuration mismatch remains outside this change.
