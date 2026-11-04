# Independent daily weekday notice review

Reviewer: /root/shows_review. Local Code Review skill and scope/delivery checks
applied to a native automation configuration, not collector code. Review time and
exact prepared artifact hashes are recorded in review-receipt.json.

## Findings

Initial P2 configuration encoding defect CONFIRMED: decoded prepared-config prompt
differed from prompt.txt and contained mojibake in the arrow/separator. Saved initial
artifacts are initial-config.json/initial-prompt.txt; tool61a0f8 confirms mismatch.
Initial P2 duplicate-delivery risk SUPPORTED by prompt inspection: there was no
durable send-intent-before-send instruction or explicit recovery when a tool send
succeeded but state persistence did not. Both findings were returned to the writer.

Both are resolved in the final frozen preparation. The config prompt now equals
the UTF-8 prompt text (excluding its trailing newline). Before sending, it requires
recipient-history reconciliation by event ID and a durable PendingSend intent
written/read back before the tool call. Uncertain sends are not replayed blindly;
daily follow-ups have their own original-event-plus-trigger-date intent key.
Malformed/unreadable existing state stops notification sending rather than resetting
history. These are explicit prompt constraints, not a claim of tested crash recovery.

Final configuration review COMPLETE/PASS; independently executed preflight 19/19
PASS (tool34fd0d), including calendar checks and protected runtime hashes. No
unresolved actionable preparation finding. Candidate approval is bounded to this
native notice configuration; all actual sends/acknowledgments remain unverified.

## Scope and reviewed behavior

Supplied exact owner approval in task.json authorizes daily19:00 Eastern weekday
notice/cleanup reminder to the five coordinators, receipt confirmation, testing,
deployment and normal-run verification. It forbids manual runs and changes to
show day0/priorities. Prepared recipients are the five supplied verified local chat
identities; reviewer does not independently claim fresh recipient responses or
access from reading those IDs. Existing task is the Shows chat, not a new chat.

Configuration uses one native thread heartbeat with daily BYHOUR19 recurrence.
Independent Windows timezone read returned Eastern Standard Time (US & Canada).
Calendar conversion and next-calendar-date wording handle DST; calendar tomorrow
is not renamed TODAY. First allowed event is November4,2026 at19:00 Eastern;
November3 is explicitly not backfilled. Config readback must confirm active native
schedule/target before claiming activation; preflight is not native scheduler proof.

The reminder explicitly grants no cleanup, collection, forwarding, repair, retry,
schema or business-state permission. Only the private receipt-state JSON is writable
by the notification task. Existing collector/runtime and source clocks remain
outside this notification scope. No dependency/runtime upgrade or new observer,
transport service, collector code, schema or follow-up schedule is proposed.

Receipts require actual matching event-ID acknowledgments and timestamps from
target responses. Successful send alone is not receipt. Bounded waits and next
normal daily wake reconciliation avoid a new polling schedule. Pending recipients
must remain honestly pending; all five acknowledgments are required before first
normal occurrence is reported complete. A prompt is behavioral instruction, not
an implemented transactional delivery system; crash recovery/ack behavior needs
actual normal-run evidence and cannot be certified by keyword checks alone.

## Independent checks and acceptance

The reviewer executes the actual offline preflight through run-preflight.cjs,
redirecting only its preflight.json evidence output into this review directory;
unexpected writes are rejected. No notice, native automation, collector, provider
or Airtable/business operation is run. Exact command/runtime/times/output are in
review-command-log.json; writer's preflight evidence is preserved separately.

Activation/readback and first normal occurrence with all five actual acknowledgments
remain unverified by this review. Do not send a manual example or manufacture an
event to close those gates. Mechanical verifier PASS means artifact integrity only.
