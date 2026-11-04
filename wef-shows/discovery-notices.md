# Shows discovery notices

Version: 1.2
Updated: November 3, 2026 (America/New_York)

## Daily weekday notice - approved November 3, 2026

One native Codex automation, `wef-shows-daily-weekday-notice`, is ACTIVE daily at
19:00 America/New_York in the existing Shows chat. It sends the evening weekday/date
handoff and cleanup reminder to Records, Timing, Messages, Waze and Publish, even
when there is no show-day. This notice does not change show day0/priorities or
execute cleanup. The locked collector and all existing tasks remain unchanged.

Actual acknowledgments from all five are required, with durable send intent,
deduplication and receipt follow-up in the same daily workflow. The only runtime
write is `.git/ringstatus-control/wef-shows-weekday-notice-state.json`.
First eligible normal run: November 4, 2026 at19:00Eastern; no November3backfill.
Configuration readback and independent19offlinechecks PASS. Actual scheduled sends
and five receipts remain PENDING that first normal run; no manual runs/sends made.
See [deployment and remaining proof](evidence/weekday-notice-20261103/deployment.json).
This narrow owner approval does not deploy the held hourly heartbeat change or
install automatic NEW SHOW/NEW SHOW-DAY discovery delivery.

Owner instruction: Shows sends verified NEW SHOW and NEW SHOW-DAY discoveries
directly to all five coordinators below. This supersedes the former Records-only
recipient list. Do not announce an unchanged pair as a new discovery.

| Recipient | Existing local Codex chat ID |
| --- | --- |
| wef-records-coordinator | 01a169bc-e58a-7de0-94c6-5d3931fcf745 |
| wef-timing-coordinator | 01a169bc-0bd6-77d3-b7b4-283a07061e90 |
| wef-messages-coordinator | 01a169cc-d938-76f0-9ae4-bbccf7266599 |
| wef-waze-coordinator | 01a169d0-8192-7851-b414-7aed7a065cea |
| wef-publish-coordinator | 01a198be-eed5-7cf1-b6b6-d218f766d86d |

The five local chat titles/IDs were resolved from the Codex chat inventory on
November 2. Use the existing chat-message route; do not create duplicate chats.

Each notice includes event type, old/new show IDs and planning day0 dates, show
name, successful normal-run ID and original completion timestamp, and verified
canonical show/day record links. Distinguish planning day0 from calendar TODAY.
State unknowns and delivery/receipt gaps explicitly. Record delivery per recipient;
sending to Records alone does not complete Shows' five-recipient notice.

## Receipt follow-up - owner instruction November 3, 2026

For every event, record the original send time and each recipient's actual
acknowledgment time/evidence. Read the recipient's response through the existing
chat tools. A successful send or a notification-state timestamp alone is NOT
confirmed receipt. Follow up with a coordinator whose acknowledgment is missing;
report the exact missing acknowledgment to the owner. Do not mark notice work
complete until all five receipts are confirmed or explicitly reported missing.
Do not claim unattended follow-up is installed merely from this task instruction.

An owner-requested resend retains the original event, original recorded send time,
and the resend time. Label it RESEND; do not manufacture a new show/day event or
repeat completed cleanup/collector work. Preserve existing recipient receipts.
No new polling task, observer, transport or repeated business execution is added.

Records' copy must retain this instruction:

> SHOWS CHANGE — ACTION REQUIRED BY WEF-RECORDS-COORDINATOR
>
> Records must forward this same verified event and source evidence once to every
> current owned Records subagent. Preserve each module's scope, approvals and
> stops. Receipt in the Records chat is not completion of the downstream handoff.

Direct coordinator notices do not remove Records' forwarding/receipt duties for
its own specialists. Avoid repeating the same event to an already-confirmed
recipient. Notices authorize no collector runs, repairs, code/schema/task/cadence
changes, stop clearing or sibling work.

This document changes the required recipient coverage. It does not install or
prove automatic delivery: the existing Records notification automation is separate,
and direct collector-to-chat transport remains unbuilt. No automation, collector,
task or Airtable changes are made by this documentation update.
