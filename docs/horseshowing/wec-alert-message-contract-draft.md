# WEC Alert/Message Contract Draft

## Purpose

This draft captures the current WEC alert/message discussion only. It is not an implementation plan for the current sprint, and it does not approve alert sends, runner changes, workflow changes, or new records.

The immediate intent is to stop alert work here and return to locking the full WEC workflow.

## Core Contract

Alerts are not only SMS alerts. Treat them as message/event records that can support SMS, mobile, two-way, print-adjacent review, thread publishing, or internal operator review later.

Creation and publishing are separate:

- Source/probe logic evaluates runtime state.
- The system creates or updates a message record when a condition is met.
- A separate publishing/sending lane may later decide whether to send, display, suppress, expire, or thread the message.

This contract does not send SMS, email, push notifications, or webhooks.

## Message Queue Concept

The message queue pattern is:

1. Probe/source state.
2. Create or update a message record.
3. Publish or send later by a separate lane.

The queue record is the durable event/message artifact. It should be safe to inspect, dedupe, suppress, expire, or publish without rerunning source probes.

## Standard Timing Fields

Use consistent short timing fields across message types:

- `starts_in`
- `ends_in`
- `go_in`

These are intended for short mobile/SMS-style output and should remain consistent across alert/message families.

## Message Families

Current message families to preserve for later design:

- `ring_now`
- `ring_status`
- `class_start_time`
- `class_status`
- `entry_go_time`
- `entry_go_time_change`
- `entry_status`
- `entry_result`
- `entry_now`
- `rider_now`
- `rider_results`

Some of these may become true alerts. Others may be better treated as mobile/two-way/thread messages. That decision is intentionally deferred.

## Entry Go Time Change

`entry_go_time_change` should represent a changed order/go-time state, not a generic entry reminder.

Short message:

```text
Entry {entry_no}: OOG {old_entry_order}->{new_entry_order}. Go {go_time}. In {go_in}m.
```

## Required Queue Fields

Future queue records should support at least:

- `message_key`
- `message_type`
- `show_no`
- `focus_day`
- `ring_visual_key`
- `class_visual_key`
- `entry_visual_key`
- `rider_key`
- `trigger_time`
- `starts_in`
- `ends_in`
- `go_in`
- `message_text_short`
- `message_text_full`
- `status`
- `channel`
- `source_table`
- `source_row_id`

## Explicit Exclusions

This document does not approve:

- implementation
- sends
- alert runner changes
- workflow runs
- Airtable record writes
- Catalyst record writes
- Webflow publish
- Production deploy

## Next Step

Return to the full WEC workflow lock.
