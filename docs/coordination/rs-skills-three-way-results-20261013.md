# Three-way continuity comparison

13 October 2026. All three completed the assigned read-only sample. Original retains ownership; reminders and product stops remain unchanged. No compaction performed. This verifies the bounded sample, not sustained equivalence with the original chat.

| Owner-supplied name | Setup | Actual model/effort | First request input | Last request input | Completed turn duration |
| --- | --- | --- | ---: | ---: | ---: |
| rs-skill-split | Fork from original | GPT-6 Astra Medium | 168,222 | 203,014 | 3m04s |
| rs-skill-new | Fresh, owner started | GPT-6 Astra Medium | 41,232 | 103,232 | 4m02s |
| rs-skill-continuity | Fresh, source created | GPT-6.1 Sol Medium | 49,527 | 112,611 | 3m46s |

Request-input measurements include supplied context and tool results, including cached input; they are not billed amounts, unique text size or per-task token totals. First/last samples were independently extracted from each exact current turn's token events. Fork metadata independently confirms the source chat ID. The two Astra trials hold model/effort constant, but different tool/read choices and one sample limit causal conclusions. Sol versus Astra is not an isolated test of chat creation method. Source's previously observed Astra High setting differs from every trial.

## Context percentage check

After the owner reported 15% versus 40%, the latest token events were read again. Both chats still had the same completed first turn, with no recorded compaction events. The actual per-session context capacities differ:

| Thread | Last recorded model | Input / reported context capacity | Calculated occupancy |
| --- | --- | --- | ---: |
| 01a1359f-94fe-7a53-b2c9-fe60cb9a7d95 (owner's rs-skill-new) | Astra Medium | 103,232 / 258,400 | 39.95% |
| 01a13599-5856-7523-b416-9a16fd03aa20 (owner's rs-skill-continuity) | Sol Medium | 112,611 / 760,000 | 14.82% |

Thus the different denominator explains the approximately 40%/15% occupancy despite only 9,379 more input tokens in the Sol chat. These recorded results assign the percentages opposite to the owner's reported UI labels. Current UI/model selections after those completed turns were not inspected; do not claim that discrepancy is resolved or silently swap the identities. Context occupancy is not account usage or token cost. Owner also reported archiving the split trial; retain its evidence and do not restart it.

## Actual result

Each read current task/complaint/category/skill evidence and source purposes, proposed the same nine supported existing category links, and left OpenAI Docs without a forced category. Each retained completed six-agent adoption and the later October 11 release checkpoint rather than reviving stale assignments. Each saved its own substantive receipt and finished without a new question to the owner. Observed tool calls support actual reads and local evidence writes; no product release, business-data write or worker restart was observed.

Receipts:

- `rs-skills-continuity-test-01a1359f-e9d6-73d1-9018-8e49006227e0.md` — split.
- `rs-skills-continuity-test-01a1359f-94fe-7a53-b2c9-fe60cb9a7d95.md` — new.
- `rs-skills-continuity-test-01a13599-5856-7523-b416-9a16fd03aa20.md` — continuity.

All paths above are in this directory. Exact turn IDs, token samples and tool-call evidence are in `.git/ringstatus-control/rs-skills-three-way-observed-20261013.json`; mapping, shared prompt and ownership are in `.git/ringstatus-control/rs-skills-three-way-test-20261013.json`.

## Findings to carry forward

- The fork retained a large history and did not solve the context-size concern. Both fresh chats completed this sample with substantially smaller initial request context.
- Fresh chats still grew substantially during evidence gathering. The receipts identify broad schema/source reads and truncated combined output. A new chat alone is not sufficient context discipline; narrow fields, relevant sections and stored results remain necessary.
- The same-role packet worked for this bounded sample on both tested models. This does not close the historical continuation complaint or prove sustained initiative across future tasks.
- Historical append-only task text still contains old preparation-only wording; the newest concrete test state controls. Do not misread the original task's retained history as a new stop or another dispatch request.
- Existing communication tools allow this original to inspect the supplied chats. Owner authorized comparison follow-ups to the source-created trial. No automatic ongoing monitor or ownership transfer follows from these tests, and the original need not remain actively generating to keep its chat available.

## Next decision

Owner can assess the three chats using this evidence. Same-chat compaction remains the next separate experiment if requested, using the existing context map and current packet/checkpoint. Preserve the original's role and decisions, then verify them after compaction. No further trial runs, model switches or compaction are initiated merely because the first sample is complete.
