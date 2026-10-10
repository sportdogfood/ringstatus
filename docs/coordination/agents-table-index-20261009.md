# Agents base — current table index

Verified from the Airtable connector on 2026-10-09.

Base: `appZahVgD156cMAe3` — https://airtable.com/appZahVgD156cMAe3

11 tables currently remain. This index reflects current names and IDs; older inventories must not be used to recreate removed tables. Table presence does not establish runtime activity or completion. Purposes below summarize the observed schema, not a content audit. No Airtable tables or records were changed by this indexing.

| Table | Purpose | Table ID | Fields |
|---|---|---|---|
| [agents-projects](https://airtable.com/appZahVgD156cMAe3/tblKjqNiCiqVFSdDL) | Projects and their linked bases. | `tblKjqNiCiqVFSdDL` | 7 |
| [agents-bases](https://airtable.com/appZahVgD156cMAe3/tbltaHwEyBfcuOHIm) | Base names, IDs and navigation links. | `tbltaHwEyBfcuOHIm` | 6 |
| [agent-skills](https://airtable.com/appZahVgD156cMAe3/tblQQqSMt8duj1bgu) | Skill names and responsibilities. | `tblQQqSMt8duj1bgu` | 8 |
| [agent-skills-lib](https://airtable.com/appZahVgD156cMAe3/tbl1oFcrBIIVarJ3V) | Skill groups and connector references. | `tbl1oFcrBIIVarJ3V` | 9 |
| [agent-connectors](https://airtable.com/appZahVgD156cMAe3/tblgEak1MkwzoIyAV) | Connector register. | `tblgEak1MkwzoIyAV` | 12 |
| [agent-complaints-lib](https://airtable.com/appZahVgD156cMAe3/tblPRAjSK54F8VA8U) | Failures, supporting evidence and remediation status. | `tblPRAjSK54F8VA8U` | 13 |
| [agents-meta](https://airtable.com/appZahVgD156cMAe3/tbls4fqSSLogwr6X9) | Connection targets, access evidence, blockers and next actions. | `tbls4fqSSLogwr6X9` | 17 |
| [connector-items](https://airtable.com/appZahVgD156cMAe3/tbluZJIPvgQdHdQjq) | Specific connector resources and linked workers. | `tbluZJIPvgQdHdQjq` | 19 |
| [workers](https://airtable.com/appZahVgD156cMAe3/tblU5l1j0wwREakCD) | Worker inventory and responsibilities. | `tblU5l1j0wwREakCD` | 9 |
| [___wec-engine](https://airtable.com/appZahVgD156cMAe3/tbl9EPO7xkuA8BUTy) | WEC workflow stages, paths and responsibilities. | `tbl9EPO7xkuA8BUTy` | 15 |
| [___tasks2](https://airtable.com/appZahVgD156cMAe3/tblSLC7sGtO88Ibkf) | Tasks, status and notes. | `tblSLC7sGtO88Ibkf` | 8 |

## Lookup reference

`agents-projects.base-id` is a lookup through its linked-base field, currently still named `rs-bases`, to `agents-bases`. The linked-field label differs from the current table name. No field was renamed.

## Field index

### agents-projects

Primary field: `fldev27Y9ZaZowstZ`.

| Field | Type | Field ID |
|---|---|---|
| projects-id | formula | `fldev27Y9ZaZowstZ` |
| Name | singleLineText | `fldylSL4SDiOP4Ozv` |
| rs-bases | multipleRecordLinks | `fldGDI8o8BtBSxchh` |
| base-id | multipleLookupValues | `fldEingvsdLL1eIIW` |
| Description | multilineText | `fldHWHYoXbNBJa7eu` |
| Status | singleSelect | `fldXo4BgYjXOBhvFe` |
| priority | number | `fldWISpy85uK2IuFE` |

### agents-bases

Primary field: `fld5Pspa3MLylRM8k`.

| Field | Type | Field ID |
|---|---|---|
| Name | singleLineText | `fld5Pspa3MLylRM8k` |
| projects | multipleRecordLinks | `flde55CgZyKg3gD6r` |
| base-id | singleLineText | `fldUqAg485WX5RFvn` |
| base | formula | `fldDidgxTWp0mVzNI` |
| base-link | formula | `fld0iZylhFrGwzOOl` |
| base-btn | button | `fldXfOEzkRDeM4Fqh` |

### agent-skills

Primary field: `fld7AV3cUBI3ja2u5`.

| Field | Type | Field ID |
|---|---|---|
| skill-id | formula | `fld7AV3cUBI3ja2u5` |
| skill | singleLineText | `fldyXJvk8AO7mtWcR` |
| main-items | singleLineText | `fldBZ9ALMvj31ziCv` |
| Responsibility | multilineText | `fld7OXgihXw4gUkJL` |
| global-agents | singleLineText | `fldmtJIxrcehuYuHm` |
| request-routers | singleLineText | `fldrZj2s7xonVWoZz` |
| rs-skills-and-agents | singleLineText | `fldbT5FpUF4IlcW1Q` |
| prompt-lib | singleLineText | `fldTOFzZFGtM6TnXZ` |

### agent-skills-lib

Primary field: `fldIYrPoUU1rZInva`.

| Field | Type | Field ID |
|---|---|---|
| Area | singleLineText | `fldIYrPoUU1rZInva` |
| Skills included | singleLineText | `fldbhp5njax3el2F9` |
| MCP/tool status observed | singleLineText | `fld56MKkWiJXUB1rL` |
| connector-items | multipleRecordLinks | `fldknWJxoEeecfzK7` |
| type | singleSelect | `fldM7QCr2ucVkOpNG` |
| rs-skills-and-agents | singleLineText | `fldN31eGcO9lkMVBS` |
| rs-zoho-crm | singleLineText | `fld4b04GUO226AV7x` |
| prompt-lib | singleLineText | `fldSaUMeZC0h0bUkQ` |
| rs-cloudflare-agent | singleLineText | `fldyy4JhKYy0ebegQ` |

### agent-connectors

Primary field: `fldU4do3kc4S02CTP`.

| Field | Type | Field ID |
|---|---|---|
| connector-id | formula | `fldU4do3kc4S02CTP` |
| Name | singleLineText | `fldmvU2AHNZssniOt` |
| main-items | singleLineText | `fldUpUORhndMFb4uR` |
| priority | number | `fldJKvah5LlfG5SCt` |
| purpose | multilineText | `fld8SpVBMTziGaP8e` |
| connector-items | multipleRecordLinks | `fldrNvThGBSGNeek4` |
| data-set | singleLineText | `fldqHOWs0OWosw4Tn` |
| global-agents | singleLineText | `fld6pImDnDwjUBoPX` |
| rs-skills-and-agents | singleLineText | `flda5obL7BHEscku8` |
| rs-meta | multipleRecordLinks | `fldbk4hep4r2UXAKc` |
| rs-meta 2 | multipleRecordLinks | `fldyqb9VvhRZXpExU` |
| rs-cloudflare-agent | singleLineText | `fldbRP2MSQFa9SUFn` |

### agent-complaints-lib

Primary field: `fld26uoOiURXkOPy3`.

| Field | Type | Field ID |
|---|---|---|
| Name | multilineText | `fld26uoOiURXkOPy3` |
| active-threads | singleLineText | `fldGQMdzzo8FcKe7R` |
| complaint | multilineText | `fldTfgHVQZfCSywak` |
| solution | multilineText | `fld4AAX7I7QwrV0zO` |
| support-doc-found | singleSelect | `fldAfkUAPjTlm5dYA` |
| complaint-status | singleSelect | `flddNm5u3fSahRDZT` |
| Last Modified | lastModifiedTime | `fldhdSB3Sx4I6z6Zu` |
| source | multilineText | `fldVC5hFr52tiM2tq` |
| Created | createdTime | `fldpCTEB6GgDT4fYt` |
| From field: performance-complaints | singleLineText | `fldJKBGkUe2rAOF83` |
| rs-skills-and-agents | singleLineText | `fldy8MOJh5DTAVXAi` |
| rs-skills-and-agents 2 | singleLineText | `fldHhFqLu4nZKguQw` |
| remediation-state | singleSelect | `fldU2n8LYGo3yT6KZ` |

### agents-meta

Primary field: `fldUfTCK1DYF64lcL`.

| Field | Type | Field ID |
|---|---|---|
| Name | singleLineText | `fldUfTCK1DYF64lcL` |
| Created | createdTime | `flda6P1VhGuo7z0ok` |
| service | singleLineText | `fldpYKYBMyHtW0R0p` |
| owner-thread | singleLineText | `fldsBd3OKLi4HBFul` |
| main-connectors | multipleRecordLinks | `fldS6aZqvJ0ByX8NE` |
| second-connectors | multipleRecordLinks | `flds617ctvQufqH82` |
| connector-items | multipleRecordLinks | `fldRirsv9iBun4iVt` |
| target-and-environment | singleLineText | `fldcfOasBCCfvS6aI` |
| connect-to | singleSelect | `fldKNKPrWvlEKWzhn` |
| surface | singleLineText | `fldwoN9mWiEN9UalY` |
| access-status | singleSelect | `fldwzkqmgOAiKsvI7` |
| end-to-end-status | singleSelect | `fldLgooXZEEY8EwQM` |
| verification-scope | multilineText | `fldQsFSRu0P7NEFGY` |
| evidence | multilineText | `fldwPUpB24wo3PbBy` |
| blocker | multilineText | `fld4aIhRBQYKgFAQT` |
| next-action | multilineText | `fldZddOzUwNBVnRgw` |
| last-checked | dateTime | `fldNsawgdTWYWWvWo` |

### connector-items

Primary field: `flddalGAvhxUoadI2`.

| Field | Type | Field ID |
|---|---|---|
| connector-id | formula | `flddalGAvhxUoadI2` |
| Parent | singleLineText | `fldhR9Z01R1OHhkrB` |
| skills.lib | multipleRecordLinks | `fldR30n3Q5VDmdgtN` |
| Name | singleLineText | `fldk9LESuzOXCDYkQ` |
| connectors | multipleRecordLinks | `fldz1TERCW9BBT4x4` |
| workers | multipleRecordLinks | `fldaLmVMko9uMBotY` |
| purpose | multilineText | `fldLwmsaALzIdi5fA` |
| priority | number | `fldS4YynJzEtnGKrO` |
| data-set | singleLineText | `fldMaDnkbRiWZzHMq` |
| codebase-items | singleLineText | `fldRIAVZvFf4HAe37` |
| show-engines | singleLineText | `fldjty647CRMXSiAM` |
| rs-webflow-agent | singleLineText | `fld1VGT8oxCXE87M3` |
| rs-inputs-agent | singleLineText | `fldZLu27SO4OHMjLb` |
| rs-sms-agent | singleLineText | `fldawoBERJWr5LE6W` |
| rs-skills-and-agents | singleLineText | `fldFDXegcDTDKiyCO` |
| rs-zoho-crm | singleLineText | `fldHrWkDfnI4zN3Jj` |
| rs-meta | multipleRecordLinks | `fldXNvFZW8ke5qb7g` |
| rs-meta 2 | singleLineText | `fldMBV83vp0sYYkIz` |
| rs-cloudflare-agent | singleLineText | `fldpBx43uGGDCFECP` |

### workers

Primary field: `fldJyrprfduVwOJIy`.

| Field | Type | Field ID |
|---|---|---|
| cloudflare-id | formula | `fldJyrprfduVwOJIy` |
| connector-items | multipleRecordLinks | `flduERaovrQDu4vFp` |
| Name | singleLineText | `fld703PBUQ3yBlT3k` |
| data-set | singleLineText | `fldkDilXPXwziIw95` |
| internal2 (from data-set) | multipleLookupValues | `fldMUSw93tMMdK9S1` |
| priority | number | `fldDhBLNpcjL5BUTJ` |
| purpose | multilineText | `fldVpwECseESvrsXI` |
| rs-sms-agent | singleLineText | `fldTZjkdMK8SQD1dv` |
| rs-cloudflare-agent | singleLineText | `fldu5rnoyhDpKpWbP` |

### ___wec-engine

Primary field: `fldEqrIgYmX7uoHAj`.

| Field | Type | Field ID |
|---|---|---|
| final-item | singleLineText | `fldEqrIgYmX7uoHAj` |
| wStages | singleLineText | `fldCAx4ke4ABNzzG6` |
| WEC paths | singleLineText | `fld9Umg3h532c6ESB` |
| Timing | singleLineText | `fldorgxtnWyEa9781` |
| Responsibility | singleLineText | `fldKXLhEfEO4qHzAX` |
| final-list | singleLineText | `fld1gpQOYYuk82gHd` |
| priority | number | `fldZBAot3gw6aCA44` |
| purpose | multilineText | `fldkjywboZc2zd56Q` |
| agent-support | singleLineText | `fldl59Pw5TsGNXFHS` |
| Stage | singleLineText | `fldrIarx72AdrdNzU` |
| From | singleLineText | `fld3qf3bVgQqfB7F7` |
| Main JS | singleLineText | `fldj5qDHpaM8uo3AA` |
| To | singleLineText | `fld9d6vsrJbzt8n2B` |
| wWEF-WEC | singleLineText | `fld4mYYTm3lzNQiVJ` |
| show-subagents | singleLineText | `fldcDgE0gZqWQBYfP` |

### ___tasks2

Primary field: `fldHYZcwt2wydvZmZ`.

| Field | Type | Field ID |
|---|---|---|
| Name | singleLineText | `fldHYZcwt2wydvZmZ` |
| task | singleLineText | `fld7ag2INOsQoTjaV` |
| Select | singleSelect | `fldgKNZ9a72bPMBGH` |
| status | singleSelect | `fldbAHntXOKpkykke` |
| task-cats | singleLineText | `fld9apd5TgvTUX1Xz` |
| Notes | multilineText | `fldJ25SlbOKv2V8qZ` |
| rs-webflow-agent | singleLineText | `fldPZ8hzh91Unkzx6` |
| rs-inputs-agent | singleLineText | `fldThSE6Lo4IrTOTl` |

