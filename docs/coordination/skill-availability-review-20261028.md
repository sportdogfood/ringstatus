# Skills availability correction — 28 October 2026

Owner authorized the needed corrections after reporting that the individually maintained skills were missing from the Codex skills list/picker. Earlier answers established file inventory but had not checked the actual client. This repeats complaint C051 (`rec0W3tGvkL0uGMbo`); the complaint remains Open.

## Verified result

The executable used by the running desktop app was identified as `C:/Users/gombc/AppData/Local/OpenAI/Codex/bin/0ab0e7a8f3779036/codex.exe`. A separate, bounded discovery-only process of that exact executable completed the supported initialize handshake and `skills/list` with `forceReload: true`. No thread, turn, model call, product action or operational restart was created.

For the RingStatus repository, all 14 custom skills were returned with `enabled: true`, at their existing source paths, with no discovery errors: boundaries, code-review, coding-instructions, context-compact, handoff, instruction-loader, planning, rebalance, recognize, request-router, ringstatus-control, ringstatus-workflow, rs-coordinator and tenants. The user-home scope returned the 12 user skills; the two repository-only skills were correctly absent there. A different installed CLI returned the same custom-skill result but was retained as comparison evidence, not desktop proof.

The existing explicit-only metadata is valid and preserved. File contents were not rewritten to guess at a UI failure. The supported default live-server proxy exited before initialization, so it did not verify or refresh the running desktop's cache. A prompt-input diagnostic rendered initial context only; it did not inject Instruction Loader and is not invocation or behavior proof.

## Applied correction and remaining acceptance

Updated the existing maintenance workflow to check the intended client and distinguish source/revision, discovery, picker visibility, instruction loading and behavior. The existing task and C051 complaint retain the actual limits; discovery success does not close the picker complaint or claim automatic compliance. Existing product DONE/stops and explicit-only invocation policies remain intact.

Picker visibility is still unverified. The installed Windows Computer Use guidance prohibits automating the ChatGPT desktop app UI. No app restart or alternative UI-control workaround was attempted. Smallest proposed human option: the owner opens Skills/the skill picker in this desktop app, searches Instruction Loader, and reports whether the existing skill appears and can be selected in this chat. This option has not been accepted or completed; the coordinator retains responsibility, and no owner assignment or successful handoff is claimed. Record an actual observation on the existing task before accepting the picker issue as resolved; no new chat or product task is required.

Evidence: `.git/ringstatus-control/skills-availability-20261028/` contains exact desktop and comparison discovery responses, the discovery-only helper, initial prompt diagnostic, before/after workflow revisions and bookkeeping readback. These establish their stated checks only.

Official sources inspected: https://learn.chatgpt.com/docs/build-skills and https://learn.chatgpt.com/docs/app-server .
