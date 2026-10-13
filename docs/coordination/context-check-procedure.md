# Active-session context checks

Owner authorized 10 October 2026. Applies to coordination work; does not activate hooks or scheduled jobs.

At startup/resumption record current UTC time and exact completed evidence. Recheck at task boundaries and on the first tool boundary after 30 minutes of active work. If idle longer, rebaseline on resume rather than calling idle time wasted work. Never keep a model running or wake an idle chat just to perform this check.

Run scripts/context-check.py with --session pointing to this chat's actual local rollout, --state pointing to its own .git/ringstatus-control/context-<thread-id>.json, and --progress giving the last concrete accepted result. At a due check, call get_usage_limits once and pass the current remaining percentage with --remaining. Never reuse another thread's state. When telemetry is unavailable, state that and use elapsed time and verified progress; do not invent token counts.

Review triggers are local operating defaults, not OpenAI limits: latest input >=100,000 tokens; >=10 percentage points account usage consumed since check; or >=30 minutes with no new accepted progress. Compare repeated reads/rework, task scope and evidence. Account usage includes other work and resets may increase remaining. These triggers request a compact handoff/progress review, not an automatic failure or permission gate. Preserve ongoing authorized work and use a safe checkpoint.

On trigger prepare a short resume packet: objective, accepted decisions, actual state, versions/paths, remaining task IDs, exact next action, permissions, worker state and automation ownership. Do not fork/import the entire conversation. A new chat is only created when requested. A packet is Prepared until actually delivered; a worker is Working only after dispatch and acknowledged progress. Recheck actual workers and reminders before any transfer; never promise they migrate automatically.

This is an agent-executed procedure with a tested local measurement helper, not guaranteed background enforcement. No new heartbeat is needed to monitor an inactive chat. Read only narrow fields/files and print aggregate results rather than full histories/schemas. Keep the same acceptance checks when comparing models.
