# Support and limits

Verify current support using primary documentation and the actual client:

- Skills, explicit invocation, repository discovery under `.agents/skills`, and `policy.allow_implicit_invocation: false`: https://learn.chatgpt.com/docs/build-skills
- Durable plans and milestones: https://developers.openai.com/blog/run-long-horizon-tasks-with-codex
- Repository instructions: https://developers.openai.com/codex/guides/agents-md
- Native multi-agent capabilities: https://developers.openai.com/codex/multi-agent

Treat this as an instruction workflow, not guaranteed enforcement. Available tools, agent capacity, discovery, and invocation depend on the actual client/version. Verify target-client behavior; file verification alone does not prove it. Do not force automatic invocation through AGENTS.md or enable hooks/configuration as a substitute.

Historical original prototype evidence: a disposable task application passed 10 tests, then 18 after enhancement; a separate manual synthetic slot passed 8 tests. These are application checks, not agent reliability benchmarks. They do not validate these new modes, RingStatus integration, recurring execution, SMS, monitors, or user-side Stop. Parent interruption did not stop a descendant in the cancellation exercise; disconnected cancellation and unattended safety remain unproven.
