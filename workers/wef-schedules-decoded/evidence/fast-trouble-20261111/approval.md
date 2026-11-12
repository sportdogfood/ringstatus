Owner-approved scope from this chat:
- "we need a new cadence ... run as fast as we can and then go back for the trouble ... one trouble run after all days are complete fast."
- "these failures need their own table and more detail of the actual problem ...5+5 should be to find the clean lane and then go."
- Owner requested simplest implementation, no unapproved gates/options, tests and Code Review before deploying, separate Day0 vs shared code Day1..5.
- Accepted plan: persist proven lane across days; single fast invoke/capture per eligible class, independent saving; one collective trouble pass; retain URLs,pause,exclusions,results.
- Owner final approval: "then create the needed changes and we need end to end without any more churn or unapproved complexity".
Implementation detail: later trouble passes test one alternative from existing profile order, retain proven lane until matching success, advance existing cached next position on failure; no arbitrary threshold,expiry or lane reset. Initial unknown discovery retains established5+5; later failures never repeat full discovery.
No manual collector/credential changes/new endpoints.
