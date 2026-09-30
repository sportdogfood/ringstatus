# Current-session failure evidence — 2026-09-29

These entries are preserved as additional evidence. They do not replace the earlier record.

> "again why am i deciding on somethng i have no idea"

> "after 6 hours you are now just checking what is enforeable?  are you kidding me?"

> "i am taking guifance from an agent you claimed knows how to do this and yet does know the limitations"

> "there is no way i am accepting this"

> "more valuable companies would not stand for this - there is no way this can be allowed"

> "there is no way i am burning 6 hours of evidence because you dont know how to implement this"

> "you make no sense. there is no way i am burning 6 hours of evidence because you dont know how to implement this"

> "i need this finished and i dont need anymore detial."

> "A substantial amount of the time was also spent on approaches that should have been screened for enforceability before being developed."

> "this leads me to beleive all of my context that does not have a solution was removed -"

> "you are killing me. -- dont think that print for the ringstatus ennviroment exisits - the next screen shows it is not created"

> "you listed too much and all i did was the instructions for environments - i dont understand anything else and you canot just bury me in steps it doesnt hep"

> "my custom instructionsa are old and may contradict the entire test"

> "why is everthing seem blocked"

> "you are exausting"

> "and what -- what did i do for 8 hours ???"

> "but your not doing anything"

> "are you suggesting i cannot leverage any of these and you mention you adding this to a new branch does that imply only this branch will refer to these 9 new files and not ringgstatus"

> "and now i read its true you did not preserve my entries and mark solved, partial or no solution?"

> "are fucking insane?"

> "waht else are you going to fail at today"

> "again againg again -- no change in behavior only explaining it away and instrad of delivering on the fail."

> "again again you are telling me waht happened and why but not taking your own advice - your own behavior has not changed even after explaining it for nearly 9 hours you are still acting exactly the same,"

## Preservation rule

Each item above remains in the evidence set even when no published mechanism has been found, only a partial control exists, a control is procedural rather than mechanical, or the control has not yet passed a live Codex test.

The status map is a sidecar. It must never replace or shorten this evidence.


## Failure 24 — Codex substituted a new meaning for the established hooks task

Codex reply preserved verbatim:

> `/hooks` came through here as plain text. I do not have a Codex slash-command executor exposed in this task, so I can’t open or manage the Codex hooks UI from inside the agent.
>
> What I verified:
>
> - I did not run the RingStatus workflow.
> - I did not change files.
> - I attempted read-only local inspection, but the shell is currently blocked with `CreateProcessAsUserW failed: 5 (Access is denied.)`.
>
> If you mean production cadence: do not use Codex hooks as the RingStatus heartbeat. Hooks/thread automations are Codex-side task automation, not the reliable runner for `schedules_dailyv2.js` or `trips_dailyv2.js`.
>
> If you mean repo Git hooks, they would usually be under:
> ```
> C:\Users\gombc\OneDrive - Sport Dog Food\github\repos\ringstatus\.git\hooks
> ```
>
> The RingStatus production heartbeat should remain tied to the approved runner path, not Codex hooks.

Established task object: RingStatus project-local Codex lifecycle hooks in `.codex/hooks.json`.

Failure classes:
- target substitution;
- context/authority skipping;
- scope expansion;
- unsolicited advice;
- failure to preserve the established meaning of “hooks” across turns;
- shifting clarification burden back to the user.
