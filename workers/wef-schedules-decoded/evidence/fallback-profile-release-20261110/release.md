# November 10 fallback profile release and Airtable logs

Approved delta adds windows-clean and android-clean after the three existing profiles and adds Sec-Fetch-Site same-site to the existing fallback token request. The runner, URLs, pause, transport and credentials are unchanged. Both request recovery and schedule health use the five profiles, first-success stop, one renewal and one repeated sweep.

Version 1c176106-d141-487c-a762-fd3ea765f2a6 deployed at 22:40:04 UTC. All 39 focused offline checks passed. Actual Cloudflare remote preview recovered both schedules and class 200031724 with nine trips using each new profile. Code Review and its packet verifier passed before deployment.

The ordinary live schedule returned nine rings through the runner. An initial forced fallback reached the previous queue version and failed; that result is preserved in live-before-rollout.json and rollout-note.md. Stored logs subsequently confirmed the new recovery version. Forced live fallback then selected windows-clean and returned nine rings for November 14/show 200000078 and November 25/show 200000081.

The sampled live class request returned HTTP 502 after all five profiles and a renewed sweep. Stored logs show upstream HTTP 200 with Empty source payload. Its preview had returned nine trips. The failure is retained and not marked resolved. No code changes or repeated class diagnostic followed it. Existing broader class-detail work remains open.

Seven protected hashes matched. No collector was run and no credentials were edited. Temporary remote preview processes were stopped and private preview cookie files were removed.

At the owner's request, WEF Shows now has decode-change-log, decoder-run-log and decoder-fail-alerts. Created historical and diagnostic records retain actual source times and environments. The existing hourly automation was updated to append health results, mirror existing warnings, and read passive Cloudflare tunnel status once per check. Warning-log remains the existing delivery authority. No new timer, Worker logging deployment, or continuous console watcher was added. First scheduled append to the new tables remains pending.

The owner's pasted tunnel excerpt showed two connections reconnecting within eighteen seconds. Those four events are recorded in decoder-run-log. They do not establish loss of all four tunnel connections.
