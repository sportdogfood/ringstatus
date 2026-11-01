# Shows scope-only release - October31,2026

Owner approved removing all entry/trip counting and retaining only shows, dates,
selected day/day-priority and Records transition alerts; no additional complexity.
Prepared2runtimefiles and3necessaryfixture/integrationtestfiles;24offlinechecksPASS.
Existing count data/stops/archives are retained. Source/priority/calendar policies,
task cadence, downstream/CWF/Data/Season implementations are unchanged.

Automatic event-to-chat delivery remains BLOCKED: no installed supported ingress
in the collector. No additional service/outbox/observer/framework is introduced.
Manual Records notification remains authorized, and Records confirmed its prior
November1day0notification was forwarded to the five other owning chats.

Independent review COMPLETE/PASS; deployed at20:29:18Eastern via existing idle-lock/atomic replacement route. Normal October31 20:34Eastern scheduled run343cdab8-1245-46d9-9e47-4808c4ccfede PASS: taskexit0; fourcanonicaldates/native links verified; selecteddayNovember1 retained; no direct counts/errors/telemetryerrors. Independent Airtable heartbeat/run-log readbackSuccess and all4dayrows (including historical counts/statuses/priorities) unchanged. See verification.json and readback.json. Tests and
mock calls are not collector runs. No manual source/collector runs or record repairs.
See task.md, baseline.json, manifest.json, tests-final.json and tests-final.tap.
