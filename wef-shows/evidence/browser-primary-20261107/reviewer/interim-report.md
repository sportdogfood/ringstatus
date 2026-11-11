# Browser primary independent review: interim findings

Reviewer /root/shows_review. Offline only; no provider/browser/collector calls or product edits. Parent remains sole writer.

P2 CONFIRMED response race, initial browser-discovery.js capture: URL readiness preceded schedule response; Promise.all snapshot missed later response and fell back before deadline. Reproduced in response-race.cjs (20ms response versus200ms allowed). Writer event-resolved correction inspected; final independent regression pending frozen candidate.

P2 SUPPORTED budget regression: existing scheduled task wef-shows-collector has ExecutionTimeLimit PT4M (read-only task inspection tool a51774). Original candidate60s browser + two proxy requests each3*30s+1.5s pauses =243s excluding publication/cleanup. Writer proposes20s browser to leave37s; final hash/test pending. Existing unbounded downstream calls are not newly introduced by this change.

P2 CONFIRMED downstream raw archive identity regression: browser fixture contains88 classes,88null class_id,88encrypted underscore fields. store.js runStagedCollector reuses canonical payload for default endpoint; normalizer.chooseScheduleVariant selects it if dated results unavailable/empty. Actual offline normalizer counterexample selects88rows with88null IDs (downstream-default.cjs/output8419a7). collector-core.prepareScheduleDay catches class-list enrichment failure and returns those rows; it cannot establish equivalent decoded fallback. Preserve existing decoded consumer contract before release; no downstream rewrite authorized. Metadata-only reader test is insufficient.

Initial independent targeted suite43/43 PASS (tool37790d); green tests do not close above findings. Exact command/runtime/times/stdout/stderr saved in review-command-log.json. Candidate still changing; final review and manifest receipt pending. Deployment and due browser branch normal scheduled proof remain UNVERIFIED; next source slot supplied November8 22:00 Eastern.
