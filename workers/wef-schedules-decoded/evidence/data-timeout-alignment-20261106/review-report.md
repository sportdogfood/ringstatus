# Timeout alignment review

No implementation findings in the approved five-line timeout delta and one test assertion. Baseline is the working shared-proxy migration, not Git HEAD. Only Data caller default deadlines are removed; explicit positive diagnostic timeouts remain available. Source routing, query parameters, byte preservation, decoded marker checks, failure logging, retry rules and cadence are unchanged. Existing proxy stage timeouts remain. No queue capacity or deadline is invented.

Preview: 36 focused tests passed against staged candidate. Full Python Source -> Node CLI staged candidate survived a 72-second injected response delay and retained decoded class data. This exercises both former18s/28s cutoffs with no network or business writes. Original preview setup failure is preserved; separately approved correction to Windows preload path passed.

Inspected shared proxy, fallback and pacing source; these files are protected and unchanged. Windows task ExecutionTimeLimit PT0S and IgnoreNew remain unchanged. Platform/network limits still apply: this change removes premature application cutoffs and does not guarantee arbitrary queue duration or scheduled collection success. Live source functionality previously verified; no collector run is part of this proof.
