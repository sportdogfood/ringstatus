# Captured runner startup - 7 November 2026

Owner authorized one existing-runner restart with captured startup output. Performed once through existing scheduled task; temporarily redirected launch output with a hidden PowerShell action, then restored exact original action. Before/restored task XML SHA256 comparison matched. No application code or credentials changed, no collectors invoked, legacy tasks remained disabled.

Captured Node PID19836 started at2026-11-07T21:08:45Z. Only initial stderr was MODULE_TYPELESS_PACKAGE_JSON warning; no fatal startup error captured. Original runner task has no repetition trigger; the minute/three-minute scheduled processes are separate tasks, not evidence this relay restarts repeatedly. Exact recurring popup source is not proven by existing evidence; no task visibility/cadence settings changed.

CORRECTION: earlier Get-NetTCPConnection/netstat table observations did not establish listener availability. A real TCP connection to127.0.0.1:18467 succeeded. Authenticated HTTP GET / on the local runner returned404 Route not found, proving its HTTP handler answered. Same authenticated GET / through permanent runner origin returned the identical404, proving tunnel delivery as well. These root probes make no SGL calls.

Direct local schedule diagnostic for2026-11-07/show200000077/customer15 returned HTTP200, x-rs-decoded=1, and exactly two bytes: {}. This is unusable source data accepted by unchanged installed decoder, not a missing port. Hosted schedule and class-detail checks returned502 with lane cloudflare-fallback and message Source fetch or protected response decoding failed. These are source diagnostics, not collection proof.

Existing evidence does not establish why SGL returned the empty object or detailed fallback failure. Cloudflare stored-log access remains denied by connector permissions and expired CLI login; dashboard requires login. Do not infer token expiry, IP throttling or need for credentials from these observations alone.
