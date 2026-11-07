# Pending preview-only correction

Confirmed: direct Cloudflare TLS socket gets HTTP200 config and a valid anonymous token without the local runner or tunnel. The next preview request returned503 Preview renewal required before any schedule source call. The preview stores its token in module memory, which is not guaranteed across Cloudflare instances. No class calls were made.

Proposed minimal correction (not implemented): when a schedule/class test has no valid in-memory token, invoke the existing preview renewal handler in the same request, check its success, wait1second, then run the existing decoder through the TCP transport. Keep the explicit /renew diagnostic route. This changes preview diagnostics only, adds no service/storage/dependency, and does not change production token cadence or the working connection.

Separate wire echo service returned an incomplete-body error and is not claimed verified. It was not patched.

No working fallback is claimed yet. Original empty-object fix remains local-only:12offline tests passed; preview and production activation are unfinished and paused for this user-prioritized fallback investigation.
