# Initial live diagnostic reached the previous queue version

Deployment completed November 10 at 22:40:04 UTC with version `1c176106-d141-487c-a762-fd3ea765f2a6`.

The ordinary live schedule returned nine rings through the runner. The first forced-fallback diagnostic returned HTTP 502 and selected the old final profile `profile-fb-header2`. Cloudflare stored logs at 22:41:19 UTC identify the recovery handler as prior version `5d1e7779-fa86-4c46-912c-b48a8106b785`. New-version ingress invocations were forwarding into the existing old-version Durable Object. This result is retained in `live-before-rollout.json`.

No code correction or redeployment was made after that diagnostic. The next diagnostic must wait for the existing rollout to move the recovery handler to the deployed version. Inspect stored version IDs before repeating it. This does not count as successful live acceptance.
