# Restore the runner backup

Original files and SHA256 are in backup.json. Restore only these two files to their same original paths: backup/worker.js and backup/runner-relay.mjs. Confirm current runner/candidate identity and preserve any later owner edits before restoration. Stop only the existing wef-decoder-runner-relay task, copy the two original files back, then start that same task once. Keep tunnel, credentials, proxy, collectors and legacy tasks unchanged. Verify restored file hashes and actual listener; observe normal scheduled behavior. A backup is not an instruction to automatically roll back after failed verification.

README-only restoration: copy backup/README.md back to workers/wef-schedules-decoded/README.md if the release documentation also needs restoration; no restart is required for a README change.
