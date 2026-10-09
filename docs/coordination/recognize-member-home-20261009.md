# Recognize landing destination — revision319

Owner supplied member-home page6ac91a47a4ac5cd8134b81c0 as the replacement for root / and explicitly left its button destinations undecided.

Live Webflow metadata verified title RS Member Home, site6982268b7543ac3c80151266, slug rs-member and publishedPath /rs-member. The actual destination is /rs-member, not an inferred /member-home.

Read existing Recognize page6ac459b135ad0c4c3b253d97 footer and deployed-source native-client.js before editing. The existing mountNativeRecognition already accepts launcherPath and passes it to silent recognition after OTP, Continue and initialization. Reused that setting; no application source changes or new redirect handler.

Added only launcherPath: "/rs-member" to the existing mount call in the Recognize footer. Independent Webflow readback exactly matches the intended footer; removing this one inserted option reproduces the entire previous footer byte-for-byte. Member Home and its buttons were not edited. No code deployment, publication, SMS or business-data operation was performed.

Complaint acceptance: C004 proven-code reuse -> existing configurable destination reused; C025 evidence accuracy -> saved-footer readback is verified, published browser redirect remains pending owner publication. No full Recognize completion is claimed.

Rollback reference: recognize-footer-before-member-home-20261009.html. Reverse only the added launcherPath option after checking for later edits; do not overwrite subsequent changes with the full backup blindly.

Pending: owner publishes updated Recognize and Member Home as needed, then verify actual /rs-recognize -> /rs-member navigation. Existing authentication/session and desktop acceptance remain separate. Early tool calls used an unsupported page action/missing labels and failed validation without writes; corrected using documented get_page_metadata and labeled actions. No connection repair or repeated mutation occurred.
