# Shared typography correction record — 2026-10-11

Status: First Home typography corrections saved in native Webflow; unpublished. Wider layout, embedded/combo overrides, downstream pages, and visual verification remain incomplete.

## Applied first pass

- l5-heading-white, l5-heading-dark, l5-h, and l5-heading-text: line-height 0.85 saved at main, medium, small, tiny, large, xl, and xxl.
- Created rs-shared-control-text: Outfit, clamp(14px, 1.2vw, 16px), line-height 1.2, weight 600, letter-spacing 0.075em; color inherited, original capitalization preserved.
- Nine Home message text children and eight tab label wrappers use that class. Fresh tree readback confirms all 17 bindings.
- Kicker2, rs10-tab-label, and both message containers base typography aligned to the same values; message containers have white text and automatic height.
- Element snapshot failed with status:false. No visual completion claim.
- Tools: 22 message/tab text children conformed to rs-shared-control-text. About: eight message children conformed. Subsequent tree readbacks verify the saved bindings.
- Existing l5-css code points to rs-public-shared-styles-v1 in the page head. Its exact writable source remains unresolved: registered get_page_scripts returns 404 Custom code block not found; freeform-code action schema is not exposed in the guide. Do not mistake this registered-script result for absence of head CSS.
- No changes to Lainey yet. No complete viewport/rendering acceptance claim. Shared-head CSS ownership must be resolved before further style changes that could be overridden.

## Owner requirements

- Repeated text roles use identical typography and responsive scale across sections and pages.
- Section headings use line-height 0.85 at every viewport.
- Kickers, message text, button labels, and tab labels use the agreed shared text sizing at each viewport; no unexplained section-specific variants.
- Message height and padding must be consistent; content length may increase height.
- Footer navigation stretches across available content width.
- Lainey introductory title uses full content width.
- Bottom copy in the left column of the supplied split section aligns with its left content edge.
- Shared gutters and local tab scrolling must prevent document overflow.
- Compare actual values and overrides in Airtable first. Visual inspection is final verification.

## Confirmed Home class differences

- Actual kicker child: Kicker2. A separate lowercase kicker2 combo exists and must not be confused with it.
- Section heading children: l5-heading-white and l5-heading-dark. Base line-height is 1, contrary to target 0.85.
- TabsLink rs6-tab-link contains l5-p wrapper. That wrapper carries paragraph font-size and line-height instead of consistent control typography.
- Message containers: chat-msg messagex-bot and chat-msg messagex-user.
- Message children alternate between l5-paragraph-white, Text Block 4202, and Text Block 4203.
- Split headings include unclassed h2 children within l4-h; they require explicit shared typography mapping without replacing the media layout.

## Airtable changes

Table rs-webflow-variables, tblakTuwVmMmxTYUo, base appara5767XpIaJIf.
- heading-line-height: all six listed modes now record owner target 0.85. Notes distinguish target from unapplied Webflow state.
- kicker-size: actual class name corrected to Kicker2 and owner rule appended.
- tab-size and tips-size: confirmed child-class differences and owner target appended.
- Added message-bot-size and message-user-size entries; mode values remain unverified.

## Execution blocker

The Webflow all-breakpoint style read returned: tool call failed for codex_apps/webflow.data_style_tool; Transport closed.
No authentication error was returned. No OAuth, reinstall, publication, or guessed style writes were attempted.

## Next action

Retrieve scoped breakpoint and combo overrides, inspect embedded CSS ownership, preserve baseline, apply shared text bindings and the approved layout corrections on Home, then conform /ring-status, /about-ringstatus, and /lainey. Read back native values before final visual checks. Published output cannot prove unpublished Designer changes.

