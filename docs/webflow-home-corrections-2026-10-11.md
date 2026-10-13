# Home corrections — 11 October 2026

Target: Home, page `6982268d7543ac3c801512cd`, site `6982268b7543ac3c80151266`.

Saved native changes: matching Kicker2, shared message text, tab labels, navigation and submit typography at all seven Webflow breakpoints; matching white/dark section heading size and .85 line-height; matching main body and prompt/description paragraph scales; responsive section gutters and vertical padding; consistent tab padding, local scrolling, and minimum widths; 44px form inputs; footer navigation wrapping and footer logo width constraints.

Removed `no-margin` from the six primary Home `fsl-method` wrappers because its fixed padding overrode the shared responsive spacing. Removed `is-rust` from four Home tab links so both menus use the same layout rules. Two nested tab-content panels now use `fsl-vw-copy`, avoiding duplicate gutters. No content, links, or elements deleted. Existing shared class changes can propagate to other pages.

Designer computed-style verification at Desktop, Tablet, Mobile landscape, Mobile portrait, 1280, 1440 and 1920: kickers, tab labels and message children match at every tested breakpoint (14–16px); the five section headings match in size and .85 line-height; the six primary section wrappers match in padding at each breakpoint (vertical 32–64px, horizontal 16–65px); both tab menus match in height at each breakpoint; inputs remain approximately 44px. Navigation text also matches the control size at each tested breakpoint.

Evidence: `webflow-home-saved-breakpoint-checks-2026-10-11.json`, `webflow-home-saved-mobile-2026-10-11.png`.

Remaining acceptance boundary: these are Designer measurements, not published-site or physical iPhone acceptance. Existing page-head custom CSS can override native styles after publishing and still requires reconciliation. Hero typography and split-section H2 roles were not fully conformed by this pass. Exact combo-style addressing through the connector failed without changes; those failed requests were not counted as saved corrections. No publish performed.
