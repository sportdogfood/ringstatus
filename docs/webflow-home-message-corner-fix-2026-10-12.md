# Home message corner correction — 12 October 2026

Current Home Designer canvas page: 6aa739aad90e0d765affe666; site: 6982268b7543ac3c80151266.

Root cause: fsl-vw-panel.is-transparent inherited 14px rounded corners and overflow:hidden. Its message ended at the panel's bottom edge, so the parent clipped the message's square bottom-left corner. The message itself correctly computed 0px.

Fix: set all four corners of fsl-vw-panel.is-transparent to 0px. Keep message combo corners explicit: top-left/top-right/bottom-right 18px, bottom-left 0px. Temporary clip-path removed; native radius alone retained.

Verified actual browser CSS through supported CUA CDP and fresh Page.captureScreenshot. Parent corners compute 0px; live screenshot shows square bottom-left matching Kicker2. Evidence: webflow-home-square-corner-verified-2026-10-12.png. No publishing.

Earlier CUA screenshot output was stale relative to live browser DOM. Use fresh supported CDP capture when screenshot and computed state disagree; do not claim a rendered fix from settings alone.
