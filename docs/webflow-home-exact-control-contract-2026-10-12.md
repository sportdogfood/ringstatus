# Exact Home control contract — 12 October 2026

Supersedes the earlier exception allowing different message/kicker casing. Owner explicitly requires one contract for buttons, kickers, messages and tabs.

Saved native base contract on Kicker2, chat-msg messagex-bot/user, rs6-tab-link, rs-page-link, nav-drop-link, member-link and Submit Button5. Removed conflicting geometry/typography across seven breakpoints and26 combo selectors. Background/text colors remain semantic variants; no copy/link/runtime edits.

Common type: Outfit, shared14–16px clamp,500 weight,1.2 line height,.075em tracking,uppercase,left alignment. Common box: flex/start/center,border-box,min44px,heightauto,12px vertical padding,shared16–32px horizontal padding,1px #eee outline,corners18/18/0/18. Text children inherit parent typography and casing. Message/kicker width100%; l5-kicker parent45%/95% restrictions removed. Tabs retain existing minimum width/overflow layout. Multiline content can grow naturally under the same min-height/padding rules.

Native writes and exact head readback passed; footer preserved. Actual live browser compares message/kicker at1279px: both width1174.9px,font15.3483px,weight500,line18.418px,tracking1.15113px,padding12px/25.5806px,left-aligned uppercase. Evidence: webflow-home-exact-control-contract-2026-10-12.json; fresh live screenshot webflow-home-exact-controls-verified-2026-10-12.png. Saved, not published or locked.
