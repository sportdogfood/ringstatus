# Home-only CSS audit — 11 October 2026

Source: https://ringstatus.webflow.io/; page 6982268d7543ac3c801512cd. Read-only audit. Earlier unpublished edits are separate from this published evidence.

Measured 91 rendered text/control candidates at 12 widths: 375,393,478,479,480,767,768,991,992,1280,1440,1920. Captured matching CSS declarations. Visual check followed measurements at393 and1920; no claim of visual review at every width or physical iPhone Edge. Hidden tab panes and animation states are outside this static pass.

## Findings

| Role / class | Published result | Consistency finding |
|---|---|---|
| Intro rs10-h1-xl |54px at393;130px at1920; line1|FAIL agreed .85; separate scaling from section headings|
| l5-heading-white / dark |62.3px/.85 at393;116px/1 white versus116px/.85 dark at1920|FAIL line-height differs; tablet dark .9 versus white1|
| Kicker2 |14px at393;16px at1920;600;line1.2|PASS between five visible kickers; FAIL matching messages/tabs|
| rs10-tab-label inside l5-p |22px at all widths|FAIL label inherits wrapper; does not match kicker scale|
| rs6-tab-link is-rust vs base |44px versus49.5px at393;57.6 versus49.5px at1920|FAIL repeated tab height and parent type differ|
| Quote l5-paragraph-white inside chat containers |19px at393 to26px at1920,400,line1.3|FAIL agreed shared message/kicker type|
| Ring6/Ring7 Text Block4202/4203 |16px every width;400 versus500;line1.3|FAIL compared with quotes and kickers|
| Main and split l5 paragraph children |19px at393;26px at1920;400;line1.3|PASS actual body type across these sections|
| paragraph-583 prompt |16px mobile,26px wide|FAIL compared with main body19px mobile|
| l4-h unclassed h2 |38px every width,line.94|FAIL shared title scale and .85 line-height|
| Footer nav rs-page-link / nav-drop-link / member-link |14/11/16px at393;20.7/20.7/16px at1920|FAIL same nav role different sizes|
| input-16 |200px height mobile versus45px larger;16px type|FAIL mobile flex basis determines height|
| Submit |16px,400,line1.3|FAIL agreed shared control type compared with kicker600,line1.2|

## Overflow and structure

At375 client360/document375. At393 and478 no document spill. At479 client464/document585;480 client465/document585. Remaining tested widths document=client. At480 footer rs0-logo-link ends585.37px, STATUS ends534.76px, question mark ends585.37px; footer MEMBERS ends550.99px. Footer logo and nowrap nav are confirmed spill sources. Local tab overflow must remain separate. Desktop scrollbar ~15px and boundary rounding affect this test; this is not physical-device proof.

Home sections reuse copy/tabs but split-media uses l4-hp/l4-h and different title sizing. Preserve media structure; it is the text rules that diverge. Footer does not share one nav type rule. Additional transparent-panel gutter and inherited wrapper typography cause inconsistent ownership.

## Rule ownership

Published native stylesheet plus inline rs-public-contract-v2 both define typography. rs10-tab-label is explicitly font-size:inherit;line-height:inherit in contract, causing l5-p22px to win over native14px. Native dark heading has .9 tablet and .85 XXL; white remains1 outside mobile. The intro contract overrides native parent-based type with54–130px viewport clamp and line1. These are source-backed conflicts, not screenshot guesses.

## Full per-element size matrix

| Element/class and text |375|393|478|479|480|767|768|991|992|1280|1440|1920|
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|member-link is-hero w-button — MEMBERS|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|rs0-logo-link-navv w-inline-block w--current — RING STATUS|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|rs-page-link w-button w--current — HOME|14px|14px|14px|14px|14px|14px|14px|14px|14px|16.0938px|17.2589px|20.7px|
|rs-page-link w-button — TOOLS|14px|14px|14px|14px|14px|14px|14px|14px|14px|16.0938px|17.2589px|20.7px|
|rs-page-link w-button — ABOUT|14px|14px|14px|14px|14px|14px|14px|14px|14px|16.0938px|17.2589px|20.7px|
|rs10-h1-xl is-white — Built for|54px|54px|54px|54px|54px|73.2032px|73.2874px|94.5576px|94.6419px|122.146px|130px|130px|
|rs10-h1-xl is-white-2 — the day|54px|54px|54px|54px|54px|73.2032px|73.2874px|94.5576px|94.6419px|122.146px|130px|130px|
|rs10-h1-xl is-rust — you get.|54px|54px|54px|54px|54px|73.2032px|73.2874px|94.5576px|94.6419px|122.146px|130px|130px|
|l5-paragraph-white — I’m Lainey, a rider and working student. I built RingSt|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|l5-paragraph-white — I need useful answers, not to get buried in notificatio|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|kicker2 is-dark — START WITH A PLAN|14px|14px|14px|14px|14px|14px|14px|14px|14px|15.3642px|16px|16px|
|l5-heading-white — Know what needs to happen.|59.0878px|62.3447px|77.6025px|54px|54px|54px|54px|54px|54px|73.3025px|84.0042px|116px|
|l5-paragraph-white — There’s getting everything to the show, caring for the |19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|paragraph-583 — Tap below to see how I keep up when things change.|16px|16px|16px|16px|16px|16px|16px|16.0046px|16.0142px|19.1278px|20.8539px|26px|
|rs6-tab-link is-rust w-inline-block w-tab-link w--current — PACK|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|18px|
|rs10-tab-label — PACK|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|
|rs6-tab-link is-rust w-inline-block w-tab-link — PREP|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|18px|
|rs10-tab-label — PREP|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|
|rs6-tab-link is-rust w-inline-block w-tab-link — SHOW|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|18px|
|rs10-tab-label — SHOW|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|
|rs6-tab-link is-rust w-inline-block w-tab-link — CONNECT|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|18px|
|rs10-tab-label — CONNECT|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|
|l5-paragraph-white — Getting the horses and tack to a show takes planning, w|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|kicker2 — MOVING TARGETS|14px|14px|14px|14px|14px|14px|14px|14px|14px|15.3642px|16px|16px|
|l5-heading-white — When the day changes, our plan needs to keep up.|59.0878px|62.3447px|77.6025px|54px|54px|54px|54px|54px|54px|73.3025px|84.0042px|116px|
|l5-paragraph-white — As soon as the day starts, it feels like the plan alrea|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|paragraph-583 — Tap below to see how I keep up when things change.|16px|16px|16px|16px|16px|16px|16px|16.0046px|16.0142px|19.1278px|20.8539px|26px|
|rs6-tab-link w-inline-block w-tab-link w--current — PACK|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|rs10-tab-label — PACK|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|
|rs6-tab-link w-inline-block w-tab-link — PREP|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|rs10-tab-label — PREP|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|
|rs6-tab-link w-inline-block w-tab-link — SHOW|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|rs10-tab-label — SHOW|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|
|rs6-tab-link w-inline-block w-tab-link — CONNECT|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|rs10-tab-label — CONNECT|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|22px|
|l5-paragraph-white — The horses and their matching tack need to arrive toget|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|kicker2 — KEEP EVERYONE IN THE LOOP.|14px|14px|14px|14px|14px|14px|14px|14px|14px|15.3642px|16px|16px|
|l5-heading-dark — Now the team needs to know.|59.0878px|62.3447px|77.6025px|54px|54px|54px|54px|54px|54px|73.3025px|84.0042px|116px|
|l5-paragrapg-dark — Once I update the plan, I need the people affected to k|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|rs10-chat-left is-show w-inline-block — I’m really proud of the two-way SMS. It’s super helpful|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|l5-paragraph-white — I’m really proud of the two-way SMS. It’s super helpful|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|kicker2 — I’M USING IT, TOO|14px|14px|14px|14px|14px|14px|14px|14px|14px|15.3642px|16px|16px|
|l5-heading-dark — I see what works. And what doesn’t.|59.0878px|62.3447px|77.6025px|54px|54px|54px|54px|54px|54px|73.3025px|84.0042px|116px|
|l5-paragrapg-dark — I’m riding, doing prep, and using RingStatus with my te|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|rs10-chat-right is-show w-inline-block — My dad calls it ‘eating my own dog food’—whatever that |14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|l5-paragraph-white — My dad calls it ‘eating my own dog food’—whatever that |19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|kicker2 — BACK TO IT.|14px|14px|14px|14px|14px|14px|14px|14px|14px|15.3642px|16px|16px|
|l5-heading-dark — It’s no secret— show days are long.|59.0878px|62.3447px|77.6025px|54px|54px|54px|54px|54px|54px|73.3025px|84.0042px|116px|
|l5-paragrapg-dark — The goal is to use RingStatus to spend less of my day c|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|rs10-chat-left is-show w-inline-block — My horse and I both ride better when I’m less stressed.|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|l5-paragraph-white — My horse and I both ride better when I’m less stressed.|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|H2 — Your horses, right beside their classes.|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|
|l5-paragrapg-dark — The horses you follow appear in the schedule with their|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|rs10-chat-right is-show w-inline-block — Ring 6|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|text-block-4202 — Ring 6|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|rs10-chat-left is-show w-inline-block — Ring 6 - as of 10:33A  First: Low Children's Hunter Sta|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|text-block-4203 — Ring 6 - as of 10:33A  First: Low Children's Hunter Sta|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|H2 — Where's|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|
|H2 — Ring 6 at?|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|
|l5-paragrapg-dark — Text “Ring 6” to see the class, how many trips have gon|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|rs10-chat-right is-show w-inline-block — Ring 7|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|
|text-block-4202 — Ring 7|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|H2 — Just ask.|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|
|H2 — In a text.|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|
|l5-paragrapg-dark — I type “Ring 7” and send it. The reply comes back in th|19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|H2 — Reminders you can forward.|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|38px|
|l5-paragrapg-dark — You’ve got more to do than keep checking the schedule. |19px|19px|19px|19px|19px|19px|19px|19px|19px|19.1278px|20.8539px|26px|
|rd-status-big-top — WHAT IS YOUR|80px|80px|80px|96px|96px|96px|96px|96px|96px|96px|128px|128px|
|rs0-logo-link w-inline-block — STATUS ?|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|footer_icon-link-2 w-inline-block — |14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|footer_icon-link-2 w-inline-block — Instagram|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|footer_icon-link-2 w-inline-block — YouTube|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|lainey-link w-inline-block — Made by Lainey Posa|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|rs0-logo-link-footer w-inline-block w--current — RING STATUS|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|heading-1644 — BUILT FOR THE DAY YOU GET|14px|14px|14px|14px|14px|14px|14px|14px|14px|15.3642px|16px|16px|
|footer_icon-link-2 w-inline-block — |14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|footer_icon-link-2 w-inline-block — Instagram|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|footer_icon-link-2 w-inline-block — YouTube|14px|14px|14px|14px|14px|14px|14px|14px|14px|14px|14.4018px|18px|
|rs-page-link w-button w--current — HOME|14px|14px|14px|14px|14px|14px|14px|14px|14px|16.0938px|17.2589px|20.7px|
|rs-page-link w-button — TOOLS|14px|14px|14px|14px|14px|14px|14px|14px|14px|16.0938px|17.2589px|20.7px|
|rs-page-link w-button — ABOUT|14px|14px|14px|14px|14px|14px|14px|14px|14px|16.0938px|17.2589px|20.7px|
|rs-page-link w-button — LAINEY|14px|14px|14px|14px|14px|14px|14px|14px|14px|16.0938px|17.2589px|20.7px|
|nav-drop-link w-button — CONTACT|11px|11px|11px|12px|12px|14px|14px|14px|14px|16.0938px|17.2589px|20.7px|
|member-link is-footer w-button — MEMBERS|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|input-16 w-input — Enter your name|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|input-16 w-input — Enter your email|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|submit-button-5 is-rust w-button — Submit|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|paragraph-589 — We’re adding barns a few at a time to make sure everyth|16px|16px|16px|16px|16px|16px|16px|16.0046px|16.0142px|19.1278px|20.8539px|26px|
|footer-legal-link-2 — Terms|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|footer-legal-link-2 — Privacy|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|
|footer-legal-link-2 — Cookies|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|16px|

Raw evidence JSON includes family, weight, line-height, tracking, padding, dimensions, position, inheritance parent, opacity, and matching CSS rules for the audit.
