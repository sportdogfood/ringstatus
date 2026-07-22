# Manual Webflow Designer checklist: rs-edit Home schema

Date: 2026-07-20

Status: handoff only; no attributes were applied and nothing was published.

## Target

- Webflow site: `ringstatus`
- Site ID: `6982268b7543ac3c80151266`
- Page: `rs-edit`
- Page ID: `6a5e20479253fee5b5fbbe13`
- Total edit targets: 90 String nodes represented by 87 parent elements
- Single-key parents: 84
- Multi-key parents: 3 FAQ paragraphs

## How to apply each row

In Webflow Designer, locate the parent by its current element ID or Navigator label. Add the custom attribute exactly as shown. Preserve all existing attributes. For `data-rs-edit-keys`, preserve the pipe-delimited order; it is DOM String order.

| MCP | Navigator label | Parent type | Current parent element ID | Exact custom attribute |
| --- | --- | --- | --- | --- |
| mcp-hero | hero-kicker | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423deb` | `data-rs-edit-key=home:01:1f3180b2-9d46-453f-ce7f-e01e84625de1` |
| mcp-hero | hero-heading1 | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423dee` | `data-rs-edit-key=home:01:1f3180b2-9d46-453f-ce7f-e01e84625de4` |
| mcp-hero | hero-heading2-span | Span | `99f2f1bd-f6a0-0e70-b993-b88d23423df1` | `data-rs-edit-key=home:01:1f3180b2-9d46-453f-ce7f-e01e84625de7` |
| mcp-hero | hero-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23423df3` | `data-rs-edit-key=home:01:1f3180b2-9d46-453f-ce7f-e01e84625de9` |
| split-media1 | split-image1-kicker | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423e15` | `data-rs-edit-key=home:03:1f3180b2-9d46-453f-ce7f-e01e84625e0b` |
| split-media1 | split-image1-header | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423e17` | `data-rs-edit-key=home:03:1f3180b2-9d46-453f-ce7f-e01e84625e0d` |
| split-media1 | split-image1-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23423e19` | `data-rs-edit-key=home:03:1f3180b2-9d46-453f-ce7f-e01e84625e0f` |
| mcp-big-text | big-text-header1 | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423e4a` | `data-rs-edit-key=home:06:1f3180b2-9d46-453f-ce7f-e01e84625e40` |
| mcp-big-text | big-text-header2 | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423e4d` | `data-rs-edit-key=home:06:1f3180b2-9d46-453f-ce7f-e01e84625e43` |
| mcp-big-text | big-text-header2-span | Span | `99f2f1bd-f6a0-0e70-b993-b88d23423e4f` | `data-rs-edit-key=home:06:1f3180b2-9d46-453f-ce7f-e01e84625e45` |
| mcp-carousel1 | carousel1-kicker | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423ef6` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625eec` |
| mcp-carousel1 | carousel1-header | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423ef8` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625eee` |
| mcp-carousel1 | carousel1-header-span | Span | `99f2f1bd-f6a0-0e70-b993-b88d23423efa` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625ef0` |
| mcp-carousel1 | carousel1-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23423efd` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625ef3` |
| mcp-carousel1 | carousel1-card1-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423f0e` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f04` |
| mcp-carousel1 | carousel1-card1-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23423f10` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f06` |
| mcp-carousel1 | carousel1-card1-name | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f14` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f0a` |
| mcp-carousel1 | carousel1-card1-subtext | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f16` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f0c` |
| mcp-carousel1 | carousel1-card2-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423f25` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f1b` |
| mcp-carousel1 | carousel1-card2-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23423f27` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f1d` |
| mcp-carousel1 | carousel1-card2-name | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f2b` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f21` |
| mcp-carousel1 | carousel1-card2-subtext | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f2d` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f23` |
| mcp-carousel1 | carousel1-card3-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423f3c` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f32` |
| mcp-carousel1 | carousel1-card3-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23423f3e` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f34` |
| mcp-carousel1 | carousel1-card3-name | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f42` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f38` |
| mcp-carousel1 | carousel1-card3-subtext | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f44` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f3a` |
| mcp-carousel1 | carousel1-card4-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423f53` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f49` |
| mcp-carousel1 | carousel1-card4-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23423f55` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f4b` |
| mcp-carousel1 | carousel1-card4-name | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f59` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f4f` |
| mcp-carousel1 | carousel1-card4-subtext | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f5b` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f51` |
| mcp-carousel1 | carousel1-card5-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423f6a` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f60` |
| mcp-carousel1 | carousel1-card5-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23423f6c` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f62` |
| mcp-carousel1 | carousel1-card5-name | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f70` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f66` |
| mcp-carousel1 | carousel1-card5-subtext | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f72` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f68` |
| mcp-carousel1 | carousel1-card6-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23423f81` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f77` |
| mcp-carousel1 | carousel1-card6-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23423f83` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f79` |
| mcp-carousel1 | carousel1-card6-name | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f87` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f7d` |
| mcp-carousel1 | carousel1-card6-subtext | Block | `99f2f1bd-f6a0-0e70-b993-b88d23423f89` | `data-rs-edit-key=home:08:1f3180b2-9d46-453f-ce7f-e01e84625f7f` |
| mcp-cards-up | carousel1-card1-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d2342402a` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e84626020` |
| mcp-cards-up | carousel1-card1-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d2342402c` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e84626022` |
| mcp-cards-up | carousel1-card1-name | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424030` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e84626026` |
| mcp-cards-up | carousel1-card1-subtext | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424032` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e84626028` |
| mcp-cards-up | carousel1-card2-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23424041` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e84626037` |
| mcp-cards-up | carousel1-card2-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23424043` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e84626039` |
| mcp-cards-up | carousel1-card2-name | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424047` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e8462603d` |
| mcp-cards-up | carousel1-card2-subtext | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424049` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e8462603f` |
| mcp-cards-up | carousel1-card3-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23424058` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e8462604e` |
| mcp-cards-up | carousel1-card3-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d2342405a` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e84626050` |
| mcp-cards-up | carousel1-card3-name | Block | `99f2f1bd-f6a0-0e70-b993-b88d2342405e` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e84626054` |
| mcp-cards-up | carousel1-card3-subtext | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424060` | `data-rs-edit-key=home:10:1f3180b2-9d46-453f-ce7f-e01e84626056` |
| mcp-timeline | timeline-card1-kicker | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424073` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e84626069` |
| mcp-timeline | timeline-card1-header | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23424075` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e8462606b` |
| mcp-timeline | timeline-card1-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23424077` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e8462606d` |
| mcp-timeline | timeline-card1-tag1 | Block | `99f2f1bd-f6a0-0e70-b993-b88d2342407a` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e84626070` |
| mcp-timeline | timeline-card1-tag2 | Block | `99f2f1bd-f6a0-0e70-b993-b88d2342407c` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e84626072` |
| mcp-timeline | timeline-card1-tag3 | Block | `99f2f1bd-f6a0-0e70-b993-b88d2342407e` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e84626074` |
| mcp-timeline | timeline-card1-tag4 | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424080` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e84626076` |
| mcp-timeline | timeline-card2-kicker | Block | `99f2f1bd-f6a0-0e70-b993-b88d2342408c` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e84626082` |
| mcp-timeline | timeline-card2-header | Heading | `99f2f1bd-f6a0-0e70-b993-b88d2342408e` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e84626084` |
| mcp-timeline | timeline-card2-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23424090` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e84626086` |
| mcp-timeline | timeline-card2-tag1 | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424093` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e84626089` |
| mcp-timeline | timeline-card2-tag2 | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424095` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e8462608b` |
| mcp-timeline | timeline-card2-tag3 | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424097` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e8462608d` |
| mcp-timeline | timeline-card2-tag4 | Block | `99f2f1bd-f6a0-0e70-b993-b88d23424099` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e8462608f` |
| mcp-timeline | timeline-card3-kicker | Block | `99f2f1bd-f6a0-0e70-b993-b88d234240a8` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e8462609e` |
| mcp-timeline | timeline-card3-header | Heading | `99f2f1bd-f6a0-0e70-b993-b88d234240aa` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e846260a0` |
| mcp-timeline | timeline-card3-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d234240ac` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e846260a2` |
| mcp-timeline | timeline-card3-tag1 | Block | `99f2f1bd-f6a0-0e70-b993-b88d234240af` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e846260a5` |
| mcp-timeline | timeline-card3-tag2 | Block | `99f2f1bd-f6a0-0e70-b993-b88d234240b1` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e846260a7` |
| mcp-timeline | timeline-card3-tag3 | Block | `99f2f1bd-f6a0-0e70-b993-b88d234240b3` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e846260a9` |
| mcp-timeline | timeline-card3-tag4 | Block | `99f2f1bd-f6a0-0e70-b993-b88d234240b5` | `data-rs-edit-key=home:11:1f3180b2-9d46-453f-ce7f-e01e846260ab` |
| mcp-footer | footer-heading1 | Heading | `99f2f1bd-f6a0-0e70-b993-b88d234240ff` | `data-rs-edit-key=home:14:1f3180b2-9d46-453f-ce7f-e01e846260f5` |
| mcp-footer | footer-heading2 | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23424102` | `data-rs-edit-key=home:14:1f3180b2-9d46-453f-ce7f-e01e846260f8` |
| mcp-footer | footer-heading2-span | Span | `99f2f1bd-f6a0-0e70-b993-b88d23424104` | `data-rs-edit-key=home:14:1f3180b2-9d46-453f-ce7f-e01e846260fa` |
| mcp-quote | quote-heading1 | Heading | `99f2f1bd-f6a0-0e70-b993-b88d2342412d` | `data-rs-edit-key=home:15:1f3180b2-9d46-453f-ce7f-e01e84626123` |
| mcp-quote | quote-heading2 | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23424130` | `data-rs-edit-key=home:15:1f3180b2-9d46-453f-ce7f-e01e84626126` |
| mcp-quote | quote-heading2-span | Span | `99f2f1bd-f6a0-0e70-b993-b88d23424132` | `data-rs-edit-key=home:15:1f3180b2-9d46-453f-ce7f-e01e84626128` |
| mcp-faq | faq-header | Heading | `99f2f1bd-f6a0-0e70-b993-b88d2342415b` | `data-rs-edit-key=home:16:1f3180b2-9d46-453f-ce7f-e01e84626151` |
| mcp-faq | faq1-header | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23424160` | `data-rs-edit-key=home:16:1f3180b2-9d46-453f-ce7f-e01e84626156` |
| mcp-faq | faq1-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23424165` | `data-rs-edit-keys=home:16:1f3180b2-9d46-453f-ce7f-e01e8462615b|home:16:1f3180b2-9d46-453f-ce7f-e01e8462615e` |
| mcp-faq | faq2-header | Heading | `99f2f1bd-f6a0-0e70-b993-b88d2342416c` | `data-rs-edit-key=home:16:1f3180b2-9d46-453f-ce7f-e01e84626162` |
| mcp-faq | faq2-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d23424171` | `data-rs-edit-keys=home:16:1f3180b2-9d46-453f-ce7f-e01e84626167|home:16:1f3180b2-9d46-453f-ce7f-e01e8462616a` |
| mcp-faq | faq3-header | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23424178` | `data-rs-edit-key=home:16:1f3180b2-9d46-453f-ce7f-e01e8462616e` |
| mcp-faq | faq3-paragraph | Paragraph | `99f2f1bd-f6a0-0e70-b993-b88d2342417d` | `data-rs-edit-keys=home:16:1f3180b2-9d46-453f-ce7f-e01e84626173|home:16:1f3180b2-9d46-453f-ce7f-e01e84626176` |
| mcp-cta | cta-heading | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23424202` | `data-rs-edit-key=home:19:1f3180b2-9d46-453f-ce7f-e01e846261f8` |
| mcp-cta | cta-heading2 | Heading | `99f2f1bd-f6a0-0e70-b993-b88d23424205` | `data-rs-edit-key=home:19:1f3180b2-9d46-453f-ce7f-e01e846261fb` |
| mcp-cta | cta-heading2-span | Span | `99f2f1bd-f6a0-0e70-b993-b88d23424207` | `data-rs-edit-key=home:19:1f3180b2-9d46-453f-ce7f-e01e846261fd` |

## Page-level and control attributes

Add these without removing existing attributes:

| Target | Attribute | Value | Purpose |
| --- | --- | --- | --- |
| Page HTML/root element | `data-rs-edit-page` | `home` | Selects the Home allowlist. |
| Page HTML/root element | `data-rs-edit-api` | `https://ringstatus.com/test/rs-edit` | Overrides the default API root explicitly. |
| Save button or link | `data-rs-edit-save` | Empty value is sufficient | The client intercepts clicks and saves the dirty fields. |
| Status text element | `data-rs-edit-status` | Empty value is sufficient | Receives loading, ready, saved, and error messages. |

## Script connection

Add this page-footer script after the status and save controls:

```html
<script src="https://ringstatus.com/test/rs-edit/client.js" defer></script>
```

The client reads `data-rs-edit-page="home"`, loads `/content?page=home`, makes every bound text parent inline-editable, and submits dirty values to `/save`. The current approved destination for newly submitted text/background edits is `rs_content_edits`; `rs_content_actions` remains the source-definition table. Verify the deployed save route honors that destination before testing Save.

## Three multi-String FAQ parents

These rows intentionally use `data-rs-edit-keys`, not `data-rs-edit-key`. Each value contains two keys in DOM text-node order. The client must wrap and bind both text nodes independently; do not reverse or alphabetize them.

## Background pickers

The current client creates a color input only for allowlisted fields whose `field_type` is `color`. A background-capable target therefore needs a corresponding color allowlist entry and a binding attribute whose key resolves to that entry. The 90 rows above are text bindings only. Do not add speculative background keys from this checklist; use the established background records and `rs_content_edits` schema.

## Images

Image editing is deferred. Do not add image inputs or image binding attributes in this pass.

## Manual verification

1. Confirm all 87 parent elements have the exact listed attribute.
2. Confirm the three FAQ values retain their two-key DOM order.
3. Confirm the page root, Save control, Status control, and footer script are present.
4. Load the page without publishing only through an approved Designer/staging preview.
5. Confirm 90 fields load and every bound target becomes editable.
6. Confirm Save writes new records to `rs_content_edits`, never `rs_content_actions`.
7. Keep the site unpublished.

