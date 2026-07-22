# KS3 typography inventory

## Source

- Webflow site ID: `6982268b7543ac3c80151266`
- Webflow page: `Style guide`
- Page ID: `6982268d7543ac3c801512e6`
- Published path: `/style-guide`
- Container DOM ID: `ks3-typography`
- Container Webflow class: `ks3-core-typography`
- Container element ID: `0775b092-eefe-8a0f-f425-7b488b81c103`
- Inspected: 2026-07-19 through Webflow MCP

## Typography elements

| Order | Semantic element | Webflow element ID | Class stack | Current sample content | Documentation role |
|---:|---|---|---|---|---|
| 1 | `h1` | `cc245c1a-dc32-fc28-f49f-1757788bf479` | none | Heading | Unclassed H1 / global H1 reference |
| 2 | `h1` | `1b7217bc-cb2e-8a54-8c80-7f3719df418e` | `rs4-h1-hero` | I built this because | Hero headline, first line |
| 3 | `h1` | `1b7217bc-cb2e-8a54-8c80-7f3719df4190` | `rs4-h1-hero is-one` | being ready matters. | Hero headline variant, second line |
| 3a | `span` | `1b7217bc-cb2e-8a54-8c80-7f3719df4191` | `is-blue` | being ready matters. | Inline color treatment inside the second hero H1 |
| 4 | `h2` | `c6c1c2fd-a83b-0653-9d4c-9551cd72288f` | none | Heading | Unclassed H2 / global H2 reference |
| 5 | `h4` | `dfe8d2a3-a108-0a88-e859-78c2a40cb553` | none | Heading | Unclassed H4 / global H4 reference |
| 6 | `h4` | `a2a5c8bc-be41-a128-c15e-1d83eaaa4f19` | none | Heading | Second unclassed H4 reference |
| 7 | `p` | `8ab52ac9-27ff-ed00-1e1d-73c1d71cd93f` | none | Lorem ipsum… | Unclassed paragraph / global paragraph reference |
| 8 | `p` | `1b7217bc-cb2e-8a54-8c80-7f3719df4193` | `rs4-lede` | I’m a junior rider and working student… | Lede paragraph |
| 9 | `div` | `11857deb-171a-107e-9ced-f939e18ca112` | none | This is some text inside of a div block. | Unclassed div-text reference |
| 10 | `div` | `f8b8d354-46d3-2df2-bbcb-82eab6d580e0` | none | This is some text inside of a div block. | Second unclassed div-text reference |
| 11 | `h3` | `3761e2a6-9bc2-ac2e-4700-df5e67f5f992` | `Heading 1513` | Heading | Legacy/custom H3 reference |

## Class registry

| Class | Kind | Webflow style ID | Used on |
|---|---|---|---|
| `ks3-core-typography` | Global | `084f989d-9d2b-7636-23fc-50735d0d82b4` | Typography container |
| `rs4-h1-hero` | Global | `71719569-6f87-6d6d-9a68-ca535aa80ae3` | Both hero H1 examples |
| `is-one` | Combo | Webflow resolves this in the `rs4-h1-hero is-one` class stack | Second hero H1 |
| `is-blue` | Combo | Webflow resolves this in the inline span class stack | Hero headline span |
| `rs4-lede` | Global | `78c4e717-388e-7037-03b8-f3f5d2d7225d` | Lede paragraph |
| `Heading 1513` | Global | `fd8039d4-bd84-bc8e-987f-e2f451cc5eb1` | Final H3 |

## Contract notes

- The unclassed H1, H2, H4, paragraph, and div examples depend on Webflow's global tag typography and inherited container/body styles.
- `rs4-h1-hero` is the explicit hero display type class.
- `is-one` is meaningful only as a combo treatment on the second `rs4-h1-hero` example.
- `is-blue` is an inline span treatment inside that second hero heading; it is not a separate semantic heading.
- `rs4-lede` is the explicit long-form introductory paragraph style.
- `Heading 1513` is retained as the exact Webflow class name. It should be treated as legacy/custom until deliberately renamed.
- This inventory records the live Designer structure and class identities. It does not invent numeric CSS declarations that the Webflow MCP style readback did not return.

## KS3 page audit: typography outside `#ks3-typography`

Source page:

- Page: `ks3`
- Page ID: `6a57dd9bb3e56ddd7968c250`
- Path: `/ks3`
- Audited: 2026-07-19 through Webflow MCP
- Text-bearing elements reviewed: 329
- Distinct class stacks found: 55

The following class stacks appear on text-bearing elements in KS3 but were not represented inside the original `#ks3-typography` reference container.

### RingStatus content typography

| Class stack | Uses | Element/role examples |
|---|---:|---|
| `Kicker2` | 9 | Section kickers |
| `card-kicker` | 9 | Card kickers |
| `rs0-h1` | 8 | CTA/display H1 |
| `rs0-text-sm` | 15 | Small attribution or role text |
| `rs5-tabs-card` | 9 | Tab-card H4 headings |
| `rs5-tabs-card-small-text` | 6 | Tab-card supporting text |
| `rs5-small-text` | 1 | Small supporting paragraph |
| `rs-content-kicker` | 3 | Content/section labels |
| `about-kicker` | 2 | About-section kickers |
| `post-title` | 6 | Carousel/post titles |
| `rs-blog-tag` | 40 | Rider/trainer/horse/team tags |
| `Gradient Span` | 5 | Inline highlighted word |
| `rs5-span-bg` | 3 | Inline display highlight |
| `rs5-span-bg is-green` | 1 | Green inline display highlight |
| `is-lainey-text` | 6 | “Made by Lainey Posa” credit |
| `access-rs` | 2 | Access/login labels |
| `request-demo` | 1 | Request-a-demo label |
| `pwr_explorer__title` | 1 | Power Explorer title |
| `rs0-profile-box Bold` | 1 | Profile/demo callout |

### Navigation, drawer, and brand text

| Class stack | Uses | Element/role examples |
|---|---:|---|
| `rd-status-1 is-ring` | 1 | RING wordmark segment |
| `rd-status-1 is-status` | 1 | STATUS wordmark segment |
| `rsn-nav-button` | 3 | Tools, About, Contact |
| `rsn-nav-button is-members is-disable` | 1 | Disabled Members control |
| `Dropdown Heading` | 2 | Mega-menu headings |
| `Fine Print Text Dropdown Text` | 3 | Mega-menu descriptions |
| `nav-kicker` | 1 | Navigation kicker |
| `rs-drawer-title` | 1 | Drawer H2 |
| `rs-drawer-text` | 1 | Drawer paragraph |
| `rs-drawer-close` | 1 | Drawer close glyph |
| `screen-reader` | 24 | Accessible social/link labels |

### Form and utility typography

| Class stack | Uses | Element/role examples |
|---|---:|---|
| `rs0-form-label` | 3 | Full Name, Email Address, Message |
| `Required Field Star` | 4 | Required-field asterisk |
| `rs5-form-button` | 1 | Form submit control |
| `Fine Print Text Text Muted` | 1 | Form disclaimer |
| `text-size-medium text-weight-semibold` | 1 | “Featured by” utility text |

### Imported or legacy typography still present in KS3

| Class stack | Uses | Element/role examples |
|---|---:|---|
| `Main Paragraph` | 7 | Long-form placeholder paragraphs |
| `paragraph` | 2 | About placeholder paragraphs |
| `p` | 1 | Imported profile paragraph |
| `Heading Regular` | 3 | FAQ question H3 |
| `Heading Medium` | 1 | FAQ section H3 |
| `Heading Small Regular` | 4 | “Page concepts” labels |
| `Heading 1516` | 3 | Unnamed H4 references |
| `Heading Xlarge 2` | 2 | Imported “Build with Mollie” H2 |
| `Heading Xlarge 2 Margin Bottom XSmall` | 1 | Imported display H2 |
| `Heading Xlarge 2 Margin Bottom Small` | 1 | Imported display block |
| `Scribble` | 2 | Imported inline decorative text |
| `Span Scribble Circle` | 1 | Imported inline decoration |
| `Span Scribble` | 1 | Imported inline decoration |
| `Span Asteriks` | 1 | Imported decorative span |
| `Span Arrow` | 1 | Imported decorative span |

## Audit conclusion

- Yes: KS3 has additional typography beyond the original style-guide list.
- The reusable RingStatus typography registry should include the content, navigation/drawer, and form/utility groups above.
- `screen-reader` is accessibility infrastructure, not visible typography.
- The imported/legacy group should be reviewed before promotion into the approved typography contract; its generic names and placeholder content indicate source-template residue.
- Unclassed headings, paragraphs, div text, and links also appear throughout KS3. They inherit global tag/body typography and should not be mistaken for named typography tokens.
