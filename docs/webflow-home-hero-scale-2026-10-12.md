# Home hero scale — 12 October 2026

Home 6982268d7543ac3c801512cd. Owner requested rs-h1-xl-text-1-2 use var(--rs-heading-size, clamp(54px, 10cqw, 116px)) and larger desktop/higher text.

Saved native base font-size binding on rs-h1-xl-text-1-2. Existing Home head contract now scopes --rs-heading-size:clamp(54px,12cqw,132px) to rs0-big-text only at min-width:992px. All three hero headings consume the same variable. Below 992px and other sections retain existing scale. Desktop cap increased from 116px to 132px. Saved head exact readback passed; footer preservation passed. Rendered preview not measured in this turn. Not published or locked.
