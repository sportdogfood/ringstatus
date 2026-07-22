# KS3 raw style contract

## Purpose

`ks3-style` is the raw Webflow style ledger corresponding to the ordered class names present in `ks3-raw`. It supports visual comparison of typography and layout across KS3 sections and elements. It does not approve, normalize, merge, or reinterpret styles.

## Extraction boundary

- Source site: `6982268b7543ac3c80151266` (`ringstatus`)
- Source page: `6a57dd9bb3e56ddd7968c250` (`/ks3`)
- Style selection: every Webflow style record whose exact name occurs in `ks3-raw.class_names`
- Duplicate Webflow style names remain separate records when their style IDs differ
- One Airtable record represents one exact Webflow style ID

## Raw evidence

Each record preserves the style ID, name, type, combo/library flags, complete properties JSON returned by Webflow, and the complete raw style object. A link to every `ks3-raw` node carrying the same exact class name provides the common visual connection.

Readable columns project commonly reviewed typography and layout properties directly from the raw `base.properties` object. Blank means Webflow returned no value on that style; it does not mean a computed value is absent after inheritance.

## Prohibited interpretation

`ks3-style` does not contain required, approved, canonical, consistent, inconsistent, inherited, computed, resolved, or exclusion decisions. It does not collapse duplicate names or infer which duplicate style ID Webflow applied when the element tree supplies only a style name.

The raw payload currently returned by Webflow contains the `base` property bucket. The table records exactly that response and does not fabricate breakpoint or pseudo-state values.
