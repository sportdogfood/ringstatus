# Profile v3 desktop sizing — 4 November 2026

Owner approved desktop sizing, right alignment, unchanged mobile, all ten desktop launcher icons visible, testing and deployment. Horses stay on hold. Airtable read-only lookup confirmed profiles / tenants tenant_id 39138 is Team 39138 with two linked memberships; no horse or mapping changes are included.

Baseline: 615191fbb7408babdca45b0f2c01da18acb5763c. Dedicated clean branch/worktree excludes unpublished name, toggle and horse work in profile-v3-prepare-20261104.

Reference: Schedule V2 rings.css at 99ef26a377418be237c9f53d08188755cf8221dc. Desktop above 600px: 820px shell, existing 18px horizontal content padding, 24px shell radius; shared action/tab controls use 14px text, 40px minimum height, 6px 12px padding, 7px gap and 10px radius. Preserve right alignment and Profile's existing launcher lane; the panel uses available viewport height. The launcher widens with the panel, retaining 44px desktop controls. Explicit compact previews and mobile are excluded from these rules.

Runtime delta is styles.css only. No dependencies, application logic, source records, Schedule files or Recognize/backend changes. Rollback is a targeted revert of this CSS commit through the existing Cloud deployment branch, not a rollback of other applications or data.

Rendered checks: PASS at 1440x900 desktop dark/light and 393x852 mobile dark/light. Desktop shell measured 820px, controls 40px with 14px text, panel and launcher share their right edge, and all ten launcher controls lie within the launcher. Additional 2560x1440 and 601x900 checks passed; no panel/launcher overlap. Mobile geometry and computed action/tab sizing exactly matched the pre-change measurements in both themes. Mobile retains its existing horizontal launcher scrolling; simultaneous ten-icon visibility applies to the widened desktop launcher. Evidence: v3-sizing-evidence/measurements.json and four screenshots. Viewport override reset after tests.

Deployment evidence: pending. No production record writes were performed during sizing verification.
