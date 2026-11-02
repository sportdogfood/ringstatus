# Keep an opened Sign in form visible

2 November 2026 — one correction awaiting approval.

Normal Member and Owner Profile flows passed from all five designated pages. A separate startup ordering check found that a late unrecognized result can close a form the visitor has already opened.

Change only the existing refresh expression in `webflow-cloud-test/src/assets/rs-recognition/widget-view.js`:

```diff
-show(!userId && showFormWhenUnrecognized)
+show(!userId && (showFormWhenUnrecognized || !panel.hidden))
```

The actual source reproduced the defect independently. The proposal passed five in-memory cases: preserve manual opening, preserve manual closing, keep untouched forms closed, hide for a recognized User, and retain explicit automatic opening. Identity, permissions, credentials, APIs, navigation and Profile save behavior stay unchanged.

Approval requested: apply this expression, verify the affected startup/normal entry behavior on all five pages, and deploy through the existing app-only /test route. No website publication or additional feature. Coordinate with Recognize and wait until its temporary diagnostic release is removed before preparing this release. Preserve all other owners' changes and the completed Profile evidence.

Evidence: main checkout `.git/ringstatus-control/profile-completion-20261102/all-pages-launcher-review/` and `all-pages-e2e-20261102.json`. Proposed source is not yet changed or deployed. Test fixtures are inactive again; original memberships and session were restored.
