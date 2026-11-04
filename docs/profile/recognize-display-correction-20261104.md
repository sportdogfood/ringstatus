# Profile signed-out presentation and desktop correction

Baseline: 9fcbe20c5. Owner authorized edit/deploy; correction only.
Reuse current mountRecognize and existing identity/Profile service. Signed-out state hides and clears Profile, hides ten-icon launcher and previews; only Recognize toggle remains. Existing Phone/PIN/Invite form opens/closes from that toggle. Sign-in restores Profile; logout clears immediately. No server, auth, cache, refresh, Airtable, dependency or schema changes.
Desktop panel capped at existing 742px shell height rather than stretching to available monitor height. Desktop controls and content 16px; labels/footer14px. Mobile full-bleed rules unchanged.
Local rendered proof: sign-out shows only Open sign in; same toggle opens and closes form; fixture sign-in restores Profile and launcher; controls/content16px. Existing eight Profile checks pass. Build passes. Live verification follows deployment.
Rollback: revert this correction commit only.
`nLive: eb4c9e598 deployment Success observed. /profile signed out renders only Open sign in. Toggle opens existing branded Phone, PIN, or Invite form; same toggle closes it back to button only. Live credential submission not performed; sign-in/logout/reload verified with local fixture. Screenshot recognize-corrected-live.png.

Final stack correction: Recognize uses Profile shared primitives/styles and back spacer, brand, section, footer markup; optional profile/actions/tabs/search are absent when signed out. Existing mountRecognize events and identity adapter preserved; default Recognize consumers unchanged. No page title. Input16px. Measured Recognize Only desktop reference420px; Profile/launcher now420px. Short-content Profile measured434.75px high; sign-in216.95px, no forced empty height. All ten desktop launcher icons verified inside bounds. Twelve targeted tests and build pass. Mobile full-bleed rule retained. Rollback is targeted revert of this correction only.
