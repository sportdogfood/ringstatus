# Profile signed-out presentation and desktop correction

Baseline: 9fcbe20c5. Owner authorized edit/deploy; correction only.
Reuse current mountRecognize and existing identity/Profile service. Signed-out state hides and clears Profile, hides ten-icon launcher and previews; only Recognize toggle remains. Existing Phone/PIN/Invite form opens/closes from that toggle. Sign-in restores Profile; logout clears immediately. No server, auth, cache, refresh, Airtable, dependency or schema changes.
Desktop panel capped at existing 742px shell height rather than stretching to available monitor height. Desktop controls and content 16px; labels/footer14px. Mobile full-bleed rules unchanged.
Local rendered proof: sign-out shows only Open sign in; same toggle opens and closes form; fixture sign-in restores Profile and launcher; controls/content16px. Existing eight Profile checks pass. Build passes. Live verification follows deployment.
Rollback: revert this correction commit only.
