# RingStatus Cloud app

Recognize-specific workflow, responsibilities and source map: [RingStatus Recognize README](../docs/recognize/README.md).

## Current live status — 2 November 2026

**Existing recognized User -> Profile -> existing Team verified live.** Start at https://ringstatus.com/recognize, choose **Open profile**, then **Team**. Direct app entry: https://ringstatus.com/test/rs-profile/ . Current Recognize and Profile/Team designs are preserved.

- Deployed commit `89abf609f816b6c9d10f05d301027c991a33280a`; existing Cloud `/test` environment deployment succeeded at 12:11:26 UTC on 2 November. Owner directed completion after the app-only deployment request. Codex did not publish the website.
- Actual browser: secure-cookie recognition -> published `/profile` -> existing real test User -> active linked Team and six-member roster. Reload passed. User, Team and membership IDs independently matched Airtable reads. No Profile/Team records were edited.
- Browser recognition, session and Profile responses were 200. App entry/assets return 200; unauthenticated Profile returns 401. The earlier missing-script/data blocker is resolved.
- Fresh phone/PIN entry, live saves/retries, live Owner status writes, GeoIP private-access confirmation and mobile acceptance remain unverified for this release. User level, Team creation/invitations, inline uploads and modified-date sorting remain unimplemented; no schema additions applied.
- Current private evidence: main checkout `.git/ringstatus-control/recognize-model-correction-20261101/live-acceptance-20261102.json` and `deployed-http-20261102.json`. Existing 16 scoped tests, source review and build remain prior preparation evidence.

The 1 November section below is historical preparation status, superseded by the live status above. Its pending-deployment statements no longer describe current operation.

## Recognize -> Profile -> Team test status — 1 November 2026

**Prepared locally; not deployed. No website publication authorized.**

- Proposed test address: https://ringstatus.webflow.io/test/rs-profile/ . This is not a ready or verified real-record test link yet.
- Source branch: `codex/profile-real-records-20261101`. Reviewed Profile connection: `95d249008b81ffb57a2f23b9552fcfa350f60a9d`; standalone test entry: `e07256ed8576af68bacd6bc4e900881d9e88b116`.
- Flow: existing Recognize identifies the user first; the server validates the cookie and exact `person_uid` before loading that User's real Profile and permitted existing Team. User Profile does not require Team membership. WEF access checks remain outside Recognize; GeoIP identity alone does not authorize private Profile data.
- Presentation: current `/recognize` dark fingerprint/single-input design and current `/profile` Profile/Team panels; no redesign or legacy `/rs-recognize` replacement.
- Real records: existing Recognize People and canonical User/Team/membership/role records. No sample identities or production business-record writes were used for verification.
- Passed: 16 scoped backend/service tests, independent bounded source review, local fixture browser journey, and build including the standalone entry. Live real-record journey remains **unverified**.
- Five page custom-code blocks (`/`, `/ring-status`, `/about-ringstatus`, `/recognize`, `/profile`) are cleaned in saved drafts only. Shared site code is preserved. Published pages still use their prior version.
- Remaining unimplemented controls: Independent/Preferred level storage, Team creation/invitations, inline file uploads and real modified-date sorting. No schema additions are approved or applied.

Existing Cloud app `d7d97751-20e1-4148-a5cf-ee58671c128a`, environment `110f06dd-c1ea-4839-98af-d829cbe77941`, mount `/test`, is still on release `833ccbcc5b6b1810164384f6ba187f528c2e82ba` (last checked 1 November). Its configured branch `work/inputs-crm-20261006` auto-deploys when pushed; **do not push or deploy without the outstanding app-only approval and shared release coordination**.

Next action: obtain approval to deploy only the prepared Cloud app under `/test`, resolve shared release reservations, then verify the actual test page against real records. Website publication remains prohibited. The application release must precede any later publication of the saved Profile loader.

Implementation and proposed backing decisions: [Profile real-record connection](../docs/coordination/profile-real-records-20261101.md). Private acceptance evidence is in the main checkout's `.git/ringstatus-control/recognize-model-correction-20261101/`; raw evidence is not committed here.

---

# Astro Starter Kit: Basics

```sh
npm create astro@latest -- --template basics
```

> 🧑‍🚀 **Seasoned astronaut?** Delete this file. Have fun!

## 🚀 Project Structure

Inside of your Astro project, you'll see the following folders and files:

```text
/
├── public/
│   └── favicon.svg
├── src
│   ├── assets
│   │   └── astro.svg
│   ├── components
│   │   └── Welcome.astro
│   ├── layouts
│   │   └── Layout.astro
│   └── pages
│       └── index.astro
└── package.json
```

To learn more about the folder structure of an Astro project, refer to [our guide on project structure](https://docs.astro.build/en/basics/project-structure/).

## 🧞 Commands

All commands are run from the root of the project, from a terminal:

| Command                   | Action                                           |
| :------------------------ | :----------------------------------------------- |
| `npm install`             | Installs dependencies                            |
| `npm run dev`             | Starts local dev server at `localhost:4321`      |
| `npm run build`           | Build your production site to `./dist/`          |
| `npm run preview`         | Preview your build locally, before deploying     |
| `npm run astro ...`       | Run CLI commands like `astro add`, `astro check` |
| `npm run astro -- --help` | Get help using the Astro CLI                     |

## 👀 Want to learn more?

Feel free to check [our documentation](https://docs.astro.build) or jump into our [Discord server](https://astro.build/chat).
