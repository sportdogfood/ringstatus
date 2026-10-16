# Agents skills inventory — 13 October 2026

Scope: complete paginated reads of Agents `skills` and `skills-included`, source-file existence and SHA256 comparison, reciprocal category links, and coverage of the three local skill roots. Source content was not fully behavior-reviewed or loaded for operational use. This inventory does not claim every installed third-party catalog skill is registered.

## Findings

- 20 categories; 127 individual/composite records.
- 119 records resolve to existing files: 115 recorded hashes match; four lacked hashes and were recorded from the existing files, then read back successfully. No mismatched recorded hashes or missing referenced files.
- Eight records have no exact source path; retain them as unresolved/procedure/composite entries, not verified standalone skills.
- Ten records lack category links.
- Three duplicate-source groups (six records) retain their IDs and links pending consolidation.
- No dangling or nonreciprocal category links detected.
- All 49 SKILL.md files in `.codex/skills`, `.agents/skills` and repository `.agents/skills` are represented. Local path presence does not prove session discovery or activation.

## Categories

| Category | Linked records | Category record |
| --- | ---: | --- |
| figma | 7 | rec0uqtO5RGeYXnVk |
| visuals | 3 | rec1LojuV6mVSQ9ed |
| zoho-catalyst | 1 | recDX3pgyAK0xxNzN |
| superpowers | 9 | recJ8kqXZGUecwAtL |
| cloudflare | 1 | recLHahYkSgS9UUt1 |
| vercel | 7 | recNGW06W8KpxDs0i |
| frontend | 4 | recOrJ79HIT79nxzV |
| sites | 1 | recOuXOidOudDDQJE |
| webflow | 30 | recSOyQ1sH9RgYb8U |
| data-analytics | 10 | recSVhbz8T4tr5fO8 |
| airtable | 3 | recSdSEfbE8vdPSUM |
| git | 2 | recW4NkjPBP3iMPhu |
| twilio | 6 | recYEOlTLS5Vk0TRc |
| skill-creator | 3 | recYWaPrpGJB7TO03 |
| computer-use | 1 | recdkpB0kY6TtPVV9 |
| product-design | 5 | recmp4ZrQDIlTvxmm |
| macos | 5 | recpPig2RfpST3Pl9 |
| documents-exports | 5 | rectj8RJ1wpafOsqg |
| ringstatus | 11 | recwIPmCiqjNKOroP |
| pages-files | 4 | recyyo7QxReZIbtkW |

## No exact source

| Record | Name | Existing review |
| --- | --- | --- |
| rec7MaoEMPoRkeHqg | context | No standalone file in inspected local roots; rs-coordinator covers this procedure. Not a separately loadable skill. |
| rec7XvGeyo8n56eeB | answers-images | No exact installed entry found in inspected current-version catalog; legacy/reference-only label. Do not claim loaded or silently substitute. |
| rec9prKV86buumzC7 | agent-browser-verify; select other catalog specialists as applicable | No exact installed entry found in inspected current-version catalog; legacy/reference-only label. Do not claim loaded or silently substitute. |
| recM9Bg3INNzFjbqi | observability | No exact installed entry found in inspected current-version catalog; legacy/reference-only label. Do not claim loaded or silently substitute. |
| recUrrX8XGt3BvBBB | capabilities | No standalone file in inspected local roots; rs-coordinator covers this procedure. Not a separately loadable skill. |
| recY0dUkxRDomAihA | vercel:nextjs | No exact installed entry found in inspected current-version catalog; legacy/reference-only label. Do not claim loaded or silently substitute. |
| receumDIbsPLIDZjB | answers-charts | No exact installed entry found in inspected current-version catalog; legacy/reference-only label. Do not claim loaded or silently substitute. |
| recoNkPB2YLkH3mBZ | react-best-practices | Ambiguous unqualified name: two installed sources (build-web-apps and vercel). Choose the named plugin per task; no single revision assigned. |

## Uncategorized records

- review-agent — rec0K8Qr0yhX2pOPH
- screenshot — rec2PhGoVbrO6iPFy
- find-skills — rec4U0SlWaDqPG5ac
- tenants — recMGLPqzCzEgshHq
- ag-update — recNcoatIXKB82puc
- openai-docs — recOCDmY4tOyuGotx
- recognize — recP2b6wfeYCi2mbG
- skill-installer — recPTldOAHiUSA6hQ
- skill-creator — recmJvhl9lcKAMGl4
- rs-coordinator — recoSiGs6XlHpymx1

## Shared source files

- `c:/users/gombc/.codex/plugins/cache/openai-curated-remote/build-web-apps/0.1.2/skills/frontend-testing-debugging/skill.md`: frontend-testing-debugging (rec8cLUoqA6ynRfj3); frontend-testing-debugging; no dedicated Astro skill in current catalog (recBMuR7TZ2GjOdp9)
- `c:/users/gombc/.codex/plugins/cache/openai-curated-remote/build-web-apps/0.1.2/skills/frontend-app-builder/skill.md`: Webflow MCP webflow_guide_tool and agent instructions; build-web-apps:frontend-app-builder (recJYiO9O0Gego0Gi); build-web-apps:frontend-app-builder (recuSBkCVMajUCNcy)
- `c:/users/gombc/.agents/skills/code-review/skill.md`: code-review; applicable repository instructions (recvR6qfyaFd6aPyu); code-review (recy5Sw5J2TJjbunN)

## Full record inventory

Source status below is the captured before-update result. The four Hash absent entries have real source files; only their factual revision metadata is updated in this pass. Names, category links and skill contents are unchanged.

| Name | Categories | Source check | Issues | Record |
| --- | --- | --- | --- | --- |
| review-agent | Uncategorized | Hash matches | Uncategorized | rec0K8Qr0yhX2pOPH |
| shadcn; other specialists only when existing stack requires them | frontend | Hash matches | None found in inventory checks | rec0oUFg5bBtwKwtu |
| kpi-reporting | data-analytics | Hash matches | None found in inventory checks | rec1gZgqvSWLLDqTP |
| screenshot | Uncategorized | Hash matches | Uncategorized | rec2PhGoVbrO6iPFy |
| twilio-security-api-auth | twilio | Hash matches | None found in inventory checks | rec2aGkzm4oS4cQbQ |
| audit | product-design | Hash matches | None found in inventory checks | rec2k8SOeZ5llRHtc |
| auth | vercel | Hash matches | None found in inventory checks | rec3uCewsBegCymNC |
| webflow-mcp:link-checker | webflow | Hash matches | None found in inventory checks | rec40pvR7LG2tz0jt |
| url-to-code | product-design | Hash matches | None found in inventory checks | rec42LhLd1NdsI0Xc |
| find-skills | Uncategorized | Hash matches | Uncategorized | rec4U0SlWaDqPG5ac |
| twilio-sms-send-message | twilio | Hash matches | None found in inventory checks | rec4V3ftoyCA7fQPf |
| webflow-mcp:cms-collection-setup | webflow | Hash matches | None found in inventory checks | rec51ubOavhftgoI3 |
| twilio-cli-reference | twilio | Hash matches | None found in inventory checks | rec6ax0hpPofZi2lx |
| webflow-mcp:safe-publish | webflow | Hash matches | None found in inventory checks | rec6bJ3XiE4AO6qqc |
| build-dashboard | data-analytics | Hash matches | None found in inventory checks | rec6l3N6BPp5xnSKa |
| context | ringstatus | No source path | No exact source | rec7MaoEMPoRkeHqg |
| answers-images | visuals | No source path | No exact source | rec7XvGeyo8n56eeB |
| webflow-code-component:convert-component | webflow | Hash matches | None found in inventory checks | rec7uhbL1CSjOSc8U |
| webflow-mcp:site-activity | webflow | Hash matches | None found in inventory checks | rec82O4mO1EqXQ6Lp |
| frontend-testing-debugging | frontend | Hash matches | Shared source with another record | rec8cLUoqA6ynRfj3 |
| test-triage | macos | Hash matches | None found in inventory checks | rec9Lc33hb5AqLBdL |
| data-analytics:index | data-analytics | Hash matches | None found in inventory checks | rec9cm4YxYMIAGKxl |
| agent-browser-verify; select other catalog specialists as applicable | vercel | No source path | No exact source | rec9prKV86buumzC7 |
| figma-generate-design | figma | Hash matches | None found in inventory checks | recAHZiKpSby7GDyG |
| prepare-plugin-submission | skill-creator | Hash matches | None found in inventory checks | recAYiBMBXlZ6ra3x |
| frontend-testing-debugging; no dedicated Astro skill in current catalog | webflow | Hash matches | Shared source with another record | recBMuR7TZ2GjOdp9 |
| rebalance | ringstatus | Hash matches | None found in inventory checks | recD8yirN4z3TNHh1 |
| webflow-cli:troubleshooter | webflow | Hash matches | None found in inventory checks | recDKveaNYWUoik8s |
| documents | documents-exports | Hash matches | None found in inventory checks | recDeIvGcRSxk5Kna |
| build-report | data-analytics | Hash matches | None found in inventory checks | recDqhhjGn897bKqV |
| validate-data | data-analytics | Hash matches | None found in inventory checks | recE4GVrshtq0gX52 |
| superpowers:dispatching-parallel-agents | superpowers | Hash matches | None found in inventory checks | recFfErF8mi3o5bNh |
| webflow-mcp:asset-audit | webflow | Hash matches | None found in inventory checks | recGG7EKbPxBZYyiB |
| figma-use-figjam | figma | Hash matches | None found in inventory checks | recGWzWfayE7urDhS |
| build-run-debug | macos | Hash matches | None found in inventory checks | recHJxpBLkQ5Z91vl |
| build-macos-apps:swiftui-patterns | macos | Hash matches | None found in inventory checks | recHulUEN4hM78zjD |
| handoff | ringstatus | Hash matches | None found in inventory checks | recIdbAqqtWxCVg9t |
| Webflow MCP webflow_guide_tool and agent instructions; build-web-apps:frontend-app-builder | webflow | Hash matches | Shared source with another record | recJYiO9O0Gego0Gi |
| receiving-code-review | superpowers | Hash matches | None found in inventory checks | recJcfP7eSVfFNu1V |
| ai-sdk | vercel | Hash matches | None found in inventory checks | recKDYLk7GLcF5X1t |
| observability | vercel | No source path | No exact source | recM9Bg3INNzFjbqi |
| tenants | Uncategorized | Hash matches | Uncategorized | recMGLPqzCzEgshHq |
| boundaries | ringstatus | Hash matches | None found in inventory checks | recNYg11KA3uJmNjM |
| ag-update | Uncategorized | Hash matches | Uncategorized | recNcoatIXKB82puc |
| openai-docs | Uncategorized | Hash matches | Uncategorized | recOCDmY4tOyuGotx |
| figma-generate-library | figma | Hash matches | None found in inventory checks | recOTtqDdiRy9k5ed |
| image-to-code | product-design | Hash matches | None found in inventory checks | recP0ruyJjHtW8baG |
| recognize | Uncategorized | Hash matches | Uncategorized | recP2b6wfeYCi2mbG |
| visualize | visuals | Hash matches | None found in inventory checks | recP5Kue0A75ODkxI |
| skill-installer | Uncategorized | Hash matches | Uncategorized | recPTldOAHiUSA6hQ |
| webflow-mcp:compress-cms-image | webflow | Hash matches | None found in inventory checks | recQprSnohblAWCzb |
| pdf | documents-exports | Hash matches | None found in inventory checks | recReExHraFrM8XGk |
| webflow-cli:devlink | webflow | Hash matches | None found in inventory checks | recSMpQQMY3WOVwQL |
| superpowers:writing-plans | superpowers | Hash absent | Hash absent | recSW0Ik8ayMa7Ci9 |
| swiftpm-macos | macos | Hash matches | None found in inventory checks | recSbKyKE334wdlnZ |
| webflow-code-component:component-audit | webflow | Hash matches | None found in inventory checks | recTXJHihUUCmiGzt |
| visualize-data | data-analytics | Hash matches | None found in inventory checks | recTXgfJu8cNRfHHd |
| update-plugin | skill-creator | Hash matches | None found in inventory checks | recUOMq2AaOdS8Zk1 |
| capabilities | ringstatus | No source path | No exact source | recUrrX8XGt3BvBBB |
| metric-diagnostics | data-analytics | Hash matches | None found in inventory checks | recVA080uBQQHYMB2 |
| webflow-mcp:figma-to-webflow | webflow | Hash matches | None found in inventory checks | recW1ozym9t0Fp3XN |
| webflow-code-component:pre-deploy-check | webflow | Hash matches | None found in inventory checks | recWKE3c7wLDuhHyG |
| airtable:airtable-overview | airtable | Hash matches | None found in inventory checks | recWu6UcbnbTtqLtH |
| vercel:nextjs | vercel | No source path | No exact source | recY0dUkxRDomAihA |
| webflow-mcp:flowkit-naming | webflow | Hash matches | None found in inventory checks | recYaaFCCAB2SFdn2 |
| webflow-cli:cloud | webflow | Hash matches | None found in inventory checks | recZMQQP5uMf2zD0h |
| webflow-mcp:custom-code-management | webflow | Hash matches | None found in inventory checks | recZkKUqk9uV28703 |
| ringstatus-control | ringstatus | Hash matches | None found in inventory checks | recZm1TLDiKu9bCew |
| product-design:index | product-design | Hash matches | None found in inventory checks | recZoN219AOanIF4l |
| webflow-university:mcp-getting-started | webflow | Hash matches | None found in inventory checks | recblWMcZ4fzXYRdQ |
| airtable-filters | airtable | Hash matches | None found in inventory checks | recc25mXZCDOyZyD9 |
| superpowers:brainstorming | superpowers | Hash absent | Hash absent | reccfmiCOqeWTwgJo |
| app-6a16bcb9a37081919b0db1d81010fb2f:catalyst-by-zoho | zoho-catalyst | Hash matches | None found in inventory checks | reccztqCWxdkITY2d |
| coding-instructions | git | Hash matches | None found in inventory checks | recd4oLNjL3FwMGW1 |
| twilio-debugging-observability | twilio | Hash matches | None found in inventory checks | recdyVrdyaAI8Ucka |
| systematic-debugging | superpowers | Hash matches | None found in inventory checks | rece3AWLRLAlxzd10 |
| request-router | ringstatus | Hash matches | None found in inventory checks | rece5Gf8RZVAyo15E |
| subagent-driven-development | superpowers | Hash matches | None found in inventory checks | receYto8ywA6XAyxa |
| answers-charts | documents-exports | No source path | No exact source | receumDIbsPLIDZjB |
| ringstatus-workflow | ringstatus | Hash matches | None found in inventory checks | recfN4BXLSeTbZDBK |
| webflow-code-component:local-dev-setup | webflow | Hash matches | None found in inventory checks | recgHyflyY88CrHB6 |
| skill-creator; plugin-management:plugin-management; plugin-creator:create-plugin | skill-creator | Hash matches | None found in inventory checks | recgVXC6769AaJvm3 |
| webflow-mcp:bulk-cms-update | webflow | Hash matches | None found in inventory checks | rechCiH85sGdEVTjt |
| deployments-cicd | vercel | Hash matches | None found in inventory checks | recigz1wCTxWGFJGz |
| maintain-space | pages-files | Hash matches | None found in inventory checks | reckAg7QBSc1sDIP3 |
| webflow-cli:designer-extension | webflow | Hash matches | None found in inventory checks | reckXf2SuDlzUO0id |
| analyze-data-quality | data-analytics | Hash matches | None found in inventory checks | recksDcVjF1MCQFKg |
| packaging-notarization; other specialists as applicable | macos | Hash matches | None found in inventory checks | recl1mCdlD59aZo1W |
| figma-generate-diagram | figma | Hash matches | None found in inventory checks | reclOHlD3Whx8Cn1M |
| skill-creator | Uncategorized | Hash matches | Uncategorized | recmJvhl9lcKAMGl4 |
| webflow-mcp:review-comments | webflow | Hash matches | None found in inventory checks | recmQTZgj6jB9pIIX |
| pages:write-page | pages-files | Hash matches | None found in inventory checks | recmbRyMwbO3FgM1Q |
| instruction-loader | ringstatus | Hash matches | None found in inventory checks | recmqWLGGMm0j6nmw |
| superpowers:executing-plans | superpowers | Hash absent | Hash absent | recmtGm8P6zX2EDWu |
| figma:figma-use | figma | Hash matches | None found in inventory checks | recnGpBZ5n8mamHkQ |
| create-data-context | data-analytics | Hash matches | None found in inventory checks | recnW7Dw52ZTE4HNl |
| Presentations | documents-exports | Hash matches | None found in inventory checks | recnmjLW7PzzCahmz |
| react-best-practices | frontend, vercel | No source path | No exact source | recoNkPB2YLkH3mBZ |
| rs-coordinator | Uncategorized | Hash matches | Uncategorized | recoSiGs6XlHpymx1 |
| organize-space | pages-files | Hash matches | None found in inventory checks | recpD8Ff61RpvkOUb |
| airtable-cli | airtable | Hash matches | None found in inventory checks | recpTOU0IXov2Oh8t |
| imagegen; image_gen image generation/editing tool | visuals | Hash matches | None found in inventory checks | recpV1WvIieYJqonw |
| planning | ringstatus | Hash matches | None found in inventory checks | recpq4X9tgB1Ggift |
| webflow-code-component:component-scaffold | webflow | Hash matches | None found in inventory checks | recr2XSA6uNhmeUhj |
| webflow-code-component:troubleshoot-deploy | webflow | Hash matches | None found in inventory checks | recrEKckIsqvIGymO |
| figma-design-to-code | figma | Hash matches | None found in inventory checks | recrkubs4ouxjRLXE |
| ideate | product-design | Hash matches | None found in inventory checks | recrv2wQbfOMm2LZ2 |
| twilio-messaging-webhooks | twilio | Hash matches | None found in inventory checks | recsWkWtqZJTMJFe4 |
| webflow-mcp:accessibility-audit | webflow | Hash matches | None found in inventory checks | rect0skicOAZv6yjh |
| twilio-developer-kit:twilio-messaging-overview | twilio | Hash matches | None found in inventory checks | rectX90sfPFfrPror |
| verification-before-completion | superpowers | Hash matches | None found in inventory checks | recttx4FIat4GvI32 |
| build-web-apps:frontend-app-builder | frontend | Hash matches | Shared source with another record | recuSBkCVMajUCNcy |
| code-review; applicable repository instructions | git | Hash matches | Shared source with another record | recvR6qfyaFd6aPyu |
| webflow-code-component:deploy-guide | webflow | Hash matches | None found in inventory checks | recvS2fQpi6ygYaOJ |
| webflow-mcp:site-audit | webflow | Hash matches | None found in inventory checks | recwF4OQgofOGN4Vb |
| webflow-mcp:cms-best-practices | webflow | Hash matches | None found in inventory checks | recwwvNfWLstS67i7 |
| Spreadsheets | documents-exports | Hash matches | None found in inventory checks | recx4grIMxetm3eZg |
| manage-schedules; openai-library:library | pages-files | Hash matches | None found in inventory checks | recxCwgdBemp6TFrD |
| sites:sites | sites | Hash matches | None found in inventory checks | recxH8cr1L2OTo7H4 |
| computer-use:computer-use | computer-use | Hash absent | Hash absent | recxqpab2C2WgTBc6 |
| code-review | ringstatus | Hash matches | Shared source with another record | recy5Sw5J2TJjbunN |
| Cloudflare specialists — available | cloudflare | Hash matches | None found in inventory checks | recyLpeTdFo3yGw4Q |
| design-kpis | data-analytics | Hash matches | None found in inventory checks | recyjlD4hinby2UEu |
| requesting-code-review | superpowers | Hash matches | None found in inventory checks | recyjoS3gwzqSbo7O |
| figma-code-connect | figma | Hash matches | None found in inventory checks | recyrXGrczA1BLBaQ |
| webflow-cli:code-component | webflow | Hash matches | None found in inventory checks | reczmyFHNlIOUmPtY |
| webflow-mcp:designer-tools | webflow | Hash matches | None found in inventory checks | reczvJ306z2R0J75q |

## Resume and evidence

Existing inventory task: https://airtable.com/appZahVgD156cMAe3/tbluT1yhP1FJMb8Cm/recxWQh2x0bMoHRpY . Remaining consolidation: resolve eight non-exact entries, categorize ten records, and reconcile three source duplicates without deleting IDs or losing category references. Current full vendor catalog reconciliation remains separate work.

Raw table snapshot: `.git/ringstatus-control/skills-inventory-20261013-source.json`. Analysis, source hashes and local coverage: `.git/ringstatus-control/skills-inventory-20261013-analysis.json`. CSV contains every record with exact path and hashes. No operational agents resumed; no skills, runtime code or table schema changed.

Inventory completion: all four revision updates, the existing task update and Agents change record were read back. Change: https://airtable.com/appZahVgD156cMAe3/tblilqxgvWvQOS83B/recJyAO2J5xoQAghw . Remaining findings are listed above; no consolidation or behavioral validation is implied.
