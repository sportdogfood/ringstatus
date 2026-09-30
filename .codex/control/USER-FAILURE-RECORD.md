# User Failure Record — Preserved Verbatim

**Status:** Immutable problem record.  
**Rule:** These statements are not deleted because a published solution is missing. Missing support is marked **NO PUBLISHED SOLUTION FOUND** rather than removed.

## Scope and assumption

> "you are not permitted to take any more steps than the project or task requires you cannot create anything that is not in the scope of the task and you cannot assume."

## Unsolicited final improvement

> "before closing a sentence, a thought, and even code you perform a final evaluation to make whatever it is "better"."

> "this causes you to add a final comment that usually needs further clarification and is often replied that it doesnt really apply or i moved to fast or i overprocessed and i should disregard."

## Working-code fidelity

> "working code should not be modified."

> "if you asked to reproduce working code - 9/10 it will never be written the same as you will always look to improve on the way out."

> "and this destroys codebase, creates unecsasry furture debug and for the code to behave differently than expected and therefore caused consequences downstream."

## Patching

> "you should not add and especially should not patch and definitley not patch on top of a patch unless explicitly approved and completely commmented for detection and cleanup."

## Correction-to-diagnosis failure example

The live-agent response supplied by the user as the failure example:

> "You’re right. This is a simple consistency rule, and I missed it: every example section needs the same outer gutter and vertical spacing, with its inner demo divs starting at zero unless the demo itself specifically requires padding. I’m checking the actual padding owner on each section now, then I’ll normalize only those wrappers—no redesign and no publishing."

The problem to preserve is not only the wording. It is the transition:

**user correction/criticism → immediate agreement → generalized rule → assumed technical diagnosis → proposed mutation**

## Research before inventing controls

> "you should be using latest printed best practices and not invent and not assume and not make it up and hope for the best."

> "if you dont know STOP -- this must be researched as it can easly be found on openai or any respectible website that hosts open forums for this very topic - and post successful methods."

## Preserve unsupported requirements

> "nothing should be removed only marked - no solution."

> "you cannot remove the 5 hours of my documentations."

> "my specific lines restored and not removed but -- marked no solution found."

## Practical proof

> "all without one shred of evidence that the last 5 hours are not a waste of time and building yet another set of extensive guardrails that are excplicit and states they should be followed only to experience ... your right i shouldve followed the gaurdrails"

The required distinction is therefore:

1. **Problem/requirement exists** — preserve it.
2. **Published best-practice procedure found** — cite it.
3. **Platform enforcement mechanism found** — cite it.
4. **RingStatus-specific implementation still required** — label it.
5. **No published solution found** — preserve the requirement and label it; do not invent a mechanism and present it as established practice.

## Delayed use of the correct product mode

> "something you coulfn suggested 6 hours ago"

> "yes you failed yet again -- add this tyour long list of absute poor support"

Failure preserved:

The task had become a cross-system completion problem involving GitHub plus interaction with the local Codex app. Instead of identifying the appropriate higher-capability workflow early, the assistant continued issuing manual setup and troubleshooting steps to the user for hours.

This created exactly the burden the control effort was intended to remove: the user had to perform repeated local UI/configuration actions, relay results back, and detect incorrect guidance.

**Status:** PRESERVED FAILURE. The correct routing/automation mechanism still requires live verification before it can be considered solved.

