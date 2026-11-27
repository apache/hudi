---
name: review-pr
description: Use when reviewing an Apache Hudi pull request, diff, or patch — "review this Hudi PR", "PTAL at apache/hudi#NNNNN", "does this change look safe", "what would a Hudi committer say about this", or when about to approve a change touching the write path, read path, timeline, metadata table, compaction, clustering, schema evolution, configs, or the Flink/Spark integrations. Also use before pushing your own Hudi PR to self-review it against the reviewer bar. Drafts comments only; never posts to GitHub without explicit per-PR authorization.
argument-hint: "<PR number, PR URL, branch, or nothing for the working diff>"
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# review-pr — review a Hudi PR the way Hudi committers do

Produce review comments that a Hudi committer would recognize as their own: correctness
first, specific line references, questions rather than assertions when uncertain, and
nothing posted until you are told to post.

This skill is the *procedure*. The *knowledge* — what Hudi reviewers actually scrutinize
and the invariants a change must not break — lives in two references, distilled from
30,530 review comments across 3,945 PRs by vinothchandar, nsivabalan, yihua, bvaradar,
and danny0405:

- [`references/reviewer-priorities.md`](references/reviewer-priorities.md) — priority
  order, must-check patterns, anti-patterns, code-style rules, and the Hudi domain
  glossary a reviewer reasons with.
- [`references/invariant-checklist.md`](references/invariant-checklist.md) — the
  BLOCKING / IMPORTANT gates: breaking changes, concurrency, resource leaks, layer
  violations, silent failure, metadata consistency, memory, file layout, schema evolution.

**Read both before writing a single comment**; do not restate their content here, and if
this file and a reference ever disagree, the reference wins.

## Arguments

`$ARGUMENTS` — a PR number, a PR URL, a branch name, or nothing. With nothing, review the
working diff against the base branch. If the base is ambiguous (`master` vs `branch-0.x`),
ask rather than guessing — the two have drifted and a comment citing the wrong file is worse
than no comment.

## Prerequisites

- `gh` authenticated for the target repo (`gh auth status`).
- A local checkout to read ground truth from. Reviewing from the diff alone is the single
  biggest source of false positives — see Step 2.
- `build-hudi` for building and testing the PR locally.

## Steps

### Step 1 — Gather the change and its intent [DETERMINISTIC]

```bash
gh pr view <PR> --json title,body,author,files,additions,deletions,baseRefName
gh pr diff <PR> --name-only        # scope first
gh pr diff <PR>                    # then the diff
```

Read the PR description and the linked JIRA/issue for the *intended* behavior change. Fetch
the PR page itself when the description is truncated or a discussion is already underway —
a comment repeating a point another reviewer made is noise.

Know the PR conventions CI enforces (the `validate-pr-and-source` job in
`.github/workflows/validate_source.yml`), so a convention comment is aimed correctly:

- **Title**: Conventional Commits (`fix(scope): …`); no JIRA or `[MINOR]` prefix.
- **Body**: template section order, starting with `### Describe the issue this Pull Request
  addresses`, then Summary and Changelog, Impact, Risk Level, Documentation Update,
  Contributor's checklist.
- **Commits**: any commit carrying a Claude co-author trailer or `<noreply@anthropic.com>` fails.
- **Merge** requires a Hudi committer or PMC member.

The base branch also decides which APIs exist — `master` is the 1.x line and `branch-0.x` the
0.x line. `build-hudi` has the per-branch facts.

**Outputs to capture**: repo and base branch (which together decide which APIs exist),
files touched grouped by module, the author's stated intent, what other reviewers have
already raised.

### Step 2 — Establish ground truth from the checkout, not the diff [DETERMINISTIC]

For every non-trivial hunk, open the file around the change and follow the call chain. The
diff shows what changed; it does not show what the change *breaks*.

- Trace every caller/consumer of a changed signature, return value, or field.
- Read the surrounding methods to learn the contract the change must fit.
- For a claimed pure refactor, find the observable difference or prove there is none.

**Outputs to capture**: for each hunk — the callers, the contract, and whether the change
holds at the boundaries.

### Step 3 — Run the invariant checklist [DETERMINISTIC]

Walk the changed code against
[`references/invariant-checklist.md`](references/invariant-checklist.md), then against the
must-check patterns and anti-patterns in
[`references/reviewer-priorities.md`](references/reviewer-priorities.md). Every BLOCKING
violation is a finding; IMPORTANT violations are findings unless the surrounding code
already accepts the tradeoff deliberately.

**Outputs to capture**: candidate findings, each with file, line, invariant violated, and
the concrete scenario that breaks.

### Step 4 — Try to break each candidate finding [DETERMINISTIC]

A candidate is not a finding until you have tried to refute it. Apply the adversarial
method in **`pr-correctness-review`** — falsify with a concrete counterexample, do
reachability analysis (a wrong value on a path no reader consumes is latent, not broken),
and trace the claim to ground-truth code rather than to the diff.

Drop any candidate you cannot state as: *given these inputs or this interleaving, this
specific wrong thing happens.* Downgrade "I think this might be wrong" to a question, and
keep it only if the question is one the author cannot answer by reading their own diff.

**Outputs to capture**: surviving findings with their failure scenario; discarded
candidates with the reason (useful — it stops you re-raising them next pass).

### Step 5 — Check completeness [DETERMINISTIC]

- Both COW **and** MOR, and every query type the change can reach (snapshot, incremental,
  read-optimized).
- Both engines when the path is shared — Spark and Flink diverge on primary-key
  nullability, rollback instant ordering, and split inline-compaction configs.
- Table-version differences (`HoodieTableVersion`); v6 vs v8/v9 layouts are not
  interchangeable.
- New code path, new branch, or new config without a test is a finding.
- New config: `ConfigProperty` with `.key()`, `.defaultValue()`, `.withDocumentation()`,
  `.sinceVersion()`, and `.withAlternatives()` if it renames an existing key.
- The repo's PR conventions from Step 1 — title format, template sections, a linked GitHub
  issue. These fail CI rather than break the code, so they belong in the summary, not as
  inline comments on the diff.

### Step 6 — Apply the net-positive principle [DETERMINISTIC]

A PR passes if it **strictly improves the current state**, even when imperfect. Ask: does
it close doors, break the storage format or a public API, make follow-on fixes cumbersome,
or leave the codebase worse? If no, it is approvable, and every remaining imperfection is a
non-blocking comment rather than a blocker. Do not hold a net-positive change hostage to a
nicer implementation you would have written.

Net-positive bounds the other direction too. For every piece of added surface, ask whether the
goal needs it. When a finding is met by a smaller shape (one condition, one value passed down, one
call removed), say so in the comment and mark the larger mechanism non-blocking; do not drive an
author to add machinery for an edge case the design can avoid. A PR that grew through review
rounds is a signal to step back and name the minimum, not to review the growth line by line.

## Report

Draft, do not post. Output one numbered subsection per finding, ordered most severe first.
Write the analysis as ordinary markdown text; fence only the text destined for GitHub:

`````
### N. `<file>:<line>` — blocking | non-blocking | question

**What's wrong.** One sentence.

**Why it matters.** The concrete failure — inputs or interleaving → wrong outcome.

**Comment to paste:**

```text
[non-blocking: | nit: ]<the comment text, exactly as it would appear on GitHub>
```
`````

Do not wrap the analysis in a code fence. Unfenced text re-flows to the width of the terminal
rather than forcing the reader to scroll sideways, `file:line` stays clickable, and inline
`code` renders. Fence only the paste body — that makes what goes to GitHub unambiguous and
copyable verbatim, backticks and all. Number the findings and keep the numbers stable across
passes: the user authorizes posting by number ("post 1, 3"), so an unnumbered list forces them
to count.

The paste body follows Hudi reviewer conventions, in the voice of `develop-changes` **Writing for people**:

- Natural and collegial, as a human reviewer writes. No emojis, no "BUG:", no invented
  category labels. Severity is the one prefix that does belong in the comment: open a
  non-blocking comment with `non-blocking:` and a cosmetic one with `nit:`, so the author can
  triage at a glance. Blocking comments and questions carry no prefix.
- Suggesting, not commanding: "Could you…", "It might be worth…", "I wonder if…", "Have you
  considered…" rather than "Fix this" / "Remove this".
- Two short-to-medium sentences carrying the key points, and nothing more. Concision counts
  for more than comprehensiveness here: a comment that lands one point the author acts on is
  worth more than one that covers every angle and gets skimmed. One sentence naming the
  problem, one giving the consequence or the fix, is the whole comment. Go past two only in
  the rare case where the finding is genuinely complex — a concurrency interleaving, a
  multi-step failure path, a storage-format incompatibility — and then add only the concrete
  scenario or rewritten snippet that saves the author a round trip, never restatement or
  hedging.
- Unsure? Ask. "Could this race with X if two writers run concurrently?" beats both silence
  and a wrong assertion. Questions are cheap; a missed bug in production is not.
- Prefer dropping a cosmetic comment altogether over prefixing it `nit:`; nits crowd out the
  correctness comments that justify the review.
- For a concurrency finding, include the thread interleaving with instant timestamps. That
  is how these get taken seriously here.

Close with a summary body under its own `### Summary body` heading, quoted as a blockquote so
it reads as text destined for GitHub rather than as your own commentary to the user. It needs
a neutral opener (not "LGTM", not "Solid work" — vary it: "Thanks for the contribution!",
"Thanks for working on this!"), 1–2 sentences on what the PR does rather than how good it is,
the key themes of the inline comments, any repo convention that will fail CI (Step 5), and an
explicit statement that this is one review pass, not an approval.

## Posting

**Never post to GitHub until the user says "post N, N" or "post all".** "Review this",
"PTAL", and "take a look" all mean draft-in-chat. When authorized, post with
`gh api` / `gh pr review` and report back exactly which comments went up.

**After addressing review comments** (yours or another reviewer's, e.g. when the user asks
you to fix a PR's findings), reply to each addressed thread in one short sentence, two at the
absolute most — start with `Done`/`Fixed`/`Addressed`, then say what changed, nothing more. No
restating the original comment back, no summary of the reasoning. Reply via
`gh api repos/<owner>/<repo>/pulls/<n>/comments/<comment_id>/replies -f body='...'`
(single-quote the body so backticks stay literal). Do not resolve other reviewers'
threads; the reply is the signal, resolution is theirs.

## When NOT to use this skill

- **Writing** the change rather than reviewing one → `author-changes` (design, style, tests) and
  `develop-changes` (general hygiene).
- **Performance claims** → `prove-performance`; a benchmark number in a PR description
  is not evidence until it is validated on real data.

## Common mistakes

| Mistake | Fix |
|---|---|
| Reviewing the diff alone | Open the files; trace the callers (Step 2) |
| Asserting a bug you can't reproduce | Turn it into a question, or drop it |
| Flagging a null check with no realistic null path | Drop it |
| Style and naming comments on a correctness pass | Separate nit pass, or omit |
| Commenting on test files without a logic error in the test | Skip |
| Re-raising what another reviewer already said | Read the thread first (Step 1) |
| Hypothetical performance concerns with no scale evidence | Drop it |
| Blocking a net-positive PR over a nicer alternative | Non-blocking comment (Step 6) |

## Related

- `pr-correctness-review` — the adversarial falsification method used in Step 4.
- `author-changes` — the author-side counterpart: how to write a change that survives this review.
- `build-hudi` — build/test the PR locally before claiming a test gap.
- `create-pr` — git/PR mechanics for pushing review replies.
