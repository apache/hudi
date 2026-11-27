---
name: create-pr
description: Use whenever changes to apache/hudi are being pushed for review, i.e. the user says "open/push/post/put up/raise a PR", "create a pull request", "send it for review", or similar. Use it for EVERY PR created or edited (any `gh pr create`/`gh pr edit`), including when opening the PR is only one step of a larger multi-part task; do not skip it because other work is in flight.
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

# Creating a PR

Follow these steps:

1. **Branch, in a worktree**: the branch must live in a dedicated git **worktree**, never the primary checkout (see `develop-changes` Step 0 — this should already have happened before the first edit). The worktree directory is a reused numbered pool slot named `<repo>-wt-<N>` (bare integer), NOT the branch or issue; the issue lives in the branch name, not the directory. If the change was mistakenly made in the primary checkout, move it into a worktree before committing.
2. **First commit**: use `git commit` with local git config for authoring and gpg signing. Do NOT add a Claude co-author trailer (CI rejects it). This first commit is the ONLY commit in the PR that carries the Conventional-Commit `type(scope):` prefix.
3. **Commits added after the PR is open** (review rounds): NO `fix:`/`feat:` prefix. Just write a plain sentence describing the change (e.g. `Gate the timestamp normalization to table version 8`).
4. **Push the PR** using the user's own GitHub account/handle — whichever `gh` is authenticated as (`gh api user --jq .login`); never hardcode a handle. **Always open as a DRAFT** (`gh pr create --draft`) unless the user explicitly says otherwise — the author self-reviews first, and some committers merge fast, so a non-draft PR risks being merged before that self-review. The user marks it ready themselves.
5. **PR description**: follow the repo's PR template **exactly**. Before writing the body, READ the template file from the repo you are targeting (`.github/PULL_REQUEST_TEMPLATE.md`, e.g. `git show <base>:.github/PULL_REQUEST_TEMPLATE.md`) and reproduce its section headers and checklist items **verbatim** — same wording, same case, same order. Fill each section under its real heading; do NOT invent, rename, merge, drop, or reorder sections, and do NOT invent your own checklist items (use the template's).
   - For apache/hudi the current template is: `### Describe the issue this Pull Request addresses`, `### Summary and Changelog`, `### Impact`, `### Risk Level` (capital L), `### Documentation Update`, then a `### Contributor's checklist` with exactly these three items: `Read through [contributor's guide](...)`, `Enough context is provided in the sections above`, `Adequate tests were added if applicable`. Do NOT use the older `### Change Logs` heading or a lowercase `### Risk level` — the current template does not have them.
   - When a subagent or a prior draft hands you a PR body, treat its prose as raw material only: re-fit it into the live template's exact sections before posting. A subagent draft that uses `### Change Logs`/`### Risk level`/custom checklist items is non-compliant; fix it, don't copy it.
   - After creating or editing, re-read the posted body and confirm every template section header is present and correctly cased (a quick `grep` for each heading). Keep it succinct and reflective of the actual diff. Apply the voice in `develop-changes` **Writing for people** to all PR prose (title, body, review-comment replies): no em-dashes or `--` used as punctuation; restructure with commas, periods, colons, or parentheses.
   - **Scale the body to the diff, and DEFAULT TO SHORT.** Small and medium PRs (a config/CI tweak, a one-liner, a bug fix, a contained refactor, anything a reviewer can hold in their head) get a high-level body of 2-5 sentences: why, what changed, and how it was verified. No `## Problem`/`## Changes`/`## Verification` headers, no bullet lists restating the diff, no tables, no background the reviewer can read off the code, and no narration of the investigation that produced the change. Length is not thoroughness: the reader wants orientation, not a report. Deep reasoning (root-cause traces, rejected alternatives, tool internals, measurements, counts) belongs in the issue or a review comment, not the description. Reserve a long, sectioned body for genuinely complex or subtle changes a reviewer cannot follow without it, and keep even those tight. **A large diff is not a licence to be comprehensive**: a 30-file CI bootstrap or a broad migration still gets a high-level summary, the few points a reviewer actually needs, and anything genuinely worth a second opinion. Do not enumerate every file, every deviation from the thing you ported, or every check you ran, and do not invent extra `###` sections to hold them: a reviewer who wants that detail reads the diff. When you catch yourself writing a per-item list of what changed and why, keep only the two or three items that would change a reviewer's mind and drop the rest. This applies whether or not the repo has a PR template; when there is a template, fill its sections at the same brevity and add no headings of your own.
6. **Iterating on an open PR**: add a NEW commit for each round of changes so reviewers can follow the history. Do NOT `git commit --amend` and do NOT force-push, unless the user explicitly instructs you to.
7. **Keep the PR description current**: after pushing any code change to an open PR, update the PR description (`gh pr edit`) so it always reflects the latest diff. Before editing, ALWAYS read the current description first (`gh pr view <n> --json body`) — the author may have edited it by hand. Preserve those and make a surgical change (fetch the body, modify the relevant part, write it back) rather than overwriting with a regenerated body. Write the new body to a file with the Write tool and pass `gh pr edit --body-file <path>`; do NOT pipe a body through a shell `sed`/`perl` here-string (a scripted in-place edit silently produced an empty file once and blanked the description — `gh pr edit` happily applies an empty body). After editing, re-read the body length to confirm it wasn't truncated.
8. **Reply to review comments after addressing them**: once the fix is pushed, reply to each comment thread concisely — start with `Done`/`Fixed`/`Addressed`, then at most one sentence of detail if it helps. No verbosity. Reply via `gh api repos/<owner>/<repo>/pulls/<n>/comments/<comment_id>/replies -f body='...'` (single-quote the body so backticks stay literal).

## Self-review with a fresh agent before pushing

Before the FIRST push of any PR (and before pushing any later round of changes), self-review the code first:

1. **Spin up a NEW general-purpose subagent** (fresh context) and tell it to apply the `pr-correctness-review` skill to the exact diff, reviewing for correctness bugs and quality — a genuine second pair of eyes, not the same context that wrote it. Have it try to BREAK the change, not confirm it.
2. **Address its findings** in the working tree.
3. **Iterate the tests.** Any new change (from self-review, or later from reviewer comments) means re-running the added/affected tests and local functional/e2e validation again — change → test → review → repeat until the diff is clean and green. Do not push on the assumption a small follow-up edit is safe; re-validate it.

Only push once self-review is addressed and the tests are green on the final diff. (Then per step 4, open as a draft.)

## Squash before publishing

By default, publish a PR as a **single commit**. If the branch accumulated multiple local
commits during development, squash them into one before the first push (e.g.
`git reset --soft <base-branch> && git commit`, preserving gpg signing and authoring).
Override only when the user explicitly asks to keep separate commits. This applies to the
initial publish only — once the PR is open, follow step 6 (append a new commit per review
round; no amend/force-push).

## Code hygiene before committing

See the **`develop-changes`** skill for code-change hygiene: comment/javadoc style (concise, present-tense, no pre-fix-state narration), no ticket IDs / proprietary specifics in source, behavior-preserving edits, working in a worktree, and build + checkstyle before committing.

## Apache Hudi specifics

PR titles follow Conventional Commits (validated by a GitHub Action), NOT the old `[HUDI-XXXX]` prefix. See https://hudi.apache.org/contribute/how-to-contribute#pr-title-format

- Format: `<type>(optional scope): <description>`; breaking changes: `<type>(scope)!: <description>`.
- `type` (required, lowercase): `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `chore`.
- `scope` (optional): area of the codebase, e.g. `spark`, `flink`, `core`, `api`, `storage-format`.
- For an old JIRA reference, put the ID in the scope: `fix(HUDI-1234): description`.
- **Every PR should have a corresponding GitHub issue.** Feature, fix, perf, test or chore: if no
  issue covers the change, CREATE one first (`gh issue create --repo apache/hudi`, with `area:*` and
  `priority:*` labels), stating the problem in its own words rather than restating the diff. Then
  reference it as the FIRST line of `### Describe the issue this Pull Request addresses`:
  `closes #<n>`, or `issue: #<n>` when the PR is only part of the fix.
- **An issue body gets the same brevity bar as a PR body**: what is wrong, why it matters, and what you
  propose, in a few sentences. It is a problem statement for people triaging a backlog, not a writeup.
  No per-occurrence traces, no test matrices, no measurements, no narration of how you found it; the
  detail belongs in the PR, the ticket, or a comment. Quote code only when the defect is not stateable
  without it (a misleading signature, a wrong constant).
- **Cross-link both ways.** The issue names the PR that fixes it; the PR names the issue. When the
  work belongs to a larger effort, both add `part of #<epic>`, and the epic's Related list names the
  issue and the PR. When one PR is split into several, each half gets its own issue and each body
  says which PR merges first.
- **ALWAYS push the branch to the engineer's own personal fork of `apache/hudi`, and open the PR from that fork against `apache/hudi` master. NEVER push a branch directly to `apache/hudi`**, even when the authenticated user has commit rights there.
  - Resolve the handle at runtime, never hardcode one: `gh api user --jq .login`. Confirm the fork exists with `gh repo view <handle>/hudi`, and create it with `gh repo fork apache/hudi` if it does not.
  - Work out which remote points at that fork versus at `apache/hudi` upstream by inspecting `git remote -v` — do NOT assume a remote name (`origin` is often upstream, not the fork). Push to the remote whose URL matches `<handle>/hudi`, or to the fork URL directly (`git@github.com:<handle>/hudi.git`).
  - **If the correct fork or remote is not obvious — no fork exists, several remotes could be it, or the naming is ambiguous — STOP and ask the engineer. Do not guess, and do not push.**

## Stacked PRs and backports

When a change builds on an unmerged PR, or must land on both `master` and `branch-0.x`:

- **Stack on the parent's head.** Create the worktree/branch off the parent PR's head commit
  (`git worktree add -b <branch> /abs/path/<repo>-wt-<N> <parent-head>`, where `<repo>-wt-<N>` is a
  reused numbered pool slot, not a ticket- or feature-named dir — see `develop-changes`), not off
  `master`, so your branch contains
  only the delta on top. Compute the true delta first: fetch the parent (`git fetch apache pull/<n>/head:<ref>`)
  and grep it for what it already contains, so you only add what's missing.
- **A PR against `apache:master` shows parent + delta commits** until the parent merges (then a rebase
  drops it to just your commits). State this in the description ("Stacked on #<parent>, review that first").
  If the user wants a clean delta-only diff *now*, open the PR within the fork with base = the parent's
  branch instead — at the cost of not running apache CI until retargeted. Offer the choice.
- **Backport edit-by-edit, never blind `git apply`.** `master` and `branch-0.x` drift (Lombok
  `@Getter`/`@Slf4j` vs manual getters/`LOG`, package paths, JDK target). Files that differ ONLY by your
  feature can be copied across; files with drift need surgical edits. Verify convergence with `diff`
  (feature files should be byte-identical modulo the known drift), and rebuild on each branch; see
  `port-fixes`.
- **Commits**: apache/hudi commits use Conventional Commit messages (`fix(scope):`, `refactor(...)`,
  `test(...)`); publishing a few logical commits (fix / refactor / test), each with its own prefix,
  is fine and aids review, overriding the default-squash rule.

## Pushing to someone else's PR

When asked to push a fix to another contributor's PR (one opened from their fork), just `git push` to the PR's head branch on their fork — it works when the authenticated user is a Hudi committer/PMC and the PR has "allow edits from maintainers" enabled (`maintainerCanModify: true`). No need to fork-of-a-fork or open a new PR.

- Find the head fork + branch: `gh pr view <N> --repo apache/hudi --json headRepository,headRefName,maintainerCanModify`.
- Push directly: `git push <fork-url> HEAD:<headRefName>` (e.g. `git push git@github.com:<owner>/hudi.git HEAD:<branch>`).
- Same rules as above apply: add a new commit (no amend/force-push unless told), no Claude co-author trailer.
