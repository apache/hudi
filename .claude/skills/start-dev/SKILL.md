---
name: start-dev
description: Use when starting a Hudi change end to end, given a GitHub issue, a HUDI-XXXX JIRA, a PR URL, a description of the change, or nothing but the current branch, to drive it from intake through an open draft PR by routing through the other skills in this directory.
argument-hint: "Hudi GitHub issue or HUDI-XXXX, PR URL, a description of the change, or nothing"
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

# Hudi work item

You are driving a Hudi change for a contributor, from intake through an open draft PR.

Your job is routing, not knowing. Each phase below names the skill that owns it. Invoke that
skill and follow it. Do NOT restate or paraphrase a skill's rules in your own plan: the skills are
the source of truth, and a summary written from memory is what goes stale. When two sources
disagree, `build-hudi` wins on branch, JDK, and profile facts, and `create-pr` on title and
template facts. Track the phases with TodoWrite.

Where a phase splits into independent parts — a fix landing on both branches, review dimensions,
a build running while you read — `manage-agents` owns the split: whether to delegate at all,
which model and effort each part deserves, and one worktree per editing agent. Phases 1 and 2
are not delegable; they end in a question for the contributor.

Input: $ARGUMENTS

## Phase 1 — Intake

Resolve the input to two things: the problem statement, and whether this is a bug fix or a
feature. Read the source, do not infer from a title.

- **Apache Hudi GitHub issue, or `HUDI-XXXX`**: read the issue or JIRA, including the discussion.
- **GitHub PR URL**: read the PR and its review comments, then say whether you are revising it,
  backporting it to another branch, or writing a companion PR.
- **Free-text description**: take it as the problem statement and confirm your reading back.
- **Nothing**: work from the current checkout, branch, and uncommitted diff. If the goal is not
  obvious from those, ask rather than inventing one.

If there is no GitHub issue, `create-pr` says to create one before the PR phase; draft it and get
the contributor's go-ahead before posting. If the issue is ambiguous about scope or which behavior
is wrong, stop and ask.

## Phase 2 — Routing decision (do this before any code is touched)

The rest of the work hangs off this, and it is cheap to fix now and expensive later. Consult
`build-hudi` and establish:

- Which base branch: `master`, `branch-0.x`, or both (`port-fixes`).
- What the PR title must look like (`create-pr`).
- Which JDK and Maven profiles that base needs.

State all of it back to the contributor in a few lines and get confirmation before proceeding.

## Phase 3 — Worktree

`develop-changes` Step 0: create and work in a git worktree BEFORE the first edit, even for a
one-line change. If edits already exist in a primary checkout, follow that skill's recovery
instructions before doing anything else.

## Phase 4 — Design and implement

`author-changes` governs the change and its tests: where the code belongs, making it assumption-free,
and what coverage actually proves the fix. `develop-changes` governs hygiene (comments, what never
belongs in source, behavior-preserving edits). One conditional addition:

- Any change claiming a CPU, latency, memory, I/O, or cost win: also `prove-performance`,
  before the number goes anywhere near a PR description.

## Phase 5 — Build and test

`build-hudi` for build, test, and checkstyle invocation, including running a single test.
`quiet-build` for shaping any command that can flood the terminal. Tests must be green on the
final diff, on every base you are landing on.

## Phase 6 — Self-review

`review-pr`, run against your own diff before pushing. Fix what it finds and re-run the tests.
It drafts comments only and never posts to GitHub.

## Phase 7 — PR

`create-pr` for branch, commit, issue, and PR mechanics. PRs open as DRAFTS.

If the fix lands on both branches, `port-fixes` owns the backport: API drift and per-base
red-then-green verification. A cherry-pick that compiles is not a ported fix.

## Stop conditions

- Never post to GitHub without the contributor's explicit go-ahead for that specific action. This
  covers issue creation, PR creation, PR body edits, and review comments.
- PRs open as drafts. The contributor marks them ready.
- Stop and ask when the routing decision (Phase 2) or the issue (Phase 1) is ambiguous.
- Do not claim a change works until the tests you ran prove it, per `develop-changes`.
