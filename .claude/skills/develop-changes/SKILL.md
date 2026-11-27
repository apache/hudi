---
name: develop-changes
description: Use whenever you write or modify code in any repo, before committing — starting an edit, deciding whether a change needs a comment, classifying a failing test as harness-vs-product, or about to claim a change works. Also use when writing prose a person reads (PR description, review comment, reply, design doc). Covers worktree discipline, the voice for that prose, comment/javadoc policy, what must never appear in source, behavior-preserving edits, and the validation bar. For git/PR mechanics see create-pr; for Apache Hudi code-change design and tests see author-changes.
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

# develop-changes — code-change hygiene

Apply this whenever you write or modify code, before committing. Related skills: `create-pr` for git/PR mechanics (branch, commit, squash, push, review replies); `author-changes` for Apache Hudi code-change design and test coverage; `review-pr` for the reviewer's bar; `manage-agents` for splitting work across agents and sizing the model and effort each part gets.

## Step 0 — worktree FIRST, before your first edit (non-negotiable)

**Always create and work in a git worktree BEFORE the first `Edit`/`Write`. Never edit or commit in the primary checkout** (your `hudi` clone), and never wait for PR time to think about it. This is a hard standing rule; do it without being asked. The full worktree mechanics (sibling path, numbered `-wt-N` pool, one per session/agent, absolute path off the base branch) are under **While editing** below. If you realize you've already started editing in the primary checkout, STOP: create the worktree, move the branch/changes into it (`git worktree add` the branch, or re-apply the diff), and continue there before committing.

## Keep out of the source (these belong in the PR/commit/tracker)

- **No ticket IDs in source/comments** (GitHub issue numbers, JIRA keys, any other tracker's ids). The PR links the issue; the code describes the behavior.
- **No proprietary or customer specifics** (company names, customer/table names, real paths, real data) in code, comments, or test fixtures. Use synthetic data (`key0`/`value0`).
- **No AI co-author trailer** on commits (CI rejects it), and no assistant name in branch names or source.
- **No em-dashes** in comments or prose (use commas or parentheses).

## Writing for people

This covers all prose a person reads: PR descriptions, review comments, replies, design docs, commit messages, and tickets.

- **Write for an expert.** Assume the reader knows distributed systems, Hudi, and lakehouses better than the average engineer, so skip background they already have and go straight to what is specific to this change.
- **One sentence per point.** If a sentence gets the point across, stop there instead of growing it into a paragraph.
- **Plain words.** Choose the everyday word over the clever one so the point lands on first read.
- **Polite, warm, and gentle on asks.** Write with care for the person on the other side, and keep requests casual ("Could you take a look at...", "Would it make sense to...") rather than directive.
- **A peer, not a teacher.** Don't lecture, and avoid flat assertions ("this is wrong", "you must") that can read as harsh; share what you saw ("I think...", "it looks like...") and leave room for context you may not have.

## Comments and javadocs

- **Default to NO new comment on a change; put the explanation in the PR description, not the code.** A diff's rationale, motivation, and background belong in the PR description (and commit/ticket), which is where reviewers and future readers look for them. Add an inline comment only when it earns its place in the code long-term: a subtle invariant, a non-obvious gotcha, a WHY that a maintainer editing this line months later must know. Never add a comment merely to narrate or justify the current PR's change. When in doubt, leave it out and say it in the PR description.
- **No verbose or line-by-line comments** restating what the code obviously does. Comments state WHAT/WHY, concisely.
- **Do not comment (or doc) a change just because it changed.** Comment only notable or non-obvious things: a subtle invariant, a non-obvious WHY, a gotcha. A self-evident edit (swapping a call for an equivalent helper, a rename, a mechanical refactor) needs no comment — the diff and the commit message already carry it. If the reason lives in the commit/PR, do not duplicate it inline.
- **Comments describe the current code, not its history.** Present-tense and concise; do NOT narrate the pre-fix or old state ("instead of…", "previously", "no longer", "used to"), the incident or its scale that motivated the change, or framework/version history. That before/after context belongs in the PR description, commit message, or ticket. Keep non-obvious correctness WHY to one concise sentence.

## While editing

- **Behavior-preserving by default.** If a change is meant to be a pure refactor, keep it observably identical and call out any intentional behavior change explicitly.
- **The minimum change that achieves the goal, with each decision in the layer that already owns the information.** Before adding a mechanism (a tracking map, a chain walk, a tie-break, a reorder of an existing loop), ask whether the goal is met without it and whether a layer upstream already knows the answer; carry the answer down, not the evidence. A review finding is a reason to reconsider the shape first, not to bolt on one fix per finding. Before committing a follow-up, re-read every hunk and drop any line the goal does not need; every extra line is review burden and regression surface.
- **Match the surrounding style**: naming, comment density, idioms, and the module's language version (e.g. Java 8 constructs only where the module targets 8).
- **Work in a git worktree, not the main checkout**, so the working tree stays isolated, and give **each session/agent its own worktree**: one Claude session (or one parallel agent within a session) uses exactly one worktree at a time — never run two agents or two builds in the same worktree (they collide on `git checkout`, the index, and the working tree). A single session running parallel agents therefore holds several worktrees at once, one per agent.
  - **Place worktrees as siblings of the repo, never nested inside it.** Use `<repo-parent>/<repo-name>-wt-<N>` (e.g. `~/src/hudi-wt-1`), not `<repo>/.claude/worktree/...`. A worktree inside the repo tree gets indexed by the IDE, so "find file" / "go to class" shows a duplicate of every class per worktree; a sibling is a separate project the IDE indexes on its own.
  - **Maintain one finite numbered pool per repo and reuse it.** Slots are `<repo>-wt-1`, `-wt-2`, …, where the suffix is a **bare integer**. **The directory name is a slot number, never the branch or ticket.** Do NOT name the worktree `<repo>-wt-issue1234`, `<repo>-wt-<feature-slug>`, `<repo>-<slug>-wt`, or `<repo>-N-wt-<slug>` — the ticket/feature belongs in the `-b <branch>` name (below), which is independent of the slot number. Before creating one, **run `git worktree list` and reuse an idle slot** (check out a fresh branch in it, or `git worktree remove` once its PR merges); only mint the next integer when every existing slot is busy. This keeps the count bounded — minting a uniquely-named worktree per task is how dozens of stale worktrees pile up. Work only in these `-wt-N` worktrees; leave the main checkout and any reference checkouts alone.
  - **Create with an ABSOLUTE path** off the repo's base branch (`git worktree add -b <branch> /abs/path/<repo>-wt-<N> <base>`); a relative path from a shell whose cwd is already inside another worktree silently nests them. The base is `master`, or `branch-0.x` for a 0.x backport; see `build-hudi`.
  - **Relocate with `git worktree move`, not `mv`.** It moves the worktree and fixes git's pointers in one step.
- **Build + checkstyle before committing.** Compile the changed modules and run checkstyle (both clean), and run the targeted tests that cover the change, before you commit.

## Always validate every code change with tests (non-negotiable)

For ANY code change, before putting up a PR, find a way to prove it works and to guard it against regression:

- **For a BUG FIX, always write a unit or functional test that reproduces the issue** and ship it as the regression test: run it against the pre-fix code and confirm it fails with the bug's exact symptom (the same exception/stack signature or the same wrong value), then confirm it passes with the fix. Reproduce the real failure mode, not a neighbor; capture the failing signature in the PR. A bug-fix PR without a reproducing test is not done.
- **Add a test, or enhance an existing one.** Prefer a test that FAILS on the old code and PASSES on the new (red/green) — that proves both the bug and the fix. Extend the nearest existing test/suite rather than creating a redundant new one when it fits.
- **Be thorough and comprehensive on cases.** Cover the happy path plus edge/boundary/negative cases, not just the one example from the report. When touching a shared util or predicate, add the case the existing tests missed (that gap is usually why the bug shipped).
- **If a pure unit test can't reach it, run a local functional or e2e validation** through the real code path (run the app/query/job, observe the real signal) and capture the evidence. A green compile is not validation.
- **Never conclude "it works" from reasoning alone.** Run it. If validation surfaces a bug, fix it and re-validate before the PR.

## Never mask a real product bug with a test change

When a failing (or newly re-enabled) test surfaces a discrepancy, FIRST classify the failure before touching the test: is it a **test-harness issue** (missing setup, a stale expectation, an un-ported step) or a **genuine product defect** (the code under test produces wrong output)? Do not skip this step, and do not reach for the test-side edit just because it makes the bar go green.

- **A test-side fix is only legitimate when the failure is genuinely a test-harness issue** — e.g. a re-enabled test missing an explicit commit after a framework dropped auto-commit (Hudi 1.x / HUDI-8470), a dropped config in a version port, or an assertion comparing something the test itself set up wrong. Mirror how a passing sibling test does it; do not invent a new expectation.
- **If the failure reflects a real product defect, NEVER loosen, delete, or "adjust" the assertion to pass.** That masks the bug and ships it. Instead: fix the product code (with the test as the regression guard), or if the fix is out of scope, keep the test disabled with an accurate reason naming the real defect and file a GitHub issue. A test that is green because the assertion was weakened is worse than an honestly-disabled one.
- **Peel the layers.** One surface test fix can reveal a deeper real failure underneath (a missing-handle test fix that then exposes a genuine writer divergence). Re-run after each layer and re-classify; do not assume the first green-ish step means the test is now valid.
- **When unsure which it is, treat it as a possible product bug** and investigate down to ground truth (diff the actual vs expected bytes/values, find the code that produces the difference) before deciding. Say which it is, with evidence, in the PR.

## Verify the claimed effect empirically, at multiple levels

Do not conclude a change works from conceptual reasoning that it "should", especially for non-functional claims (memory, performance, concurrency, resource use). Confirm it with independent tests, and treat agreement across different methods and levels as the evidence:

- **Micro / unit**: isolate the mechanism (e.g. a serializer that streams >1 GiB while holding one element; a bounded-heap pass/fail assertion).
- **End-to-end through the real code path**: exercise the ACTUAL changed methods, not a reimplementation, and show the real change passes where the real baseline fails, in the same setup at the same scale.
- **Scale / production**: reproduce at (or scaled toward) the real trigger and watch the real signal (before/after metrics, dashboards, the exact OOM or log signature that fired).

Vary the measurement METHOD too, not just the level: bounded-heap pass/fail, retained-heap delta (post-GC), allocation profile (async-profiler `-e alloc`), before/after peak on the metric that matters. One green build with plausible-looking logic is not confirmation.

For a **performance** change specifically, this bar is higher and has its own skill: **`prove-performance`**. In short: profile first; validate at micro + e2e-real-path + REAL-DATA levels (real data can overturn a synthetic benchmark, e.g. a microbenchmark "16x" that is a no-op on the real workload); measure on BOTH local (CPU-bound) and cloud/S3 (I/O-bound), since a local win may not transfer; assert correctness inside the benchmark; and ship broad unconditional wins before narrow eligibility-gated ones. Do not present a local synthetic number as the production number.
