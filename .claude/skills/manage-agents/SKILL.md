---
name: manage-agents
description: Use when a task splits into parts that could run independently, or before spawning a subagent — deciding whether to delegate at all, which model and reasoning effort a part deserves, how many agents to run at once, and what a subagent must be told to come back with a usable answer. Also use when parallel agents need separate worktrees, or when a subagent's report is about to be treated as evidence. For the worktree mechanics themselves see develop-changes.
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

# manage-agents — delegating and coordinating parallel work

Applies whenever a piece of work breaks into parts. Delegation is for throughput and for
context you do not want to carry, not for offloading judgement: **you stay accountable for
every claim a subagent hands back**.

## Delegate, or do it inline?

Delegate when the work is **independent** and its intermediate output is noise to you:

- Fan-out reads — sweeping many files, modules, or naming conventions for one answer.
- Genuinely parallel workstreams — the same fix against three bases, several unrelated review
  dimensions, a long build or log mine running while you keep working.
- A fresh pair of eyes — an adversarial pass on a diff must run in a context that did not write
  it (`create-pr` and `review-pr` both depend on this).

Do it inline when:

- You know the file and the symbol. One `Read` beats a round trip.
- The parts are coupled — edits to one file, or a decision each step feeds the next.
- The engineer has to confirm it: the `start-dev` Phase 2 routing decision, an issue's scope,
  anything posted to GitHub. Delegating these hides the question the engineer needs.

Never spawn a second agent to redo work already in flight, and never write down what a pending
agent "found" — its result arrives on its own.

## Pick the model and effort from difficulty AND criticality

Two axes, and they are not the same one. Difficulty is how hard the answer is to produce;
criticality is what a wrong answer costs. **Criticality is the tiebreaker: an easy question
whose wrong answer ships a bug gets the higher tier, not the cheaper one.** "Is this refactor
behavior-preserving?" is one sentence to answer and expensive to get wrong.

| The part | Model | Effort |
|---|---|---|
| Mechanical and checkable — grep a corpus, list callers, run a build and report pass/fail, collect log lines | `haiku` | `low` |
| Routine judgement, recoverable if wrong — summarize a ticket, draft a PR section, gather timeline evidence | `sonnet` | `medium` |
| Correctness-critical — adversarial diff review, root-causing a test failure, deciding where a fix belongs, anything on the write path, timeline, metadata table, compaction, or schema evolution | `opus` / `fable` | `high` / `xhigh` |
| Gates a push or a customer-visible claim — final pre-push verification, confirming a performance number is real | `fable` | `max`, and more than one agent — see below |

Wrong-tier symptoms both ways: a cheap model on a correctness question returns a fluent summary
that dissolves when you check it, and a top-tier model on a grep burns latency you feel on every
step of a routing loop. When a task straddles two rows, split it — a `haiku` agent collects the
evidence, an `opus`/`fable` agent judges it.

**Where each knob lives.** The `Agent` tool takes `model`; its effort comes from the agent
definition or the session, so put the depth you want in the prompt ("read every call site before
concluding", "stop at the first match"). `Workflow`'s `agent()` takes both `model` and `effort`
per call — but a workflow needs the engineer's explicit opt-in, so default to `Agent` and reach
for `Workflow` only when they ask for orchestration.

## Coordination

- **One worktree per agent, always.** Two agents in one worktree collide on the index, the
  working tree, and `git checkout`. Mechanics (sibling path, numbered `-wt-N` pool, absolute
  paths) are in `develop-changes`; the rule here is simply that a fan-out over N editing agents
  needs N worktrees, allocated before they start.
- **Read-only fan-out needs no worktree.** Agents that only read can share the checkout.
- **Launch independent agents in one message** so they actually run concurrently. Sequential
  spawns serialize the wall clock for no benefit.
- **Keep the fan-out small enough to reason about.** Prefer a handful of well-scoped agents over
  a dozen overlapping ones; overlapping agents return the same finding three times and you pay
  to reconcile it.

## What a subagent must be told

A subagent inherits none of this conversation. Anything you leave implicit, it invents.

- **The absolute path** of the worktree or checkout to work in, never "the repo" or a relative path.
- **The skills to invoke, by name** (`build-hudi`, `pr-correctness-review`) — they
  are the source of truth, so do not paraphrase their rules into the prompt.
- **The exact question**, and the shape of the answer you want back: findings with file:line, a
  pass/fail plus the failing signature, a list of paths. "Look into X" comes back as an essay.
- **What is out of scope**, explicitly — no commits, no pushes, no issue or PR writes, unless that is
  the task.

## A subagent's report is not evidence

The failure mode this skill exists to prevent: a delegated summary gets promoted to fact and a
PR is written on top of it.

- Before a claim gates a push, **verify it yourself** against the file, the test output, or the
  timeline. A subagent asserting a function is unused is a lead, not a finding
  (`pr-correctness-review`: never trust a summary about a file, function, or line without
  looking at it).
- For a **ship-blocking** judgement, run more than one agent and treat disagreement as the
  signal to look yourself. Independent agents given different lenses (correctness, does-it-
  reproduce, what-breaks-downstream) catch what redundant ones cannot.
- **Report what came back honestly**, including an agent that failed or returned nothing. A
  silently dropped agent reads as coverage you never had.
