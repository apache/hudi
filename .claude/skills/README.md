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

# Hudi development skills

Skills for coding agents working on the Apache Hudi codebase itself: building it, writing and
reviewing changes, backporting, and opening PRs. Claude Code loads them automatically when its
working directory is inside an `apache/hudi` checkout. The skills for operating Hudi tables live
in [`hudi-ai-operator`](../../hudi-ai-operator/README.md) instead.

Start with `/start-dev <issue | PR URL | description>`, which routes a change from intake to a
draft PR through the rest. Each skill also triggers on its own from its description.

| Skill | Use it when |
|---|---|
| `start-dev` | Driving a change end to end: intake, branch choice, worktree, implement, build, self-review, draft PR. |
| `build-hudi` | Compiling, testing, or running checkstyle; which JDK and profiles `master` and `branch-0.x` need. |
| `author-changes` | Writing the change itself: where code goes, assumption-free edits, tests that prove something. |
| `develop-changes` | General hygiene: worktrees, comments, what never belongs in source, the validation bar. |
| `review-pr` | Reviewing a Hudi PR or diff against the committer rubric and invariant checklist. Drafts comments; never posts without authorization. |
| `pr-correctness-review` | Adversarially falsifying a claimed-correct diff. `review-pr` uses it for its verification step. |
| `port-fixes` | One fix has to land on both `master` and `branch-0.x`. |
| `prove-performance` | Any change claiming to reduce CPU, latency, memory, I/O, or cost. |
| `create-pr` | Branching, committing, and opening a PR that passes the repo's PR checks. |
| `quiet-build` | Composing a command that can flood the terminal. |
| `manage-agents` | Splitting work across parallel agents: whether to delegate, and what a subagent must be told. |

The `review-pr` references were distilled from the public `apache/hudi` review history (30,530
comments across 3,945 PRs by five senior reviewers). The corpus and method are not in this repo.

## Contributing

Edit `.claude/skills/<name>/SKILL.md` and open a PR like any other change. Skills are prose, so
the review bar is whether the guidance is correct and would change what an agent does. Two
conventions keep them working:

- **Descriptions are triggering conditions, not summaries.** A description that summarizes the
  workflow gives agents a shortcut they will take instead of reading the skill.
- **One owner per fact.** Branch, JDK, and profile facts live in `build-hudi`; PR conventions in
  `create-pr`. Other skills point there rather than restating them.
