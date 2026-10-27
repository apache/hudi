---
name: pr-correctness-review
description: >-
  Use for an adversarial correctness pass on a Hudi diff — write path, merge handles, file
  formats, sources/syncs — or to verify a claimed "behavior-preserving" / "pure refactor"
  change. Also invoked by `review-pr` at its Step 4 as the falsification pass. For a full
  review of a Hudi PR, start with `review-pr` instead.
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

# PR Correctness Review

This skill makes correctness reviews comprehensive and pointed. The bar is not "did I read the diff
and it looks reasonable" — it is "did I try to break every invariant the code relies on, trace each
claim to the actual leaf code, and reason about the domain's real edge cases."

## The core stance: falsify, don't confirm

The single most important lesson: a clean-looking review that *confirms* the code usually misses the
bug. A real correctness review tries to *falsify* it.

When you catch yourself writing "this holds in practice" / "the guard protects us" / "it's behind a
flag so it's fine" / "the existing tests pass" — **stop**. Those are comfort signals, not evidence.
For every invariant the code depends on, your job is to construct a concrete input that violates it.
If you genuinely cannot after trying, *then* it's probably correct.

Worked example: a fix gated timestamp-precision changes in `reconcileSchema` and added a test
that passed. A confirming review sees the gate and the green test and moves on. The adversarial pass
asks "what concrete input reaches the writer-schema decision without passing through this gate?" →
the default `set.null.for.missing.columns=false` path, where the schema-compatibility check accepts
the precision change, so it passes silently and the next commit flips the persisted type. Same code, same gate spotted both times. The difference was:
confirm vs. try-to-break.

## Confirm tricky behavior by RUNNING it, not just reading code

Code reading gives you a hypothesis; it does not confirm behavior. Whenever a conclusion is
uncertain, or the behavior is subtle / version-dependent / timing-dependent / parser- or
analyzer-mediated / dependent on a format normalizer, **confirm it with an executable test before
asserting it — do not conclude from the code alone.** A wrong "it's behavior-preserving" or "this
is/isn't supported" shipped on a code-reading inference is exactly the miss this skill exists to
prevent. Do this proactively; do not wait to be asked to "run a test to confirm."

- **Write (or add) a focused test that exercises the exact path and observe the real output.** A
  throwaway test you run and then `git checkout --` to revert is fine. Run it on the real
  engine/profile the behavior depends on (e.g. the actual Spark version) — behavior can differ by
  version (Spark 3.3 rejects a time-travel subquery at parse time; 3.4+ defer to the analysis rule —
  provable only by running each).
- **For "same as before" / "same as master" / before-vs-after claims, RUN it on BOTH sides** — the
  base/master commit AND the PR branch — not just one. "The rejection line predates the PR, so master
  must reject too" is a hypothesis: check out the base commit, add the assertion, and run it there as
  well. (Use a separate worktree for the base build so you don't disturb the branch; note that `.m2`
  installs are shared, so serialize the two builds.)
- **"Does format/spelling X work?" is an empirical question**, not a code-reading one: run each
  spelling/format through the real path and observe. (For example, epoch-seconds and
  `yyyy-MM-dd HH:mm:ss` time-travel timestamps resolve; `now()` throws `Unsupported query instant
  time format` because the expression is never folded — only running it revealed the split.)
- **When you genuinely cannot run it, say so explicitly and label the conclusion UNVERIFIED**, rather
  than presenting a code-reading inference as confirmed.

## Method (apply in order)

### 1. Identify the load-bearing invariants, then attack each one
List every assumption the change relies on for correctness. For each, ask:
- **Who guarantees it?** (the planner? the caller? a parquet property? an upstream component?)
- **Is it asserted/enforced, or just assumed?** Unasserted invariants are where bugs live.
- **What concrete input violates it?** Construct an actual example with numbers, not a hypothetical.
- **What's the failure mode if violated — hard crash (fail-fast) or silent data corruption?**
  Fail-fast is recoverable embarrassment; silent corruption is the real danger. Always classify.

### 2. Trace claims to ground truth — go two layers below the abstraction
Bugs hide *below* the interface you're reasoning at. When code says "X is the source of truth for Y,"
do not stop there: open X's population/derivation and read the leaf logic.
- "The compatibility check guards schema evolution" → read what the checker actually compares
  for the case at hand (see the schema-compatibility trap below). The check and the property you
  needed can be different things, and that gap is where the bug lives.
- Never trust a summary (yours, a subagent's, or a PR description) about a file/function/line without
  reading it. Verify with `git show <commit>:<path>` / Read / Grep before asserting a bug exists.

### 3. Reachability analysis — "does this actually happen?"
For every degenerate/corner case, ask whether it occurs in production, *with the domain in mind*. A
"pathological corner" can be a first-class case:
- A MOR file slice with no base file looks pathological — until you remember that inserts can
  land in log files (Flink, bucket index). Then it's the steady state, and the corner-case handling
  is the main path.
- Ask the user about workload shape when it changes severity ("are out-of-order events / empty
  commits / log-only file slices expected here?").
- **Separate "wrong bytes/state" from "wrong behavior" by who consumes it.** A field can be wrong
  on disk (or in memory) yet harmless if no code path reads it — or a real bug for one consumer and
  a non-event for another. Trace the field to its actual reader before grading severity: the native
  `hudi-io` HFile reader is forward-only and never reads the data-block prevBlockOffset, so a wrong
  value is latent for Hudi, but the HBase reader's `seekBefore()` does read it and throws. Grep the
  reader for the field/constant and look for the specific code path (backward seek, reverse scan)
  that would consume it; don't grade a format-spec violation as "broken" or "harmless" in the
  abstract.

### 4. Producer/consumer contract checks
When two components share a value across a boundary, verify **both sides compute the same thing from
the same source**: the writer and the reader of a format field, or the planner that schedules a table
service and the executor that runs it. Confirm they derive the value identically (same inputs, same
ordering semantics). Mismatched derivations that happen to coincide on the happy path are classic
latent bugs.

### 5. Cross-cutting consistency
Ask "is X the same across all Y?" for every dimension the code iterates:
- Same across all row groups / pages / partitions / table versions / engines?
- Order-dependent vs order-independent consumers (e.g., does stats order matter)?

### 6. "Behavior-preserving" / "pure refactor" claims are claims to disprove
When a PR says it preserves behavior, hunt the **error paths and dropped validations**, not the happy
path. Diff the *removed* lines: a refactor that drops a bloom-filter check, an empty-row-group guard,
or an ordinal validation is a behavior change even if the happy path is identical. Ask "what did this
stop rejecting?"

### 7. Resource, lifecycle, and exception paths
- Iterators/readers/writers: created once vs per-iteration? consumed exactly once? position maintained
  across pages/chunks? Any lookahead/prefetch that straddles a boundary the caller assumes is clean?
- Closeables closed on the **exception** path, not just the happy path? Executors shut down? Buffers
  released (matters more with large records — a leaked decompressed buffer is big)?
- Double-close / double-finalize / double-complete.

### 8. Numeric and type correctness
Integer division vs float (`(double)` placement), overflow in `a*b` before a divide, `Math.abs` vs
`Math.floorMod`, `long` vs `int` for sizes that can exceed 2GB, `Integer.MAX_VALUE` page/buffer
ceilings, `.equals` on `byte[]`.

### 9. Escalate high-stakes paths to adversarial multi-agent verification
For the riskiest path (data routing, merge correctness, anything that could silently corrupt), spawn
subagents with the Agent tool: fan out independent readers over each component that must line up, then
run 2+ adversarial verifiers whose explicit job is to *construct a failing input* (prompt them to default to
"refuted only if I truly can't break it"). Independent perspectives catch what a single confirm-pass
rationalizes away.

## The pointed questions to ask proactively

These are the review probes that consistently surface real issues. Ask them yourself before the user
has to:
- "Does this case actually happen?" (reachability)
- "Is this decision/value the same across all columns / row groups / table versions / engines?"
- "Should it fall back / short-circuit when the operation is a no-op or can't actually help?"
- "Scrutinize function F for corner cases" — pick the hairy function and trace every branch and
  loop-termination/empty/overflow boundary.
- "Is the iterator/resource used correctly across pages/chunks?"
- "Does this compile under the module's Java target, and on `branch-0.x` if it is backported?"
- "Why does the PR claim this is behavior-preserving?" → then disprove it.
- "What input makes the planned count and the actual count diverge?"
- "Is each piece of added machinery required for the stated goal, or does it recover downstream a
  decision an upstream layer already had in hand?" The smallest shape is usually a value passed
  down, not a chain walked later.

## Redundancy and single source of truth

Correctness is the priority, but also flag logic that is redundant once a single mechanism already
covers it. When two constructs achieve the same effect, prefer the one that does it in one place and
drop the other unless there is a concrete reason to keep it. (Source: review feedback on
apache/hudi#19062.)

- **One closing site, not two.** If an inline `try/finally` (or try-with-resources) already closes a
  resource on every exit path, do not also track it in a field and re-close it in `@AfterEach`; the
  try/finally is the single source of truth. Keep a separate `@AfterEach` close only when the resource
  genuinely outlives the try scope or is shared across tests, and then say why.
- **Use `try/finally` for closing.** Resources created inside a test or method should be closed by a
  `try/finally` or try-with-resources local to that method, not by a parallel teardown mechanism.
- **Reuse existing close helpers.** Before writing a local close helper, check for one that already
  exists (e.g. `FileIOUtils.closeQuietly(Closeable)`); adapt at the call site (a `Closeable` method
  reference works when `close()` does not throw) rather than duplicating swallow-and-log logic.
- **No duplicated rationale comments.** A comment that restates a rationale documented elsewhere (a
  field comment, another method) drifts out of sync; trim it to a one-line pointer to the canonical
  comment instead of repeating it.

Ask proactively: "Is this extra cleanup necessary if the inline path already closes the resource?" and
"Can one change (the `try/finally`) be the single place this is handled?"

## Hudi domain traps

These have produced or nearly produced real bugs. Check them by name.

- **Out-of-order deletes are normal.** A delete whose ordering value loses to the stored record is
  KEPT by the merger. Any code that counts, sizes, or plans off the incoming deletes rather than the
  merged result over-counts. Event-time / ordering-field semantics make this a supported pattern,
  not an exotic one.
- **Native HFile writer must stay HBase-readable, byte for byte.** `hudi-io`'s `HFileWriterImpl`
  writes MDT HFiles a shaded-HBase reader may read. Traps that produced real bugs: (1) the block
  index per-entry keyLength must be Hadoop `WritableUtils` VInt, not Protobuf varint — they diverge
  at >= 128 (content length >= 116) and the wrong one yields `NegativeArraySizeException`; the
  trailer/file-info length prefixes are the opposite (Protobuf, `parseDelimitedFrom`). (2) Each
  multi-block data block's prevBlockOffset must point at the previous block, not itself — the native
  reader never reads it (forward-only) but HBase `seekBefore()` throws on a wrong value. (3) Native
  and HBase writer bytes match only for a single-block file (HBase cuts blocks differently and stores
  shortened "fake first key" index separators), so cross-writer goldens must be single-block.
- **Dropped guards in "refactors."** Watch removed validations: bloom-filter rejection, empty/0-row
  row-group guards, ordinal-match checks. Confirm they're either preserved or consciously retired.
- **Prefer fail-fast over silent corruption.** When you find a contract that can be violated, check
  whether the code throws (good) or silently writes wrong data (bad). Recommend turning silent
  divergences into asserts.
- **A guard added to one path is not "the fix" — enumerate every path to the same decision.** When a
  change adds a gate/validation, list all code paths that reach that decision and confirm each is
  gated. A fix placed in the branch named in the bug report routinely misses the sibling branches.
  The write-schema decision (`HoodieSchemaUtils.deduceWriterSchema`) alone forks on
  `set.null.for.missing.columns`, `reconcile.schema`, mergeInto, new-vs-existing table, and Spark
  datasource vs streamer — a gate in `reconcileSchema` only covers `set.null=true`.
- **Schema-compatibility checks are weaker than they look for timestamps.** On `branch-0.x`,
  `AvroSchemaUtils.checkSchemaCompatible` → `AvroSchemaCompatibility.checkReaderWriterCompatibility`
  compare base Avro types only, so two `long`-backed timestamps (timestamp-millis vs
  timestamp-micros) are judged COMPATIBLE. On `master`,
  `HoodieSchemaCompatibilityChecker#checkTimestampCompatibility` checks precision but deliberately
  accepts a micros writer read as millis. Either way the default (`set.null=false`) write path can
  pass a precision change through and the next commit flips the persisted logical type. Any guard on timestamp
  precision must sit at the writer-schema *decision* (applied to the deduced schema for all
  branches), not rely on the compatibility check to catch it.
- **Test with data that a WRONG outcome would corrupt, not data where the happy path reads fine.** A
  logical-repair fixture whose mislabeled column already holds the target-precision values reads
  correctly whether or not the fix is right, so it cannot fail on a wrong flip. Coverage must include
  the genuinely-correct case (e.g. real micros values, ~16 digits) that an erroneous flip would send
  to ~year 58466, and must cross the writer-path branches (transformer on/off, target schema provider
  present/`NullTarget`, `BULK_INSERT`+row-writer vs `UPSERT`, `ParquetDFSSource`/`HoodieIncrSource`).

## Mechanics

Fetch and scope the PR precisely (stacked PRs are common here):
```bash
gh pr view <N> --repo apache/hudi --json title,body,baseRefName,headRefName,commits,files
gh pr diff <N> --repo apache/hudi
```
- **Stacked PR?** `gh pr diff` against `master` may fold in the parent PR. Find the true parent and
  diff against it: `git fetch origin <headSha>`, `git rev-list --parents -n 1 <headSha>`, then
  `git diff <parentSha> <headSha>` to isolate *this* PR's changes.
- Read **changed** files at the PR commit with `git show <headSha>:<path>` (the working tree may be on
  a different branch). Read **unchanged** collaborators (iterators, mergers, readers) from the working
  tree — but confirm the PR didn't change them first.
- Don't rely on the truncated diff for new files; read the whole file.

## Output format

Lead with a clear verdict, then evidence. Keep it correctness-focused when the user asks for
correctness — drop perf/style/design asides unless they asked.

```
## Verdict
<correct / correct-for-happy-path-with-caveats / bug found> — one sentence.

## What I verified holds
- <invariant> — why, with file:line.

## Bug(s) / risks  [ordered by severity]
- <title> — root cause (file:line), a CONCRETE repro with numbers, manifestation
  (crash vs silent corruption), preconditions, blast radius, fix direction.

## Test gaps
- <what isn't covered that should be>
```
Every claim cites `file:line`. Every bug includes a concrete, numeric repro and says whether it
fails-fast or silently corrupts. Offer to draft the fix and a regression test.

## Posting review comments on the PR

When asked to leave the findings as GitHub review comments (inline or summary), keep each comment
concise: 2-4 sentences that state the defect, the concrete consequence, and the fix direction.
No preamble, no restating the diff, no hedging, no bullet lists inside a comment. The long-form
evidence (repro numbers, traces) stays in the chat report; the PR comment carries only what the
author needs to act. Post one inline comment per finding, anchored to the offending line, via a
single review (`gh api repos/<org>/<repo>/pulls/<N>/reviews --input <json>` with a `comments`
array, `event: COMMENT`).

## Related

- `review-pr` — the Apache Hudi reviewer rubric (priorities, invariant checklist, comment
  style). Use that for a Hudi PR; this skill supplies its Step 4 falsification method.
- `port-fixes` — the 0.x / 1.x drift a backported diff can trip.
- `author-changes` — the author-side counterpart.
