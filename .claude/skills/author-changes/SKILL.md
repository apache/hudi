---
name: author-changes
description: Use when writing or revising the CODE CHANGE in an Apache Hudi PR — editing Hudi Java (especially hudi-io / hfile / storage-format code), deciding where a new method belongs, backporting a fix between master and branch-0.x, adding tests to a bug fix or a coverage-only PR, or being told to "make it robust", "add tests", or "address review comments". Use before pushing, not after review lands.
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

# author-changes — writing Hudi changes that survive review

This skill is about the *quality of the change itself*: where code goes, how to make it
assumption-free, and what test coverage actually proves. `create-pr` covers PR mechanics,
`build-hudi` covers building and running the tests, and `review-pr` covers the
reviewer's side of the same bar — read that one to see what your change will be measured
against.

The worked example behind many of these rules — the HFile keyLength / varint-encoding bug
and its test matrix — is in
[`references/hfile-case-study.md`](references/hfile-case-study.md). Read it when working in
`hudi-io` / hfile / storage-format code; the rules below stand on their own elsewhere.

## Prerequisites

- A worktree, created **before** the first edit (`develop-changes` Step 0).
- `build-hudi` for the JDK and Maven profiles each branch takes. Get it wrong and you waste
  an hour, so look it up rather than assuming.

## Where code goes

- **Put a decision where its inputs already are.** If the component that sees a failure can
  classify it, pass the classification downstream as a value (an enum, a flag) rather than
  exporting the raw material and teaching every consumer to recover the decision. The second
  shape multiplies code paths and tests for the same outcome and leaks one layer's internals
  into the next.
- **Shared logic goes on the shared base class, not a util.** When two subclasses need the
  same helper, put it on their common base. Reviewers here prefer "avoid adding more util
  methods if possible". Place a new helper next to its existing peers, or on the class whose
  format or framing it defines.
- **Single-source anything two code paths must keep identical.** If two writers must emit
  the same bytes, extract one writer so they cannot drift. Drift between duplicated encoders
  is a classic latent bug.
- **Name by format or domain, not implementation.** Don't leak a specific reader or impl
  into a name or javadoc — a "KeyValue key" is a file-format concept, not an HBase one.
- **Never conflate two encodings that agree on small values.** Hadoop `WritableUtils` VInt
  and Protobuf varint are byte-identical for 0..127 and diverge at ≥128. An ambiguously
  named helper silently picked the wrong one and corrupted long keys. Give each an
  encoding-specific name, put it on the class whose framing it defines, and have each
  javadoc cross-reference the other.
- **"marker" is reserved for Hudi writer markers** (`.hoodie/.temp/<instant>/…` files
  tracking an in-flight commit's writes). Never call another watermark a marker. The
  meta-sync watermark is **"last commit time synced"** (`lastCommitTimeSynced` /
  `HOODIE_LAST_COMMIT_TIME_SYNC`) — in code, javadocs, commit messages, and PR prose.
- **Keep constants private to their owner.** No `public static final` on a util unless a
  caller outside the class genuinely needs it.
- **Respect existing comments; don't over-comment.** If a reviewer wrote a comment, restore
  it rather than rephrasing it. Put the WHY in the javadoc once, not inline at every call
  site.
- **JUnit 5 test methods are package-private — never `public`.** Write `void testX()`,
  `@BeforeEach void setUp()`, `@ParameterizedTest void testY(...)`. Match the siblings in
  the file; don't churn existing methods, just don't add new `public` ones.
- **Static-import static members when it reads cleaner** — `mock(...)`, `when(...)`,
  `assertEquals(...)`, `DELETE_KEY` over their class-qualified forms. Keep the qualifier
  only when it disambiguates or when the owning class names the domain
  (`DebeziumConstants.FLATTENED_OP_COL_NAME`). Be consistent within a file and drop the
  now-unused import so checkstyle stays clean.

## Update the spec when you change the format

- If you change on-disk bytes, update `hudi-io/hfile_format.md` concisely in the same PR.
- **Tighten the spec where its ambiguity enabled the bug.** When a vague phrase
  ("variable-length encoded") let the writer pick the wrong encoding, say which encoding it
  is. Extend the in-repo spec; never start a parallel one.
- **Spec and investigation writeup are different artifacts.** The in-repo spec describes
  the format. A cross-cutting RCA or compatibility finding belongs in the GitHub issue or
  PR discussion, not folded into the spec.

## Robust, assumption-free changes

- **Don't bake in invariants that merely happen to hold.** A method taking a `byte[]` slice
  takes `(buf, offset, length)` and honors all three, even if every current caller passes
  offset 0. Watch the related trap: `getLength()` (content length) vs `getBytes().length`
  (backing array) vs `getContentLength()` (reads a length prefix) are three different
  numbers — pick deliberately.
- **Say whether it's a live bug or latent fragility.** "Not a bug today because all callers
  pass offset 0, but fragile" is a different claim from "this is broken". Don't overstate.
- **Distinguish wrong bytes from wrong reads by reachability per reader.** A field can be
  wrong on disk yet harmless if no read path consumes it. Trace the field to its actual
  consumer before calling it broken *or* harmless — then fix it anyway if a compatible
  reader relies on it.
- **Behavior-preserving refactors must be byte-identical** — prove it with a golden plus
  per-field tests, not "the suite is green".
- **Backporting a fix across branches: adapt, don't blind-patch.** `master` and `branch-0.x`
  have drifted — method names, Lombok `@Slf4j log` vs a manual `LOG`, package paths, magic
  baselines in tests. A raw `git apply` of the other branch's diff will fail or silently
  corrupt. Port edit-by-edit and rebuild; see `port-fixes`.
- **Fetch before every push; the branch moves under you.** Co-authors push CI fixes, review
  nits, and follow-on correctness commits onto your branch. `git fetch`, compare tips, and
  if the remote advanced, `reset --hard origin/<branch>` and re-apply your edits on top
  rather than force-pushing over theirs.
- **Reverting a teammate's commit is a decision, not a mechanic.** Read what it contains
  first — it may be a real correctness fix. Surface what the revert drops and confirm before
  dropping it.

## Test coverage that proves something

- **A regression test must fail without the fix; prove it by reverting.** Temporarily revert
  the fix, run the new test, confirm it fails with the *expected* symptom (the exact
  exception or byte diff), then restore. A test that passes both ways guards nothing. Put
  the failing signature in the PR.
- **Reproduce the exact failure mode, not a neighbor.** Enumerate the conditions the bug
  needs and make the test hit all of them at once. A test that relaxes one condition is
  false confidence — that relaxation is usually why the bug shipped.
- **Derive boundary values from the math and test above them, at several widths.** When
  behavior changes at an encoding boundary, test below, at, and well above it, spanning each
  encoding width, so no single value accidentally coincides.
- **Think in a matrix.** Enumerate writer × reader (× compression) cells and state which are
  *broken* versus merely *uncovered*. Parametrize the cells that matter.
- **Byte-level assertions, field by field.** Assert each field individually rather than
  comparing an opaque blob — per-field assertions localize a regression.
- **Golden byte format-locks, scoped to deterministic content only.** Never whole-file byte
  compares: comparator class name, AVG_KEY_LEN, file-info schema, creation timestamp, bloom
  design, and checksum all make them brittle. Scope the golden; for cross-writer parity,
  assert the *reader* sees identical key and value bytes instead.
- **Capture goldens from a logged run; never hand-compute them.** Log the actual hex, paste
  it into the constant, split long constants into concatenated 96-char chunks for the
  line-length check. Regenerate deliberately, only after reviewing compatibility.
- **Explain every magic number you change.** If a format change moved a golden's expected
  size, comment why.
- **Verify the tests actually RUN in CI.** A module can have zero CI coverage despite green
  local runs. Confirm a CI job runs the module's tests (its surefire reports and `jacoco.xml`
  show up), and that a new test's tag (`functional`, `functional-b`, …) puts it in a job.
- **Keep test code concise; verbosity hides the signal.** A test's signal is its assertions.
  One- or two-sentence javadoc, no line-by-line narration, small shared helpers instead of
  repeated boilerplate. Per-field assertions are coverage, not verbosity — keep those.
- **Consolidate overlapping tests; keep the simple unit tests.** Merge near-duplicate
  classes; a fixture-driven matrix harness can replace scattered per-scenario classes while
  pure unit tests stay. When a reviewer asks for fewer test methods, fold the new assertion
  into an existing test that already builds the right fixture — but keep a check in the file
  where its reader lives.
- **Grow expensive functional tests and ITs through a case table, not new tests.** A test
  that runs a sync, a Spark job, or a cluster round trip costs tens of seconds to minutes
  per run, so add a new scenario as one more row of an existing case table: the input
  mutation plus, per mode under test, the outcome it must produce. Assert the resulting
  records or state, not counts. Add a run or an arm only for a code path the bug needs,
  such as an update that reaches the merger or an index type that changes routing; axes
  that leave the path alone stay unit tests. Confirm each parameter reaches the config: an
  axis that is never applied reruns the same case and covers nothing.

### Coverage-only PRs

When the PR's whole point is raising coverage rather than fixing a bug, reviewers apply a
sharper bar:

- **Don't reproduce coverage that already exists.** Grep for the suites already exercising
  the class or statement before adding anything. A duplicate test gets dropped in review
  even though Codecov counts it. Narrow to the genuinely uncovered gap — often one missing
  case folded into the test that already sets up the fixture, and whose title may already
  promise the case it never exercised.
- **Assertions must PIN behavior, not existence.** A test named "parse a BLOB column" that
  only asserts a column named `b` exists passes even when `blob` maps to the wrong type — it
  verifies nothing. Assert the meaningful property so the test fails if the parse or
  serialization is wrong. Coverage that can't fail on wrong output is not coverage.
- **Know the code path before adding scaffolding.** If the method under test is purely
  syntactic and never consults the catalog, a `create table` setup is dead weight that
  misleads the reader and slows the run. Trace what it actually touches and drop setup it
  cannot observe.
- **Fold into the existing suite rather than standing up a parallel one.** A new standalone
  coverage class invites the "this duplicates X" comment.

## Performance changes

For a performance change the "test" is a benchmark plus a real-data evaluation, not a
correctness test — see **`prove-performance`**. Name manual benchmarks `*Benchmark` so
they stay out of CI, gate real-data tests behind a `-D<path>` property with `assumeTrue`,
assert optimized-vs-baseline output identity inside the benchmark, and validate on both
local (CPU-bound) and cloud object storage (I/O-bound). A microbenchmark number on synthetic data is not the
production win.

## Related

- `review-pr` — the reviewer's rubric; self-review against it before pushing.
- `build-hudi` — JDK, Maven profiles, single-test invocation, benchmark traps.
- `develop-changes` — worktree discipline, comment policy, what never belongs in source.
- `create-pr` — branch, commit, push, review replies.
- `prove-performance` — the higher bar for performance claims.
