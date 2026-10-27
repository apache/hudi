---
name: prove-performance
description: Use whenever a change claims to reduce CPU, latency, memory, I/O, or cost — compaction/merge speedups, serde/compression/allocation cuts, "skip work on unchanged data", caching, batching, prefetch — or when a benchmark shows a big win and you are about to write it into a PR description. Also use when asked "is this actually faster", "prove the speedup", or when a local number needs to be defended against real-data or S3 behavior. Complements develop-changes (general hygiene) and author-changes (Hudi test coverage).
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

# prove-performance — proving a performance win is real

A performance change is not done when it compiles and a microbenchmark prints a big number. It is
done when the win is proven at multiple levels, on real data, and on the deployment substrate
(local AND cloud/S3). This skill is the checklist that keeps a plausible-looking speedup from being
an illusion. Related: `develop-changes` (comment/source hygiene, worktree, build+checkstyle before
commit), `author-changes`/`build-hudi` for Hudi specifics, `create-pr` for PR mechanics.

The one lesson behind all of it: **synthetic benchmarks point a direction; real data decides.** The
case that produced this skill: a whole-block-copy compaction optimization measured 16-250x on
synthetic data and looked like a 3x end-to-end win. A real file group from a live table then showed the
workload was 93% inserts, making the optimization a near no-op there — the reliable wins turned out
to be two unrelated levers (disabling an unused bloom filter, switching codec). Shipped on the
microbenchmark alone, it would have been the wrong bet.

## 1. Profile before you optimize

- Get a real profile of the real workload first (CPU flamegraph, allocation profile, wall vs CPU,
  thread dump). Do not optimize from intuition.
  `.claude/skills/prove-performance/scripts/parse_flamegraph.py` parses
  async-profiler HTML without a browser; capture on a real executor with async-profiler
  (`-e itimer` CPU, `-e wall` off-CPU/blocking, `-e alloc` allocation).
- Rank costs and attack the biggest, MOST BROADLY APPLICABLE one first. A lever that helps every
  operation with no eligibility condition (e.g. drop an unused computation, change a codec) beats a
  narrow lever that only fires on a subset of inputs, even if the narrow one is flashier.
- Separate CPU-bound from I/O-bound up front (wall vs CPU in the profile). It determines whether the
  bottleneck is compute (serde, compression, allocation) or I/O (S3 GETs, listing, latency), and
  therefore which optimizations can possibly help and whether local benchmarks will even reflect
  production (see section 4).

## 2. Validate at three levels (all three, not one)

Each level catches what the others miss. Skipping a level is how wrong wins ship.

1. **Micro** — isolate the mechanism (the serializer, the codec, the merge step) through the REAL
   changed classes, not a reimplementation. Fastest to iterate; proves the mechanism works and
   bounds its ceiling. Caveat: synthetic inputs mislead. Compressible synthetic data inflates
   compression/copy wins; uniform synthetic keys hide clustering effects. Treat micro numbers as
   upper bounds under favorable data, not as the expected production gain.
2. **E2E through the real code path** — drive the ACTUAL production entry point (the real
   compactor / merge handle / reader), not the helper. This is where wiring reality bites: the fast
   path may never be reached, may hit an unimplemented branch, or may fall back. In one investigation the
   e2e test revealed the optimized handle fell back every time (no base file on first compaction; an
   unsupported delete-emit path). A green microbenchmark said nothing about that.
3. **Real data** — run against a real input pulled from a live table (a real file group, a real
   dataset). This is the level that OVERTURNS synthetic conclusions. Measure the actual data
   characteristic the optimization depends on (see section 3) on real inputs before believing any
   projected win.

Vary the measurement METHOD too, not just the level: wall time, CPU time (itimer samples),
allocation profile (`-e alloc`), retained-heap delta post-GC, and a correctness check. Agreement
across methods is the evidence; one green number is not.

## 3. Compute the fraction the optimization actually depends on — on real data

Most perf wins are conditional: "skip work on unchanged/clean/cached items", "fast path when X".
The win scales with how often the condition holds. Do not assume it; compute it, then measure it on
real data.

- For skip-unchanged-block / skip-unchanged-record optimizations the win is set by the DIRTY
  fraction. Occupancy math: if a fraction `u` of items change and there are `n` items per unit
  (records per block), the probability a unit is dirty is `1 - (1 - u)^n`. Large units (n big) go
  ~100% dirty even at tiny `u`, so "copy clean units" copies nothing. Compute this before building.
- Then MEASURE it on real data: e.g. read the real base + the real delta logs, classify each change
  as touching-an-existing-unit vs new, and count the actually-dirty units. In one investigation this
  turned an assumed "94% clean, big win" into a real "55% dirty, ~2x at best, and the wired path
  falls back entirely" because the real logs were insert-dominated.
- Inserts vs updates vs deletes are NOT interchangeable for layout-preserving optimizations.
  Enumerate the real mix; a fast path gated to one of them may never fire in the real workload.

## 4. Measure on BOTH local and cloud (S3)

Local disk and S3 have opposite cost structures, so a local number does not transfer.

- **Local** (SSD, warm page cache): CPU-bound. CPU-side wins (serde, compression, allocation, block
  copy) show their full effect; I/O is nearly free. Good for isolating compute wins and iterating.
- **Cloud / S3**: latency- and throughput-bound. Each block read can be a ranged GET (~tens of ms);
  listing is slow; throttling is real. A change that removes CPU work but not I/O can be a big local
  win and a small cloud win; conversely, readahead / whole-file download / fewer-GET changes barely
  move locally but dominate on S3. A 16x local speedup can be 2x on S3 if I/O dominates the wall.
- Therefore: run the SAME benchmark against a local copy AND against the real object store, and
  report both. Use the scripts under
  `.claude/skills/prove-performance/scripts/` (below). If a real cloud run is not
  possible, say so explicitly and label the local number as CPU-bound-only; do not present it as the
  production number.
- For the fullest cloud signal, capture an async-profiler flamegraph (CPU + wall) on a real Spark
  executor running the change on S3 data (the wall profile shows the I/O parking that local runs
  hide), and diff against baseline.

## 5. Benchmark hygiene (so the numbers mean something)

- **Warmup + medians.** Discard 1+ warmup rounds (JIT, class load, page cache), report the median of
  several measured rounds, not the mean (GC spikes skew means). Note variance.
- **Assert correctness INSIDE the benchmark.** A fast path that produces wrong output is worse than
  no change. Assert byte-identity (or record-identity) between the optimized and baseline output in
  the same harness, so a fast-but-wrong result fails the run instead of printing a great number.
- **Name manual benchmarks so CI skips them.** A class named `*Benchmark` (not `Test*`/`*Test`) does
  not match surefire include patterns and never runs in CI, but is still runnable on demand with
  `-Dtest=<name> -Dsurefire.failIfNoSpecifiedTests=false`. Gate real-data tests behind a
  `-D<prop>=<path>` system property and `assumeTrue(...)` so they skip without the data and commit
  no production data.
- **Maven incremental-compile skips new bench files.** New/edited test-source files can be skipped
  with "target directory newer than sources". Run with
  `-Dmaven.compiler.useIncrementalCompilation=false` and `touch` the file, or the old class runs.
- **Heap and read strategy for large real files.** A multi-hundred-MB real file needs heap: the fork already runs with `-Xmx4g` from the root pom's `argLine`
  property, and `-DargLine` on the command line never reaches it (see `port-fixes`). For more, raise
  that property locally and do not commit it. Prefer streaming from storage over loading the whole file into a `byte[]` when you only
  need counts / index / point lookups; only materialize bytes for the part you actually rewrite.
- **Measure the phase that matters, not the setup.** Time only the operation under test (exclude
  fixture writes, JVM start). Print what was excluded so the number is honest.

## 6. Shipping order and safety

- Ship the broad, unconditional, format-neutral wins FIRST (they help every operation, low risk):
  e.g. remove an unused computation, right-size or disable an unused structure, change a codec.
  Ship the narrow, eligibility-gated wins after, and set expectations for how often they fire.
- Gate every optimized path behind a config flag, DEFAULT OFF. Fall back to the correct baseline on
  ineligibility or any error. Decide eligibility fully BEFORE the first irreversible write (once you
  start writing the output you cannot fall back).
- Prove behavior preservation with a differential test (optimized vs baseline, identical output) and
  the multi-level validation above, not "the suite is green".

## 7. Report honestly, especially when real data contradicts the plan

- Lead with the real-data number and the condition it depends on, not the synthetic best case.
- If real data shows the optimization is narrow or a no-op for the target workload, say so plainly
  and redirect to the levers that do apply. That is a successful investigation, not a failure.
- State the deployment caveat: "measured local (CPU-bound); S3/I/O-bound gain expected lower/higher
  because ...". Never let a local micro number stand in for the production number.

## Scripts

They live in `.claude/skills/prove-performance/scripts/` — run them from the repo root by that
path.

- `pull_file_group_from_s3.sh` — download a
  Hudi file group (base file + its delta log files, matched by file id) or an arbitrary prefix from S3
  to a local dir, for real-data analysis. Uses `aws s3`.
- `run_perf_test.sh` — run a gated
  benchmark / analysis in a Hudi repo with the correct flags (heap, incremental-compile off, the
  `-D<data path>` property), against a LOCAL path.
- `run_perf_s3.sh` — run the same benchmark
  reading directly from `s3a://` (no download), so the measured wall time includes real S3 I/O;
  documents the AWS creds + `hadoop-aws` requirements.
- `parse_flamegraph.py` — CPU/wall/alloc
  flamegraph analysis of a local or cloud run, from async-profiler HTML, without a browser.

Read each script's header before running; they take explicit paths/args and do not assume a table.

## Related

- `develop-changes` — worktree, comment policy, and the general validate-empirically rule.
- `author-changes` — how a performance change's tests and benchmarks should be shaped for review.
- `build-hudi` — the benchmark invocation traps (`*Benchmark` naming, stale-class
  recompilation, surefire heap).
