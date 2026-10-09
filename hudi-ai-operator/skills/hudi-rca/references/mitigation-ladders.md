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
# Mitigation ladders

Thirteen operational procedures, each ordered. They are not tied to one failure signature — they are
the discipline that makes the difference between diagnosing a Hudi failure and making it worse.
`failure-catalog.md` rows reference them by number.

Several of these say **do nothing**, **do not restart**, or **turn the number down**. Those steps are
not hedges. They are the steps most often got wrong, and the reason is usually that the obvious lever
points the other way.

**The single most expensive mistake available is restarting before capturing evidence.** That is L9,
and it earns its place by frequency.

---

## L1 — Locate the real exception in a multi-table writer

When one driver writes many tables, the trace you find first is often not the trace you need.

1. Search driver logs for `ERROR`, then search **within those results** for the table name. Keep
   scrolling — one polling cycle can contain several tables' traces interleaved.
2. **Save the full trace somewhere durable the moment you find it.** Re-finding it is expensive, and
   log rotation does not wait.
3. If you cannot attribute the failure to a table, temporarily move that table to a **single-table**
   writer. One table per job makes the trace unambiguous. Move it back afterwards.
4. If the first pass (ERROR only) returns nothing, widen to INFO and WARN. Some stalls produce **no
   ERROR line at all** — see L8 and HUDI-RCA-038.
5. Only then start root-causing.

---

## L2 — Classify a Spark OOM before reacting

An OOM is not one failure mode. These six have genuinely different remedies, and reaching for heap
first is right in only one of them.

1. **Container OOM-kill (exit 137), Java heap fine** → native or off-heap memory, not heap. Raise
   `spark.executor.memoryOverheadFactor` (commonly to 0.35, up to around 0.5). Suspect compression
   codecs and shuffle transfer.
2. **JVM heap OOM (exit 52)** → a heap dump is usually written automatically. **Get it.** This is a
   code or data-shape problem, not a sizing problem: memory buys time, not a fix.
3. **`OutOfDirectMemoryError` / `Direct buffer memory`** → raise overhead and direct-buffer memory. As
   an emergency measure only, stop Spark preferring direct buffers for shuffle transfer, at a
   throughput cost. **Do not leave that on.**
4. **Evicted mid-stage with `FetchFailedException` on shuffle files** → **this is not memory at all.**
   It is **ephemeral local storage**. Evicted executors delete their shuffle files, surviving tasks
   fail to fetch, Spark retries the whole stage, writes more shuffle, and evicts more executors. Raise
   the executors' local-disk request and limit. **This does not show up in the Spark UI** — check pod
   and node events instead. Treating a FetchFailed as a memory problem is a wasted tuning cycle, and
   it is the most commonly misread entry in this list.
5. **Driver OOM requesting a large array** → the JVM cannot allocate a single contiguous array above
   roughly 2 GB. This is a **structural ceiling**; more heap will not help. See HUDI-RCA-021.
6. **A few partitions consistently OOM while others are fine** → skew, or large rows. Check per-record
   size in the stage detail. Above roughly 1 MB average, lower
   `spark.shuffle.spill.numElementsForceSpillThreshold`.
7. **GC pressure rather than hard OOM** → **reduce** `spark.memory.fraction` and
   `spark.memory.storageFraction`. Counter-intuitive: giving Spark's managed regions *less* leaves
   more heap for user objects. Confirm G1GC and enable GC logging before tuning further.
8. Check for a pathological CPU-to-memory ratio. Powerful CPUs paired with small memory cause this
   repeatedly, and no Hudi config will fix it.

---

## L3 — Safe-deletion protocol

**Never delete a file from a Hudi table outside this envelope.** Nine steps, in order, no exceptions.

1. Inform stakeholders.
2. **Pause the writer.**
3. **Take a savepoint.**
4. Verify the deletion premise with **two independent checks** — for example, the file is untracked in
   commit metadata **and** its instant is named by a completed rollback. One check is not enough.
5. **Copy the files you intend to delete to a backup prefix.**
6. Delete.
7. Validate: re-run the failing operation, or the validator.
8. Resume the writer.
9. After a few successful commits, pause, **delete the savepoint**, resume.

**If any verification is ambiguous, stop.** Deleting a non-orphan corrupts the table silently — there
is no error, and you will find out later from query results. The ambiguity is the signal, not an
obstacle to work around.

A human drives this procedure. An agent may explain it, point at the evidence that would satisfy
step 4, and say which files it believes are implicated — but **an agent must not perform, script, or
sequence the deletion.** See the explain-only block on HUDI-RCA-025.

---

## L4 — Decide whether a stuck table service is actually stuck

Diagnose in this order. **Each link gates the next**, so fixing a later link while an earlier one is
blocked accomplishes nothing.

1. **Is archival healthy?** The newest file in `.hoodie/archived/` should be recent — within about an
   hour on an active table. Look for a successful-archive log line and the absence of archive
   failures. If the active timeline holds fewer than roughly 1000 instants, archival is almost
   certainly not your problem.
2. **If archival is behind, look at clean.** Read `earliestCommitToRetain` from the newest `.clean`.
   **Archival will not archive anything after that timestamp**, so a stale clean fully explains a
   growing timeline. This is why tuning archival configs in response to a growing timeline so often
   does nothing — the cleaner is holding the line.
3. **If clean is behind, look at compaction.** The cleaner can only clean a file group that has more
   than one version. On MOR, new versions appear when compaction runs. No compaction means no new
   versions, which means nothing to clean, which means nothing archivable.
4. **If compaction is behind, look for a blocker.** An inflight commit or a long-running rollback on
   the active timeline blocks everything after it. Find the culprit instant. If it is a delta commit
   whose Spark job is hung, cancel it so it can be rolled back. **If the rollback itself is slow, read
   HUDI-RCA-021 before restarting anything** — restarting makes a listing-based rollback strictly
   worse.
5. Compaction also cannot touch a file group a writer is actively writing to. A file group that looks
   permanently skipped may simply be hot.

`HoodieTableHealthChecker --checks archival,cleaner,compaction,mdt-compaction --output JSON` walks
this chain for you. Prefer it to manual listing.

---

## L5 — Investigate a metadata-table / filesystem divergence

1. **Classify the direction per table** before theorising. One validator run commonly covers several
   tables, and two can diverge in **opposite directions** from the same defect.
2. **Archive the timelines before mitigating.** Both remedies overwrite the diverged state; once you
   rebuild, the evidence is gone and the question becomes unanswerable.
3. **Scan by instant, not by filename.** Cleans and rollbacks name the instant they acted on, never
   individual files. The instant needle has by far the highest yield.
4. **Do not `grep` timeline Avro files.** They contain NUL bytes, and some `grep` implementations
   **silently return no match** for content after a NUL on the same line. Use a byte-oriented scanner.
   A rollback that explained a real divergence was found by a NUL-safe tool and **missed by system
   `grep -a` on the same file** — the false negative looked exactly like a true negative.
5. **A scan that prints "no hits" on an empty needle file proves nothing.** Assert your needle file is
   non-empty before believing a negative result.
6. **Decode Avro records properly rather than running `strings`.** `strings` cannot render an empty
   array or an integer, so exactly the two fields that distinguish "the rollback failed to delete
   these files" from "it never saw them" — an empty rollback-request list, a zero deleted-file count —
   are invisible to it.
7. **Measure your actual timeline coverage** before concluding that no event touched these files.
   Count completed commit-family **instants** per month, not state files: each instant has up to three
   state files, and rollbacks linger on the active timeline for months, so a raw file count can read
   as nine events where the truth is three rollbacks and zero commits.
8. Instants are **17 digits** (`yyyyMMddHHmmssSSS`). A pattern that captures 13 will make every later
   search miss.
9. **A negative is a finding.** "No instant across N timeline entries names these files, and coverage
   spans the divergent instant" is a conclusion, not a dead end.

---

## L6 — Check that a correctness signal is even real before investigating

Three cheap checks. Each has, on a real incident, been the reason an expensive investigation was
unwarranted.

1. **One table, or everything at once?** Several unrelated tables or jobs failing the same check
   within minutes usually means a change to the **check** — a rule rename, a threshold edit, a deploy
   — not simultaneous data corruption. Correlate against recent validator and alert configuration
   changes first.
2. **Is the signal fresh?** If your validator pushes to a store that retains the last value
   indefinitely, a validator that died days ago presents as a steady, confident failure. Check the
   **job's** last successful run, not just the metric. If the last success is older than about two
   schedule intervals, no per-table conclusion drawn from it is safe. **A dead data-correctness
   validator is a higher-severity problem than the divergence it was going to report.**
3. **Is the condition new?** A value that has been non-zero for weeks with no step change is a
   standing backlog, not an incident.

---

## L7 — Decide whether "no clean commits" is a problem

The decisive, cheap check: in recent commit metadata, **if `numWrites == numInserts` for a file, that
write created a new file group**, so there is no older version to clean — the cleaner is working
correctly. Hudi also retains one prior version as a buffer, so a file group with two base files still
has nothing to clean.

Confirm this before changing any cleaner configuration. Most reports of "the cleaner is not running"
are non-incidents, and changing retention in response is how a non-incident becomes one. Full
treatment in HUDI-RCA-037.

---

## L8 — Diagnose a stalled table with no visible exception

When a job looks healthy — no exceptions, no restarts — and nothing is ingested:

1. **Compare the committed checkpoint against the source's current position.** Ahead of source latest
   means a recreated or aged-out source (HUDI-RCA-012). Far behind means genuine lag (L10). **Equal
   and frozen** means HUDI-RCA-038.
2. **Check for a stuck inflight instant** (`.commit.inflight`, `.replacecommit.inflight`,
   `.rollback.inflight`) and note its age. For a rollback or clustering instant, note the **plan file
   size** — a multi-MB plan is itself the diagnosis.
3. **Check that the driver is healthy in a way your health check can actually see.** **A driver can
   pass a liveness probe and do no useful work** — for instance after an OOM cleanup closed its shared
   filesystem client, so every subsequent operation fails instantly while the health endpoint answers
   fine and the restart count stays at zero. Never infer health from a probe alone. Verify that
   commits are landing, **per table**.
4. **Beware per-table blind spots.** On a multi-table job, "some tables are committing" does not
   establish that the *affected* table is. Check the table in question specifically.

---

## L9 — Capture evidence before restarting

**A restart is the most common and most expensive mistake in this corpus.**

1. If a driver is wedged or crash-looping, **take the heap dump first.** Several failures were only
   ever root-caused because dumps were captured while the wedge was live. In more than one case,
   reading logs produced a plausible and **wrong** hypothesis that only a heap dump falsified.
2. Capture a busy-thread dump. If nearly all the hottest threads are GC threads, you have a memory
   problem, not a CPU problem, and adding cores will not help.
3. To attribute a leak to a table, string-scan the heap dump for storage URIs and rank by frequency.
   The dominant table is usually the overwhelming majority of the leak.
4. **For a rollback or clustering wedge, a restart actively harms you** — it abandons in-progress work
   and re-requests a larger unit. A rollback plan file that grows across successive attempts is the
   proof that this is happening.
5. Write down, on the ticket, which hypotheses you have **falsified**. Investigations repeatedly lose
   hours to a hypothesis that was already disproved but never retracted.

---

## L10 — Read lag metrics correctly during a backfill

Lag computed as (source head position − committed checkpoint position) **does not decrease smoothly**
when the source is organized into a few very large units of work. If a backfill pages through one
large source instant in budget-limited chunks, the file cursor advances while the instant does not, so
lag stays pinned and then drops in a step when that instant drains. **A staircase, not a slope, by
design.**

Before escalating "lag isn't moving":

1. Confirm commits are landing and carrying real volume.
2. Confirm which component of the checkpoint the lag metric actually tracks.
3. Check whether the job is budget-pinned by a per-sync read limit. If so, the ceiling is the limit,
   not the cluster: raising the read limit (and reverting it afterwards) is the lever, and **adding
   executors is not**.
4. Check that your executor-scaling policy can actually reach the backlog. A conservative dynamic
   allocation ratio can target far fewer executors than the work justifies and never scale up, leaving
   most of the cluster idle through an entire backfill.

---

## L11 — Keep batch-size settings coupled to the memory they assume

A per-batch limit — messages per poll, records per request, bytes per sync — is valid **only for the
heap it was sized against**. A driver shrunk for cost while its batch override, sized for a much
larger driver, was left in place produces an unbreakable OOM loop on the next cold start.

1. Record, next to any batch-size override, the driver memory it assumes.
2. When you change driver or executor memory, **re-derive every dependent batch-size override.**
3. If the two live in different files or systems, treat that as a known hazard and check both on every
   resize.
4. **Size for the cold start, not the steady state.** A cold start meets the full accumulated backlog
   in one batch.

---

## L12 — Verify that a config you set is actually in effect

Several failures trace to a config that was set and silently ignored — a misspelled key, segments in
the wrong order, or a value overridden rather than merged by a layered configuration system.

1. Read the **effective** configuration from the running job — the resolved write-config dump in the
   driver log, or live JVM inspection — rather than the file you edited.
2. If your configuration system **overrides rather than merges** list-valued settings (transformer
   chains being the classic case), re-list every value you still want, not just the new one. Adding
   one transformer can silently drop the one that was already there.
3. **A misspelled Hudi config key produces no error.** The library default simply applies. Treat "I
   set it and nothing changed" as evidence that **the key is wrong**, not that the setting does not
   work.
4. Beware empty maps or dictionaries in layered configuration formats — some merge implementations
   null out an entire block.

---

## L13 — Triage an unmatched stack trace into a failure family

When no catalog row matches, classify from the top-level exception class and the first two or three
`Caused by` frames. **This is a starting hint, not an action** — it narrows where to look, it does not
attribute a cause.

| Family | Heuristic |
|---|---|
| Meta-sync failure | Frames in `org.apache.hudi.sync.*` (catalog, Iceberg, or Glue sync) |
| Write failure | `HoodieWriteException` / `HoodieUpsertException` / `HoodieInsertException` in the top class or the caused-by chain |
| Write conflict | Concurrency-control exceptions — check the lock provider and concurrency mode |
| Transform failure | Frames in your own or a platform transformer package. Check **whose** code it really is before blaming the platform |
| Schema compatibility | Avro schema-mismatch frames, `IncompatibleSchemaException` → HUDI-RCA-003 |
| Schema fetch | Schema-registry client frames → HUDI-RCA-011 |
| Read-from-source | Source consumer, object lister, or CDC connector frames at the top of the chain |
| Connection timeout | `SocketTimeoutException` or a connection-pool timeout → HUDI-RCA-031, HUDI-RCA-024 |

**The governing rule: if even one frame is in a Hudi or user-code package, look there first.** A trace
consisting purely of engine internals is the only case where a generic family heuristic is the best
available answer — and a bare `NullPointerException` with no Hudi or user frame anywhere is
specifically **not attributable**. For that case, say so, and note that
`hoodie.streamer.row.throw.explicit.exceptions=true` will surface the underlying exception on a
re-run. It costs performance; it is a diagnostic, not a setting, so turn it back off afterwards.
