<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# RFC-100 Part 2 (Design v3): Append-Only Blob Reference Registry

**Status: DRAFT, revision 3 (2026-10-27). Supersedes
[rfc-100-blob-cleaner-design-v2.md](rfc-100-blob-cleaner-design-v2.md). Problem statement,
constraints (C1-C11) and requirements (R1-R9) are unchanged and live in
[rfc-100-blob-cleaner-problem.md](rfc-100-blob-cleaner-problem.md).**

What changed from v2, in one list:

- The registry source moves off `HoodieWriteStat` (serialized into commit metadata) onto the
  in-memory `WriteStatus`. A commit file carries at most one boolean, `blobRefsWritten`.
- The registry becomes append-only. Liveness is "a registry entry whose file is still in the
  FILES partition", so the clean never writes to the registry. Hygiene sidecar, registry
  tombstones, per-file manifests, reconciliation sweep and pending-deletes queue are cut.
- Stale registry entries are dropped by a filter inside MDT compaction of `blob_refs`, never by
  a free-standing sweep.
- The plan-time snapshot comes from a dedicated metadata reader, and the locked step of plan
  scheduling checks the `blobRefsWritten` flag of commits completed since the snapshot, with
  unlocked retry.
- The writer preCommit veto also covers cleans completed after the writer's own instant time.
- Rollback and restore hand orphaned managed paths to the next clean; plan replay re-verifies
  the old sidecar list.

The protocol is model-checked: see [model/](model/README.md) and section 11.

Figures: [architecture, v2 and v3 side by side](rfc-100-blob-cleaner-v3-architecture.svg) and
[six sequence diagrams](rfc-100-blob-cleaner-v3-scenarios.svg). "Panel N" below refers to the
scenarios figure.

---

## 1. The problem in one paragraph

A BLOB column can store its bytes out of line: the row holds a `BlobReference`
(`external_path`, `offset`, `length`, `managed`) and the bytes live in a separate object such
as `s3://bucket/img/123.jpg`. Hudi's existing cleaner deletes expired file slices (base and
log files) but knows nothing about what those files pointed at. If a blob is referenced only
by files the cleaner just deleted, the blob is now an orphan that nobody will ever reclaim.
If the cleaner deletes a blob that some other live file still points at, a query returns a
dangling reference. The blob cleaner has to delete the first kind and never the second kind.

Two requirements dominate everything below:

- R1, no premature deletion. A blob is deleted only when no file that a reader can still
  open references it.
- R2, no permanent orphans. Every blob that stops being referenced is eventually deleted.

Plus the cost rules: no global serialization of writers (R4), cost proportional to the work
actually done (R5), zero cost for tables without BLOB columns (R6).

## 2. Why the obvious approaches do not work

**Per-file-group cleanup is not enough.** One blob is commonly referenced from many file
groups and many partitions (C3). The expiring file slice in partition A tells you nothing
about whether partition B still points at the same path. So the cleaner needs a cross-table
answer to "who else references this path".

**A full table scan per clean is too expensive.** It would read every base and log file on
every clean cycle to answer that question. That violates R5 for large tables.

**Snapshot liveness is the wrong notion.** Hudi retains old file slices for time travel and
incremental queries. A reference that was overwritten in the latest slice of a file group
may still be present in an older retained slice of that same file group, and a query
inside the retention window can read it. So liveness must be defined over retained files,
not over the latest snapshot. This is what killed the earlier secondary-index design: the
secondary index only answers snapshot questions.

**Writers race the cleaner.** A writer can legitimately write a brand-new reference to an
old path (re-upload the same object, copy a row, restore a backup). If that happens while
the cleaner is deciding the path is dead, the cleaner must notice.

**Blob references must stay out of commit metadata.** Storing which blobs a commit wrote in
the commit file does not scale and was ruled out on the PR (see also C10). Everything below
keeps the commit file free of blob paths. The only blob-related thing a commit file may carry
is a single boolean (section 4).

## 3. The one idea: a registry of (path, file) pairs

This design adds a metadata-table partition called `blob_refs`. Each record says
"file F contains at least one OUT_OF_LINE reference to path P". The key is path-first,
`escape(P)$escape(partition)$escape(F)`, so that a prefix scan on P returns every file that
references P. File groups of the partition are hashed on `escape(P)` alone, the same trick
the secondary index v2 uses, so a prefix lookup for P is routed to one file group instead
of scanning all of them.

Two facts make this partition cheap to keep correct:

1. **Entries are created at write time, by the writer, from what it just wrote.** Each
   writer task already knows the rows it emitted. It collects the distinct OUT_OF_LINE
   paths per output file into its `WriteStatus` (the in-memory per-task result, never
   serialized to the timeline) and the metadata-table writer turns them into registry
   records distributedly, the same way the record-level index is generated. The registry
   records land in the same metadata delta commit as the FILES entry for F, before the data
   commit is marked complete. So a completed commit always has its registry entries, and a
   failed commit's entries are hidden by the same completed-instant fence that hides its
   FILES entries.

2. **Entries are never deleted by the clean.** This is the key difference from v2. An entry
   `(P, F)` is "dead" the moment F is no longer in the FILES partition. The cleaner does not
   need a tombstone to learn that, it can just check FILES. Dead entries are physically
   dropped later, during metadata-table compaction of `blob_refs`, by a compaction filter.

   The filter runs only inside MDT compaction, never as a free-standing sweep. That matters:
   the compaction instant is chosen below the earliest data commit that is still pending, so
   every entry in the compaction input belongs to a completed commit. A sweep that read FILES
   through the completed-instant filter at an arbitrary time would drop the entry of a commit
   whose metadata delta has landed but whose data commit has not completed yet, and that
   commit would then complete with no registry entry (the model finds this in 11 steps).
   To keep the filter cheap it does not load all of FILES; it drops entries whose file appears
   in the deleted-file sets of the cleans completed since the previous compaction, read from
   those cleans' metadata.

With that, liveness has a precise and cheap definition:

> Path P is live if and only if some registry entry `(P, F)` exists where F is present in
> the FILES partition, both read at the same metadata-table snapshot.

"Same snapshot" matters, and it is not free with today's reader. `HoodieBackedTableMetadata`
memoizes the set of valid data instants per reader instance, and the table's cached reader
is reset by the file-system view whenever it syncs. So the cleaner uses a dedicated reader
instance for blob verification, performs no reset between the registry read and the FILES
read, and defines T0 as the instant set that reader actually used, not as a separately
loaded timeline. Reusing the table's reader lets a writer land between the two loads and be
invisible to both the snapshot and the delta check below.

Why FILES and not the file-system view: the file-system view hides file groups that a
replace commit (insert_overwrite, clustering) superseded, but those file groups stay
readable by time travel until the clean deletes them. Raw FILES membership keeps them live.
That is the C7 trap shown in red in panel 3.

## 4. The lifecycle, step by step

Panel 1 draws this for one row whose blob pointer moves from P to Q.

**Write.** Writer emits rows into file F1 with references to P. It records `{P} for F1` in
its WriteStatus. At commit, inside the writer's transaction lock, the metadata writer
appends `(P, F1)` to `blob_refs` and `F1` to FILES in one metadata delta commit, then marks
the data commit complete. If the commit wrote any OUT_OF_LINE reference, it also sets one
boolean in its commit `extraMetadata`, `blobRefsWritten=true`. That is the only blob-related
content a commit file ever carries and it is constant size.

**Clean plan (unlocked part).** The normal cleaner plan decides which file slices expire.
If the table has no `blob_refs` partition the blob logic is skipped entirely, satisfying R6.
Otherwise, for each expiring file F the cleaner enumerates the OUT_OF_LINE paths in F
(reading the file, bounded by the number of expiring files, which is the work the clean is
already doing). It then does one routed prefix scan per candidate path against `blob_refs`
and one membership check against FILES, both through the dedicated reader at snapshot T0,
and keeps only the paths with no live entry outside the expiring set. Those are the
candidates. A second candidate source is merged in here: paths handed over by rollback and
restore (section 8).

**Clean plan (locked part).** `scheduleCleaning` already takes the transaction lock to
persist the plan, but it computes the plan before taking it, so the blob re-check is new
locked work and has to be small. Inside the lock the cleaner reloads the timeline and looks
at the commits that completed after T0. If none of them carries `blobRefsWritten`, the
candidates are final. If some do, the cleaner does not try to find out which paths they
touched under the lock (no read path returns only the registry entries of an instant range,
and an MDT compaction inside the window would fold them into a base file anyway). Instead it
releases the lock, re-runs the unlocked verification at a fresh T0, and retries, up to a
small bounded number of times. If the table is so busy that every retry finds a flagged
commit, the last resort is the exact per-candidate registry lookup under the lock, which is
correct and merely slower. Candidates are never silently dropped on this path: a candidate
whose files are being deleted in this clean and that is not verified now will never be
enumerated again.

The surviving list is written to the plan sidecar, `.hoodie/.aux/clean/<instant>.blob_deletes.parquet`,
sorted by path and with a bloom filter in its footer, and the plan is saved as REQUESTED
pointing at it (`HoodieCleanerPlan.extraMetadata["blobDeletesPath"]`). From this moment the
list is durable.

**Clean execute.** The executor deletes the expired file slices as today, then deletes the
blobs listed in the sidecar. Slice deletion throws on any failure, so the blob stage only
runs after every slice delete succeeded. Blob deletion is idempotent, so a crash and re-run
of the same clean instant is safe. The clean then writes its normal metadata updates (FILES
deletes, column-stats deletes). It writes nothing to `blob_refs`.

**Later, metadata compaction.** When `blob_refs` is compacted, the filter drops `(P, F1)`
because F1 is in a completed clean's deleted-file set. Until then the stale entry is
harmless: any lookup that finds it also finds F1 absent from FILES and treats it as dead.

## 5. Writer veto

The candidate list is durable from REQUESTED onward, and the clean may sit in REQUESTED or
INFLIGHT for a while (async cleaning). A writer that starts after the plan was persisted
could commit a new reference to a candidate path before the executor deletes it. The locked
re-check in the plan cannot see that writer because it did not exist yet. A writer that
wrote its files before the clean started and only takes the commit lock after the clean
completed is in the same position: the clean never saw it, and the path it references is
already gone.

So the writer side has a check too. In `preCommit()`, a writer that emitted any OUT_OF_LINE
reference compares its path set against the plan sidecars of:

- every clean that is REQUESTED or INFLIGHT, and
- every COMPLETED clean whose completion time is later than the writer's own instant time.

The second bullet is what covers the long-running writer, and the model shows it cannot be
dropped. Bounding it by the writer's start instant instead of "everything still in the
active timeline" keeps the set small: with the default of a clean per commit and archival
keeping twenty to thirty commits, an unbounded check would read every one of those sidecars.

The check is cheap in the common case. The sidecar is sorted with a bloom filter, so a
no-overlap probe is one small read. The writer runs the probe distributed, before taking
the lock, against the cleans visible at that time. Under the lock it re-checks only cleans
that became REQUESTED since the probe. On overlap the writer aborts with
`HoodieBlobReferenceConflictException`. The abort is not a plain retry: the writer referenced
bytes that are gone or about to go, so it has to re-derive or re-upload them before trying
again. Only ingestion and insert_overwrite need this check; clustering and compaction copy
pointers that were already visible and are covered by the FILES rules.

Panel 2 shows the three ways a concurrent writer can interleave with a clean:

| Timing | Writer commits | What catches it |
| --- | --- | --- |
| A | before the cleaner's snapshot T0 | registry + FILES at T0 shows the new file live, P retained |
| B | between T0 and the locked plan step | locked flag check sees the commit, verify is re-run |
| C | after the plan is persisted | writer's preCommit veto aborts the writer |

There is no fourth timing: the writer's commit path holds one lock across preCommit,
metadata write, and completion, and the locked plan step holds the same lock, so a commit
is entirely before T0, entirely inside (T0, locked step], or entirely after the plan is
visible. The model checks this exhaustively for two writers and two clean cycles, including
crashes and rollbacks; removing the veto, the locked check, or the completed-clean bullet
each produces a counterexample in about ten steps.

## 6. Defer, do not annul

When verification finds that a candidate has a live reference elsewhere, the design
defers it: the path is simply not deleted this cycle, and it comes back as a candidate the
next time a file referencing it expires. There is no "annul" step that tries to re-derive
whether the path is really live, because that re-derivation has silent failure modes
(enumeration can fail, the managed flag can differ across file groups). Deferring is safe
for R1 and costs nothing for R2 exactly because a live reference exists: the file holding it
will be enumerated when it expires. Deferring a candidate for any other reason (a retry
budget, a cap) is not safe for R2 and is not done.

## 7. What v2 had on top, and why it is cut

The architecture figure shows these lane by lane.

v2 used two files it called sidecars. Keep them apart:

- The **plan sidecar** `<instant>.blob_deletes.parquet` is the verified delete list. Kept.
- The **hygiene sidecar** `<instant>.blob_refs.parquet` listed every (path, file) pair in
  every cleaned file so the MDT conversion could tombstone them. Cut.

| v2 mechanism | Purpose in v2 | Why v3 drops it |
| --- | --- | --- |
| Registry source on `HoodieWriteStat` | Carry per-file path sets from the write to the metadata writer | `HoodieWriteStat` is serialized into `HoodieCommitMetadata`, so this stores blob references in the commit file (C10, and ruled out on the PR). `WriteStatus` carries the same sets in memory. |
| Per-file manifests (Parquet footer key, new log-block header type) | Enumerate paths in an expiring file without reading rows | Enumeration cost is bounded by the expiring files, which the clean already touches. The log-block header is a versioned on-disk format change. Nothing correctness-critical is lost. |
| Hygiene sidecar `<instant>.blob_refs.parquet` | Feed registry tombstones for every (path, file) in every cleaned file | Tombstones are O(rows) for row-level-distinct blobs and v2 wrote them under the clean lock, which serializes writers. FILES membership makes them unnecessary. |
| Registry tombstones, invariant I2 | Keep the registry exactly equal to the live set | Replaced by "dead if file not in FILES" plus compaction-time GC. |
| New locked REQUESTED-to-INFLIGHT transition in the executor | Run the delta re-check at execution time | The plan already has a lock. The re-check there is new work but small: a boolean per commit, with unlocked retry. Cost is the wider veto window above. |
| Reconciliation sweep | Repair registry drift from annul and crash cases | No drift sources remain: the clean never writes to the registry. |
| Pending-deletes queue with 24h grace | Give restore a window to undo a delete | Optional later phase. Without it, the one unrecoverable case is a restore to a state that references a blob a completed clean already deleted. The file-slice cleaner has the same gap for data files today. |

Kept from v2 unchanged: the registry partition itself, plan-time durable candidates in the
plan sidecar, the two-step verify, the writer preCommit veto, defer-over-annul.

## 8. Edge cases, one line each

Panels 3 to 6 draw these.

- **Cross-file-group sharing.** P referenced from two partitions; expiring one leaves the
  other's entry live, so P is retained. Deleted only when the last referencing file leaves
  FILES.
- **Clustering and insert_overwrite.** The new file group gets its own `(P, F_new)` entry at
  commit. The replaced group stays in FILES until cleaned, so P is live throughout.
- **MOR log files.** FILES lists log files too, so a reference that exists only in a log
  file keeps P live. A log block that supersedes the base file's reference does not make P
  dead; the base file still contains it. P is over-retained until the base file is
  physically deleted after compaction. Log reads during enumeration are fenced to completed
  instants so a failed commit's block never produces a candidate.
- **Failed commit and rollback.** A writer that crashed after its metadata delta commit
  landed has entries in `blob_refs` and FILES that the completed-instant fence hides, so
  they are neither a retain vote nor a delete vote. Rollback then removes the whole
  metadata delta commit, entries included. Correctness against the crashed-then-completed
  case rests on the locked flag check, not on inflight entries.
- **Blobs orphaned by rollback and restore.** If the rolled-back file was the only one
  referencing managed path P, no future clean will ever expire it, so the clean alone cannot
  reclaim P (C9). The rollback and restore executors therefore enumerate the OUT_OF_LINE
  paths of the files they delete (managed references only) and write them as a candidate
  input for the next clean, `.hoodie/.aux/clean/pending/<instant>.blob_candidates.parquet`,
  in the same format as the plan sidecar. The next clean verifies them like any other
  candidate.
- **Restore.** Restore drops and rebuilds `blob_refs` from the restored files, like the
  other file-keyed partitions. While the rebuild is inflight the cleaner refuses to delete
  any blob. The same refusal applies while the partition is being built by the async
  indexer on an existing table.
- **Enumeration failure.** If the cleaner cannot read an expiring file, it keeps the slice
  and retries next cycle. v2's "proceed and flag" would delete the slice and permanently
  orphan everything only it referenced.
- **Crash mid-execution.** Re-running the clean instant replays the sidecar; deletes are
  idempotent.
- **Plan replay.** The one replay path in Hudi re-runs the last COMPLETED clean whose
  completion file came out empty. Its files are already deleted, so the blob list cannot be
  re-derived from them. The replay keeps the old sidecar's path list and runs it through the
  full verification again (fresh T0 plus locked check) before deleting. Pending REQUESTED
  and INFLIGHT plans run unchanged, as they do today.
- **Overlapping cleans.** Hudi's pending-clean check is unlocked, so two plans can overlap.
  The later plan re-includes the earlier plan's files, so the result is a double delete,
  which is harmless. Files in any pending plan's delete set are treated as expiring when
  computing liveness, so a retention change between the two plans cannot lose a delete.
- **Savepoints.** Savepointed slices never expire, so they are never enumerated and their
  files never leave FILES; every path they reference stays live (C8).
- **Table-internal paths.** A reference whose path parses as a Hudi base or log file name is
  never a candidate. Today a Lance-sourced row can carry a self-reference to its own Lance
  file with `type=INLINE` and null data; candidacy keys on `type == OUT_OF_LINE` and excludes
  by file-name shape, not by base-path prefix (Phase 3 managed blobs will live under the
  base path and must stay eligible).
- **Metadata-only bootstrap tables.** FILES lists the skeleton file while the BLOB data
  lives in the source file. Out of scope for the first version; the gate refuses blob
  deletion on bootstrapped tables until registry entries are keyed to the skeleton and
  enumeration reads skeleton plus source.

## 9. Gating and lock modes

**When the cleaner runs.** `blob_refs` is created automatically when a table has a BLOB
column and the metadata table is enabled. The cleaner's gate is "does the `blob_refs`
partition exist", not "does the current schema have a BLOB column", so dropping the column
later does not switch reclamation off while old files still reference blobs. With the
metadata table disabled there is no registry and no blob reclamation; the config
documentation says so and a metric reports blobs left unverified.

**Lock modes.** The correctness argument in section 5 needs the plan's locked step and the
writer's preCommit to exclude each other.

| Deployment | Status |
| --- | --- |
| Multi-writer with an external lock provider | Supported |
| Single writer, all services inline | Safe by sequentiality; clean runs in the writer's thread after its commit |
| Single writer, async services, metadata table on | Hudi auto-selects an in-process lock; supported |
| Standalone `HoodieCleaner` process on a single-writer table | No cross-process exclusion. Hudi silently replaces a user-set provider with the in-process one. Blob deletion must fail fast here. |

**Configuration.** One new tunable: `hoodie.clean.blob.verify.max.retries` (default 3), the
number of unlocked re-verification rounds before the locked per-candidate lookup. Names are
provisional until implementation.

**Observability (R9).** The clean's completion metadata records blobs deleted, blobs
deferred, bytes reclaimed, and blobs left unverified (metadata table off or partition
inflight); the writer reports veto aborts.

## 10. Phases

1. Registry partition with routed file groups, write-path record generation from
   WriteStatus, `blobRefsWritten` commit flag, partition auto-creation and gate, compaction
   GC filter.
2. Cleaner: enumeration, dedicated-reader snapshot, locked flag check with unlocked retry,
   plan sidecar with bloom filter, blob delete stage, plan replay handling, writer veto,
   rollback and restore candidate hand-off.
3. Optional: grace window and pending-deletes queue for restore SLAs; managed blobs under
   `.hoodie/blobs/`; bootstrap-table support.

## 11. Model check

[model/](model/README.md) holds an explicit-state Python model and a TLA+ transcription of
the protocol: two writers with up to two commit attempts each, one cleaner with up to two
cycles, crashes before and after the metadata delta commit, rollback, and compaction-time
GC. Invariants are R1 and R2 (R2 checked in quiescent states). Both checkers explore the
same state space.

| Configuration | Result |
| --- | --- |
| Design as written (Base.cfg) | no violation, 6,856,069 distinct states, depth 36 |
| Veto ignores COMPLETED cleans | R1 violation, about 10 steps |
| No writer veto | R1 violation, timing C |
| No locked re-check | R1 violation, timing B |
| GC reads FILES through the completed-instant filter | R1 violation, inflight commit loses its entry |

The model is safety-only and abstracts away file contents, partitions and the metadata
table's internal timeline; it is a check on the protocol, not on the implementation.

## 12. Constraint and requirement coverage

| Item | How v3 meets it |
| --- | --- |
| C1 blob immutability | Registry entries are keyed by path and never need updating; a path's content cannot change under an entry. |
| C2 delete-and-re-add | A new reference to an old path is a new `(P, F_new)` entry. A re-add racing a plan is caught by the locked flag check or the writer veto; a re-add after deletion is a fresh live path. |
| C3 cross-file-group sharing | One routed prefix scan on P returns referencing files across all partitions. |
| C4 MOR shadowing | The base file still contains the shadowed reference, so P is over-retained until the base file is physically deleted (section 8). |
| C5 per-file-group cleaner | Candidates come from the per-file-group expiring set; the cross-table question is answered by the registry, not by widening the cleaner. |
| C6 per-file-group OCC | No global counter, lock or bitmap. Writers add registry records in their own metadata delta; the only shared lock is the existing transaction lock, held for a timeline reload and a boolean check. |
| C7 replace commits | Liveness reads raw FILES membership, not the file-system view, so replaced file groups stay live until cleaned. |
| C8 savepoints | Savepointed slices never expire and never leave FILES, so their paths stay live. |
| C9 rollback and restore | Rollback removes the failed commit's metadata delta; orphaned managed paths are handed to the next clean; restore rebuilds `blob_refs` and deletion is refused while the rebuild is inflight. |
| C10 archival | No blob paths in commit metadata. The `blobRefsWritten` flag is read only for commits completed after T0, which are in the active timeline; plan sidecars follow their clean instant through archival. |
| C11 cross-FG verification at scale | Routed file groups make each lookup one file group; the veto probe is one bloom-filter read per sidecar. |
| R1 no premature deletion | Registry joined with FILES at one snapshot, locked flag check, writer veto. Model-checked (section 11). |
| R2 no permanent orphans | Every expiring file is enumerated; candidates are never silently dropped; deferral happens only when a live reference exists; rollback and restore hand off what the clean cannot reach. |
| R3 MOR correctness | As C4; log enumeration is fenced to completed instants. |
| R4 no global serialization | The locked additions are a timeline reload and a boolean check on the plan side, and a bloom-probe re-check of newly REQUESTED cleans on the writer side. |
| R5 cost proportional to work | Enumeration is bounded by expiring files; verification by candidate count; GC by the cleans completed since the last compaction. |
| R6 zero cost for non-blob tables | The `blob_refs` partition exists only when a BLOB column is present; without it no blob code path runs. |
| R7 all cleaning policies | The design consumes the expiring and retained sets the policy produces and does not inspect the policy. |
| R8 crash safety and idempotency | The plan sidecar is durable at REQUESTED; deletes are idempotent; replay re-verifies the old list instead of re-reading deleted files. |
| R9 observability | Counters in clean completion metadata and writer metrics (section 9). |

## Glossary

- **OUT_OF_LINE**: a blob whose bytes live outside the Hudi file, located by `BlobReference`.
- **FILES**: the metadata-table partition listing every data file per partition, log files included.
- **blob_refs**: the proposed metadata-table partition of (path, file) reference records.
- **T0**: the instant set used by the cleaner's dedicated metadata reader for both the `blob_refs` and FILES reads.
- **Plan sidecar**: `<instant>.blob_deletes.parquet`, the verified delete list next to the clean plan. Kept.
- **Hygiene sidecar**: v2's per-clean (path, file) list that fed tombstones. Cut.
- **blobRefsWritten**: the one boolean a commit may carry, meaning "this commit wrote OUT_OF_LINE references".
- **Veto**: the writer-side preCommit check that aborts a commit referencing a planned path.
- **Defer**: skip deleting a path this cycle because a live reference exists; retry later.
