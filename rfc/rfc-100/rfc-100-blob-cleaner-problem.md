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

# External Blob Cleaner: Problem Statement

## 1. Goal

When old file slices are cleaned, external out-of-line blob files they reference may become orphaned
-- still consuming storage but unreachable by any query. The blob cleaner must identify and delete
these unreferenced blob files without premature deletion (deleting a blob that is still referenced by
a live record). This document defines the problem scope, design constraints, requirements, and
illustrative failure modes. It contains no solution content.

This document focuses on **external blobs** -- the Phase 1 use case of RFC-100 where users have
existing blob files in external storage (e.g., `s3://media-bucket/videos/`) and Hudi manages the
*references* via the `BlobReference` schema, not the *storage layout*.

---

## 2. Scope

### In scope

- Cleanup of **external out-of-line blob files** (referenced via `BlobReference.external_path`) when
  references to them exist only in expired (cleaned) file slices.
- All table types: **COW** and **MOR**.
- All cleaning policies: `KEEP_LATEST_COMMITS`, `KEEP_LATEST_FILE_VERSIONS`,
  `KEEP_LATEST_BY_HOURS`.
- Interaction with table services: **compaction**, **clustering**.
- Interaction with replace commits: **clustering**, **insert_overwrite**,
  **insert_overwrite_table**.
- Interaction with timeline operations: **savepoints**, **rollback**, **restore**, **archival**.
- Single-writer and multi-writer (OCC) concurrency modes.

| Property                  | External blobs                               |
|---------------------------|----------------------------------------------|
| Path uniqueness           | Not guaranteed (user controls)               |
| Cross-FG sharing          | Common (multiple records, same blob)         |
| Writer/cleaner race       | Can occur (external paths outside MVCC)      |
| Delete-and-re-add         | Real concern (user controls paths)           |
| Per-FG cleanup sufficient | No -- cross-FG verification needed           |

### Out of scope

- **Inline blobs.** Inline blob data lives inside the base/log file and is deleted when the file
  slice is cleaned. No additional cleanup needed.
- **Storing blob references in commit metadata.** Persisting blob reference sets within commit-level
  metadata is an anti-pattern that does not scale and is not considered in this problem statement.
- **Schema evolution.** Adding or removing blob columns does not change the cleanup problem.

### Stance on the `managed` flag

The `BlobReference` schema includes a `managed` boolean field (`reference.managed`). The RFC states
that only managed blobs are cleaned. This document acknowledges the flag and treats it as a
**filter** -- unmanaged blobs are excluded from cleanup consideration. However, the cleanup design
must be **correct regardless of the flag's value**. The flag selects *which* blobs enter the cleanup
pipeline; it must not be used as a correctness lever within the pipeline itself. The flag may later
serve as an optimization (skip cleanup work for unmanaged blobs), but the problem statement and any
solution must not depend on it for safety.

**Managed-to-unmanaged transitions.** An external blob's `reference.managed` flag can change across
writes. At time t1, a record references `s3://ext/video.mp4` with `managed=true`. At time t2, an
update to the same record changes `managed` to `false` for the same path. The blob was managed in
the expired slice but is unmanaged in the retained slice. The cleaner must decide: does the t1
managed reference make the blob eligible for cleanup when t1 is expired, even though the t2
reference says unmanaged?

To safely handle this: If *any* retained reference to the same `external_path` exists (regardless of
the `managed` flag), the blob must not be deleted. The `managed` flag filters which blob references
*enter* the cleanup pipeline, but the liveness check must consider *all* references to a path, not
just managed ones. A transition from managed to unmanaged is effectively the user saying "Hudi
should stop managing this blob" -- the blob must survive the transition.

The inverse case (unmanaged at t1, managed at t2) is straightforward: the blob is now managed and
subject to cleanup when t2's file slice eventually expires.

---

## 3. Background: Existing Cleaner

The existing Hudi cleaner provides the execution framework that blob cleanup must integrate with.

### Plan-execute model

Cleaning is a two-phase operation:

1. **Plan** (`CleanPlanner`): For each partition and file group, determine which file slices are
   expired based on the cleaning policy. Produce a `HoodieCleanerPlan` listing file paths to delete.
2. **Execute** (`CleanActionExecutor`): Delete the files listed in the plan. Record results in
   `HoodieCleanMetadata` on the timeline.

### Per-partition, per-file-group iteration

The cleaner iterates file groups within a partition. For each file group, it compares file slices
against the retention policy and produces a list of file paths to delete. The cleaner has no concept
of cross-file-group dependencies.

### Savepoint awareness

The cleaner collects all savepointed timestamps and their associated data files. File slices that
overlap with savepointed files are excluded from cleaning. This preserves the savepoint invariant: a
savepoint freezes a consistent snapshot including all data files it references.

### OCC conflict resolution

Concurrent writer conflict resolution operates at the `(partition, fileId)` granularity. There is
no global serialization point. Concurrent writers to different file groups proceed without
contention.

### MVCC: Cleaner does not conflict with writes

Under Hudi's MVCC design, the cleaner and writers operate on non-overlapping sets of file slices.
Writers create new file slices; the cleaner deletes old (expired) ones. The cleaner only targets
file slices that are older than the retention boundary, which by definition are not being written to.
Consequently, the cleaner never conflicts with concurrent writers in the existing design -- there is
no write-clean contention on the same file slice.

Blob cleanup must preserve this property: it must not introduce scenarios where the cleaner and a
writer contend over the same blob file. However, external blob files are **not** covered by MVCC --
a writer's new file slice may reference an external blob that the cleaner is simultaneously
evaluating for deletion. This gap is a central problem for external blob cleanup.

### Rollback vs. Restore

Hudi has two distinct undo operations that affect blob liveness:

1. **Rollback of an inflight commit:** Removes the effects of a single uncommitted write and reverts
   the affected file groups to their previous state. Scope is limited to one commit.
2. **Table restore to a savepoint:** Reverts the table to a prior consistent snapshot, potentially
   undoing multiple committed writes. Scope can affect many file groups across many partitions.

Blob cleanup must handle both: rollback may remove the sole reference to a blob (making it orphaned)
or resurrect a previously-shadowed reference, while restore may revert the table to a state where
different blob references are live. These are distinct failure modes with different scopes and are
addressed separately in the constraints.

### Critical gap

The existing cleaner operates on file paths (base files + log files) within a single file group. It
has **no concept of transitive references** -- it does not know that a file slice contains pointers
to external blob files that may need separate cleanup. Blob cleanup requires extending the cleaner
to follow these references and determine blob-level liveness.

---

## 4. Design Constraints

Each constraint is a fact about the Hudi system that any blob cleanup solution must respect. Violating
any constraint leads to data corruption, premature deletion, or permanent orphans.

### C1: Blob immutability

Once a blob file is written, its content never changes. Blob files are append-once, read-many. This
means a blob file's identity is stable for its entire lifetime.

*Source: RFC-100, general storage semantics.*

### C2: Delete-and-re-add same path

A blob file can be deleted from external storage and a new file created at the same path with
different content. Since the user controls external blob paths, path reuse is a real concern. The
cleanup algorithm must not assume that two references to the same `reference.external_path` at
different times refer to the same physical content.

**Concurrent writer caveat.** Consider two concurrent writers, A and B, both referencing external
blob X at the same path. Writer A commits first. Later, the cleaner evaluates A's expired slice and
marks blob X as a deletion candidate. Meanwhile, Writer B is still inflight and also references
blob X. If the cleaner deletes blob X before Writer B commits, Writer B's commit creates a dangling
reference. This race exists because external blob paths are not under Hudi's control -- unlike file
slices, the cleaner cannot rely on MVCC guarantees for external blob files. Any cleanup solution
must account for inflight writers that may reference the same external blob path being considered
for deletion.

*Source: RFC-100; external blob storage semantics.*

### C3: Cross-file-group blob sharing

An external blob can be referenced by records in multiple file groups and multiple partitions. This
is explicitly supported: two records in different file groups can point to the same
`reference.external_path`. Cross-file-group sharing is the **common case** for external blobs
(e.g., a shared media library where a popular video appears in multiple user playlists across
partitions). Any cleanup algorithm that assumes blobs are scoped to a single file group will produce
premature deletions.

*Source: RFC-100 lines 196-198 (Option 1 scans all active file slices).*

### C4: MOR log updates shadow base file blob refs

In MOR tables, a log file update to a record's blob reference supersedes the base file's blob
reference for that record. The base file's blob ref appears live (it exists in an active file slice)
but is actually dead (the log update replaced it). Reading only the base file produces a **superset**
of live references. Over-retention (keeping the shadowed blob longer) is safe. Under-retention
(treating the log-shadowed base ref as already cleaned) would cause premature deletion if the log
update is later rolled back.

*Source: RFC-100 line 122 (merge mode determines which blob reference is returned); MOR semantics.*

### C5: Existing cleaner is per-file-group scoped

The cleaner iterates per file group within each partition. It determines expired file slices within
a single file group. There is no existing mechanism to evaluate cross-file-group dependencies during
cleaning.

*Source: `CleanPlanner.getDeletePaths()`, per-file-group iteration in
`getFilesToCleanKeepingLatestCommits()`.*

### C6: OCC is per-file-group (no global contention allowed)

Concurrent writer conflict resolution operates at `(partition, fileId)` granularity. Any solution
that introduces a global contention point (global counter, global lock, global bitmap) violates this
constraint and degrades write throughput under concurrency.

*Source: Concurrent writer conflict resolution strategy; per-file-group OCC semantics.*

### C7: Replace commits move blob refs between file groups

Several operations produce `replacecommit` actions that replace one set of file groups with another:

- **Clustering:** Reads records from source file groups and rewrites them to target file groups. For
  external blobs, clustering copies the pointer (same `reference.external_path`) to the target file
  group. After clustering, the source file group's slices still reference the original external
  blobs until those slices are cleaned. The target file group's slices reference the same external
  blobs.

- **insert_overwrite / insert_overwrite_table:** Replaces an entire partition (or table) with new
  data. The replacement records may reference entirely different external blobs, or the same ones,
  or a mix. The replaced file groups become eligible for cleaning after retention expires.

In all replace commit scenarios, the key property is: after the replace, both the old (replaced) and
new (replacement) file groups may reference the same external blob. The cleaner must not delete an
external blob referenced by a replaced file group if the replacement file group (or any other active
file group) still references it.

For external blobs, the append-vs-replace distinction does not affect blob identity. The cleaner's
question is always: "Is this `external_path` referenced by any active file slice anywhere in the
table?" Whether the reference arrived via an append, an upsert, a clustering copy, or an
insert_overwrite is irrelevant -- if any live reference exists, the blob must not be deleted.

*Source: Hudi replace commit semantics; clustering and insert_overwrite operations.*

### C8: Savepoints freeze file slices and their blob refs

A savepoint preserves a consistent snapshot. File slices covered by a savepoint are excluded from
cleaning. This means any blob referenced by a savepointed file slice must also be preserved, even if
the blob would otherwise be considered orphaned. The cleaner already handles savepoint exclusion for
file slices; blob cleanup must extend this guarantee to the blobs they reference.

*Source: Savepoint handling in CleanPlanner.*

### C9: Rollback and restore can invalidate or resurrect references

Two distinct undo operations affect blob liveness:

**Rollback of an inflight commit:** Can remove file slices that were the sole reference to a blob
(the blob becomes orphaned). Conversely, rolling back a commit that updated a record's blob
reference can resurrect the previous reference (an older blob that appeared orphaned is now live
again).

**Table restore to a savepoint:** Can undo multiple committed writes simultaneously. All blob
references introduced after the restore point become orphaned. All blob references that were live at
the restore point are resurrected. The scope is broader than single-commit rollback: it may affect
many file groups across many partitions simultaneously.

Any blob cleanup solution must account for both directions (orphaning and resurrection) under both
operations.

*Source: Hudi rollback and restore semantics; timeline management.*

### C10: Archival removes commit metadata from active timeline

Hudi's archival process moves completed commits from the active timeline to the archived timeline.
If blob cleanup depends on information in commit metadata (e.g., which blobs were written by a
commit), that information becomes unavailable after archival unless it is persisted elsewhere. The
cleaner must either complete blob reference resolution before archival, or ensure the necessary
information survives archival.

Note: Storing blob reference sets within commit metadata would compound this problem -- commit
metadata grows with the number of blob references, and archival would either lose or have to
specially preserve this information. This is an additional reason not to use commit metadata as the
source of truth for blob reference liveness.

*Source: Hudi archival semantics.*

### C11: External blobs require cross-file-group verification at scale

For external blobs, cross-file-group blob sharing (C3) is the **common case**, not an edge case.
Users referencing external blobs (e.g., a shared media library) will frequently have multiple
records across different file groups and partitions pointing to the same blob file. Any cleanup
algorithm that treats cross-FG verification as a rare fallback will impose disproportionate cost on
external blob workloads. The cross-FG verification path must be designed for volume, not just
correctness.

*Source: C3 (cross-FG sharing is common for external blobs).*

---

## 5. Requirements

### R1: No premature deletion (hard invariant)

A blob file must not be deleted while any live record still references it. This is the single most
critical requirement. A premature deletion causes silent data corruption: queries return null or error
for the affected records, and the data is unrecoverable.

### R2: No permanent orphans (bounded cleanup)

Every orphaned blob must eventually be cleaned. The number of cleanup cycles required to reclaim an
orphan must be bounded (e.g., cleaned within N cleaner invocations after the last referencing file
slice is expired). Unbounded accumulation of orphaned blobs wastes storage indefinitely.

### R3: MOR correctness

For MOR tables, blob cleanup must be safe in the presence of log updates that shadow base file blob
references. Over-retention (keeping a shadowed blob until post-compaction) is acceptable.
Under-retention (prematurely deleting a blob whose reference appears shadowed but could be resurrected
by rollback) is not.

### R4: Concurrency safety (no global serialization)

Blob cleanup must not introduce global contention points. Write throughput for tables with blobs must
not degrade compared to tables without blobs under concurrent writers. Per-file-group scoping (C6)
must be preserved.

### R5: Scale proportional to work, not table size

Cross-FG verification is required for external blobs (C11), but the cost must be proportional to the
number of **candidate blobs requiring verification**, not the total number of active file slices in
the table. A table with 100K file groups where 50 external blob candidates need cross-FG
verification must not scan all 100K file groups -- it must use targeted lookups or indexes to resolve
those 50 candidates efficiently.

### R6: No cost for non-blob tables

Tables without blob columns must pay zero additional cost. The blob cleanup path must not be entered
if no blob columns exist. This includes no additional metadata, no additional timeline entries, and no
additional I/O.

### R7: All cleaning policies supported

Blob cleanup must work correctly under all three cleaning policies: `KEEP_LATEST_COMMITS`,
`KEEP_LATEST_FILE_VERSIONS`, and `KEEP_LATEST_BY_HOURS`. The blob cleanup logic should be
policy-agnostic -- it operates on the set of expired vs. retained file slices determined by the
policy, not on the policy itself.

### R8: Crash safety and idempotency

If the cleaner crashes after planning but before completing all deletions, restarting must be safe.
Blob deletions must be idempotent (deleting an already-deleted file is a no-op, not an error).
The cleaner plan must include enough information to resume blob cleanup without re-reading expired
file slices (which may no longer exist after a partial execution).

### R9: Observability

Blob cleanup must report metrics: number of blob files deleted, number of blob files retained
(over-retained due to MOR), and total storage reclaimed. These metrics enable operators to understand
blob storage growth and cleanup effectiveness.

---

## 6. Illustrative Examples

Each example demonstrates a specific failure mode. These are not exhaustive -- they are designed to
make the constraints and requirements concrete.

### Example 1: Cross-file-group sharing -- per-FG cleanup deletes shared blob

**Demonstrates:** C3, C5, R1

```
Setup:
  Partition P1, File Group FG-1:
    Slice @t1: row1.blob_ref = {external_path: "s3://shared/video.mp4", managed: true}

  Partition P2, File Group FG-2:
    Slice @t1: row2.blob_ref = {external_path: "s3://shared/video.mp4", managed: true}

Action:
  Cleaner expires FG-1's slice @t1 (no retained slices in FG-1).

Per-FG cleanup (incorrect):
  FG-1 expired refs = {"s3://shared/video.mp4"}
  FG-1 retained refs = {}
  Orphaned within FG-1 = {"s3://shared/video.mp4"}
  -> DELETE s3://shared/video.mp4

Result:
  FG-2 still has an active slice @t1 referencing video.mp4.
  Query on row2 -> FILE NOT FOUND. Data corruption.

Correct behavior:
  Before deleting, verify that no other active file slice in any file group
  references s3://shared/video.mp4. FG-2's active slice references it, so
  the blob must be retained.
```

### Example 2: Delete-and-re-add -- path reuse causes identity confusion

**Demonstrates:** C2, R1

```
Setup:
  At time t1: User writes row1 with
    blob_ref = {external_path: "s3://user/photo.jpg", managed: true}
  At time t2: User deletes the file at s3://user/photo.jpg externally
  At time t3: User writes row2 with
    blob_ref = {external_path: "s3://user/photo.jpg", managed: true}
    (new file at same path, different content)

Cleanup scenario:
  Cleaner expires slice @t1. Slice @t3 is retained.
  Expired refs = {"s3://user/photo.jpg"}
  Retained refs = {"s3://user/photo.jpg"}
  Same path in both sets -> retain. (Correct by coincidence.)

  But consider: if the cleaner had cached blob identity by path and assumed
  "same path = same blob," it would not detect that the t1 and t3 references
  point to different physical content.

  Edge case: if t3 is also expired and t1 is the only reference, the cleaner
  would correctly delete. But if a new writer at t4 references the same path
  AGAIN (third incarnation), the cleaner's identity model must not confuse the
  three incarnations.

Concurrent writer scenario (C2 caveat):
  Writer A commits at t1, referencing s3://user/photo.jpg (managed=true).
  Writer B starts at t0, also referencing s3://user/photo.jpg (managed=true).
  Cleaner expires A's slice at t1. B is still inflight.
  Cleaner deletes s3://user/photo.jpg.
  Writer B commits -> dangling reference. Data corruption.

  This race is specific to external blobs: the cleaner cannot rely on MVCC
  guarantees for external blob files that are outside Hudi's control.
```

### Example 3: MOR log shadow -- base file ref appears live when superseded

**Demonstrates:** C4, R3

```
Setup (MOR table):
  File Group FG-1:
    Base file @t1: row1.blob_ref = {external_path: "s3://ext/blob_A.bin", managed: true}
    Log file @t2: row1.blob_ref = {external_path: "s3://ext/blob_B.bin", managed: true}

  After merge: row1's effective blob_ref points to blob_B.bin.
  blob_A.bin is no longer referenced by any live record.

Cleanup scenario (pre-compaction):
  Cleaner does not expire slice @t1 (it's retained).
  Reading blob refs from the retained slice:
    Base @t1: {"s3://ext/blob_A.bin"}
    Log @t2: {"s3://ext/blob_B.bin"}
    Union: {"s3://ext/blob_A.bin", "s3://ext/blob_B.bin"}

  blob_A.bin appears live (it's in the retained set) even though it's been
  superseded by the log update. This is over-retention -- safe but wasteful.

After compaction:
  Compacted base @t3: row1.blob_ref = {external_path: "s3://ext/blob_B.bin", managed: true}
  Now the only retained ref is {"s3://ext/blob_B.bin"}.
  blob_A.bin is no longer in any retained set -> eligible for deletion.

Why over-retention is the correct default:
  If the log file @t2 is rolled back, row1 reverts to blob_A.bin from the base
  file. If blob_A.bin had been prematurely deleted, the rollback produces a
  dangling reference. Over-retention prevents this.
```

### Example 4: Writer-cleaner race -- three scenarios

**Demonstrates:** C6, R1, R4

```
A writer and cleaner operate concurrently on the same table.

Note: Under Hudi's MVCC design, the cleaner and writers operate on non-overlapping
file slices -- the cleaner never conflicts with writers on file slice operations.
However, external blob files are NOT covered by MVCC: a writer's new file slice may
reference an external blob that the cleaner is simultaneously evaluating for deletion.
The following scenarios illustrate this gap.

Scenario A: Writer commits BEFORE cleaner's timeline fence
  t1: Writer starts, references blob_X (external)
  t2: Writer commits (blob_X is now in a retained slice)
  t3: Cleaner plans cleanup
  t4: Cleaner checks timeline fence -- sees writer's commit at t2
  t5: Cleaner removes blob_X from orphan candidates
  -> Safe. Timeline fence catches the new reference.

Scenario B: Writer commits AFTER cleaner's timeline fence, BEFORE delete
  t1: Cleaner plans cleanup, blob_X is a candidate for deletion
  t2: Cleaner checks timeline fence -- no new commits
  t3: Writer commits, referencing blob_X (external)
  t4: Cleaner deletes blob_X
  -> UNSAFE. The timeline fence did not see the writer's commit.
     blob_X is deleted, but the writer's new slice references it.

Scenario C: Writer commits AFTER cleaner deletes
  t1: Cleaner plans and executes, deletes blob_X
  t2: Writer commits, referencing blob_X (e.g., user-provided external path)
  -> UNSAFE. The blob is already gone. The writer's commit creates a dangling
     reference.

Scenarios B and C are real concerns for external blobs where the user can
reference any path. Any solution must close this gap -- for example, via a
writer-side conflict check at commit time.
```

### Example 5: Replace commits move refs -- replaced FG appears to have no retained slices

**Demonstrates:** C7, C3, R1

```
Setup:
  File Group FG-1:
    Slice @t1: row1.blob_ref = {external_path: "s3://ext/video.mp4", managed: true}

  Clustering at t2 rewrites FG-1's records to FG-2:
    File Group FG-2:
      Slice @t2: row1.blob_ref = {external_path: "s3://ext/video.mp4", managed: true}
                 (external: pointer copied, same blob)

  FG-1 is now a replaced file group. Its slice @t1 is eligible for cleaning
  after the retention policy expires.

Cleanup (incorrect per-FG only):
  Cleaner cleans FG-1's slice @t1.
  Expired refs = {"s3://ext/video.mp4"}
  Retained refs within FG-1 = {} (FG-1 has no retained slices -- it's replaced)
  s3://ext/video.mp4 appears orphaned within FG-1 -> DELETE

  FG-2's live reference is destroyed. Data corruption.

Alternative scenario -- insert_overwrite:
  At t2, an insert_overwrite replaces partition P1 with new data in FG-3.
  FG-3's records also reference s3://ext/video.mp4 (same external blob).
  FG-1 is replaced. Same per-FG cleanup error: FG-1 concludes the blob is
  orphaned, but FG-3 still references it.

Correct behavior:
  For external blobs, a cross-FG check is required before deletion, regardless
  of which replace commit type (clustering, insert_overwrite, insert_overwrite_table)
  created the replacement. Per-FG cleanup alone is never sufficient for external blobs.
```

### Example 6: External blobs at scale -- cross-FG verification is the common path

**Demonstrates:** C3, C11, R5

```
Setup:
  A media company stores 10M video files in s3://media-library/.
  They create a Hudi table with a blob column referencing these videos.
  Hudi manages refs, not storage layout.

  The table has 50K file groups across 1K partitions.
  Many videos are referenced by multiple records (e.g., a popular video
  appears in multiple user playlists across different partitions).

  Partition users/alice, FG-101:
    Slice @t1: row1.blob_ref = {external_path: "s3://media-library/video_X.mp4", managed: true}

  Partition users/bob, FG-202:
    Slice @t1: row2.blob_ref = {external_path: "s3://media-library/video_X.mp4", managed: true}

  Partition users/carol, FG-303:
    Slice @t1: row3.blob_ref = {external_path: "s3://media-library/video_X.mp4", managed: true}

Action:
  Cleaner expires FG-101's slice @t1 (alice deleted her playlist entry).

Naive per-FG cleanup (incorrect):
  FG-101 expired refs = {"s3://media-library/video_X.mp4"}
  FG-101 retained refs = {}
  Orphaned within FG-101 -> DELETE video_X.mp4
  Bob and Carol lose their video. Data corruption.

Naive full-table scan (correct but expensive):
  To verify video_X.mp4 is safe to delete, scan ALL 50K file groups
  for references. This is correct but violates R5 -- the cost is
  proportional to table size, not to the number of candidates.

Scale concern:
  If the cleaner expires 500 file groups and produces 2,000 external
  blob candidates, and each candidate requires a full-table scan,
  the cleanup cost is 2,000 * 50K = 100M file group checks.
  This is prohibitive.

Correct behavior:
  Cross-FG verification for external blobs must use a targeted
  mechanism (e.g., index lookup, partitioned scan with predicate
  pushdown) that scales with the number of candidates, not with
  the total table size. The mechanism must be a first-class design
  element, not a fallback path.
```

### Example 7: MOR log-chain transient blob -- introduced and superseded within logs

**Demonstrates:** C4, R2

```
Setup (MOR table):
  File Group FG-1:
    Base file @t1: row1.blob_ref = {external_path: "s3://ext/blob_A.bin", managed: true}
    Log file @t2: row1.blob_ref = {external_path: "s3://ext/blob_B.bin", managed: true}
    Log file @t3: row1.blob_ref = {external_path: "s3://ext/blob_C.bin", managed: true}

  After merge: row1's effective blob_ref points to blob_C.bin.
  blob_B.bin was introduced at t2 and superseded at t3 -- it exists ONLY in log @t2.

After compaction @t4:
  Compacted base @t4: row1.blob_ref = {external_path: "s3://ext/blob_C.bin", managed: true}
  The pre-compaction slice (base @t1 + logs @t2, @t3) is now expired.

Cleanup scenario:
  Cleaner expires the pre-compaction slice.
  Retained slice = compacted base @t4, refs = {"s3://ext/blob_C.bin"}.

  If expired slice reads only the base file:
    expired_refs = {"s3://ext/blob_A.bin"}  (from base @t1)
    local_orphans = {"s3://ext/blob_A.bin"} - {"s3://ext/blob_C.bin"}
                  = {"s3://ext/blob_A.bin"}
    blob_A.bin is correctly identified as orphaned.
    blob_B.bin is MISSED -- it exists only in expired log @t2.
    blob_B.bin becomes a permanent orphan (R2 violation).

  If expired slice reads base + log files:
    expired_refs = {"s3://ext/blob_A.bin", "s3://ext/blob_B.bin", "s3://ext/blob_C.bin"}
    local_orphans = {"s3://ext/blob_A.bin", "s3://ext/blob_B.bin", "s3://ext/blob_C.bin"}
                    - {"s3://ext/blob_C.bin"}
                  = {"s3://ext/blob_A.bin", "s3://ext/blob_B.bin"}
    Both orphaned blobs are correctly identified and deleted.

Why this matters:
  Transient blob refs that are introduced and superseded entirely within
  the log chain never appear in any base file. They can only be discovered
  by reading the expired log files. Without log reads on the expired side,
  every such transient blob becomes a permanent orphan that accumulates
  storage indefinitely.
```

### Example 9: Why blobFilesToDelete must be in the plan -- writer-cleaner conflict resolution

**Demonstrates:** C7, R1, R5 (extends Example 5, Scenario B)

```
Setup:
  File Group FG-1 (partition users/alice):
    Slice @t1 (expired): row1.blob_ref = (s3://ext/video.mp4, managed=true)
    Slice @t3 (retained): row1.blob_ref = (s3://ext/photo.png, managed=true)
    video.mp4 is locally orphaned in FG-1 (updated to photo.png at t3).
    No other FG references video.mp4 at plan time.

  File Group FG-2 (partition users/bob):
    (exists, but does not reference video.mp4 yet)

Approach A: blobFilesToDelete NOT in plan (execution-time computation)
  t1: Cleaner plans at timeline fence T.
      Plan = {filePathsToBeDeleted: [FG-1/@t1]}
      No blob info written to plan. Plan goes to timeline as REQUESTED.

  t2: Writer commits to FG-2, adds row2.blob_ref = (s3://ext/video.mp4).
      Writer checks for conflicts: the clean plan on the timeline has no
      blob info -- only filePathsToBeDeleted for FG-1. Writer is on FG-2.
      No conflict detected. Writer succeeds.

  t3: Cleaner transitions to INFLIGHT. Executor computes blob deletes:
      Reads FG-1/@t1 -> expired_refs = {video.mp4}
      Reads FG-1/@t3 -> retained_refs = {photo.png}
      video.mp4 locally orphaned -> Stage 2 cross-FG check at fence T
      -> does NOT see writer's commit at t2 -> globally orphaned -> DELETE

  Result: FG-2 row2 now has a dangling reference to video.mp4.
  Bob queries his data and gets a missing blob error. Data corruption.

  Why it cannot be fixed: the blob delete decision existed only in the
  executor's memory. There was no artifact on the timeline for the writer's
  conflict resolution to check against. The cross-FG conflict was invisible.

Approach B: blobFilesToDelete IN the plan (plan-time computation)
  t1: Cleaner plans at timeline fence T.
      Stage 1: video.mp4 locally orphaned in FG-1.
      Stage 2: cross-FG check -> no other FG references video.mp4.
      Plan = {filePathsToBeDeleted: [FG-1/@t1],
              blobFilesToDelete: [s3://ext/video.mp4]}
      Plan goes to timeline as REQUESTED.

  t2: Writer commits to FG-2, adds row2.blob_ref = (s3://ext/video.mp4).
      Writer's conflict resolution checks inflight/requested clean plan.
      Sees blobFilesToDelete contains video.mp4 -- the same blob the
      writer is referencing. CONFLICT DETECTED. Writer aborts and retries
      after the clean cycle completes (or clean plan is rolled back).
      -> Safe. The conflict is caught before corruption can occur.

  Alternative: if the writer commits first (wins the race), the clean
  plan's conflict resolution at INFLIGHT transition detects that a new
  commit references a blob in blobFilesToDelete. Clean plan is invalidated
  and re-planned in the next cycle, where it will see FG-2's reference
  and retain video.mp4.

Key insight:
  Today, clean actions are not part of OCC conflict resolution
  (TransactionUtils.getInflightAndRequestedInstants excludes CLEAN_ACTION,
  and ConcurrentOperation.init throws for clean actions). Adding external
  blob cleanup requires extending conflict resolution to check
  blobFilesToDelete. This is only possible if the blob delete list is a
  durable artifact on the timeline -- i.e., part of the plan.
```

---

## 7. Open Questions

These questions must be answered by any solution design. They are not prescriptive -- multiple valid
answers exist for each.

**Q1: What is blob identity?**
How does the cleanup algorithm identify a specific blob? By `reference.external_path` alone? For
external blobs where container files are out of scope, path-based identity may be sufficient. The
identity model determines how deduplication and delete-and-re-add (C2) are handled.

**Q2: Where is liveness computed?**
Is the set of live blob references computed at write time (incremental), at clean time (batch), or
some combination? Write-time computation amortizes cost but requires additional metadata storage.
Clean-time computation avoids write overhead but may be expensive at scale. Note: storing liveness
data within commit metadata is not a viable option -- it does not scale (see C10).

**Q3: What is the unit of cleanup planning?**
Does blob cleanup plan per-file-group (aligned with the existing cleaner), per-partition, or globally?
Per-FG is naturally aligned with OCC (C6) but cannot handle cross-FG sharing (C3) without extension.
Global planning handles cross-FG sharing but risks violating C6.

**Q4: How does blob cleanup interact with archival?**
If the cleanup algorithm depends on commit metadata to determine which blobs were written, what
happens when those commits are archived (C10)? Must blob cleanup complete before archival? Must the
relevant metadata be persisted outside the active timeline?

**Q5: Extension or separate service?**
Should blob cleanup be an extension of the existing file slice cleaner (same plan, same execution
phase) or a separate service (independent schedule, independent timeline action)? Extension aligns
lifecycle but increases cleaner complexity. Separation simplifies each component but introduces
coordination challenges.

**Q6: Failure mode and recovery if premature deletion occurs?**
Despite best efforts, what happens if a blob is prematurely deleted? Is there a detection mechanism
(query-time error surfacing)? Is there a recovery path (rebuild from an external source)? How does
the system distinguish "blob correctly not present" from "blob incorrectly deleted"?

**Q7: How does cross-FG verification scale for external blobs?**
For external blob workloads where cross-FG sharing is common, what mechanism makes cross-FG
verification efficient? Options include: an MDT index mapping blob paths to referencing file groups,
predicate pushdown on the blob ref column during targeted scans, a reference count maintained at
write time, or a bloom filter index. The chosen mechanism must satisfy R5 (cost proportional to
candidates, not table size) and C6 (no global serialization). How does this mechanism interact with
writes, and what is its maintenance cost?

**Q8: How should the cleaner handle managed-to-unmanaged transitions?**
If a blob reference transitions from `reference.managed = true` to `reference.managed = false`
across writes (or vice versa), what is the cleaner's behavior? Should the `managed` flag be
evaluated at the time of the expired reference, the retained reference, or both? See the managed
flag discussion in Section 2.

**Q9: How does blob cleanup interact with table restore?**
Table restore (as distinct from single-commit rollback) can undo multiple committed writes,
potentially orphaning many blobs at once and resurrecting others. Does the cleaner need special
handling for post-restore cleanup, or does the standard cleanup algorithm handle it naturally? What
if a restore occurs after a cleaner run has already deleted blobs that the restored state references?
