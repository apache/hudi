# Blob Cleaner: Problem Statement

## 1. Goal

When old file slices are cleaned, out-of-line blob files they reference may become orphaned -- still
consuming storage but unreachable by any query. The blob cleaner must identify and delete these
unreferenced blob files without premature deletion (deleting a blob that is still referenced by a live
record). This document defines the problem scope, design constraints, requirements, and illustrative
failure modes. It contains no solution content.

---

## 2. Scope

### In scope

- Cleanup of **out-of-line blob files** when references to them exist only in expired (cleaned) file
  slices.
- All table types: **COW** and **MOR**.
- All cleaning policies: `KEEP_LATEST_COMMITS`, `KEEP_LATEST_FILE_VERSIONS`,
  `KEEP_LATEST_BY_HOURS`.
- Interaction with table services: **compaction**, **clustering**, **blob compaction**.
- Interaction with timeline operations: **savepoints**, **rollback**, **archival**.
- Single-writer and multi-writer (OCC) concurrency modes.
- Both **Hudi-created blobs** (stored under `{table}/.hoodie/blobs/...`) and **user-provided
  external blobs** (arbitrary paths).

### Two entry flows

Blob cleanup must support two distinct entry flows. These are not edge cases of each other --
they are co-equal paths with different properties, different volumes, and different cleanup costs.

**Flow 1: Path-dispatched (Hudi-created blobs).** Blobs created by Hudi's write path and stored
under `{table}/.hoodie/blobs/{partition}/{col}/{instant}/{blob_id}`. The path structure guarantees
uniqueness (C11), file-group scoping, and eliminates cross-FG sharing for normal writes. This is the
expected majority flow for Phase 3 workloads.

**Flow 2: Non-path-dispatched (user-provided external blobs).** Users have existing blob files in
external storage (e.g., `s3://media-bucket/videos/`, a shared NFS mount, or any user-controlled
path). Records reference these blobs directly by path. The user does **not** want to bootstrap --
they do not want Hudi to copy, move, or reorganize the blob files into `.hoodie/blobs/`. Hudi
manages the *references*, not the *storage layout*. This is the expected primary flow for Phase 1
workloads and remains a supported flow in Phase 3.

The non-path-dispatched flow has fundamentally different properties:

| Property                  | Path-dispatched (Hudi-created)    | Non-path-dispatched (external)       |
|---------------------------|-----------------------------------|--------------------------------------|
| Path uniqueness           | Guaranteed (instant in path, C11) | Not guaranteed (user controls)       |
| Cross-FG sharing          | Does not occur (FG-scoped)        | Common (multiple records, same blob) |
| Writer/cleaner race       | Cannot occur (D2)                 | Can occur (D3)                       |
| Delete-and-re-add (C2)    | Eliminated                        | Real concern                         |
| Volume                    | Scales with writes                | Can be large from day one            |
| Per-FG cleanup sufficient | Yes                               | No -- cross-FG verification needed   |

Any solution that treats the non-path-dispatched flow as a rare edge case will fail at scale for
Phase 1 workloads. The cleanup algorithm must be efficient for **both** flows independently, and
must not impose the cost structure of one flow on the other.

### Out of scope

- **Inline blobs.** Inline blob data lives inside the base/log file and is deleted when the file
  slice is cleaned. No additional cleanup needed.
- **Blob compaction internals.** Blob compaction (repacking partially-live container files) is a
  separate service. This document defines the interface point (when to hand off to blob compaction)
  but not its internal design.
- **Schema evolution.** Adding or removing blob columns does not change the cleanup problem.

### Stance on the `managed` flag

The BlobReference schema includes a `managed` boolean field
(`HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED`). The RFC states that only managed blobs are
cleaned. This document acknowledges the flag and treats it as a **filter** -- unmanaged blobs are
excluded from cleanup consideration. However, the cleanup design must be **correct regardless of the
flag's value**. The flag selects *which* blobs enter the cleanup pipeline; it must not be used as a
correctness lever within the pipeline itself. The flag may later serve as an optimization (skip
cleanup work for unmanaged blobs), but the problem statement and any solution must not depend on it
for safety.

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

`CleanPlanner.getDeletePaths(partitionPath, earliestCommitToRetain)` iterates file groups within a
partition. For each file group, it compares file slices against the retention policy and produces a
list of `CleanFileInfo` objects (file paths to delete). The cleaner has no concept of cross-file-group
dependencies.

### Savepoint awareness

The cleaner collects all savepointed timestamps and their associated data files. File slices that
overlap with savepointed files are excluded from cleaning
(`isFileSliceExistInSavepointedFiles`). This preserves the savepoint invariant: a savepoint freezes a
consistent snapshot including all data files it references.

### OCC conflict resolution

`SimpleConcurrentFileWritesConflictResolutionStrategy` resolves write-write conflicts at the
`(partition, fileId)` granularity. There is no global serialization point. Concurrent writers to
different file groups proceed without contention.

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

*Source: RFC-100 blob cleaner design, general storage semantics.*

### C2: Delete-and-re-add same path

A blob file can be deleted from storage and a new file created at the same path with different
content. This is a real concern for user-provided external blobs (the user controls the path). For
Hudi-created blobs, it is structurally eliminated by C11 (instant in path guarantees uniqueness).

*Source: RFC-100 blob cleaner design; alternatives analysis constraint C2.*

### C3: Cross-file-group blob sharing

An out-of-line blob can be referenced by records in multiple file groups and multiple partitions. This
is explicitly supported for user-provided external blobs: two records in different file groups can
point to the same external file. For Hudi-created blobs, cross-FG sharing does not occur because the
blob is created within a specific file group's storage scope (see C11). However, after clustering
(C8), references to the same Hudi-created blob could temporarily exist in both the source and target
file groups until the source is cleaned.

*Source: RFC-100 lines 196-198 (Option 1 scans all active file slices); alternatives analysis F6.*

### C4: Container files

Multiple blobs can be packed into a single container file, distinguished by `(offset, length)` within
the BlobReference. A container file can only be deleted when **all** byte ranges within it are
unreferenced. If some ranges are orphaned but others are still live, the container cannot be deleted --
it must be handed off to blob compaction for repacking.

*Source: BlobReference schema fields `offset` and `length`; RFC-100 lines 164-165 (container config);
alternatives analysis F1.*

### C5: MOR log updates shadow base file blob refs

In MOR tables, a log file update to a record's blob reference supersedes the base file's blob
reference for that record. The base file's blob ref appears live (it exists in an active file slice)
but is actually dead (the log update replaced it). Reading only the base file produces a **superset**
of live references. Over-retention (keeping the shadowed blob longer) is safe. Under-retention
(treating the log-shadowed base ref as already cleaned) would cause premature deletion if the log
update is later rolled back.

*Source: RFC-100 line 122 (merge mode determines which blob reference is returned); MOR semantics.*

### C6: Existing cleaner is per-file-group scoped

`CleanPlanner` iterates per `HoodieFileGroup` within each partition. It determines expired file slices
within a single file group. There is no existing mechanism to evaluate cross-file-group dependencies
during cleaning.

*Source: `CleanPlanner.getDeletePaths()`, `CleanPlanner.getFilesToCleanKeepingLatestCommits()`;
alternatives analysis F11.*

### C7: OCC is per-file-group (no global contention allowed)

Concurrent writer conflict resolution operates at `(partition, fileId)` granularity. Any solution that
introduces a global contention point (global counter, global lock, global bitmap) violates this
constraint and degrades write throughput under concurrency.

*Source: `SimpleConcurrentFileWritesConflictResolutionStrategy`; alternatives analysis F12.*

### C8: Clustering moves blob refs between file groups

Clustering reads records from source file groups and rewrites them to target file groups. For
Hudi-managed blobs, clustering creates **new** blob files in the target file group. For external
blobs, clustering copies the pointer (same path, same offset/length) to the target file group. After
clustering, the source file group's slices still reference the original blobs until those slices are
cleaned. The target file group's slices reference either new blobs (Hudi-managed) or the same
external blobs.

*Source: RFC-100 lines 212-214.*

### C9: Savepoints freeze file slices and their blob refs

A savepoint preserves a consistent snapshot. File slices covered by a savepoint are excluded from
cleaning. This means any blob referenced by a savepointed file slice must also be preserved, even if
the blob would otherwise be considered orphaned. The cleaner already handles savepoint exclusion for
file slices; blob cleanup must extend this guarantee to the blobs they reference.

*Source: `CleanPlanner.savepointedTimestamps`, `isFileSliceExistInSavepointedFiles()`.*

### C10: Rollback can invalidate or resurrect references

Rolling back a commit can remove file slices that were the sole reference to a blob (the blob becomes
orphaned). Conversely, rolling back a commit that updated a record's blob reference can resurrect the
previous reference (an older blob that appeared orphaned is now live again). Any blob cleanup solution
must account for both directions.

*Source: Hudi rollback semantics; timeline management.*

### C11: Hudi-created blob paths include instant (structurally unique)

Hudi-created blob files are stored at
`{table_path}/.hoodie/blobs/{partition}/{column_name}/{instant}/{blob_id}`. Because the commit
instant is embedded in the path, two different writes always produce different blob paths. This
eliminates the delete-and-re-add problem (C2) for Hudi-created blobs and means they are inherently
scoped to a single file group's write context.

*Source: RFC-100 line 170; alternatives analysis F3.*

### C12: Archival removes commit metadata from active timeline

Hudi's archival process moves completed commits from the active timeline to the archived timeline.
If blob cleanup depends on information in commit metadata (e.g., which blobs were written by a
commit), that information becomes unavailable after archival unless it is persisted elsewhere. The
cleaner must either complete blob reference resolution before archival, or ensure the necessary
information survives archival.

*Source: Hudi archival semantics; `HoodieActiveTimeline` vs `HoodieArchivedTimeline`.*

### C13: Non-path-dispatched blobs require cross-FG verification at scale

For the non-path-dispatched flow (Flow 2), cross-file-group blob sharing (C3) is the **common
case**, not an edge case. Users referencing external blobs (e.g., a shared media library) will
frequently have multiple records across different file groups and partitions pointing to the same
blob file. Any cleanup algorithm that treats cross-FG verification as a rare fallback will impose
disproportionate cost on Flow 2 workloads. The cross-FG verification path must be designed for
volume, not just correctness.

*Source: Two entry flows (Section 2); C3; alternatives analysis D1, D3.*

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

### R3: Container awareness (range-level liveness)

Cleanup must track liveness at the `(path, offset, length)` tuple level, not just the path level.
A container file may have some live ranges and some dead ranges. Only when all ranges are dead can the
container file be deleted. Partially-dead containers should be flagged for blob compaction.

### R4: MOR correctness

For MOR tables, blob cleanup must be safe in the presence of log updates that shadow base file blob
references. Over-retention (keeping a shadowed blob until post-compaction) is acceptable.
Under-retention (prematurely deleting a blob whose reference appears shadowed but could be resurrected
by rollback) is not.

### R5: Concurrency safety (no global serialization)

Blob cleanup must not introduce global contention points. Write throughput for tables with blobs must
not degrade compared to tables without blobs under concurrent writers. Per-file-group scoping (C7)
must be preserved.

### R6: Scale proportional to work, not table size

For path-dispatched blobs (Flow 1): the cost of blob cleanup must be proportional to the number of
file groups being cleaned, not the total table size. A table with 100K file groups cleaning 1K of
them must not scan all 100K file groups.

For non-path-dispatched blobs (Flow 2): cross-FG verification is required (C13), but the cost must
be proportional to the number of **candidate blobs requiring verification**, not the total number of
active file slices in the table. A table with 100K file groups where 50 external blob candidates
need cross-FG verification must not scan all 100K file groups -- it must use targeted lookups or
indexes to resolve those 50 candidates efficiently.

### R7: No cost for non-blob tables

Tables without blob columns must pay zero additional cost. The blob cleanup path must not be entered
if no blob columns exist. This includes no additional metadata, no additional timeline entries, and no
additional I/O.

### R8: All cleaning policies supported

Blob cleanup must work correctly under all three cleaning policies: `KEEP_LATEST_COMMITS`,
`KEEP_LATEST_FILE_VERSIONS`, and `KEEP_LATEST_BY_HOURS`. The blob cleanup logic should be
policy-agnostic -- it operates on the set of expired vs. retained file slices determined by the
policy, not on the policy itself.

### R9: Crash safety and idempotency

If the cleaner crashes after planning but before completing all deletions, restarting must be safe.
Blob deletions must be idempotent (deleting an already-deleted file is a no-op, not an error).
The cleaner plan must include enough information to resume blob cleanup without re-reading expired
file slices (which may no longer exist after a partial execution).

### R10: Observability

Blob cleanup must report metrics: number of blob files deleted, number of blob files retained
(over-retained due to MOR or containers), number of container files flagged for blob compaction,
and total storage reclaimed. These metrics enable operators to understand blob storage growth and
cleanup effectiveness.

---

## 6. Illustrative Examples

Each example demonstrates a specific failure mode. These are not exhaustive -- they are designed to
make the constraints and requirements concrete.

### Example 1: Cross-file-group sharing -- per-FG cleanup deletes shared blob

**Demonstrates:** C3, C6, R1

```
Setup:
  Partition P1, File Group FG-1:
    Slice @t1: row1.blob_ref = (s3://shared/video.mp4, 0, 10MB, managed=true)

  Partition P2, File Group FG-2:
    Slice @t1: row2.blob_ref = (s3://shared/video.mp4, 0, 10MB, managed=true)

Action:
  Cleaner expires FG-1's slice @t1 (no retained slices in FG-1).

Per-FG cleanup (incorrect):
  FG-1 expired refs = {(s3://shared/video.mp4, 0, 10MB)}
  FG-1 retained refs = {}
  Orphaned within FG-1 = {(s3://shared/video.mp4, 0, 10MB)}
  -> DELETE s3://shared/video.mp4

Result:
  FG-2 still has an active slice @t1 referencing video.mp4.
  Query on row2 -> FILE NOT FOUND. Data corruption.

Correct behavior:
  Before deleting, verify that no other active file slice in any file group
  references (s3://shared/video.mp4, 0, 10MB). FG-2's active slice references
  it, so the blob must be retained.
```

### Example 2: Container file partial liveness -- deleting container destroys live ranges

**Demonstrates:** C4, R3

```
Setup:
  File Group FG-1:
    Slice @t1: row1.blob_ref = (container_A.bin, 0, 1MB, managed=true)
    Slice @t2: row2.blob_ref = (container_A.bin, 1MB, 2MB, managed=true)

Action:
  Cleaner expires slice @t1, retains slice @t2.

Path-level cleanup (incorrect):
  Expired paths = {container_A.bin}
  Retained paths = {container_A.bin}
  container_A.bin is in both sets -> retain.
  (This happens to be safe by accident, but only because the same path appears.)

  Alternative scenario -- different slices, same container:
    Slice @t1 in FG-1: row1.blob_ref = (container_A.bin, 0, 1MB)  -- expired
    Slice @t1 in FG-2: row2.blob_ref = (container_A.bin, 1MB, 2MB)  -- retained

  If FG-1 concludes container_A.bin is orphaned (no retained refs within FG-1):
  -> DELETE container_A.bin
  FG-2's live range at offset 1MB is destroyed.

Correct behavior:
  Track liveness at (path, offset, length). Only delete the container file when
  ALL ranges are unreferenced. If some ranges are dead and others live, flag the
  container for blob compaction instead of deleting it.
```

### Example 3: Delete-and-re-add -- path reuse causes identity confusion

**Demonstrates:** C2, R1

```
Setup:
  At time t1: User writes row1.blob_ref = (s3://user/photo.jpg, managed=true)
  At time t2: User deletes the file at s3://user/photo.jpg externally
  At time t3: User writes row2.blob_ref = (s3://user/photo.jpg, managed=true)
              (new file at same path, different content)

Cleanup scenario:
  Cleaner expires slice @t1. Slice @t3 is retained.
  Expired refs = {s3://user/photo.jpg}
  Retained refs = {s3://user/photo.jpg}
  Same path in both sets -> retain. (Correct by coincidence.)

  But consider: if the cleaner had cached blob identity by path and assumed
  "same path = same blob," it would not detect that the t1 and t3 references
  point to different physical content.

  Edge case: if t3 is also expired and t1 is the only reference, the cleaner
  would correctly delete. But if a new writer at t4 references the same path
  AGAIN (third incarnation), the cleaner's identity model must not confuse the
  three incarnations.

Note: This problem does not arise for Hudi-created blobs (C11 -- instant in
path guarantees each write produces a unique path).
```

### Example 4: MOR log shadow -- base file ref appears live when superseded

**Demonstrates:** C5, R4

```
Setup (MOR table):
  File Group FG-1:
    Base file @t1: row1.blob_ref = (blob_A.bin, managed=true)
    Log file @t2: row1.blob_ref = (blob_B.bin, managed=true)  -- update

  After merge: row1's effective blob_ref is blob_B.bin.
  blob_A.bin is no longer referenced by any live record.

Cleanup scenario (pre-compaction):
  Cleaner does not expire slice @t1 (it's retained).
  Reading blob refs from the retained slice:
    Base @t1: {blob_A.bin}
    Log @t2: {blob_B.bin}
    Union: {blob_A.bin, blob_B.bin}

  blob_A.bin appears live (it's in the retained set) even though it's been
  superseded by the log update. This is over-retention -- safe but wasteful.

After compaction:
  Compacted base @t3: row1.blob_ref = (blob_B.bin, managed=true)
  Now the only retained ref is {blob_B.bin}.
  blob_A.bin is no longer in any retained set -> eligible for deletion.

Why over-retention is the correct default:
  If the log file @t2 is rolled back, row1 reverts to blob_A.bin from the base
  file. If blob_A.bin had been prematurely deleted, the rollback produces a
  dangling reference. Over-retention prevents this.
```

### Example 5: Writer-cleaner race -- three scenarios

**Demonstrates:** C7, R1, R5

```
A writer and cleaner operate concurrently on the same table.

Scenario A: Writer commits BEFORE cleaner's timeline fence
  t1: Writer starts, references blob_X
  t2: Writer commits (blob_X is now in a retained slice)
  t3: Cleaner plans cleanup
  t4: Cleaner checks timeline fence -- sees writer's commit at t2
  t5: Cleaner removes blob_X from orphan candidates
  -> Safe. Timeline fence catches the new reference.

Scenario B: Writer commits AFTER cleaner's timeline fence, BEFORE delete
  t1: Cleaner plans cleanup, blob_X is a candidate for deletion
  t2: Cleaner checks timeline fence -- no new commits
  t3: Writer commits, referencing blob_X
  t4: Cleaner deletes blob_X
  -> UNSAFE. The timeline fence did not see the writer's commit.
     blob_X is deleted, but the writer's new slice references it.

Scenario C: Writer commits AFTER cleaner deletes
  t1: Cleaner plans and executes, deletes blob_X
  t2: Writer commits, referencing blob_X (e.g., user-provided external path)
  -> UNSAFE. The blob is already gone. The writer's commit creates a dangling
     reference.

Note: Scenario B and C cannot occur for Hudi-created blobs (C11 + D2 from
alternatives analysis: new writes always produce new paths, and UPSERT carries
forward refs from retained slices). They are real concerns for user-provided
external blobs where the user can reference any path.
```

### Example 6: Clustering moves refs -- replaced FG appears to have no retained slices

**Demonstrates:** C8, C3, R1

```
Setup:
  File Group FG-1:
    Slice @t1: row1.blob_ref = (blob_A.bin, managed=true)

  Clustering at t2 rewrites FG-1's records to FG-2:
    File Group FG-2:
      Slice @t2: row1.blob_ref = (blob_A_new.bin, managed=true)
                 (Hudi-managed: new blob created in FG-2's scope)

  FG-1 is now a replaced file group. Its slice @t1 is eligible for cleaning
  after the retention policy expires.

Cleanup:
  Cleaner cleans FG-1's slice @t1.
  Expired refs = {blob_A.bin}
  Retained refs within FG-1 = {} (FG-1 has no retained slices -- it's replaced)
  blob_A.bin appears orphaned within FG-1 -> DELETE

  This is actually CORRECT for Hudi-managed blobs: clustering created a new
  blob (blob_A_new.bin) in FG-2, so blob_A.bin is genuinely orphaned.

  BUT if the blob were an external user-provided blob (same path referenced
  from both FG-1 and FG-2 after clustering):

  File Group FG-2:
    Slice @t2: row1.blob_ref = (s3://ext/video.mp4, managed=true)
               (external: pointer copied, same blob)

  FG-1's cleanup concludes s3://ext/video.mp4 is orphaned within FG-1.
  Deleting it destroys FG-2's live reference.

Correct behavior:
  For Hudi-managed blobs, per-FG cleanup is safe because clustering always
  creates new blob files in the target FG. For external blobs, a cross-FG
  check is required before deletion.
```

### Example 7: Non-bootstrapped external blobs at scale -- cross-FG verification is the common path

**Demonstrates:** C3, C13, R6 (Flow 2)

```
Setup:
  A media company stores 10M video files in s3://media-library/.
  They create a Hudi table with a blob column referencing these videos.
  They do NOT bootstrap -- Hudi manages refs, not storage layout.

  The table has 50K file groups across 1K partitions.
  Many videos are referenced by multiple records (e.g., a popular video
  appears in multiple user playlists across different partitions).

  Partition users/alice, FG-101:
    Slice @t1: row1.blob_ref = (s3://media-library/video_X.mp4, managed=true)

  Partition users/bob, FG-202:
    Slice @t1: row2.blob_ref = (s3://media-library/video_X.mp4, managed=true)

  Partition users/carol, FG-303:
    Slice @t1: row3.blob_ref = (s3://media-library/video_X.mp4, managed=true)

Action:
  Cleaner expires FG-101's slice @t1 (alice deleted her playlist entry).

Naive per-FG cleanup (incorrect):
  FG-101 expired refs = {(s3://media-library/video_X.mp4, 0, 50MB)}
  FG-101 retained refs = {}
  Orphaned within FG-101 -> DELETE video_X.mp4
  Bob and Carol lose their video. Data corruption.

Naive full-table scan (correct but expensive):
  To verify video_X.mp4 is safe to delete, scan ALL 50K file groups
  for references. This is correct but violates R6 -- the cost is
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

### Example 8: MOR log-chain transient blob -- introduced and superseded within logs

**Demonstrates:** C5, R2

```
Setup (MOR table):
  File Group FG-1:
    Base file @t1: row1.blob_ref = (blob_A.bin, managed=true)
    Log file @t2: row1.blob_ref = (blob_B.bin, managed=true)  -- update
    Log file @t3: row1.blob_ref = (blob_C.bin, managed=true)  -- another update

  After merge: row1's effective blob_ref is blob_C.bin.
  blob_B.bin was introduced at t2 and superseded at t3 -- it exists ONLY in log @t2.

After compaction @t4:
  Compacted base @t4: row1.blob_ref = (blob_C.bin, managed=true)
  The pre-compaction slice (base @t1 + logs @t2, @t3) is now expired.

Cleanup scenario:
  Cleaner expires the pre-compaction slice.
  Retained slice = compacted base @t4, refs = {blob_C.bin}.

  If expired slice reads only the base file:
    expired_refs = {blob_A.bin}  (from base @t1)
    local_orphans = {blob_A.bin} - {blob_C.bin} = {blob_A.bin}
    blob_A.bin is correctly identified as orphaned.
    blob_B.bin is MISSED -- it exists only in expired log @t2.
    blob_B.bin becomes a permanent orphan (R2 violation).

  If expired slice reads base + log files:
    expired_refs = {blob_A.bin, blob_B.bin, blob_C.bin}
    local_orphans = {blob_A.bin, blob_B.bin, blob_C.bin} - {blob_C.bin}
                  = {blob_A.bin, blob_B.bin}
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
How does the cleanup algorithm identify a specific blob? By path alone? By the tuple
`(path, offset, length)`? By `(path, generation/version)`? The identity model determines how
deduplication, container handling, and delete-and-re-add (C2) are handled.

**Q2: Where is liveness computed?**
Is the set of live blob references computed at write time (incremental), at clean time (batch), or
some combination? Write-time computation amortizes cost but requires additional metadata storage.
Clean-time computation avoids write overhead but may be expensive at scale.

**Q3: What is the unit of cleanup planning?**
Does blob cleanup plan per-file-group (aligned with the existing cleaner), per-partition, or globally?
Per-FG is naturally aligned with OCC (C7) but cannot handle cross-FG sharing (C3) without extension.
Global planning handles cross-FG sharing but risks violating C7.

**Q4: How does blob cleanup interact with archival?**
If the cleanup algorithm depends on commit metadata to determine which blobs were written, what
happens when those commits are archived (C12)? Must blob cleanup complete before archival? Must the
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

**Q7: How does cross-FG verification scale for non-path-dispatched blobs?**
For Flow 2 workloads where cross-FG sharing is common, what mechanism makes cross-FG verification
efficient? Options include: an MDT index mapping blob paths to referencing file groups, predicate
pushdown on the blob ref column during targeted scans, a reference count maintained at write time,
or a bloom filter index. The chosen mechanism must satisfy R6 (cost proportional to candidates, not
table size) and C7 (no global serialization). How does this mechanism interact with writes, and what
is its maintenance cost?
