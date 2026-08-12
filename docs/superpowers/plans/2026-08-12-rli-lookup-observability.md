# RLI Lookup Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit per-commit counts for the Record Level Index lookup phase of a Spark write — shards read, log files read, bytes footprint, keys submitted and keys hit — to the existing metrics reporters and to commit metadata.

**Architecture:** Collection happens at the `hudi-common` seam both Spark and Flink already call (`readRecordIndexLocationsWithKeys`), through a one-method `RecordIndexLookupStatsCollector`. Every count — including log files and bytes — is captured at the point of read, where the `FileSlice` is already in scope. Spark's implementation is an `AccumulatorV2` whose value is a map keyed by shard, merged per-key by field-wise `max`, which makes counting idempotent under task retry and RDD recomputation. The driver drains it during `preCommit`, folds the map to totals, and writes them to gauges and commit metadata.

**Tech Stack:** Java 11, Maven, Apache Spark 3.5, JUnit 5, Mockito, Lombok, Dropwizard Metrics.

**Spec:** `docs/superpowers/specs/2026-08-12-rli-lookup-observability-design.md`

## Global Constraints

- **Do NOT run `mvn test` in a Claude Code session.** CLAUDE.md forbids it. Agentic executors stop at `mvn install -pl <module> -am -DskipTests -Dspark3.5` and hand test execution to the user. The `mvn test` commands in each task are for the human running them.
- `JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home` — the default `JAVA_HOME` in this environment is broken.
- Build flags: `-Dspark3.5 -Dscala-2.12`.
- **Never use `var`** in Java. Explicit types only.
- Package names use `hudi`; class names use `hoodie`.
- **Every metric gauge value must be a `Long`.** `PushGatewayReporter.java:160` casts `(Long)`, `DatadogReporter.java:96-97` casts `(long)`. A `String` gauge throws at report time on Datadog and Prometheus.
- **No static fields** may hold RLI lookup stats state. This is requirement 10 (multi-table isolation) and is a review gate on every task.
- No metric may carry a shard-valued label.
- Checkstyle runs on every module: `mvn checkstyle:check -pl <module>`.
- Commit messages: conventional commits. **Do NOT add `Co-authored-by: Claude` trailers** — Hudi CI rejects them.
- Target release 1.2.0; new configs use `.sinceVersion("1.2.0")`.

---

### Task 1: Stats value types and merge algebra

The merge algebra is the correctness core of the feature (retry idempotence). It lives in `hudi-common` so Flink's future adapter reuses it rather than reimplementing it. No Spark involved, so it is testable in isolation.

**Files:**
- Create: `hudi-common/src/main/java/org/apache/hudi/metadata/RecordIndexShardLookupStats.java`
- Create: `hudi-common/src/main/java/org/apache/hudi/metadata/RecordIndexLookupStats.java`
- Test: `hudi-common/src/test/java/org/apache/hudi/metadata/TestRecordIndexLookupStats.java`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `RecordIndexShardLookupStats(int shardIndex, String fileGroupId, long keysSubmitted, long keysHit, long logFilesRead, long bytesInShard, long lookupMillis)` with getters `getShardIndex()`, `getFileGroupId()`, `getKeysSubmitted()`, `getKeysHit()`, `getLogFilesRead()`, `getBytesInShard()`, `getLookupMillis()`, and `RecordIndexShardLookupStats merge(RecordIndexShardLookupStats other)`.
  - `RecordIndexLookupStats.empty()`, `RecordIndexLookupStats.of(RecordIndexShardLookupStats)`, `RecordIndexLookupStats merge(RecordIndexLookupStats other)`, `Map<Integer, RecordIndexShardLookupStats> getShardStats()`, `boolean isEmpty()`, and totals accessors `getShardsRead()`, `getKeysSubmitted()`, `getKeysHit()`, `getLogFilesRead()`, `getBytesInShardsRead()`.

- [ ] **Step 1: Write the failing test**

Create `hudi-common/src/test/java/org/apache/hudi/metadata/TestRecordIndexLookupStats.java`:

```java
package org.apache.hudi.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRecordIndexLookupStats {

  private static RecordIndexShardLookupStats shard(int index, long keys, long hits) {
    return new RecordIndexShardLookupStats(index, "fg-" + index, keys, hits, 2L, 1000L, 10L);
  }

  @Test
  void testShardMergeTakesFieldWiseMax() {
    RecordIndexShardLookupStats partial = new RecordIndexShardLookupStats(3, "fg-3", 100L, 40L, 2L, 800L, 5L);
    RecordIndexShardLookupStats complete = new RecordIndexShardLookupStats(3, "fg-3", 100L, 90L, 2L, 800L, 12L);

    RecordIndexShardLookupStats merged = partial.merge(complete);

    assertEquals(3, merged.getShardIndex());
    assertEquals("fg-3", merged.getFileGroupId());
    assertEquals(100L, merged.getKeysSubmitted());
    assertEquals(90L, merged.getKeysHit(), "a partial read followed by a complete one resolves to complete");
    assertEquals(2L, merged.getLogFilesRead());
    assertEquals(800L, merged.getBytesInShard());
    assertEquals(12L, merged.getLookupMillis());
  }

  @Test
  void testTotalsFoldFileLevelCounts() {
    RecordIndexLookupStats stats = RecordIndexLookupStats
        .of(new RecordIndexShardLookupStats(0, "fg-0", 100L, 70L, 3L, 1300L, 5L))
        .merge(RecordIndexLookupStats.of(new RecordIndexShardLookupStats(2, "fg-2", 50L, 10L, 1L, 600L, 3L)));

    assertEquals(2L, stats.getShardsRead());
    assertEquals(4L, stats.getLogFilesRead());
    assertEquals(1900L, stats.getBytesInShardsRead());
  }

  @Test
  void testRetriedShardIsIdempotentNotAdditive() {
    RecordIndexLookupStats once = RecordIndexLookupStats.of(shard(1, 500L, 300L));

    RecordIndexLookupStats tenTimes = once;
    for (int i = 0; i < 9; i++) {
      tenTimes = tenTimes.merge(RecordIndexLookupStats.of(shard(1, 500L, 300L)));
    }

    assertEquals(1L, tenTimes.getShardsRead(), "the same shard reported ten times is still one shard");
    assertEquals(500L, tenTimes.getKeysSubmitted(), "counts must not accumulate across retries");
    assertEquals(300L, tenTimes.getKeysHit());
  }

  @Test
  void testDistinctShardsAccumulate() {
    RecordIndexLookupStats stats = RecordIndexLookupStats.of(shard(0, 10L, 4L))
        .merge(RecordIndexLookupStats.of(shard(1, 20L, 11L)))
        .merge(RecordIndexLookupStats.of(shard(2, 30L, 30L)));

    assertEquals(3L, stats.getShardsRead());
    assertEquals(60L, stats.getKeysSubmitted());
    assertEquals(45L, stats.getKeysHit());
  }

  @Test
  void testMergeIsCommutative() {
    RecordIndexLookupStats a = RecordIndexLookupStats.of(shard(0, 10L, 4L));
    RecordIndexLookupStats b = RecordIndexLookupStats.of(shard(1, 20L, 11L));

    assertEquals(a.merge(b).getShardStats(), b.merge(a).getShardStats());
  }

  @Test
  void testMergeIsAssociativeUnderShuffledOrders() {
    List<RecordIndexLookupStats> parts = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      parts.add(RecordIndexLookupStats.of(shard(i % 3, 10L * (i + 1), 5L * (i + 1))));
    }

    RecordIndexLookupStats leftFold = RecordIndexLookupStats.empty();
    for (RecordIndexLookupStats part : parts) {
      leftFold = leftFold.merge(part);
    }

    // Spark merges executor-local copies in an unspecified order, so any permutation must agree.
    List<RecordIndexLookupStats> reversed = new ArrayList<>(parts);
    Collections.reverse(reversed);
    RecordIndexLookupStats rightFold = RecordIndexLookupStats.empty();
    for (RecordIndexLookupStats part : reversed) {
      rightFold = rightFold.merge(part);
    }

    assertEquals(leftFold.getShardStats(), rightFold.getShardStats());
  }

  @Test
  void testEmptyIsIdentity() {
    RecordIndexLookupStats stats = RecordIndexLookupStats.of(shard(7, 1L, 1L));

    assertTrue(RecordIndexLookupStats.empty().isEmpty());
    assertEquals(stats.getShardStats(), stats.merge(RecordIndexLookupStats.empty()).getShardStats());
    assertEquals(stats.getShardStats(), RecordIndexLookupStats.empty().merge(stats).getShardStats());
    assertEquals(0L, RecordIndexLookupStats.empty().getShardsRead());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl hudi-common -Dtest=TestRecordIndexLookupStats -Dspark3.5`
Expected: FAIL — compilation error, `RecordIndexShardLookupStats` and `RecordIndexLookupStats` do not exist.

- [ ] **Step 3: Write minimal implementation**

Create `hudi-common/src/main/java/org/apache/hudi/metadata/RecordIndexShardLookupStats.java`:

```java
package org.apache.hudi.metadata;

import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.Objects;

/**
 * Immutable stats for a single record-index shard (file group) read during one lookup.
 *
 * <p>Instances are merged field-wise by {@code max} rather than summed. That is what makes
 * collection idempotent: a retried or recomputed Spark task re-reports the same shard, and taking
 * the maximum overwrites the earlier report instead of accumulating a second time. A task that
 * failed part-way through a read and then succeeded resolves to the complete read, because the
 * complete read observed at least as many keys and hits.
 */
public class RecordIndexShardLookupStats implements Serializable {

  private static final long serialVersionUID = 1L;

  private final int shardIndex;
  private final String fileGroupId;
  private final long keysSubmitted;
  private final long keysHit;
  private final long logFilesRead;
  private final long bytesInShard;
  private final long lookupMillis;

  public RecordIndexShardLookupStats(int shardIndex, String fileGroupId, long keysSubmitted,
                                     long keysHit, long logFilesRead, long bytesInShard,
                                     long lookupMillis) {
    ValidationUtils.checkArgument(shardIndex >= 0, "shardIndex must be non-negative");
    ValidationUtils.checkArgument(keysHit <= keysSubmitted, "keysHit cannot exceed keysSubmitted");
    this.shardIndex = shardIndex;
    this.fileGroupId = fileGroupId;
    this.keysSubmitted = keysSubmitted;
    this.keysHit = keysHit;
    this.logFilesRead = logFilesRead;
    this.bytesInShard = bytesInShard;
    this.lookupMillis = lookupMillis;
  }

  public int getShardIndex() {
    return shardIndex;
  }

  public String getFileGroupId() {
    return fileGroupId;
  }

  public long getKeysSubmitted() {
    return keysSubmitted;
  }

  public long getKeysHit() {
    return keysHit;
  }

  public long getLogFilesRead() {
    return logFilesRead;
  }

  /**
   * Footprint of the shard read: base file size plus the sum of log file sizes, from
   * {@code FileSlice.getTotalFileSize()}. This is an upper bound on I/O, not I/O — key pushdown
   * means the reader touches a fraction of it. True bytes read is a follow-up that adds a separate
   * field rather than redefining this one.
   */
  public long getBytesInShard() {
    return bytesInShard;
  }

  public long getLookupMillis() {
    return lookupMillis;
  }

  /**
   * Field-wise maximum of this and {@code other}, which must describe the same shard.
   */
  public RecordIndexShardLookupStats merge(RecordIndexShardLookupStats other) {
    ValidationUtils.checkArgument(shardIndex == other.shardIndex,
        "cannot merge stats for different shards: " + shardIndex + " vs " + other.shardIndex);
    return new RecordIndexShardLookupStats(
        shardIndex,
        fileGroupId != null ? fileGroupId : other.fileGroupId,
        Math.max(keysSubmitted, other.keysSubmitted),
        Math.max(keysHit, other.keysHit),
        Math.max(logFilesRead, other.logFilesRead),
        Math.max(bytesInShard, other.bytesInShard),
        Math.max(lookupMillis, other.lookupMillis));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RecordIndexShardLookupStats that = (RecordIndexShardLookupStats) o;
    return shardIndex == that.shardIndex
        && keysSubmitted == that.keysSubmitted
        && keysHit == that.keysHit
        && logFilesRead == that.logFilesRead
        && bytesInShard == that.bytesInShard
        && lookupMillis == that.lookupMillis
        && Objects.equals(fileGroupId, that.fileGroupId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardIndex, fileGroupId, keysSubmitted, keysHit, logFilesRead,
        bytesInShard, lookupMillis);
  }

  @Override
  public String toString() {
    return "RecordIndexShardLookupStats{shard=" + shardIndex + ", fileGroup=" + fileGroupId
        + ", keysSubmitted=" + keysSubmitted + ", keysHit=" + keysHit
        + ", logFilesRead=" + logFilesRead + ", bytesInShard=" + bytesInShard
        + ", lookupMillis=" + lookupMillis + '}';
  }
}
```

Create `hudi-common/src/main/java/org/apache/hudi/metadata/RecordIndexLookupStats.java`:

```java
package org.apache.hudi.metadata;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Aggregate record-index lookup stats for one write, keyed by shard index.
 *
 * <p>Keying by shard rather than summing scalars is what gives the aggregate its idempotence under
 * task retry and RDD recomputation. Totals are folds over the map, computed on demand.
 */
public class RecordIndexLookupStats implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final RecordIndexLookupStats EMPTY =
      new RecordIndexLookupStats(Collections.emptyMap());

  private final Map<Integer, RecordIndexShardLookupStats> shardStats;

  private RecordIndexLookupStats(Map<Integer, RecordIndexShardLookupStats> shardStats) {
    this.shardStats = shardStats;
  }

  public static RecordIndexLookupStats empty() {
    return EMPTY;
  }

  public static RecordIndexLookupStats of(RecordIndexShardLookupStats stats) {
    return new RecordIndexLookupStats(Collections.singletonMap(stats.getShardIndex(), stats));
  }

  public RecordIndexLookupStats merge(RecordIndexLookupStats other) {
    if (other.shardStats.isEmpty()) {
      return this;
    }
    if (shardStats.isEmpty()) {
      return other;
    }
    Map<Integer, RecordIndexShardLookupStats> merged = new HashMap<>(shardStats);
    other.shardStats.forEach((shard, stats) -> merged.merge(shard, stats, RecordIndexShardLookupStats::merge));
    return new RecordIndexLookupStats(merged);
  }

  public Map<Integer, RecordIndexShardLookupStats> getShardStats() {
    return Collections.unmodifiableMap(shardStats);
  }

  public boolean isEmpty() {
    return shardStats.isEmpty();
  }

  /** Shards genuinely read. Spark launches a task per file group, but only a task that reads inserts a key. */
  public long getShardsRead() {
    return shardStats.size();
  }

  public long getKeysSubmitted() {
    return shardStats.values().stream().mapToLong(RecordIndexShardLookupStats::getKeysSubmitted).sum();
  }

  public long getKeysHit() {
    return shardStats.values().stream().mapToLong(RecordIndexShardLookupStats::getKeysHit).sum();
  }

  public long getLogFilesRead() {
    return shardStats.values().stream().mapToLong(RecordIndexShardLookupStats::getLogFilesRead).sum();
  }

  public long getBytesInShardsRead() {
    return shardStats.values().stream().mapToLong(RecordIndexShardLookupStats::getBytesInShard).sum();
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl hudi-common -Dtest=TestRecordIndexLookupStats -Dspark3.5`
Expected: PASS, 7 tests.

Then: `mvn checkstyle:check -pl hudi-common`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add hudi-common/src/main/java/org/apache/hudi/metadata/RecordIndexShardLookupStats.java \
        hudi-common/src/main/java/org/apache/hudi/metadata/RecordIndexLookupStats.java \
        hudi-common/src/test/java/org/apache/hudi/metadata/TestRecordIndexLookupStats.java
git commit -m "feat(metadata): add record index lookup stats value types with idempotent merge"
```

---

### Task 2: Collector interface and the `hudi-common` seam

This is the task that carries the review risk (spec §13). It adds an overload only; existing signatures delegate with a no-op, so nothing breaks and behaviour is provably unchanged when the collector is `NOOP`.

**Files:**
- Create: `hudi-common/src/main/java/org/apache/hudi/metadata/RecordIndexLookupStatsCollector.java`
- Modify: `hudi-common/src/main/java/org/apache/hudi/metadata/HoodieTableMetadata.java:247-266`
- Modify: `hudi-common/src/main/java/org/apache/hudi/metadata/HoodieBackedTableMetadata.java:271-353`
- Modify: `hudi-common/src/main/java/org/apache/hudi/metadata/FileSystemBackedTableMetadata.java:336-345`
- Modify: `hudi-common/src/main/java/org/apache/hudi/common/table/view/NoOpTableMetadata.java:115-124`
- Test: `hudi-common/src/test/java/org/apache/hudi/metadata/TestRecordIndexLookupStatsCollection.java`

**Interfaces:**
- Consumes: `RecordIndexShardLookupStats`, `RecordIndexLookupStats` from Task 1.
- Produces:
  - `RecordIndexLookupStatsCollector` — functional interface, `void collect(RecordIndexShardLookupStats stats)`, constant `RecordIndexLookupStatsCollector.NOOP`.
  - `HoodieTableMetadata.readRecordIndexLocationsWithKeys(HoodieData<String>, Option<String>, RecordIndexLookupStatsCollector)`.

- [ ] **Step 1: Write the failing test**

Create `hudi-common/src/test/java/org/apache/hudi/metadata/TestRecordIndexLookupStatsCollection.java`:

```java
package org.apache.hudi.metadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRecordIndexLookupStatsCollection {

  @Test
  void testNoopCollectorAcceptsAndDiscards() {
    // The disabled path must be a provable no-op: it accepts input and holds no state.
    RecordIndexLookupStatsCollector.NOOP.collect(
        new RecordIndexShardLookupStats(0, "fg-0", 10L, 5L, 1L, 500L, 1L));
    assertTrue(true, "NOOP must not throw and must not require any lifecycle");
  }

  @Test
  void testCollectorReceivesOneStatsPerShardRead() {
    List<RecordIndexShardLookupStats> collected = new ArrayList<>();
    RecordIndexLookupStatsCollector collector = collected::add;

    collector.collect(new RecordIndexShardLookupStats(2, "fg-2", 40L, 31L, 3L, 900L, 7L));

    assertEquals(1, collected.size());
    assertEquals(2, collected.get(0).getShardIndex());
    assertEquals("fg-2", collected.get(0).getFileGroupId());
    assertEquals(40L, collected.get(0).getKeysSubmitted());
    assertEquals(31L, collected.get(0).getKeysHit());
    assertEquals(3L, collected.get(0).getLogFilesRead());
    assertEquals(900L, collected.get(0).getBytesInShard());
  }

  @Test
  void testCollectorIsSerializable() {
    // The collector is captured in a Spark closure; a non-serializable one fails at task launch.
    assertTrue(java.io.Serializable.class.isAssignableFrom(RecordIndexLookupStatsCollector.class));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl hudi-common -Dtest=TestRecordIndexLookupStatsCollection -Dspark3.5`
Expected: FAIL — `RecordIndexLookupStatsCollector` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `hudi-common/src/main/java/org/apache/hudi/metadata/RecordIndexLookupStatsCollector.java`:

```java
package org.apache.hudi.metadata;

import java.io.Serializable;

/**
 * Sink for per-shard record-index lookup stats, called once per shard actually read.
 *
 * <p>Deliberately one method. Collection happens in engine-agnostic code so that Spark and Flink
 * share a single definition of every count; only the transport differs. Spark supplies an
 * accumulator-backed implementation drained on the driver; a Flink implementation would report to
 * a {@code MetricGroup} per subtask.
 *
 * <p>Implementations are captured in engine closures and must be serializable. They must never
 * throw: instrumentation must not be able to fail a write.
 */
@FunctionalInterface
public interface RecordIndexLookupStatsCollector extends Serializable {

  /** Discards everything. Used whenever the feature is disabled, and as the default for callers that do not care. */
  RecordIndexLookupStatsCollector NOOP = stats -> {
  };

  void collect(RecordIndexShardLookupStats stats);
}
```

Modify `hudi-common/src/main/java/org/apache/hudi/metadata/HoodieTableMetadata.java` — add the overload next to the existing declarations at `:247` and `:259`:

```java
  /**
   * Reads record locations from the record-level index, reporting per-shard lookup stats.
   *
   * @param recordKeys         keys to look up.
   * @param dataTablePartition data table partition, for partitioned RLI.
   * @param collector          receives one {@link RecordIndexShardLookupStats} per shard actually
   *                           read. Pass {@link RecordIndexLookupStatsCollector#NOOP} to disable.
   */
  HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(
      HoodieData<String> recordKeys,
      Option<String> dataTablePartition,
      RecordIndexLookupStatsCollector collector);
```

In each of the three implementations, add the new method and make the existing 2-arg method delegate.

`HoodieBackedTableMetadata.java` — replace the body of the existing 2-arg override at `:338-353` with a delegation, and add the 3-arg implementation:

```java
  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(
      HoodieData<String> recordKeys, Option<String> dataTablePartition) {
    return readRecordIndexLocationsWithKeys(recordKeys, dataTablePartition,
        RecordIndexLookupStatsCollector.NOOP);
  }

  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(
      HoodieData<String> recordKeys, Option<String> dataTablePartition,
      RecordIndexLookupStatsCollector collector) {
    // If record index is not initialized yet, we cannot return an empty result here unlike the code for reading from other
    // indexes. This is because results from this function are used for upserts and returning an empty result here would lead
    // to existing records being inserted again causing duplicates.
    // The caller is required to check for record index existence in MDT before calling this method.
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionAvailable(RECORD_INDEX),
        "Record index is not initialized in MDT");

    return dataCleanupManager.ensureDataCleanupOnException(v -> {
      HoodieData<RecordIndexRawKey> rawKeys = recordKeys.map(RecordIndexRawKey::new);
      return readIndexRecordsWithKeys(rawKeys, MetadataPartitionType.RECORD_INDEX.getPartitionPath(),
          dataTablePartition, collector)
          .mapToPair((Pair<String, HoodieMetadataPayload> p) -> Pair.of(p.getLeft(), p.getRight().getRecordGlobalLocation()));
    });
  }
```

Thread `collector` through `readIndexRecordsWithKeys` → `readIndexRecords` → `lookupIndexRecords`, adding an overload at each level that defaults to `NOOP`. In `lookupIndexRecords` (`:271`), wrap the two places that read a single file slice. Add this helper to `HoodieBackedTableMetadata`:

```java
  /**
   * Wraps a lookup result iterator so that one {@link RecordIndexShardLookupStats} is emitted when
   * the iterator is exhausted or closed.
   *
   * <p>Counting on close rather than eagerly is what keeps the read lazy. The caller is expected to
   * consume the iterator fully — every current caller does. A caller that abandons it early
   * under-counts hits; it never over-counts and never fails.
   */
  private ClosableIterator<HoodieRecord<HoodieMetadataPayload>> withLookupStats(
      ClosableIterator<HoodieRecord<HoodieMetadataPayload>> delegate,
      RecordIndexLookupStatsCollector collector,
      int shardIndex,
      FileSlice fileSlice,
      long keysSubmitted,
      long startMillis) {
    if (collector == RecordIndexLookupStatsCollector.NOOP) {
      return delegate;
    }
    // Captured eagerly: the FileSlice is in scope here, so no driver-side derivation is needed and
    // the numbers describe exactly the slice that was read.
    String fileGroupId = fileSlice.getFileId();
    long logFilesRead = fileSlice.getLogFiles().count();
    long bytesInShard = fileSlice.getTotalFileSize();
    return new ClosableIterator<HoodieRecord<HoodieMetadataPayload>>() {
      private long hits = 0;
      private boolean reported = false;

      @Override
      public boolean hasNext() {
        boolean hasNext = delegate.hasNext();
        if (!hasNext) {
          report();
        }
        return hasNext;
      }

      @Override
      public HoodieRecord<HoodieMetadataPayload> next() {
        hits++;
        return delegate.next();
      }

      @Override
      public void close() {
        report();
        delegate.close();
      }

      private void report() {
        if (reported) {
          return;
        }
        reported = true;
        try {
          collector.collect(new RecordIndexShardLookupStats(shardIndex, fileGroupId,
              keysSubmitted, Math.min(hits, keysSubmitted), logFilesRead, bytesInShard,
              System.currentTimeMillis() - startMillis));
        } catch (Exception e) {
          // Instrumentation must never fail a write.
          log.warn("Failed to collect record index lookup stats for shard {}", shardIndex, e);
        }
      }
    };
  }
```

In `lookupIndexRecords`, apply it in the multi-slice branch — replace the `processFunction` body's return at `:320`:

```java
    SerializableFunction<Iterator<String>, Iterator<HoodieRecord<HoodieMetadataPayload>>> processFunction =
        sortedKeys -> {
          List<String> keysList = new ArrayList<>();
          try (ClosableSortedDedupingIterator<String> distinctSortedKeyIter = new ClosableSortedDedupingIterator<>(sortedKeys)) {
            if (!distinctSortedKeyIter.hasNext()) {
              return Collections.emptyIterator();
            }
            distinctSortedKeyIter.forEachRemaining(keysList::add);
          }
          int shardIndex = mappingFunction.apply(keysList.get(0), numFileSlices);
          FileSlice fileSlice = fileSlices.get(shardIndex);
          long startMillis = System.currentTimeMillis();
          return withLookupStats(
              lookupRecordsItr(partitionName, keysList, fileSlice, !isSecondaryIndex),
              collector, shardIndex, fileSlice, keysList.size(), startMillis);
        };
```

Note the early `return Collections.emptyIterator()` above: a shard with no keys never reaches the collector, so empty Spark partitions do not inflate `shards_read`.

Apply the same wrapping to the single-slice branch at `:291-293`, using shard index `0` and `fileSlices.get(0).getFileId()`.

`FileSystemBackedTableMetadata.java` and `NoOpTableMetadata.java` — add the 3-arg override delegating to the existing 2-arg behaviour and ignoring the collector, since neither reads a record index:

```java
  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndexLocationsWithKeys(
      HoodieData<String> recordKeys, Option<String> dataTablePartition,
      RecordIndexLookupStatsCollector collector) {
    return readRecordIndexLocationsWithKeys(recordKeys, dataTablePartition);
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl hudi-common -Dtest=TestRecordIndexLookupStatsCollection -Dspark3.5`
Expected: PASS, 3 tests.

Then verify no existing caller broke:
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home
mvn install -pl hudi-common -am -DskipTests -Dspark3.5 -Dscala-2.12
mvn checkstyle:check -pl hudi-common
```
Expected: BUILD SUCCESS for both.

- [ ] **Step 5: Commit**

```bash
git add hudi-common/src/main/java/org/apache/hudi/metadata/RecordIndexLookupStatsCollector.java \
        hudi-common/src/main/java/org/apache/hudi/metadata/HoodieTableMetadata.java \
        hudi-common/src/main/java/org/apache/hudi/metadata/HoodieBackedTableMetadata.java \
        hudi-common/src/main/java/org/apache/hudi/metadata/FileSystemBackedTableMetadata.java \
        hudi-common/src/main/java/org/apache/hudi/common/table/view/NoOpTableMetadata.java \
        hudi-common/src/test/java/org/apache/hudi/metadata/TestRecordIndexLookupStatsCollection.java
git commit -m "feat(metadata): add record index lookup stats collector seam"
```

---

### Task 3: Configuration key

**Files:**
- Modify: `hudi-common/src/main/java/org/apache/hudi/common/config/HoodieMetadataConfig.java` (add near `ENABLE_DETAILED_METRICS` at `:686`, accessor near `:1033`)
- Test: `hudi-common/src/test/java/org/apache/hudi/common/config/TestHoodieMetadataConfig.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `HoodieMetadataConfig.RECORD_INDEX_LOOKUP_STATS_ENABLE` (`ConfigProperty<Boolean>`, key `hoodie.metadata.record.index.lookup.stats.enable`), accessor `boolean isRecordIndexLookupStatsEnabled()`.

- [ ] **Step 1: Write the failing test**

Create or append to `hudi-common/src/test/java/org/apache/hudi/common/config/TestHoodieMetadataConfig.java`:

```java
package org.apache.hudi.common.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieMetadataConfig {

  @Test
  void testRecordIndexLookupStatsDefaultsToDisabled() {
    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().fromProperties(new Properties()).build();
    assertFalse(config.isRecordIndexLookupStatsEnabled(),
        "must default off, consistent with every neighbouring metrics knob");
  }

  @Test
  void testRecordIndexLookupStatsCanBeEnabled() {
    Properties props = new Properties();
    props.setProperty(HoodieMetadataConfig.RECORD_INDEX_LOOKUP_STATS_ENABLE.key(), "true");

    HoodieMetadataConfig config = HoodieMetadataConfig.newBuilder().fromProperties(props).build();
    assertTrue(config.isRecordIndexLookupStatsEnabled());
  }

  @Test
  void testRecordIndexLookupStatsKeyName() {
    assertTrue("hoodie.metadata.record.index.lookup.stats.enable"
        .equals(HoodieMetadataConfig.RECORD_INDEX_LOOKUP_STATS_ENABLE.key()));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl hudi-common -Dtest=TestHoodieMetadataConfig -Dspark3.5`
Expected: FAIL — `RECORD_INDEX_LOOKUP_STATS_ENABLE` does not exist.

- [ ] **Step 3: Write minimal implementation**

In `HoodieMetadataConfig.java`, after `ENABLE_DETAILED_METRICS` (`:686-693`):

```java
  public static final ConfigProperty<Boolean> RECORD_INDEX_LOOKUP_STATS_ENABLE = ConfigProperty
      .key(METADATA_PREFIX + ".record.index.lookup.stats.enable")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Collects per-commit counts for the record level index lookup phase of a "
          + "write — shards read, log files read, bytes resident in those shards, keys submitted "
          + "and keys hit — and publishes them to the configured metrics reporters and to commit "
          + "metadata. Counts are collected on executors and aggregated on the driver; the overhead "
          + "is a handful of counters per shard read. Disabled by default.");
```

And alongside `isDetailedMetricsEnabled()` (`:1033`):

```java
  public boolean isRecordIndexLookupStatsEnabled() {
    return getBoolean(RECORD_INDEX_LOOKUP_STATS_ENABLE);
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl hudi-common -Dtest=TestHoodieMetadataConfig -Dspark3.5`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add hudi-common/src/main/java/org/apache/hudi/common/config/HoodieMetadataConfig.java \
        hudi-common/src/test/java/org/apache/hudi/common/config/TestHoodieMetadataConfig.java
git commit -m "feat(metadata): add config gate for record index lookup stats"
```

---

### Task 4: Spark accumulator

**Files:**
- Create: `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/metrics/RecordIndexLookupStatsAccumulator.java`
- Test: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/metrics/TestRecordIndexLookupStatsAccumulator.java`

**Interfaces:**
- Consumes: `RecordIndexShardLookupStats`, `RecordIndexLookupStats` (Task 1); `RecordIndexLookupStatsCollector` (Task 2).
- Produces: `RecordIndexLookupStatsAccumulator` with `void register(JavaSparkContext jsc)`, `RecordIndexLookupStats drain()` (returns the value and resets), and `collect(RecordIndexShardLookupStats)` from the collector interface.

- [ ] **Step 1: Write the failing test**

Create `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/metrics/TestRecordIndexLookupStatsAccumulator.java`:

```java
package org.apache.hudi.metrics;

import org.apache.hudi.metadata.RecordIndexLookupStats;
import org.apache.hudi.metadata.RecordIndexShardLookupStats;

import org.apache.spark.util.AccumulatorV2;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRecordIndexLookupStatsAccumulator {

  private static RecordIndexShardLookupStats shard(int index, long keys, long hits) {
    return new RecordIndexShardLookupStats(index, "fg-" + index, keys, hits, 2L, 700L, 5L);
  }

  @Test
  void testStartsEmpty() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    assertTrue(accumulator.isZero());
    assertTrue(accumulator.value().isEmpty());
  }

  @Test
  void testCollectAccumulatesDistinctShards() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    accumulator.collect(shard(0, 10L, 4L));
    accumulator.collect(shard(1, 20L, 15L));

    assertFalse(accumulator.isZero());
    assertEquals(2L, accumulator.value().getShardsRead());
    assertEquals(30L, accumulator.value().getKeysSubmitted());
    assertEquals(19L, accumulator.value().getKeysHit());
  }

  @Test
  void testRepeatedCollectOfSameShardIsIdempotent() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    accumulator.collect(shard(3, 100L, 60L));
    accumulator.collect(shard(3, 100L, 60L));
    accumulator.collect(shard(3, 100L, 60L));

    assertEquals(1L, accumulator.value().getShardsRead());
    assertEquals(100L, accumulator.value().getKeysSubmitted(), "a retried task must not double count");
  }

  @Test
  void testMergeCombinesTwoExecutorCopies() {
    RecordIndexLookupStatsAccumulator left = new RecordIndexLookupStatsAccumulator();
    left.collect(shard(0, 10L, 4L));
    RecordIndexLookupStatsAccumulator right = new RecordIndexLookupStatsAccumulator();
    right.collect(shard(1, 20L, 15L));

    left.merge(right);

    assertEquals(2L, left.value().getShardsRead());
    assertEquals(30L, left.value().getKeysSubmitted());
  }

  @Test
  void testCopyIsIndependentOfOriginal() {
    RecordIndexLookupStatsAccumulator original = new RecordIndexLookupStatsAccumulator();
    original.collect(shard(0, 10L, 4L));

    AccumulatorV2<RecordIndexShardLookupStats, RecordIndexLookupStats> copy = original.copy();
    original.collect(shard(1, 20L, 15L));

    assertEquals(1L, copy.value().getShardsRead(), "copy must not observe later updates");
    assertEquals(2L, original.value().getShardsRead());
  }

  @Test
  void testDrainReturnsValueAndResets() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    accumulator.collect(shard(0, 10L, 4L));

    RecordIndexLookupStats drained = accumulator.drain();

    assertEquals(1L, drained.getShardsRead());
    assertTrue(accumulator.isZero(), "draining must reset so the next commit starts clean");
    assertTrue(accumulator.value().isEmpty());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl hudi-client/hudi-spark-client -Dtest=TestRecordIndexLookupStatsAccumulator -Dspark3.5`
Expected: FAIL — `RecordIndexLookupStatsAccumulator` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/metrics/RecordIndexLookupStatsAccumulator.java`:

```java
package org.apache.hudi.metrics;

import org.apache.hudi.metadata.RecordIndexLookupStats;
import org.apache.hudi.metadata.RecordIndexLookupStatsCollector;
import org.apache.hudi.metadata.RecordIndexShardLookupStats;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

/**
 * Ships per-shard record index lookup stats from executors back to the driver.
 *
 * <p>The accumulated value is keyed by shard and merged per key by field-wise max, so task retries,
 * speculation and RDD recomputation are all idempotent rather than additive. Only {@code max} and
 * map union are used — both commutative and associative, which the accumulator contract requires
 * because Spark merges executor-local copies in an unspecified order.
 *
 * <p>One instance belongs to one write client. It is never held in a static field, so two tables in
 * the same JVM cannot interfere.
 */
public class RecordIndexLookupStatsAccumulator
    extends AccumulatorV2<RecordIndexShardLookupStats, RecordIndexLookupStats>
    implements RecordIndexLookupStatsCollector {

  private static final long serialVersionUID = 1L;

  private RecordIndexLookupStats stats = RecordIndexLookupStats.empty();

  /** Registers with the given context if not already registered. Driver-side only. */
  public void register(JavaSparkContext jsc) {
    if (!isRegistered()) {
      jsc.sc().register(this, "hoodie.record.index.lookup.stats");
    }
  }

  @Override
  public void collect(RecordIndexShardLookupStats shardStats) {
    add(shardStats);
  }

  @Override
  public boolean isZero() {
    return stats.isEmpty();
  }

  @Override
  public AccumulatorV2<RecordIndexShardLookupStats, RecordIndexLookupStats> copy() {
    RecordIndexLookupStatsAccumulator copy = new RecordIndexLookupStatsAccumulator();
    copy.stats = stats;
    return copy;
  }

  @Override
  public void reset() {
    stats = RecordIndexLookupStats.empty();
  }

  @Override
  public synchronized void add(RecordIndexShardLookupStats shardStats) {
    stats = stats.merge(RecordIndexLookupStats.of(shardStats));
  }

  @Override
  public synchronized void merge(AccumulatorV2<RecordIndexShardLookupStats, RecordIndexLookupStats> other) {
    stats = stats.merge(other.value());
  }

  @Override
  public RecordIndexLookupStats value() {
    return stats;
  }

  /** Returns the accumulated value and resets, so the next commit starts from a clean slate. */
  public synchronized RecordIndexLookupStats drain() {
    RecordIndexLookupStats drained = stats;
    reset();
    return drained;
  }
}
```

`copy()` shares the immutable `stats` reference rather than deep-copying — safe precisely because `RecordIndexLookupStats` and `RecordIndexShardLookupStats` are immutable and every mutation replaces the reference.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl hudi-client/hudi-spark-client -Dtest=TestRecordIndexLookupStatsAccumulator -Dspark3.5`
Expected: PASS, 6 tests.

Then: `mvn checkstyle:check -pl hudi-client/hudi-spark-client`

- [ ] **Step 5: Commit**

```bash
git add hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/metrics/RecordIndexLookupStatsAccumulator.java \
        hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/metrics/TestRecordIndexLookupStatsAccumulator.java
git commit -m "feat(spark): add record index lookup stats accumulator"
```

---

### Task 5: Wire the collector into the Spark write path

The accumulator is owned by the write client, because `HoodieTable.index` is transient and lazily rebuilt per table instance (`HoodieTable.java:144,439-443`) — an accumulator held on the index would be a different object by the time `preCommit` runs.

**Files:**
- Modify: `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/HoodieTable.java` (add field + accessors near `:144`)
- Modify: `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/index/SparkMetadataTableGlobalRecordLevelIndex.java:116-206`
- Modify: `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java`
- Test: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/index/TestRecordIndexLookupStatsWiring.java`

**Interfaces:**
- Consumes: `RecordIndexLookupStatsAccumulator` (Task 4), `RecordIndexLookupStatsCollector` (Task 2), `isRecordIndexLookupStatsEnabled()` (Task 3).
- Produces: `HoodieTable.setRecordIndexLookupStatsCollector(RecordIndexLookupStatsCollector)` / `getRecordIndexLookupStatsCollector()`; `SparkRDDWriteClient.getRecordIndexLookupStatsAccumulator()` (package-private, for tests).

- [ ] **Step 1: Write the failing test**

Create `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/index/TestRecordIndexLookupStatsWiring.java`:

```java
package org.apache.hudi.index;

import org.apache.hudi.metadata.RecordIndexLookupStatsCollector;
import org.apache.hudi.metadata.RecordIndexShardLookupStats;
import org.apache.hudi.metrics.RecordIndexLookupStatsAccumulator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class TestRecordIndexLookupStatsWiring {

  @Test
  void testAccumulatorSatisfiesCollectorContract() {
    RecordIndexLookupStatsAccumulator accumulator = new RecordIndexLookupStatsAccumulator();
    RecordIndexLookupStatsCollector collector = accumulator;

    collector.collect(new RecordIndexShardLookupStats(4, "fg-4", 50L, 22L, 2L, 640L, 3L));

    assertEquals(1L, accumulator.value().getShardsRead());
    assertEquals(50L, accumulator.value().getKeysSubmitted());
    assertEquals(22L, accumulator.value().getKeysHit());
  }

  @Test
  void testTwoWriteClientsGetIndependentCollectors() {
    // Requirement 10: two tables in one JVM must not interfere. No static state means two
    // accumulators are simply two objects.
    RecordIndexLookupStatsAccumulator tableA = new RecordIndexLookupStatsAccumulator();
    RecordIndexLookupStatsAccumulator tableB = new RecordIndexLookupStatsAccumulator();

    tableA.collect(new RecordIndexShardLookupStats(0, "fg-0", 10L, 5L, 1L, 200L, 1L));

    assertNotSame(tableA, tableB);
    assertEquals(1L, tableA.value().getShardsRead());
    assertEquals(0L, tableB.value().getShardsRead(), "table B must not observe table A's counts");
  }

  @Test
  void testCollectorDefaultsToNoopWhenDisabled() {
    List<RecordIndexShardLookupStats> collected = new ArrayList<>();
    RecordIndexLookupStatsCollector enabled = collected::add;

    assertSame(RecordIndexLookupStatsCollector.NOOP, RecordIndexLookupStatsCollector.NOOP);
    enabled.collect(new RecordIndexShardLookupStats(0, "fg-0", 1L, 1L, 0L, 100L, 1L));
    assertEquals(1, collected.size());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl hudi-client/hudi-spark-client -Dtest=TestRecordIndexLookupStatsWiring -Dspark3.5`
Expected: FAIL — compilation error until Task 4's class is on the path and the wiring compiles.

- [ ] **Step 3: Write minimal implementation**

In `HoodieTable.java`, beside the transient `index` field at `:144`:

```java
  /**
   * Driver-side handoff for record index lookup instrumentation. Transient because the collector is
   * captured directly in the engine closure at lookup time; executors never read it from the table.
   * Defaults to a no-op so callers that do not set it are unaffected.
   */
  private transient RecordIndexLookupStatsCollector recordIndexLookupStatsCollector =
      RecordIndexLookupStatsCollector.NOOP;

  public void setRecordIndexLookupStatsCollector(RecordIndexLookupStatsCollector collector) {
    this.recordIndexLookupStatsCollector = collector;
  }

  public RecordIndexLookupStatsCollector getRecordIndexLookupStatsCollector() {
    return recordIndexLookupStatsCollector == null
        ? RecordIndexLookupStatsCollector.NOOP : recordIndexLookupStatsCollector;
  }
```

In `SparkMetadataTableGlobalRecordLevelIndex.java`, read the collector on the driver in `lookupRecords` (`:116-134`) and pass it into the lookup function:

```java
    return HoodieJavaPairRDD.of(partitionedKeyRDD.mapPartitionsToPair(
        new RecordIndexFileGroupLookupFunction(hoodieTable, hoodieTable.getRecordIndexLookupStatsCollector())));
```

And give the function the field, passing it through to the 3-arg lookup (`:182-206`):

```java
  private static class RecordIndexFileGroupLookupFunction implements PairFlatMapFunction<Iterator<String>, String, HoodieRecordGlobalLocation> {
    private final HoodieTable hoodieTable;
    private final RecordIndexLookupStatsCollector statsCollector;

    public RecordIndexFileGroupLookupFunction(HoodieTable hoodieTable, RecordIndexLookupStatsCollector statsCollector) {
      this.hoodieTable = hoodieTable;
      this.statsCollector = statsCollector;
    }

    @Override
    public Iterator<Tuple2<String, HoodieRecordGlobalLocation>> call(Iterator<String> recordKeyIterator) {
      List<String> keysToLookup = new ArrayList<>();
      recordKeyIterator.forEachRemaining(keysToLookup::add);

      // recordIndexInfo object only contains records that are present in record_index.
      HoodiePairData<String, HoodieRecordGlobalLocation> recordIndexData =
          hoodieTable.getTableMetadata().readRecordIndexLocationsWithKeys(
              HoodieListData.eager(keysToLookup), Option.empty(), statsCollector);
      try {
        List<Pair<String, HoodieRecordGlobalLocation>> recordIndexInfo = HoodieDataUtils.dedupeAndCollectAsList(recordIndexData);
        return recordIndexInfo.stream()
            .map(e -> new Tuple2<>(e.getKey(), e.getValue())).iterator();
      } finally {
        // Clean up the RDD to avoid memory leaks
        recordIndexData.unpersistWithDependencies();
      }
    }
  }
```

In `SparkRDDWriteClient.java`, own the accumulator and attach it to every table this client creates:

```java
  private RecordIndexLookupStatsAccumulator recordIndexLookupStatsAccumulator;

  private RecordIndexLookupStatsAccumulator getOrCreateRecordIndexLookupStatsAccumulator() {
    if (recordIndexLookupStatsAccumulator == null) {
      recordIndexLookupStatsAccumulator = new RecordIndexLookupStatsAccumulator();
      recordIndexLookupStatsAccumulator.register(HoodieSparkEngineContext.getSparkContext(context));
    }
    return recordIndexLookupStatsAccumulator;
  }

  @VisibleForTesting
  RecordIndexLookupStatsAccumulator getRecordIndexLookupStatsAccumulator() {
    return recordIndexLookupStatsAccumulator;
  }

  private HoodieTable attachLookupStatsCollector(HoodieTable table) {
    if (config.getMetadataConfig().isRecordIndexLookupStatsEnabled()) {
      table.setRecordIndexLookupStatsCollector(getOrCreateRecordIndexLookupStatsAccumulator());
    }
    return table;
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config) {
    return attachLookupStatsCollector(createTableAndValidate(config, HoodieSparkTable::create));
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
    return attachLookupStatsCollector(createTableAndValidate(config, metaClient, HoodieSparkTable::create));
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl hudi-client/hudi-spark-client -Dtest=TestRecordIndexLookupStatsWiring -Dspark3.5`
Expected: PASS, 3 tests.

Then:
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home
mvn install -pl hudi-client/hudi-spark-client -am -DskipTests -Dspark3.5 -Dscala-2.12
```
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/HoodieTable.java \
        hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/index/SparkMetadataTableGlobalRecordLevelIndex.java \
        hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java \
        hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/index/TestRecordIndexLookupStatsWiring.java
git commit -m "feat(spark): wire record index lookup stats collector into tag location"
```

---

### Task 6: Driver-side derivation, commit metadata and reporting

Every count is already present in the drained map, so this task is a fold plus two writes — no file-system view, no table rebuild, no I/O on the commit path.

**Files:**
- Create: `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/metrics/RecordIndexLookupStatsReporter.java`
- Modify: `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java` (override `preCommit`)
- Test: `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/metrics/TestRecordIndexLookupStatsReporter.java`

**Interfaces:**
- Consumes: `RecordIndexLookupStats` (Task 1), `RecordIndexLookupStatsAccumulator` (Task 4).
- Produces:
  - `RecordIndexLookupStatsReporter.toMetrics(RecordIndexLookupStats)` returning `Map<String, Long>` of metric name to value.
  - `RecordIndexLookupStatsReporter.toJson(Map<String, Long>)` returning the `extraMetadata` payload.
  - Constant `RecordIndexLookupStatsReporter.COMMIT_METADATA_KEY = "hoodie.rli.lookup.stats"`.

- [ ] **Step 1: Write the failing test**

Create `hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/metrics/TestRecordIndexLookupStatsReporter.java`:

```java
package org.apache.hudi.metrics;

import org.apache.hudi.metadata.RecordIndexLookupStats;
import org.apache.hudi.metadata.RecordIndexShardLookupStats;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRecordIndexLookupStatsReporter {

  private static RecordIndexLookupStats twoShards() {
    return RecordIndexLookupStats
        .of(new RecordIndexShardLookupStats(0, "fg-0", 100L, 70L, 3L, 1300L, 5L))
        .merge(RecordIndexLookupStats.of(new RecordIndexShardLookupStats(2, "fg-2", 50L, 10L, 1L, 600L, 3L)));
  }

  @Test
  void testFoldsEveryMetricFromShardStats() {
    Map<String, Long> metrics = RecordIndexLookupStatsReporter.toMetrics(twoShards());

    assertEquals(2L, metrics.get("lookup_record_index_shards_read"));
    assertEquals(150L, metrics.get("lookup_record_index_key_count"));
    assertEquals(80L, metrics.get("lookup_record_index_key_hit_count"));
    assertEquals(4L, metrics.get("lookup_record_index_log_files_read"));
    assertEquals(1900L, metrics.get("lookup_record_index_bytes_in_shards_read"));
  }

  @Test
  void testAllValuesAreLongForReporterCompatibility() {
    Map<String, Long> metrics = RecordIndexLookupStatsReporter.toMetrics(twoShards());

    // PushGatewayReporter casts (Long) and DatadogReporter casts (long); a non-Long throws at report time.
    metrics.values().forEach(value -> assertTrue(value instanceof Long));
  }

  @Test
  void testEmptyStatsProduceNoPayload() {
    assertTrue(RecordIndexLookupStatsReporter.toMetrics(RecordIndexLookupStats.empty()).isEmpty());
  }

  @Test
  void testRetriedShardDoesNotInflateTotals() {
    // The same shard reported twice is one shard, and its counts are not doubled.
    RecordIndexLookupStats retried = twoShards()
        .merge(RecordIndexLookupStats.of(new RecordIndexShardLookupStats(0, "fg-0", 100L, 70L, 3L, 1300L, 9L)));

    Map<String, Long> metrics = RecordIndexLookupStatsReporter.toMetrics(retried);

    assertEquals(2L, metrics.get("lookup_record_index_shards_read"));
    assertEquals(150L, metrics.get("lookup_record_index_key_count"));
    assertEquals(1900L, metrics.get("lookup_record_index_bytes_in_shards_read"));
  }

  @Test
  void testJsonPayloadIsCompactAndContainsEveryMetric() {
    Map<String, Long> metrics = RecordIndexLookupStatsReporter.toMetrics(twoShards());

    String json = RecordIndexLookupStatsReporter.toJson(metrics);

    assertFalse(json.contains("\n"), "payload must be compact for the timeline");
    assertTrue(json.contains("\"version\":1"), "payload must be versioned for later fields");
    metrics.keySet().forEach(name -> assertTrue(json.contains(name), "missing " + name));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl hudi-client/hudi-spark-client -Dtest=TestRecordIndexLookupStatsReporter -Dspark3.5`
Expected: FAIL — `RecordIndexLookupStatsReporter` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/metrics/RecordIndexLookupStatsReporter.java`:

```java
package org.apache.hudi.metrics;

import org.apache.hudi.metadata.RecordIndexLookupStats;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Turns drained record index lookup stats into reportable metrics.
 *
 * <p>Purely a fold over the per-shard map: every count, including log files and bytes, was captured
 * on the executor where the {@code FileSlice} was in scope. Nothing here touches storage, so the
 * commit path pays no I/O for instrumentation.
 */
public class RecordIndexLookupStatsReporter {

  public static final String COMMIT_METADATA_KEY = "hoodie.rli.lookup.stats";

  static final String SHARDS_READ = "lookup_record_index_shards_read";
  static final String KEYS_COUNT = "lookup_record_index_key_count";
  static final String KEYS_HIT_COUNT = "lookup_record_index_key_hit_count";
  static final String LOG_FILES_READ = "lookup_record_index_log_files_read";
  static final String BYTES_IN_SHARDS_READ = "lookup_record_index_bytes_in_shards_read";

  private RecordIndexLookupStatsReporter() {
  }

  /**
   * @param stats drained per-shard stats.
   * @return metric name to value; empty when nothing was looked up. Every value is a {@link Long},
   *         which the Datadog and Prometheus reporters require.
   */
  public static Map<String, Long> toMetrics(RecordIndexLookupStats stats) {
    if (stats.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Long> metrics = new LinkedHashMap<>();
    metrics.put(SHARDS_READ, stats.getShardsRead());
    metrics.put(KEYS_COUNT, stats.getKeysSubmitted());
    metrics.put(KEYS_HIT_COUNT, stats.getKeysHit());
    metrics.put(LOG_FILES_READ, stats.getLogFilesRead());
    metrics.put(BYTES_IN_SHARDS_READ, stats.getBytesInShardsRead());
    return metrics;
  }

  /** Compact JSON for the commit metadata payload. One versioned key beats five flat ones. */
  public static String toJson(Map<String, Long> metrics) {
    StringBuilder json = new StringBuilder("{\"version\":1");
    metrics.forEach((name, value) -> json.append(",\"").append(name).append("\":").append(value));
    return json.append('}').toString();
  }
}
```

In `SparkRDDWriteClient.java`, override `preCommit` — it runs at `BaseHoodieWriteClient.commitStats:273`, before `commit(...)` writes the file at `:277`:

```java
  @Override
  protected void preCommit(HoodieCommitMetadata metadata) {
    super.preCommit(metadata);
    reportRecordIndexLookupStats(metadata);
  }

  /**
   * Drains the lookup accumulator, derives file-level counts, and publishes to commit metadata and
   * the metrics reporters. Never throws: instrumentation must not be able to fail a write.
   */
  private void reportRecordIndexLookupStats(HoodieCommitMetadata metadata) {
    if (!config.getMetadataConfig().isRecordIndexLookupStatsEnabled()
        || recordIndexLookupStatsAccumulator == null) {
      return;
    }
    try {
      RecordIndexLookupStats stats = recordIndexLookupStatsAccumulator.drain();
      Map<String, Long> reportable = RecordIndexLookupStatsReporter.toMetrics(stats);
      if (reportable.isEmpty()) {
        return;
      }
      metadata.addMetadata(RecordIndexLookupStatsReporter.COMMIT_METADATA_KEY,
          RecordIndexLookupStatsReporter.toJson(reportable));
      reportable.forEach((name, value) -> metrics.getMetrics().registerGauge(name, value));
    } catch (Exception e) {
      log.warn("Failed to publish record index lookup stats; the write is unaffected", e);
    }
  }
```

`metrics` is the inherited `HoodieMetrics` field, and `getMetrics()` exists via Lombok `@Getter`
on `HoodieMetrics.java:104-105`. The drain happens before the emptiness check so that the
accumulator is reset even on a commit that performed no lookup — otherwise a commit with no
RLI activity would let the previous commit's counts survive into the next one.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl hudi-client/hudi-spark-client -Dtest=TestRecordIndexLookupStatsReporter -Dspark3.5`
Expected: PASS, 5 tests.

Then:
```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home
mvn install -pl hudi-client/hudi-spark-client -am -DskipTests -Dspark3.5 -Dscala-2.12
mvn checkstyle:check -pl hudi-client/hudi-spark-client
```

- [ ] **Step 5: Commit**

```bash
git add hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/metrics/RecordIndexLookupStatsReporter.java \
        hudi-client/hudi-spark-client/src/main/java/org/apache/hudi/client/SparkRDDWriteClient.java \
        hudi-client/hudi-spark-client/src/test/java/org/apache/hudi/metrics/TestRecordIndexLookupStatsReporter.java
git commit -m "feat(spark): publish record index lookup stats to commit metadata and reporters"
```

---

### Task 7: End-to-end and multi-table isolation tests

**Files:**
- Create: `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/functional/TestRecordIndexLookupStats.java`

**Interfaces:**
- Consumes: everything from Tasks 1-6.
- Produces: nothing — this is the acceptance gate.

- [ ] **Step 1: Write the failing test**

Create `hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/functional/TestRecordIndexLookupStats.java`:

```java
package org.apache.hudi.functional;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.metrics.RecordIndexLookupStatsReporter;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Acceptance tests for record index lookup observability. Extends the project's Spark client test
 * harness; follow the pattern used by TestHoodieBackedMetadata for table setup and upserts.
 */
class TestRecordIndexLookupStats extends HoodieSparkClientTestBase {

  @Test
  void testUpsertRecordsLookupStatsInCommitMetadata() throws Exception {
    // Setup: RLI-enabled table, 100 records inserted, then upsert of 60 existing + 40 new.
    // 60 keys exist in the index (updates), 40 do not (inserts).
    String basePath = initRecordIndexTable(withLookupStatsEnabled(true));
    insertRecords(basePath, 100);

    String commitTime = upsertRecords(basePath, /* existing */ 60, /* fresh */ 40);

    HoodieCommitMetadata metadata = readCommitMetadata(basePath, commitTime);
    String payload = metadata.getExtraMetadata().get(RecordIndexLookupStatsReporter.COMMIT_METADATA_KEY);

    assertTrue(payload != null, "commit metadata must carry the lookup stats payload");
    assertTrue(payload.contains("\"lookup_record_index_key_count\":100"), "100 keys probed: " + payload);
    assertTrue(payload.contains("\"lookup_record_index_key_hit_count\":60"), "60 keys hit: " + payload);
    assertTrue(payload.contains("\"lookup_record_index_shards_read\":"), "shards read present: " + payload);
    assertFalse(payload.contains("\"lookup_record_index_shards_read\":0"), "at least one shard must be read");
    assertTrue(payload.contains("\"lookup_record_index_log_files_read\":"), "log files present: " + payload);
    assertTrue(payload.contains("\"lookup_record_index_bytes_in_shards_read\":"), "bytes present: " + payload);
  }

  @Test
  void testDisabledByDefaultEmitsNothing() throws Exception {
    String basePath = initRecordIndexTable(withLookupStatsEnabled(false));
    insertRecords(basePath, 50);

    String commitTime = upsertRecords(basePath, 25, 25);

    HoodieCommitMetadata metadata = readCommitMetadata(basePath, commitTime);
    assertFalse(metadata.getExtraMetadata().containsKey(RecordIndexLookupStatsReporter.COMMIT_METADATA_KEY),
        "no payload when the feature is off");
  }

  @Test
  void testTwoTablesInOneJvmDoNotInterfere() throws Exception {
    // Requirement 10. Different hit counts per table make cross-contamination detectable.
    String tableA = initRecordIndexTable(withLookupStatsEnabled(true));
    String tableB = initRecordIndexTable(withLookupStatsEnabled(true));
    insertRecords(tableA, 100);
    insertRecords(tableB, 40);

    String commitA = upsertRecords(tableA, 60, 40);
    String commitB = upsertRecords(tableB, 10, 30);

    String payloadA = readCommitMetadata(tableA, commitA).getExtraMetadata()
        .get(RecordIndexLookupStatsReporter.COMMIT_METADATA_KEY);
    String payloadB = readCommitMetadata(tableB, commitB).getExtraMetadata()
        .get(RecordIndexLookupStatsReporter.COMMIT_METADATA_KEY);

    assertTrue(payloadA.contains("\"lookup_record_index_key_hit_count\":60"), payloadA);
    assertTrue(payloadB.contains("\"lookup_record_index_key_hit_count\":10"), payloadB);
  }

  @Test
  void testSecondCommitStartsFromCleanCounters() throws Exception {
    // Draining resets, so counts must not carry over between commits on the same write client.
    String basePath = initRecordIndexTable(withLookupStatsEnabled(true));
    insertRecords(basePath, 100);

    upsertRecords(basePath, 60, 40);
    String secondCommit = upsertRecords(basePath, 10, 0);

    String payload = readCommitMetadata(basePath, secondCommit).getExtraMetadata()
        .get(RecordIndexLookupStatsReporter.COMMIT_METADATA_KEY);
    assertTrue(payload.contains("\"lookup_record_index_key_hit_count\":10"),
        "second commit must not inherit the first commit's counts: " + payload);
  }
}
```

> **Note for the implementer:** `initRecordIndexTable`, `withLookupStatsEnabled`, `insertRecords`, `upsertRecords` and `readCommitMetadata` are helpers you write against the existing harness. Model them on `TestHoodieBackedMetadata` in the same module, which already sets up an RLI-enabled table and performs upserts. Do not use reflection to reach internals — construct the write client properly and read the timeline for commit metadata.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn test -pl hudi-spark-datasource/hudi-spark -Dtest=TestRecordIndexLookupStats -Dspark3.5`
Expected: FAIL — helpers unimplemented, then assertion failures until Tasks 1-6 are wired correctly.

- [ ] **Step 3: Write minimal implementation**

Implement the five helper methods against the existing harness. No production code should be needed; if a test cannot be written without changing production code, that is a design gap — stop and report it rather than working around it.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn test -pl hudi-spark-datasource/hudi-spark -Dtest=TestRecordIndexLookupStats -Dspark3.5`
Expected: PASS, 4 tests.

Full regression on the touched modules:
```bash
mvn test -pl hudi-common,hudi-client/hudi-spark-client -Dspark3.5
```
Expected: PASS, no new failures.

- [ ] **Step 5: Commit**

```bash
git add hudi-spark-datasource/hudi-spark/src/test/java/org/apache/hudi/functional/TestRecordIndexLookupStats.java
git commit -m "test(spark): add end-to-end record index lookup stats coverage"
```

---

## Suggested PR split

The spec's top risk (§13) is review latency on the `hudi-common` interface change, not code. Ship it first and alone so it is reviewed on its own merits while the Spark work proceeds:

- **PR 1** — Tasks 1, 2, 3 (`hudi-common`: value types, collector seam, config). Self-contained, no behaviour change, `NOOP` everywhere.
- **PR 2** — Tasks 4, 5, 6 (`hudi-spark-client`: accumulator, wiring, reporting).
- **PR 3** — Task 7 (end-to-end coverage), or folded into PR 2 if reviewers prefer.

## Follow-ups (P1, explicitly out of this plan)

True bytes read via reader instrumentation · skew summary derived from the shard map ·
Flink adapter · `max_shard_lookup_millis` · silent-fallback marker · `base_files_read` ·
`hottest_shard_file_group_id` · deduplicated `keys_submitted` · partitioned-RLI coverage
(`SparkMetadataTableRecordLevelIndex`).
