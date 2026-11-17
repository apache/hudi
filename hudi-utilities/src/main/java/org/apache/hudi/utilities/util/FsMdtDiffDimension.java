/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utilities.util.archive.ArchiveTimelineInstantBatchResolvers;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;
import org.apache.hudi.utilities.util.archive.InstantSearchState;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.utilities.util.HoodieMetadataValidationUtils.FileSliceComparator;

/**
 * The FS-MDT comparison: a partition's latest file slices according to the metadata table against
 * a storage listing, with a verdict on every difference.
 *
 * <p>Phase 1 emits verdict-less difference records (only the difference and its writing instant are
 * knowable before {@code T_aft} is read); phase 2 adjudicates them against the timeline delta.
 * {@link #readAfterBarrier} enforces that phase 1 completes before {@code T_aft} is read.
 */
public final class FsMdtDiffDimension {

  private static final Logger LOG = LoggerFactory.getLogger(FsMdtDiffDimension.class);

  private static final String LATEST_FILE_SLICES_V2_LABEL = "latest file slices (v2)";

  /** How many failing records fail-fast names in the log before it stops naming them. */
  private static final int MAX_FAILING_RECORDS_LOGGED = 50;

  /** How many partitions the run's failure names before it reports the rest as a count. */
  private static final int MAX_PARTITIONS_NAMED = 10;

  private FsMdtDiffDimension() {
  }

  /** What the dimension found and what it cost: the input to the run's verdict, report and summary. */
  public static final class Result {

    /** Every difference the run found, with a verdict on each, sorted. */
    private final List<FileDiffRecord> records;
    /**
     * Every file either side's listing considered, across every partition. Not derivable from
     * {@link #records}: a clean partition contributes files but no record.
     */
    private final long filesSeen;
    /**
     * Files excluded from adjudication, by class. Dropped files never become records, so without
     * this a run that dropped everything is indistinguishable from a clean one.
     */
    private final Map<String, Long> dropCounts;
    /** The run's one validation failure, or empty when every verdict was benign. */
    private final Option<HoodieValidationException> failure;
    private final ServiceActivityDelta dlt;
    /** The newest instant on the aft active timeline; the run's summary line reports it. */
    private final String aftNewestInstant;
    private final long diffCount;
    private final long unmatchedStateCount;
    private final long phaseOneElapsedMs;
    private final long phaseTwoElapsedMs;

    /**
     * Why this run reached no verdict, or empty on every run that adjudicated. Not a finding: the
     * caller decides what it costs, because the two validation modes answer it differently.
     */
    private final Option<Inconclusive> inconclusive;

    Result(List<FileDiffRecord> records, long filesSeen, Map<String, Long> dropCounts,
           Option<HoodieValidationException> failure, ServiceActivityDelta dlt,
           String aftNewestInstant, long diffCount, long unmatchedStateCount,
           long phaseOneElapsedMs, long phaseTwoElapsedMs) {
      this(records, filesSeen, dropCounts, failure, dlt, aftNewestInstant, diffCount,
          unmatchedStateCount, phaseOneElapsedMs, phaseTwoElapsedMs, Option.empty());
    }

    Result(List<FileDiffRecord> records, long filesSeen, Map<String, Long> dropCounts,
           Option<HoodieValidationException> failure, ServiceActivityDelta dlt,
           String aftNewestInstant, long diffCount, long unmatchedStateCount,
           long phaseOneElapsedMs, long phaseTwoElapsedMs, Option<Inconclusive> inconclusive) {
      this.records = records;
      this.filesSeen = filesSeen;
      this.dropCounts = dropCounts;
      this.failure = failure;
      this.dlt = dlt;
      this.aftNewestInstant = aftNewestInstant;
      this.diffCount = diffCount;
      this.unmatchedStateCount = unmatchedStateCount;
      this.phaseOneElapsedMs = phaseOneElapsedMs;
      this.phaseTwoElapsedMs = phaseTwoElapsedMs;
      this.inconclusive = inconclusive;
    }

    /** Empty on a run that reached a verdict; otherwise the cause and the reading behind it. */
    public Option<Inconclusive> getInconclusive() {
      return inconclusive;
    }

    public boolean isInconclusive() {
      return inconclusive.isPresent();
    }

    public List<FileDiffRecord> getRecords() {
      return records;
    }

    public long getFilesSeen() {
      return filesSeen;
    }

    public Map<String, Long> getDropCounts() {
      return dropCounts;
    }

    /** The run's validation failure, or empty when it found nothing that fails validation. */
    public Option<HoodieValidationException> getFailure() {
      return failure;
    }

    public ServiceActivityDelta getServiceActivity() {
      return dlt;
    }

    /** The newest instant on the aft active timeline, or null when it held none. */
    public String getAftNewestInstant() {
      return aftNewestInstant;
    }

    public long getDiffCount() {
      return diffCount;
    }

    public long getUnmatchedStateCount() {
      return unmatchedStateCount;
    }

    public long getPhaseOneElapsedMs() {
      return phaseOneElapsedMs;
    }

    public long getPhaseTwoElapsedMs() {
      return phaseTwoElapsedMs;
    }
  }

  /**
   * Runs the dimension: phase 1, the barrier, {@code T_aft}, phase 2.
   *
   * <p>Phase 1 is forced to completion before {@code readTAft} is called (see
   * {@link #readAfterBarrier}): a record from a listing taken after {@code T_aft} is one the
   * timeline delta cannot explain.
   *
   * @param jsc           the Spark context both fan-outs run on
   * @param allPartitions every partition either side of the table reports
   * @param taskCount     how many Spark tasks phase 1 splits them across
   * @param ctx           what each task needs; see {@link Context}
   * @param tPrev         the timeline the metadata snapshot is pinned to
   * @param prevIndex     the state of every instant on {@code tPrev}
   * @param readTAft      reads the active timeline again, through a meta client of its own; called
   *                      only once phase 1 has been forced to completion
   * @param archivalAtPrev the archive's reading taken when the snapshot was pinned; compared
   *                      against a second reading at {@code T_aft} (see {@link ArchivalProbe})
   */
  public static Result run(JavaSparkContext jsc, List<String> allPartitions, int taskCount,
                           Context ctx, HoodieTimeline tPrev, InstantStateIndex prevIndex,
                           Supplier<HoodieTimeline> readTAft, String archivalAtPrev) {
    PhaseOne phaseOne = null;
    // Neither figure is a fact about a record, so they travel on accumulators, flushed once per task.
    LongAccumulator filesSeen = jsc.sc().longAccumulator("mdtValidatorV2DiffFilesSeen");
    CollectionAccumulator<Map<String, Long>> dropCounts =
        jsc.sc().collectionAccumulator("mdtValidatorV2DiffDropCounts");
    // Declared above the try so the catch can report the real phase-1 elapsed time.
    long phaseOneElapsedMs = 0L;
    try {
      // The listing MUST be materialised before T_aft is read; Spark's laziness would otherwise run
      // it afterwards.
      long phaseOneStartMs = System.currentTimeMillis();
      phaseOne = runPhaseOne(jsc, allPartitions, taskCount, ctx, filesSeen, dropCounts);
      phaseOneElapsedMs = System.currentTimeMillis() - phaseOneStartMs;

      HoodieTimeline tAft = readAfterBarrier(phaseOne, readTAft);

      // Taken against the same T_aft the delta is built from, so the two readings bracket the
      // window dlt describes. The archiver removes instants without leaving a trace the delta can
      // name, so an instant archived mid-run looks like one a rollback erased: stop inconclusive.
      String archivalAtAft = ArchivalProbe.signature(ctx.metaClient);
      if (ArchivalProbe.archivalRan(archivalAtPrev, archivalAtAft)) {
        BranchBeacon.hit(Branch.FSM_ARCHIVAL_RAN_INCONCLUSIVE);
        String reason = ArchivalProbe.describe(archivalAtPrev, archivalAtAft);
        LOG.warn("Archival ran while validating table {}: {}. Phase 2 is not run and no verdict is "
            + "reached", ctx.basePath, reason);
        return new Result(Collections.emptyList(), 0L, Collections.emptyMap(), Option.empty(),
            null, null, phaseOne.diffCount, 0L, phaseOneElapsedMs, 0L,
            Option.of(new Inconclusive(InconclusiveCause.ARCHIVAL_RAN, reason)));
      }

      // The state of every instant on the full active timeline at T_aft, built once on the driver.
      InstantStateIndex aftIndex = InstantStateIndex.from(tAft);
      String aftNewestInstant = newestInstantOf(aftIndex);
      // From THIS T_aft read, not T_prev; see AdjudicationContext#earliestActiveWriteCommit.
      String earliestActiveWriteCommit = InstantBuckets.earliestCompletedWriteCommit(
          tAft.getCommitsTimeline().filterCompletedInstants());
      ServiceActivityDelta dlt = ServiceActivityDelta.between(prevIndex, tAft, ctx.metaClient);
      // Cleans and rollbacks pending at EITHER read: one pending at both contributes nothing to a
      // state diff yet may already have deleted files, and one scheduled mid-run is absent from
      // T_prev. dlt covers only the ones that completed during the run. All plan reads happen here,
      // on the driver.
      PendingServiceState pending = PendingServiceState.across(tPrev, tAft, ctx.metaClient);

      long phaseTwoStartMs = System.currentTimeMillis();
      PhaseTwo phaseTwo = runPhaseTwo(jsc, phaseOne, prevIndex, aftIndex,
          dlt, pending, ctx, tAft, earliestActiveWriteCommit);
      long phaseTwoElapsedMs = System.currentTimeMillis() - phaseTwoStartMs;

      // Asked after phase 2's last action: each re-read of the cached RDD can recompute a lost
      // block off a fresh listing. Null-guarded per PhaseOne#baseline's contract.
      if (phaseOne.baseline() != null && phaseOne.baseline().recomputed()) {
        return recomputedResult(ctx, phaseOne.baseline(), phaseOne.diffCount, phaseOneElapsedMs,
            phaseTwoElapsedMs);
      }

      // Accumulators are read only after phase 2's last action has returned.
      return new Result(phaseTwo.records, filesSeen.value(), mergeDropCounts(dropCounts.value()),
          diffFailure(ctx, phaseTwo.records), dlt, aftNewestInstant, phaseOne.diffCount,
          phaseTwo.unmatchedStateCount, phaseOneElapsedMs, phaseTwoElapsedMs);
    } catch (RuntimeException e) {
      // discardForRecompute classifies infrastructure failures BEFORE a recompute may discard this
      // failure. A lost executor is both the commonest cause of a recompute and an infrastructure
      // failure; checking recomputed() alone would let a preempted node report success instead of
      // paging. Otherwise a finding raised while phase 1 was recomputed cannot be attributed to the
      // pinned window and must not fail the run; it is discarded loudly with the original attached.
      if (phaseOne != null && phaseOne.baseline() != null
          && ValidationFailures.discardForRecompute(e, phaseOne.baseline().recomputed())) {
        LOG.warn("Discarding an FS-MDT diff failure raised while phase 1 was being recomputed for "
            + "table {}; the finding cannot be attributed to the window this run pins to",
            ctx.basePath, e);
        return recomputedResult(ctx, phaseOne.baseline(), phaseOne.diffCount, phaseOneElapsedMs,
            0L);
      }
      throw e;
    } finally {
      if (phaseOne != null) {
        phaseOne.release();
      }
    }
  }

  /**
   * The inconclusive result a recompute produces. Phase 2's output is discarded: observations that
   * cannot be attributed to the pinned window say nothing, and a partial answer reads as complete.
   */
  private static Result recomputedResult(Context ctx, RecomputeProbe.Baseline baseline,
                                         long diffCount, long phaseOneElapsedMs,
                                         long phaseTwoElapsedMs) {
    BranchBeacon.hit(Branch.FSM_RECOMPUTED_INCONCLUSIVE);
    String reason = baseline.describe();
    LOG.warn("Phase 1 was recomputed while validating table {}: {}. No verdict is reached",
        ctx.basePath, reason);
    return new Result(Collections.emptyList(), 0L, Collections.emptyMap(), Option.empty(), null,
        null, diffCount, 0L, phaseOneElapsedMs, phaseTwoElapsedMs,
        Option.of(new Inconclusive(InconclusiveCause.PHASE_ONE_RECOMPUTED, reason)));
  }

  /** Phase 1's output: the cached record RDD and what was derived from it. */
  @VisibleForTesting
  public static final class PhaseOne {

    /**
     * Every difference phase 1 found, as individual records (the unit of the phase-2 shuffle).
     * Cached and persisted; the run's two counters travel on accumulators instead.
     */
    private final JavaRDD<FileDiffRecord> records;

    private final long diffCount;

    /** How many phase-1 tasks the run expects to have run: one per Spark partition of the fan-out. */
    private final long expectedTasks;

    /**
     * How many phase-1 tasks actually ran, counted on the executors, so that replacing the forcing
     * {@code count()} with a lazy transformation is caught by {@link #readAfterBarrier}.
     */
    private final long observedTasks;

    /**
     * The barrier reading, kept so the run can ask after phase 2 whether the RDD was recomputed.
     * Null on a {@code PhaseOne} built from raw counts (tests), so callers must null-guard.
     */
    private final RecomputeProbe.Baseline baseline;

    public PhaseOne(JavaRDD<FileDiffRecord> records, long diffCount,
                    long expectedTasks, long observedTasks) {
      this(records, diffCount, expectedTasks, observedTasks, null);
    }

    PhaseOne(JavaRDD<FileDiffRecord> records, long diffCount, RecomputeProbe.Baseline baseline) {
      this(records, diffCount, baseline.expectedTasks(), baseline.observedTasks(), baseline);
    }

    private PhaseOne(JavaRDD<FileDiffRecord> records, long diffCount, long expectedTasks,
                     long observedTasks, RecomputeProbe.Baseline baseline) {
      this.records = records;
      this.diffCount = diffCount;
      this.expectedTasks = expectedTasks;
      this.observedTasks = observedTasks;
      this.baseline = baseline;
    }

    RecomputeProbe.Baseline baseline() {
      return baseline;
    }

    /**
     * Whether every phase-1 task has run. {@code >=} because a speculative or retried task counts
     * again, and over-counting still proves the listings happened.
     */
    boolean isMaterialised() {
      return expectedTasks > 0 && observedTasks >= expectedTasks;
    }

    /**
     * Releases the cached blocks, best effort. Never throws: it runs in {@code run()}'s
     * {@code finally}, where an exception would replace the real failure.
     */
    void release() {
      try {
        records.unpersist(false);
      } catch (Exception e) {
        LOG.warn("Could not unpersist the phase-1 RDD", e);
      }
    }
  }

  /**
   * Reads {@code T_aft}, refusing unless phase 1 has already been forced to completion.
   *
   * <p>Catches both ways the ordering can silently break: moving the {@code T_aft} read above phase
   * 1 (no phase 1) or making phase 1 lazy (no executor ran it). Throws {@link IllegalStateException},
   * a prerequisite failure that fails the run in both modes rather than a finding about the table.
   */
  @VisibleForTesting
  public static <T> T readAfterBarrier(PhaseOne phaseOne, Supplier<T> readTimeline) {
    if (phaseOne == null || !phaseOne.isMaterialised()) {
      throw new IllegalStateException(
          "T_aft must not be read until phase 1 has been forced to completion: phase 1 "
              + (phaseOne == null ? "has not run at all" : "ran " + phaseOne.observedTasks + " of "
                  + phaseOne.expectedTasks + " tasks")
              + ". Every diff record would otherwise describe storage as it was after the timeline "
              + "delta that is supposed to explain it");
    }
    return readTimeline.get();
  }

  /**
   * Phase 1: the FS-MDT diff per partition, materialised before the driver reads {@code T_aft}.
   *
   * <p>The materialisation is a correctness property, and best effort: a cached block lost to a
   * decommissioned executor is recomputed after {@code T_aft}, which {@link RecomputeProbe} detects.
   * Phase 2 re-reads the cached RDD (three times, or twice on the empty-counts path), each re-run
   * only unpacking cached data, never listing.
   */
  private static PhaseOne runPhaseOne(JavaSparkContext jsc, List<String> allPartitions,
                                      int taskCount, Context ctx, LongAccumulator filesSeen,
                                      CollectionAccumulator<Map<String, Long>> dropCounts) {
    RecomputeProbe probe = RecomputeProbe.start(jsc, "mdtValidatorV2PhaseOneTasks", taskCount);
    // Resolved on the driver: RecomputeProbe is not Serializable, so calling probe.counter() inside
    // the lambda would fail task submission with NotSerializableException.
    LongAccumulator phaseOneTasks = probe.counter();
    JavaRDD<FileDiffRecord> records = jsc.parallelize(allPartitions, taskCount)
        .mapPartitions(partitionIterator ->
            diffPartitionBatch(partitionIterator, ctx, phaseOneTasks, filesSeen, dropCounts));
    records.persist(StorageLevel.MEMORY_AND_DISK_SER());
    try {
      // Forces the listing. A clean table still runs every task, so the barrier is answerable.
      long diffCount = records.count();
      // Sealed only after the action returns: accumulators hold a completed action's updates.
      return new PhaseOne(records, diffCount, probe.seal());
    } catch (Exception e) {
      // No PhaseOne handle exists on this path for run()'s finally to release, so unpersist here,
      // best effort, without replacing the real failure.
      try {
        records.unpersist(false);
      } catch (Exception unpersistFailure) {
        LOG.warn("Could not unpersist the phase-1 RDD after phase 1 failed for table {}",
            ctx.basePath, unpersistFailure);
      }
      // Phase 1 decides no verdicts, so this is a listing or teardown failure that fails the run.
      throw ValidationFailures.classifyFailure(e);
    }
  }

  /**
   * Phase 1 over one task's batch: fetches every partition's expected slices from the driver, then
   * diffs them against storage in bounded concurrent rounds.
   *
   * <p>No verdict is decided here: {@code T_aft} has not been read yet, so a record says only that a
   * file differs and which instant wrote it. The counters are flushed once, at the end.
   */
  private static Iterator<FileDiffRecord> diffPartitionBatch(
      Iterator<String> partitionIterator, Context ctx, LongAccumulator phaseOneTasks,
      LongAccumulator filesSeen, CollectionAccumulator<Map<String, Long>> dropCounts) {
    List<String> batch = new ArrayList<>();
    partitionIterator.forEachRemaining(batch::add);
    DiffTally taskTally = new DiffTally();
    // Counts every attempt that goes on to return: a failed attempt reports no user accumulator
    // (Task#collectAccumulatorUpdates keeps only countFailedValues accumulators, none here). So a
    // recompute inside a task that then throws is invisible to RecomputeProbe and that failure is
    // not discarded, which errs toward failing the run.
    phaseOneTasks.add(1L);
    LogFileReader logFileReader = logFileReaderFor(ctx.metaClient);
    // A round never holds more partitions than the pool has threads, and runDiffRound drains it
    // before the next is formed, which bounds a task's memory independent of batch size.
    try (CloseableExecutor partitionPool =
             CloseableExecutor.fixedPool(Math.min(ctx.partitionThreads, batch.size()))) {
      List<Pair<String, List<FileSlice>>> expectedByPartition =
          fetchExpectedSlices(batch, remoteView(ctx.metaClient, ctx.remoteViewConfig, ctx.props));
      int nextPartition = 0;
      while (nextPartition < expectedByPartition.size()) {
        List<Pair<String, List<FileSlice>>> round = nextRound(expectedByPartition, nextPartition, ctx);
        nextPartition += round.size();
        runDiffRound(round, partitionPool.service(), ctx, logFileReader, taskTally);
      }
    }
    // Flushed once, from the task thread, only on the returning path, so a retry contributes the
    // batch once. LongAccumulator#add is not thread-safe, which is why it is not called in the pool.
    filesSeen.add(taskTally.getFilesSeen());
    if (!taskTally.getDropCounts().isEmpty()) {
      dropCounts.add(new LinkedHashMap<>(taskTally.getDropCounts()));
    }
    return taskTally.getRecords().iterator();
  }

  /**
   * Diffs one round's partitions concurrently and folds the results into the task's tally on the
   * task thread; {@link DiffTally} is not synchronized, so pool threads must never touch it.
   */
  private static void runDiffRound(
      List<Pair<String, List<FileSlice>>> round, ExecutorService partitionPool, Context ctx,
      LogFileReader logFileReader, DiffTally taskTally) {
    List<Future<DiffTally>> futures = new ArrayList<>();
    for (Pair<String, List<FileSlice>> entry : round) {
      futures.add(partitionPool.submit(() -> diffOnePartition(entry, ctx, logFileReader)));
    }
    for (Future<DiffTally> future : futures) {
      try {
        taskTally.merge(future.get());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HoodieException("Interrupted while diffing partitions of " + ctx.basePath, e);
      } catch (ExecutionException e) {
        throw ValidationFailures.classifyFailure(e);
      }
    }
  }

  /**
   * One partition's FS-MDT diff. A failed listing propagates: a partition that could not be listed
   * is a check that did not run, an infrastructure failure in both modes.
   */
  @VisibleForTesting
  public static DiffTally diffOnePartition(
      Pair<String, List<FileSlice>> entry, Context ctx, LogFileReader logFileReader) {
    String partitionPath = entry.getLeft();
    // The shared view lists this partition from storage on first access, on this executor.
    List<FileSlice> fromFs = ctx.fsViewTemplate.getLatestFileSlices(partitionPath)
        .sorted(new FileSliceComparator()).collect(Collectors.toList());
    List<FileSlice> fromMdt = entry.getRight();
    DiffTally tally = FileSliceMismatchAnalyzer.analyze(fromMdt, fromFs,
        () -> ctx.fsViewTemplate.getAllFileSlices(partitionPath).collect(Collectors.toList()),
        partitionPath, logFileReader);
    // The storage side, not the union: the smaller denominator gives a consumer's blast-radius
    // ceiling the more cautious fraction. Counted for clean partitions too.
    tally.addFilesSeen(countFiles(fromFs));
    return tally;
  }

  /** Phase 2's output: every record with a verdict on it, and the run-wide gap counter. */
  private static final class PhaseTwo {

    /** Every phase-1 record, adjudicated, sorted into the order the documents are written in. */
    private final List<FileDiffRecord> records;

    /**
     * How many records no adjudication row named: a fact about {@link FileDiffAdjudicator}'s table,
     * not the data, so it is reported separately rather than folded into {@code notSettledCount}.
     */
    private final long unmatchedStateCount;

    PhaseTwo(List<FileDiffRecord> records, long unmatchedStateCount) {
      this.records = records;
      this.unmatchedStateCount = unmatchedStateCount;
    }
  }

  /**
   * Phase 2: decides what every phase-1 record means, against its writing instant's state before
   * and after the run.
   *
   * <p>Records {@link #takesTheAdjudicationShuffle} selects are range-repartitioned by writing
   * instant, so each output partition searches the archived timeline once for a contiguous range,
   * and adjudicated by {@link FileDiffAdjudicator}. The rest are settled by
   * {@link #settleWithoutTheTimeline}. Every record gets a verdict, since {@code diff/} promises the
   * full population.
   */
  private static PhaseTwo runPhaseTwo(JavaSparkContext jsc, PhaseOne phaseOne,
                                      InstantStateIndex prevIndex, InstantStateIndex aftIndex,
                                      ServiceActivityDelta dlt, PendingServiceState pendingServices,
                                      Context ctx, HoodieTimeline tAft,
                                      String earliestActiveWriteCommit) {
    boolean failFast = ctx.validationMode != ValidationMode.EXHAUSTIVE;
    List<FileDiffRecord> all;
    long unmatchedStateCount = 0;
    try {
      // The exact per-instant counts the range boundaries are computed from. A partition recomputed
      // later may yield an instant the counts never saw; InstantRangePartitioner is total over the
      // key domain, so it still routes somewhere and is adjudicated.
      JavaPairRDD<String, FileDiffRecord> keyed = phaseOne.records
          .filter(record -> takesTheAdjudicationShuffle(record))
          .mapToPair(record -> new Tuple2<>(record.getWritingInstant(), record));
      Map<String, Long> counts = InstantRangeBalancer.countByInstant(keyed);

      // One broadcast serves both stages, rather than each encoding its own copy of maps whose size
      // is unbounded in file names.
      AdjudicationContext adjudicationContext = new AdjudicationContext(prevIndex, aftIndex, dlt,
          pendingServices, ExecutorMetaClientFactory.from(ctx.metaClient), ctx.basePath, tAft,
          failFast, earliestActiveWriteCommit);
      // Wrapped because parts of this graph only round-trip under Java serialization, not
      // spark.serializer; see JavaSerializedPayload.
      Broadcast<JavaSerializedPayload<AdjudicationContext>> adjudicationBroadcast =
          jsc.broadcast(new JavaSerializedPayload<>(adjudicationContext));
      try {
        // Both filters call the SAME predicate, so the populations are exact complements: no record
        // is decided twice or dropped.
        JavaRDD<FileDiffRecord> settled = phaseOne.records
            .filter(record -> !takesTheAdjudicationShuffle(record))
            .mapPartitions(records -> settleBatch(records, adjudicationBroadcast), true);

        if (counts.isEmpty()) {
          all = settled.collect();
        } else {
          LongAccumulator unmatchedStates = jsc.sc().longAccumulator("mdtValidatorV2UnmatchedStates");
          all = InstantRangeBalancer.repartition(jsc, keyed, counts)
              .mapPartitions(
                  records -> adjudicateBatch(records, adjudicationBroadcast, unmatchedStates), true)
              .union(settled)
              .collect();
          // Read only after the action has returned.
          unmatchedStateCount = unmatchedStates.value();
          LOG.info("Adjudicated the FS-MDT diff record(s) of table {} over {} distinct writing "
                  + "instant(s); {} of {} phase-1 record(s) came back",
              ctx.basePath, counts.size(), all.size(), phaseOne.diffCount);
        }
      } finally {
        try {
          adjudicationBroadcast.destroy(false);
        } catch (Exception e) {
          LOG.warn("Could not destroy the phase-2 broadcast of table {}", ctx.basePath, e);
        }
      }
    } catch (Exception e) {
      throw ValidationFailures.classifyFailure(e);
    }
    return new PhaseTwo(sorted(all), unmatchedStateCount);
  }

  /**
   * Returns a sorted copy of the records, so two runs over an unchanged table write byte-identical
   * documents regardless of shuffle completion order.
   */
  @VisibleForTesting
  public static List<FileDiffRecord> sorted(List<FileDiffRecord> records) {
    List<FileDiffRecord> ordered = new ArrayList<>(records);
    Collections.sort(ordered);
    return ordered;
  }

  /**
   * Sums the per-task drop counts by class.
   *
   * <p>Summing is safe because a task's entry reaches the driver at most once: failed attempts
   * report nothing, and phase 1's job is a single shuffle-free stage whose losing speculative
   * duplicates {@code DAGScheduler} drops. A cached block recomputed by a later action does
   * double-count, but that also trips {@link RecomputeProbe} and makes the run inconclusive.
   */
  @VisibleForTesting
  public static Map<String, Long> mergeDropCounts(List<Map<String, Long>> perTask) {
    Map<String, Long> merged = new TreeMap<>();
    for (Map<String, Long> counts : perTask) {
      for (Map.Entry<String, Long> drop : counts.entrySet()) {
        merged.merge(drop.getKey(), drop.getValue(), Long::sum);
      }
    }
    return merged;
  }

  /**
   * Whether this record needs a timeline lookup and so takes the adjudication shuffle.
   *
   * <p>True for an {@code FS_EXCLUSIVE} record or an {@code MDT_EXCLUSIVE} base file with a resolved
   * writing instant. An MDT-exclusive log file is unattributable on every table version (it is not
   * on storage, and below version 8 its name carries the base commit), and {@code BOTH_DISAGREE}
   * needs no timeline.
   */
  @VisibleForTesting
  public static boolean takesTheAdjudicationShuffle(FileDiffRecord record) {
    if (record.getWritingInstant() == null) {
      return false;
    }
    return record.getDiffPattern() == DiffPattern.FS_EXCLUSIVE
        || (record.getDiffPattern() == DiffPattern.MDT_EXCLUSIVE
            && record.getFileType() == MismatchRecord.FileType.BASE);
  }

  /**
   * Settles a record that skips the shuffle, from driver-established inputs only. Runs on the
   * executors and is a pure function of its arguments.
   *
   * <ul>
   *   <li>{@code BOTH_DISAGREE} → {@code MDT_CORRUPTION}, unconditionally.
   *   <li>an {@code MDT_EXCLUSIVE} <b>log file</b> → per the pending-service reconciler:
   *       {@code SANCTIONED}, {@code NOT_SETTLED} when a relevant plan was unreadable, else
   *       {@code MDT_CORRUPTION}. {@code UNRESOLVED} keeps the fatal verdict from being reached by
   *       exclusion alone.
   *   <li>a resolved instant whose (diff pattern, file type) pair has no shuffle arm (in practice an
   *       {@code MDT_EXCLUSIVE} record with no file type) → {@code NOT_SETTLED}.
   *   <li>no resolved writing instant → {@code NOT_SETTLED}, already counted in {@code dropCounts}.
   * </ul>
   */
  @VisibleForTesting
  public static FileDiffRecord settleWithoutTheTimeline(FileDiffRecord record,
                                          ServiceActivityDelta dlt, PendingServiceState pendingServices) {
    if (record.getDiffPattern() == DiffPattern.BOTH_DISAGREE) {
      return settled(record, Branch.SWT_BOTH_DISAGREE, Verdict.MDT_CORRUPTION,
          "both sides hold the file and disagree about it");
    }
    if (record.getDiffPattern() == DiffPattern.MDT_EXCLUSIVE
        && record.getFileType() == MismatchRecord.FileType.LOG) {
      switch (PendingServiceReconciler.reconcile(record.getPartition(), record.getFileName(),
          record.getWritingInstant(), dlt, pendingServices)) {
        case SANCTIONED:
          return settled(record, Branch.SWT_MDT_LOG_SANCTIONED, Verdict.SANCTIONED, "a clean or rollback explains this file");
        case UNRESOLVED:
          return settled(record, Branch.SWT_MDT_LOG_UNRESOLVED, Verdict.NOT_SETTLED,
              "a clean or rollback plan that could have explained this file could not be read");
        default:
          return settled(record, Branch.SWT_MDT_LOG_CORRUPTION, Verdict.MDT_CORRUPTION,
              "the metadata table names a log file storage does not hold, no clean or rollback "
                  + "explains it, and no writing instant can be attributed to it on any table "
                  + "version");
      }
    }
    if (record.getWritingInstant() != null) {
      // A fact about the record's shape, not the file. Kept separate from the fall-through below,
      // whose message (no writing instant) would be false here and misdirect an operator.
      return settled(record, Branch.SWT_SHAPE_TAKES_NO_ROW, Verdict.NOT_SETTLED,
          "this file's writing instant (" + record.getWritingInstant() + ") resolved, but its diff "
              + "pattern (" + record.getDiffPattern() + ") and file type (" + record.getFileType()
              + ") match no adjudication row, so there is no question to ask the timeline about it. "
              + "Nothing is wrong with the file on this evidence");
    }
    return settled(record, Branch.SWT_NO_WRITING_INSTANT, Verdict.NOT_SETTLED,
        "no writing instant could be attributed to this file, so it was excluded from adjudication "
            + "rather than adjudicated; see this partition's dropCounts");
  }

  /**
   * Attaches a verdict and hits the branch beacon, since the verdict alone does not identify which
   * group settled the record. See {@link BranchBeacon}.
   */
  private static FileDiffRecord settled(FileDiffRecord record, Branch branch, Verdict verdict,
                                       String detail) {
    BranchBeacon.hit(branch);
    return record.withVerdict(verdict, detail);
  }

  /**
   * The run's one validation failure, or empty when no record reached a fatal verdict.
   *
   * <p>{@link Verdict#SANCTIONED} and {@link Verdict#NOT_SETTLED} are not failures: "could not tell"
   * is reported, never acted on. The message names a bounded set of partitions, and fail-fast (which
   * writes no document) also logs a bounded sample of the failing records.
   */
  @VisibleForTesting
  public static Option<HoodieValidationException> diffFailure(Context ctx,
                                                              List<FileDiffRecord> records) {
    Map<String, Long> byVerdict = new TreeMap<>();
    Set<String> partitions = new TreeSet<>();
    List<FileDiffRecord> failing = new ArrayList<>();
    for (FileDiffRecord record : records) {
      Verdict verdict = record.getVerdict();
      if (verdict != null && verdict.isValidationFailure()) {
        byVerdict.merge(verdict.name(), 1L, Long::sum);
        partitions.add(String.valueOf(record.getPartition()));
        if (failing.size() < MAX_FAILING_RECORDS_LOGGED) {
          failing.add(record);
        }
      }
    }
    if (byVerdict.isEmpty()) {
      return Option.empty();
    }
    long fatal = byVerdict.values().stream().mapToLong(Long::longValue).sum();
    if (ctx.validationMode != ValidationMode.EXHAUSTIVE) {
      logFailingSample(ctx, failing, fatal);
    }
    return Option.of(new HoodieValidationException(String.format(
        "Validation of %s failed for table: %s. %d of %d differing file(s) fail validation across "
            + "%d partition(s); see the findings written for this run. Counts by verdict: %s. "
            + "Partition(s): %s",
        LATEST_FILE_SLICES_V2_LABEL, ctx.basePath, fatal, records.size(), partitions.size(),
        byVerdict, namePartitions(partitions))));
  }

  /** The affected partitions, capped so one message cannot grow with the table. */
  private static String namePartitions(Set<String> partitions) {
    if (partitions.size() <= MAX_PARTITIONS_NAMED) {
      return String.join(", ", partitions);
    }
    List<String> named = new ArrayList<>(partitions).subList(0, MAX_PARTITIONS_NAMED);
    return String.join(", ", named) + " and " + (partitions.size() - MAX_PARTITIONS_NAMED)
        + " more";
  }

  /** The fail-fast sample: what an operator gets in place of the document fail-fast never writes. */
  private static void logFailingSample(Context ctx, List<FileDiffRecord> sample, long fatal) {
    for (FileDiffRecord record : sample) {
      LOG.warn("FS-MDT difference failing validation in table {}: partition={} file={} pattern={} "
              + "verdict={} instant={} - {}", ctx.basePath, record.getPartition(),
          record.getFileName(), record.getDiffPattern(), record.getVerdict(),
          record.getWritingInstant(), record.getVerdictDetail());
    }
    if (fatal > sample.size()) {
      LOG.warn("{} further file(s) of table {} fail validation and are not named here",
          fatal - sample.size(), ctx.basePath);
    }
    LOG.warn("Metadata table validation (fail-fast) found {} FS-MDT difference(s) failing validation "
        + "in table {}. Fail-fast writes no report; re-run with --validation-mode exhaustive for the "
        + "full findings", fatal, ctx.basePath);
  }

  /**
   * Everything phase 2 ships to an executor, as one broadcast {@link Serializable} object.
   *
   * <p>All contents were read on the driver, so executors read no plan or commit metadata; they read
   * only the archive, for instants absent from the active timeline. It carries an
   * {@link ExecutorMetaClientFactory} rather than a meta client, whose active timeline would drag
   * {@code T_prev} along. The accumulator is not here: a broadcast is shared by every task on an
   * executor, so it would not report per task.
   */
  @VisibleForTesting
  public static final class AdjudicationContext implements Serializable {
    private static final long serialVersionUID = 1L;

    private final InstantStateIndex prevIndex;
    private final InstantStateIndex aftIndex;
    private final ServiceActivityDelta dlt;
    private final PendingServiceState pendingServices;
    private final ExecutorMetaClientFactory metaClients;
    private final String basePath;
    /**
     * {@code T_aft} as the driver read it, shipped whole and filtered on the executor, because
     * {@code BaseHoodieTimeline.instantReader} is transient and a filtered timeline deserializes with
     * a null reader. It is the timeline {@code aftIndex} was built from.
     */
    private final HoodieTimeline tAft;
    private final boolean failFast;
    /**
     * The earliest write commit on {@link #tAft}, computed on the driver. It MUST come from
     * {@code T_aft}, never {@code T_prev}: {@code completedActive} is narrowed from the same
     * {@link #tAft}, and a {@code T_prev} cutoff would make an instant archived between the two reads
     * look like one that never committed.
     */
    private final String earliestActiveWriteCommit;

    AdjudicationContext(InstantStateIndex prevIndex, InstantStateIndex aftIndex, ServiceActivityDelta dlt,
                        PendingServiceState pendingServices, ExecutorMetaClientFactory metaClients,
                        String basePath, HoodieTimeline tAft, boolean failFast,
                        String earliestActiveWriteCommit) {
      this.prevIndex = prevIndex;
      this.aftIndex = aftIndex;
      this.dlt = dlt;
      this.pendingServices = pendingServices;
      this.metaClients = metaClients;
      this.basePath = basePath;
      this.tAft = tAft;
      this.failFast = failFast;
      this.earliestActiveWriteCommit = earliestActiveWriteCommit;
    }

    /** Test-only construction that builds the same shape of context {@code runPhaseTwo} does. */
    @VisibleForTesting
    public static AdjudicationContext forTesting(InstantStateIndex prevIndex, InstantStateIndex aftIndex,
                                                 ServiceActivityDelta dlt, PendingServiceState pendingServices,
                                                 HoodieTableMetaClient metaClient, String basePath,
                                                 HoodieTimeline tAft, boolean failFast) {
      String earliestActiveWriteCommit = InstantBuckets.earliestCompletedWriteCommit(
          tAft.getCommitsTimeline().filterCompletedInstants());
      return new AdjudicationContext(prevIndex, aftIndex, dlt, pendingServices,
          ExecutorMetaClientFactory.from(metaClient), basePath, tAft, failFast,
          earliestActiveWriteCommit);
    }

    /** {@code T_aft} as shipped, exposed so a test can check it after a serialization round trip. */
    @VisibleForTesting
    public HoodieTimeline getTAft() {
      return tAft;
    }

    /** The factory as shipped, exposed so a test can use it after a serialization round trip. */
    @VisibleForTesting
    public ExecutorMetaClientFactory getMetaClients() {
      return metaClients;
    }
  }

  /**
   * Phase 2 over one shuffle partition: builds the adjudicator on the executor and decides every
   * record the partition owns.
   *
   * <p>{@link InstantMembershipResolver}, {@link FileDiffAdjudicator} and the archive lookup are built
   * here because none is {@link Serializable}. The archive is read ONCE per task, for every writing
   * instant before {@link AdjudicationContext#earliestActiveWriteCommit}, and both the resolver and
   * {@link ArchiveStateLookup} read that one {@link ArchivedInstantWorkingSet}.
   */
  private static Iterator<FileDiffRecord> adjudicateBatch(
      Iterator<FileDiffRecord> records, Broadcast<JavaSerializedPayload<AdjudicationContext>> ctxBroadcast,
      LongAccumulator unmatchedStates) {
    return adjudicateBatch(records, ctxBroadcast.value().get(), unmatchedStates);
  }

  @VisibleForTesting
  static Iterator<FileDiffRecord> adjudicateBatch(
      Iterator<FileDiffRecord> records, AdjudicationContext ctx,
      LongAccumulator unmatchedStates) {
    List<FileDiffRecord> batch = new ArrayList<>();
    records.forEachRemaining(batch::add);
    if (batch.isEmpty()) {
      return Collections.emptyIterator();
    }
    // Storage-only: built without the active timeline, and nothing reachable from here may call
    // getActiveTimeline() on it, which would build one on this executor.
    HoodieTableMetaClient client = ctx.metaClients.newStorageOnlyClient();
    // Completed-only, as InstantMembershipResolver requires (a pending instant has no readable
    // metadata). getCommitsTimeline() first: a savepoint shares its commit's instant time and the
    // resolver de-duplicates with putIfAbsent, so both would make the answer order-dependent.
    HoodieTimeline completedActive = ctx.tAft.getCommitsTimeline().filterCompletedInstants();

    // The one archived pass for the whole batch, never per file group. Fail-fast leaves it null,
    // and ArchiveStateLookup then answers unresolved for every question.
    ArchivedInstantWorkingSet<ArchivedCommitPaths> archived = null;
    if (!ctx.failFast) {
      Set<String> archiveTargets = new HashSet<>();
      for (FileDiffRecord record : batch) {
        String writingInstant = record.getWritingInstant();
        if (writingInstant != null
            && InstantBuckets.pokesArchive(writingInstant, ctx.earliestActiveWriteCommit)) {
          archiveTargets.add(writingInstant);
        }
      }
      if (!archiveTargets.isEmpty()) {
        // Load bearing: the v2 batch resolver's warmUp throws by design, so on a v2-layout table the
        // working set stays null and every archive-bucket record refuses.
        if (ArchiveTimelineInstantBatchResolvers.readsV1Archive(client)) {
          archived = ArchiveTimelineInstantBatchResolvers
              .forTable(client, new CommitPathProjector(client.getBasePath(), wantedGroups(batch)))
              .warmUp(archiveTargets);
        }
      }
    }
    InstantMembershipResolver membership =
        new InstantMembershipResolver(client, completedActive, ctx.earliestActiveWriteCommit, archived);
    FileDiffAdjudicator adjudicator = new FileDiffAdjudicator(ctx.prevIndex, ctx.aftIndex, ctx.dlt,
        ctx.pendingServices, membership, new ArchiveStateLookup(archived), ctx.failFast);

    List<FileDiffRecord> decided = new ArrayList<>(batch.size());
    for (FileDiffRecord record : batch) {
      decided.add(adjudicator.adjudicate(record));
    }
    unmatchedStates.add(adjudicator.getUnmatchedStateCount());
    return decided.iterator();
  }

  /** Every file group the batch names, which is the scope the archived projection is bounded to. */
  private static Set<HoodieFileGroupId> wantedGroups(List<FileDiffRecord> batch) {
    Set<HoodieFileGroupId> groups = new HashSet<>();
    for (FileDiffRecord record : batch) {
      if (record.getPartition() != null && record.getFileGroupId() != null) {
        groups.add(new HoodieFileGroupId(record.getPartition(), record.getFileGroupId()));
      }
    }
    return groups;
  }

  /**
   * Settles one Spark partition of the complement via {@link #settleWithoutTheTimeline}, resolving
   * the broadcast once per task.
   */
  private static Iterator<FileDiffRecord> settleBatch(
      Iterator<FileDiffRecord> records, Broadcast<JavaSerializedPayload<AdjudicationContext>> ctxBroadcast) {
    AdjudicationContext ctx = ctxBroadcast.value().get();
    List<FileDiffRecord> settled = new ArrayList<>();
    records.forEachRemaining(
        record -> settled.add(settleWithoutTheTimeline(record, ctx.dlt, ctx.pendingServices)));
    return settled.iterator();
  }

  /**
   * What the archive says about an instant the active timeline does not hold: the only way to tell a
   * completed-and-archived instant from one that never existed.
   *
   * <p>Reads no archive of its own; every answer comes from the {@link ArchivedInstantWorkingSet}
   * {@code adjudicateBatch} materializes once per task, keyed by file group. A {@code null} working
   * set (fail-fast, no archive bucket, or a v2-layout table) answers unresolved, never absence.
   *
   * <ul>
   *   <li>{@code NOT_FOUND} is a proven absence ({@code NULL}); an empty or missing v1 archive
   *       directory already yields it, while an unreadable one yields {@code INDETERMINATE}.
   *   <li>{@code FOUND} with nothing recorded for this file group stays unresolved, not absent.
   *       For a storage-only file, {@link #isCompletedWithWriteStats} lets the adjudicator ask
   *       membership instead, which reads that absence by the active path's rules.
   *   <li>An instant outside the working set's targets is unresolved. Real orphans inside the active
   *       window are therefore refused rather than settled: widening the targets would authorise
   *       deletions from archive content never searched, the worse failure.
   * </ul>
   *
   * <p>Never throws: inputs are null-guarded and the non-target check precedes the working-set call
   * that would throw. The one working-set read per batch cannot go stale, because
   * {@link ArchivalProbe} holds the archive constant for the run. {@link Serializable} only because
   * the interface requires it; nothing serializes an instance.
   */
  @VisibleForTesting
  public static final class ArchiveStateLookup implements FileDiffAdjudicator.ArchivedStateLookup {

    private static final long serialVersionUID = 1L;

    /** The working set one archived pass materialized for this task, or {@code null} when none ran. */
    private final ArchivedInstantWorkingSet<ArchivedCommitPaths> archived;

    public ArchiveStateLookup(ArchivedInstantWorkingSet<ArchivedCommitPaths> archived) {
      this.archived = archived;
    }

    @Override
    public InstantStateIndex.Resolution resolve(String partitionPath, String fileGroupId,
                                                String instantTime) {
      if (partitionPath == null || fileGroupId == null || instantTime == null) {
        BranchBeacon.hit(Branch.ASL_INPUT_INCOMPLETE);
        return InstantStateIndex.Resolution.unresolved();
      }
      if (archived == null) {
        BranchBeacon.hit(Branch.ASL_ARCHIVE_NOT_ALLOWED);
        return InstantStateIndex.Resolution.unresolved();
      }
      if (!archived.targets().contains(instantTime)) {
        // Must precede stateOf, which THROWS for a non-target; this class promises never to throw.
        BranchBeacon.hit(Branch.ASL_ARCHIVE_INSTANT_NOT_A_TARGET);
        return InstantStateIndex.Resolution.unresolved();
      }
      switch (archived.stateOf(instantTime)) {
        case FOUND:
          Set<String> named = archived.contentOf(instantTime).orElse(ArchivedCommitPaths.empty())
              .namesOf(new HoodieFileGroupId(partitionPath, fileGroupId));
          if (named.isEmpty()) {
            // The commit completed but recorded nothing for this group: a refusal, not an absence.
            BranchBeacon.hit(Branch.ASL_ARCHIVE_RECORDED_NOTHING);
            return InstantStateIndex.Resolution.unresolved();
          }
          BranchBeacon.hit(Branch.ASL_FOUND_COMPLETED);
          return InstantStateIndex.Resolution.of(InstantState.COMPLETED);
        case NOT_FOUND:
          BranchBeacon.hit(Branch.ASL_ABSENCE_TRUSTED_NULL);
          return InstantStateIndex.Resolution.of(InstantState.NULL);
        default:
          // INDETERMINATE: the pass could not cover what it needed to.
          BranchBeacon.hit(Branch.ASL_UNRESOLVED);
          LOG.debug("The archive could not place instant {} of file group {} in partition {}; "
              + "reporting it unresolved", instantTime, fileGroupId, partitionPath);
          return InstantStateIndex.Resolution.unresolved();
      }
    }

    @Override
    public boolean isCompletedWithWriteStats(String instantTime) {
      if (instantTime == null || archived == null || !archived.targets().contains(instantTime)
          || archived.stateOf(instantTime) != InstantSearchState.FOUND) {
        return false;
      }
      return archived.contentOf(instantTime).map(ArchivedCommitPaths::isCompletedWithWriteStats)
          .orElse(false);
    }
  }

  /** Everything a task needs to diff its batch of partitions, as one serializable closure object. */
  public static final class Context implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ValidationMode validationMode;
    private final HoodieTableMetaClient metaClient;
    private final FileSystemViewStorageConfig remoteViewConfig;
    private final HoodieTableFileSystemView fsViewTemplate;
    private final String basePath;
    private final int partitionThreads;
    private final long maxFilesPerRound;
    private final LongAccumulator partitionValidationMs;
    private final Properties props;

    public Context(ValidationMode validationMode,
                   HoodieTableMetaClient metaClient, FileSystemViewStorageConfig remoteViewConfig,
                   HoodieTableFileSystemView fsViewTemplate, String basePath, int partitionThreads,
                   long maxFilesPerRound, LongAccumulator partitionValidationMs, Properties props) {
      this.validationMode = validationMode;
      this.metaClient = metaClient;
      this.remoteViewConfig = remoteViewConfig;
      this.fsViewTemplate = fsViewTemplate;
      this.basePath = basePath;
      this.partitionThreads = partitionThreads;
      this.maxFilesPerRound = maxFilesPerRound;
      this.partitionValidationMs = partitionValidationMs;
      this.props = props;
    }
  }

  /** Expected slices for every partition in the batch, as point lookups against the warmed view. */
  private static List<Pair<String, List<FileSlice>>> fetchExpectedSlices(
      List<String> batch, SyncableFileSystemView metadataView) {
    List<Pair<String, List<FileSlice>>> expectedByPartition = new ArrayList<>();
    for (String partitionPath : batch) {
      expectedByPartition.add(Pair.of(partitionPath, metadataView.getLatestFileSlices(partitionPath)
          .sorted(new FileSliceComparator()).collect(Collectors.toList())));
    }
    return expectedByPartition;
  }

  /**
   * Takes partitions from {@code from} until the thread pool is full or the files they would hold
   * in flight reach the cap, which is what bounds executor memory on wide partitions. A single
   * partition larger than the cap still runs, alone.
   */
  private static List<Pair<String, List<FileSlice>>> nextRound(
      List<Pair<String, List<FileSlice>>> expectedByPartition, int from, Context ctx) {
    List<Pair<String, List<FileSlice>>> round = new ArrayList<>();
    long filesInRound = 0;
    int next = from;
    while (next < expectedByPartition.size() && round.size() < ctx.partitionThreads) {
      Pair<String, List<FileSlice>> candidate = expectedByPartition.get(next);
      long candidateFiles = countFiles(candidate.getRight());
      if (!round.isEmpty() && filesInRound + candidateFiles > ctx.maxFilesPerRound) {
        break;
      }
      round.add(candidate);
      filesInRound += candidateFiles;
      next++;
    }
    LOG.debug("Validating {} partitions of table {} concurrently ({} files in flight)",
        round.size(), ctx.basePath, filesInRound);
    return round;
  }

  /** Reads a log file's writing instant through the given (executor-side) meta client. */
  private static LogFileReader logFileReaderFor(HoodieTableMetaClient client) {
    StoragePath tableRoot = client.getBasePath();
    return (partitionPath, fileName) -> LogFileInstantReader.read(client,
        StringUtils.isNullOrEmpty(partitionPath)
            ? new StoragePath(tableRoot, fileName)
            : new StoragePath(new StoragePath(tableRoot, partitionPath), fileName));
  }

  /** Files a set of slices covers, used to bound how many are validated concurrently. */
  private static long countFiles(List<FileSlice> fileSlices) {
    return fileSlices.stream()
        .mapToLong(slice -> (slice.getBaseFile().isPresent() ? 1 : 0) + slice.getLogFiles().count())
        .sum();
  }

  /**
   * The task's read-only handle on the driver-hosted metadata view, from a
   * {@link FileSystemViewManager}.
   *
   * <p>Deliberately never closed: {@link RemoteHoodieTableFileSystemView#close()} clears the view on
   * the timeline server for every client, so the first task to finish would destroy the view the
   * others are reading. The driver releases it via {@code stopForBasePath}. Forced to
   * {@code REMOTE_ONLY} so a failure is loud instead of silently opening the metadata table on the
   * executor.
   */
  private static SyncableFileSystemView remoteView(
      HoodieTableMetaClient metaClient, FileSystemViewStorageConfig remoteViewConfig, Properties props) {
    FileSystemViewStorageConfig remoteOnlyConfig = FileSystemViewStorageConfig.newBuilder()
        .fromProperties(remoteViewConfig.getProps())
        .withStorageType(FileSystemViewStorageType.REMOTE_ONLY)
        .build();
    return FileSystemViewManager.createViewManager(
        new HoodieLocalEngineContext(metaClient.getStorageConf()),
        HoodieMetadataConfig.newBuilder().enable(true).build(),
        remoteOnlyConfig,
        // Seeded from --props/--hoodie-conf so the operator's disk-map settings apply here too.
        HoodieCommonConfig.newBuilder().fromProperties(props).build()).getFileSystemView(metaClient);
  }

  /** The newest instant the aft index holds (its instants are ascending), or null when empty. */
  private static String newestInstantOf(InstantStateIndex index) {
    List<String> ordered = index.getOrderedInstants();
    return ordered.isEmpty() ? null : ordered.get(ordered.size() - 1);
  }
}
