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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.utilities.util.archive.ArchiveTimelineInstantBatchResolvers;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import scala.Tuple2;

/**
 * The duplicate-base-file dimension: lists each partition raw, reports every file slice holding more
 * than one base file, and in exhaustive mode adjudicates each suspect against the timeline.
 *
 * <p>A duplicate set is a fact about a raw listing alone (the file system view collapses a set to
 * one member, which is why it survives ordinary validation), so phase 1 reads no metadata view and
 * no timeline: cardinality alone makes a suspect. Once phase 1 is forced to completion and
 * {@code T_aftD} is read, the classify stage sanctions any suspect whose writing instant could have
 * been in flight between the two timeline reads ({@link DuplicateInstantWindow}); the rest are
 * findings.
 *
 * <p>Fail-fast raises the validation failure from the classify stage on the executor. Exhaustive
 * runs phase 2, which keys survivors by writing instant, range-repartitions them with
 * {@link InstantRangeBalancer} so each task scans one contiguous archived range, and adjudicates
 * them with a {@link DanglingAdjudicator}; its single failure is raised by the caller after the
 * reports are written. Non-validation failures (listing errors, broken prerequisites) propagate
 * immediately in both modes.
 *
 * <p>Rounds are bounded by {@code --partition-threads} only, not {@code --max-files-per-round}: that
 * cap needs expected-slice file counts, which this dimension does not fetch.
 */
public final class DuplicateBaseFileDimension {

  private static final Logger LOG =
      LoggerFactory.getLogger(DuplicateBaseFileDimension.class);

  private static final String DUPLICATE_BASE_FILES_LABEL = "duplicate base files (v2)";

  /**
   * How many suspects one fail-fast task logs before it stops classifying and throws. Fail-fast
   * writes no report, so these lines are the only record of file names; the bound keeps a badly
   * damaged table from burying the log.
   */
  private static final int MAX_SUSPECTS_LOGGED_PER_TASK = 50;

  private DuplicateBaseFileDimension() {
  }

  /** Everything a phase-1 task needs, bundled into one serializable closure object. */
  public static final class Context implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ValidationMode validationMode;
    private final HoodieTableMetaClient metaClient;
    /** Earliest write commit on {@code T_prevD}, the archival cutoff phase 2 buckets against. */
    private final String earliestActiveWriteCommit;
    private final String basePath;
    private final int partitionThreads;
    private final boolean ignoreFailed;
    /** Whether this dimension runs at all; see {@code --validate-duplicate-base-files}. */
    private final boolean enabled;
    private final LongAccumulator listingMs;
    /**
     * Every file phase 1 listed, across all tasks: the denominator a mitigation job sizes a delete
     * list against.
     */
    private final LongAccumulator filesSeen;

    public Context(ValidationMode validationMode,
                   HoodieTableMetaClient metaClient, String earliestActiveWriteCommit,
                   String basePath, int partitionThreads, boolean ignoreFailed, boolean enabled,
                   LongAccumulator listingMs, LongAccumulator filesSeen) {
      this.validationMode = validationMode;
      this.metaClient = metaClient;
      this.earliestActiveWriteCommit = earliestActiveWriteCommit;
      this.basePath = basePath;
      this.partitionThreads = partitionThreads;
      this.ignoreFailed = ignoreFailed;
      this.enabled = enabled;
      this.listingMs = listingMs;
      this.filesSeen = filesSeen;
    }

    /** Whether a surviving suspect must abort the run where it is found, on the executor. */
    private boolean failsOnTheExecutor() {
      return validationMode != ValidationMode.EXHAUSTIVE && !ignoreFailed;
    }
  }

  /** What the dimension found: the surviving suspects, the run's verdict, and what phase 2 proved
   * about them. */
  public static final class Result {

    /** Surviving suspects, sorted, for the exhaustive report. Empty in fail-fast, which collects none. */
    private final List<MismatchRecord> suspects;
    /**
     * The single validation failure, or empty. Present only when not already thrown: fail-fast with
     * {@code --ignore-failed}, or exhaustive, where the caller raises it after reports are written.
     */
    private final Option<HoodieValidationException> failure;
    /** Suspects proven unreferenced. Empty in fail-fast (no phase 2), so empty does not mean clean. */
    private final List<MismatchRecord> confirmedDangling;
    private final long referencedCount;
    private final long notSettledCount;
    /** Surviving suspects, counted from the RDD since fail-fast collects no records. */
    private final long suspectCount;
    /** Suspect groups the inflight window sanctioned; this count is their only trace. */
    private final long sanctionedCount;
    /** Every file phase 1's listings considered; see {@link Context#filesSeen}. */
    private final long filesSeen;
    /**
     * Why no verdict was reached ({@link ArchivalProbe} saw archival mid-run, or
     * {@link RecomputeProbe} saw phase 1 recomputed), or empty. Not a finding; the caller decides
     * what it costs.
     */
    private final Option<Inconclusive> inconclusive;

    Result(List<MismatchRecord> suspects, Option<HoodieValidationException> failure,
           List<MismatchRecord> confirmedDangling, long referencedCount, long notSettledCount,
           long suspectCount, long sanctionedCount, long filesSeen) {
      this(suspects, failure, confirmedDangling, referencedCount, notSettledCount, suspectCount,
          sanctionedCount, filesSeen, Option.empty());
    }

    Result(List<MismatchRecord> suspects, Option<HoodieValidationException> failure,
           List<MismatchRecord> confirmedDangling, long referencedCount, long notSettledCount,
           long suspectCount, long sanctionedCount, long filesSeen,
           Option<Inconclusive> inconclusive) {
      this.suspects = suspects;
      this.failure = failure;
      this.confirmedDangling = confirmedDangling;
      this.referencedCount = referencedCount;
      this.notSettledCount = notSettledCount;
      this.suspectCount = suspectCount;
      this.sanctionedCount = sanctionedCount;
      this.filesSeen = filesSeen;
      this.inconclusive = inconclusive;
    }

    /**
     * A dimension that was switched off or never ran: empty lists and zero counts. The caller decides
     * whether to report the run as skipped.
     */
    public static Result none() {
      return new Result(Collections.emptyList(), Option.empty(), Collections.emptyList(),
          0L, 0L, 0L, 0L, 0L);
    }

    /** The surviving suspects, sorted; empty in fail-fast, which collects none. */
    public List<MismatchRecord> getSuspects() {
      return suspects;
    }

    /** The dimension's validation failure, or empty when every suspect was explained. */
    public Option<HoodieValidationException> getFailure() {
      return failure;
    }

    /** Suspects across every partition, counted from the RDD and never from the records kept. */
    public long getSuspectCount() {
      return suspectCount;
    }

    public List<MismatchRecord> getConfirmedDangling() {
      return confirmedDangling;
    }

    /** Suspects a completed commit still names. Zero in fail-fast, which asks nothing. */
    public long getReferencedCount() {
      return referencedCount;
    }

    /** Suspects the timeline could not answer for. Zero in fail-fast, which asks nothing. */
    public long getNotSettledCount() {
      return notSettledCount;
    }

    /** Suspect groups the inflight window sanctioned. */
    public long getSanctionedCount() {
      return sanctionedCount;
    }

    /** Every file phase 1's listings considered, findings and agreements alike. */
    public long getFilesSeen() {
      return filesSeen;
    }

    /** Empty on a run that reached a verdict; otherwise the cause and the reading behind it. */
    public Option<Inconclusive> getInconclusive() {
      return inconclusive;
    }

    public boolean isInconclusive() {
      return inconclusive.isPresent();
    }
  }

  /**
   * Runs the dimension over {@code partitions} and returns what it found.
   *
   * <p>Phase 1 lists and groups by cardinality; once it is forced to completion {@code readTAftD} is
   * called and suspects are classified against the window {@code [T_prevD, T_aftD]}. Exhaustive
   * then runs phase 2; fail-fast stops after classification. The whole run, including a fail-fast
   * throw, completes before the FS-MDT diff's DAG executes.
   *
   * <p>If archival ran during the window the result is inconclusive, because phase 2's archived
   * reads cannot be trusted ({@link ArchivalProbe}); the caller decides what that costs.
   *
   * @param jsc           the Spark context the fan-out runs on
   * @param engineContext the engine context phase 2's range repartition runs on
   * @param partitions    every partition either side of the table reports
   * @param taskCount     how many Spark tasks to split them across
   * @param ctx           what each task needs; see {@link Context}
   * @param tPrevD        the active timeline (every action, every state) read before phase 1 starts
   * @param archivalAtPrevD the archive signature read with {@code tPrevD}, to detect archival during
   *                      the window
   * @param readTAftD     reads the active timeline again; called only after phase 1 completes
   */
  public static Result run(JavaSparkContext jsc, HoodieSparkEngineContext engineContext,
                           List<String> partitions, int taskCount, Context ctx,
                           HoodieTimeline tPrevD, String archivalAtPrevD,
                           Supplier<HoodieTimeline> readTAftD) {
    RecomputeProbe probe =
        RecomputeProbe.start(jsc, "mdtValidatorV2DuplicateListingTasks", taskCount);
    // Resolved outside the lambda: RecomputeProbe is not Serializable, so calling probe.counter()
    // inside the closure would fail task submission.
    LongAccumulator listingTasks = probe.counter();
    JavaRDD<MismatchRecord> rawSuspects = jsc.parallelize(partitions, taskCount)
        .mapPartitions(partitionIterator -> detectBatch(partitionIterator, ctx, listingTasks));
    // Cached: classification and the exhaustive collect both reread it.
    rawSuspects.persist(StorageLevel.MEMORY_AND_DISK_SER());
    JavaRDD<MismatchRecord> classified = null;
    RecomputeProbe.Baseline baseline = null;
    try {
      // Barrier: phase 1 must be complete before T_aftD is read, or a suspect could postdate the
      // window meant to explain it.
      long rawCount = rawSuspects.count();
      // Sealed after the action: accumulators hold only completed actions' updates.
      baseline = probe.seal();
      // Checks that the barrier really materialised phase 1. A non-Hoodie exception, so it fails the
      // run in both modes as a prerequisite failure rather than a finding.
      if (!baseline.materialised()) {
        throw new IllegalStateException(
            "T_aftD must not be read until phase 1 has been forced to completion: phase 1 ran "
                + baseline.observedTasks() + " of " + baseline.expectedTasks() + " tasks. Every "
                + "suspect would otherwise be free to describe storage as it was after the window "
                + "that is supposed to explain it");
      }
      HoodieTimeline tAftD = readTAftD.get();
      String archivalAtAftD = ArchivalProbe.signature(ctx.metaClient);
      if (ArchivalProbe.archivalRan(archivalAtPrevD, archivalAtAftD)) {
        // No beacon, matching Branch.FSM_ARCHIVAL_RAN_INCONCLUSIVE for the FS-MDT diff.
        String reason = ArchivalProbe.describe(archivalAtPrevD, archivalAtAftD);
        LOG.warn("Archival ran while checking table {} for duplicate base files ({} suspect(s) from "
                + "phase 1): {}. Classification does not run and no verdict is reached",
            ctx.basePath, rawCount, reason);
        return new Result(Collections.emptyList(), Option.empty(), Collections.emptyList(),
            0L, 0L, 0L, 0L, 0L,
            Option.of(new Inconclusive(InconclusiveCause.ARCHIVAL_RAN, reason)));
      }

      DuplicateInstantWindow window = DuplicateInstantWindow.between(tPrevD, tAftD);
      LongAccumulator sanctioned = jsc.sc().longAccumulator("mdtValidatorV2DuplicateSanctioned");
      // Captured, not broadcast: the window's two instant sets are small.
      classified = rawSuspects.mapPartitions(
          partitionIterator -> classifyBatch(partitionIterator, ctx, window, sanctioned), true);
      classified.persist(StorageLevel.MEMORY_AND_DISK_SER());
      // Second barrier: materialise the cache before phase 2 reads it. In fail-fast this count is
      // the action the executor throws from.
      long classifiedCount = classified.count();

      if (ctx.validationMode != ValidationMode.EXHAUSTIVE) {
        // Fail-fast has either already thrown on the executor or runs with --ignore-failed.
        if (baseline.recomputed()) {
          return recomputedResult(ctx, baseline);
        }
        return failFastResult(ctx, classifiedCount, sanctioned.value());
      }
      Result phaseTwo = runPhaseTwo(engineContext, classified, ctx, sanctioned.value(), tPrevD);
      // Checked after phase 2's last action, since any action can recompute a lost block.
      return baseline.recomputed() ? recomputedResult(ctx, baseline) : phaseTwo;
    } catch (Exception e) {
      // Infrastructure must be classified before a recompute may discard the failure: a lost
      // executor both causes recomputes and must surface as an infrastructure failure. Do not replace this with
      // baseline.recomputed() alone.
      if (baseline != null && ValidationFailures.discardForRecompute(e, baseline.recomputed())) {
        // A finding raised while phase 1 recomputed may rest on storage listed after T_aftD, so it
        // is unattributable and discarded, logged with the original exception.
        LOG.warn("Discarding a duplicate-base-file failure raised while phase 1 was being "
            + "recomputed for table {}; the finding cannot be attributed to the window this run "
            + "pins to", ctx.basePath, e);
        return recomputedResult(ctx, baseline);
      }
      // Unwraps Spark's task failure so a validation failure surfaces as such; in fail-fast this is
      // how a duplicate base file pre-empts the FS-MDT diff.
      throw ValidationFailures.classifyFailure(e);
    } finally {
      try {
        rawSuspects.unpersist(false);
      } catch (Exception e) {
        LOG.warn("Could not unpersist the phase-1 RDD of table {}", ctx.basePath, e);
      }
      if (classified != null) {
        try {
          classified.unpersist(false);
        } catch (Exception e) {
          LOG.warn("Could not unpersist the classified RDD of table {}", ctx.basePath, e);
        }
      }
    }
  }

  /**
   * The fail-fast result after classification. Reached with survivors only under
   * {@code --ignore-failed} (otherwise the executor has thrown); the failure is built from the count
   * because fail-fast collects no records onto the driver.
   */
  private static Result failFastResult(Context ctx, long suspectCount, long sanctionedCount) {
    LOG.info("Checked the partitions of table {} for duplicate base files; {} suspect(s) survived "
            + "classification, {} sanctioned as possibly in-flight. Fail-fast writes no report and "
            + "runs no adjudication", ctx.basePath, suspectCount, sanctionedCount);
    return new Result(Collections.emptyList(), collectedFailure(ctx, suspectCount),
        Collections.emptyList(), 0L, 0L, suspectCount, sanctionedCount, ctx.filesSeen.value());
  }

  /**
   * The inconclusive result for a recomputed phase 1. Everything computed is discarded, since a
   * partial answer would be read downstream as a complete one.
   */
  private static Result recomputedResult(Context ctx, RecomputeProbe.Baseline baseline) {
    BranchBeacon.hit(Branch.DUP_RECOMPUTED_INCONCLUSIVE);
    String reason = baseline.describe();
    LOG.warn("Phase 1 was recomputed while checking table {} for duplicate base files: {}. "
        + "No verdict is reached", ctx.basePath, reason);
    return new Result(Collections.emptyList(), Option.empty(), Collections.emptyList(),
        0L, 0L, 0L, 0L, 0L,
        Option.of(new Inconclusive(InconclusiveCause.PHASE_ONE_RECOMPUTED, reason)));
  }

  /**
   * Phase 2: range-repartitions surviving suspects by writing instant so each task scans one
   * contiguous archived range, adjudicates them on executors, and collects the result.
   */
  private static Result runPhaseTwo(HoodieSparkEngineContext engineContext,
                                    JavaRDD<MismatchRecord> classified, Context ctx,
                                    long sanctionedCount, HoodieTimeline tPrevD) {
    // Suspects naming no instant cannot be adjudicated (the balancer needs non-null keys) but still
    // reach the report, which is built from all of classified. Sanctioned suspects were already
    // dropped in classifyBatch: adjudicating a possibly in-flight instant would condemn it as
    // DANGLING_NO_COMPLETED_COMMIT.
    //
    // The partitioner is total over the key domain, so an instant the counts missed (from a
    // recomputed phase-1 partition) still routes. Such an instant is not on T_prevD and would condemn
    // the whole duplicate set, live file included; the recompute check in run discards that result.
    JavaPairRDD<String, MismatchRecord> keyed = classified
        .filter(record -> record.getBaseInstantTime() != null)
        .mapToPair(record -> new Tuple2<>(record.getBaseInstantTime(), record));
    Map<String, Long> counts = InstantRangeBalancer.countByInstant(keyed);
    if (counts.isEmpty()) {
      // No suspect named an instant: every suspect is reported without a verdict.
      return exhaustiveResult(ctx, classified.collect(), Collections.emptyList(), 0L, 0L,
          sanctionedCount);
    }

    LongAccumulator referenced =
        engineContext.getJavaSparkContext().sc().longAccumulator("mdtValidatorV2DuplicateReferenced");
    LongAccumulator notSettled =
        engineContext.getJavaSparkContext().sc().longAccumulator("mdtValidatorV2DuplicateNotSettled");
    AdjudicationContext adjudicationContext = new AdjudicationContext(
        ExecutorMetaClientFactory.from(ctx.metaClient),
        ctx.earliestActiveWriteCommit, tPrevD);
    // Broadcast because T_prevD dominates the closure and a task binary is deserialized per task.
    // The cutoff travels with its timeline so they cannot disagree. JavaSerializedPayload because
    // parts of this graph only round-trip under Java serialization.
    Broadcast<JavaSerializedPayload<AdjudicationContext>> adjudicationBroadcast =
        engineContext.getJavaSparkContext().broadcast(new JavaSerializedPayload<>(adjudicationContext));
    try {
      List<MismatchRecord> confirmed = InstantRangeBalancer
          .repartition(engineContext.getJavaSparkContext(), keyed, counts)
          .mapPartitions(
              records -> adjudicateBatch(records, adjudicationBroadcast, referenced, notSettled), true)
          .collect();

      // Collected only after phase 2, so no driver-side materialisation interrupts either phase.
      List<MismatchRecord> suspects = classified.collect();

      LOG.info("Adjudicated the duplicate base file suspects of table {} over {} distinct instant(s): "
              + "{} confirmed dangling, {} still referenced, {} unsettled, {} sanctioned as possibly "
              + "in-flight",
          ctx.basePath, counts.size(), confirmed.size(), referenced.value(), notSettled.value(),
          sanctionedCount);
      return exhaustiveResult(ctx, suspects, confirmed, referenced.value(), notSettled.value(),
          sanctionedCount);
    } finally {
      // Destroyed only after every action that can reach it, including classified.collect(), which
      // can recompute a lost block. Non-blocking so teardown stays cheap.
      try {
        adjudicationBroadcast.destroy(false);
      } catch (Exception e) {
        LOG.warn("Could not destroy the phase-2 broadcast of table {}", ctx.basePath, e);
      }
    }
  }

  /**
   * Exhaustive mode's verdict: any survivor fails the run. The failure is returned, not thrown, so
   * the caller can write reports first. Suspects are sorted so an unchanged table yields identical
   * documents.
   */
  @VisibleForTesting
  static Result exhaustiveResult(Context ctx, List<MismatchRecord> suspects,
                                 List<MismatchRecord> confirmedDangling, long referencedCount,
                                 long notSettledCount, long sanctionedCount) {
    List<MismatchRecord> sorted = new ArrayList<>(suspects);
    Collections.sort(sorted);
    return new Result(sorted, collectedFailure(ctx, sorted.size()), confirmedDangling,
        referencedCount, notSettledCount, sorted.size(), sanctionedCount, ctx.filesSeen.value());
  }

  /**
   * The single driver-side validation failure for {@code suspectCount} survivors, or empty when none
   * survived. File names belong in the report, not the message.
   */
  private static Option<HoodieValidationException> collectedFailure(Context ctx,
                                                                    long suspectCount) {
    if (suspectCount == 0) {
      return Option.empty();
    }
    return Option.of(new HoodieValidationException(String.format(
        "Validation of %s failed for table: %s. %d file(s) sit in a file slice that holds more than "
            + "one base file and no in-flight write explains them; see the findings written for "
            + "this run, or the 'Duplicate base file' lines in the executor logs",
        DUPLICATE_BASE_FILES_LABEL, ctx.basePath, suspectCount)));
  }

  /**
   * The classify stage: drops suspects the {@link DuplicateInstantWindow} sanctions and returns the
   * survivors. Each decision depends only on the record's own writing instant.
   *
   * <p>In fail-fast without {@code --ignore-failed} it logs up to
   * {@link #MAX_SUSPECTS_LOGGED_PER_TASK} survivors and throws on the executor, stopping the run
   * before the FS-MDT diff starts. That log is fail-fast's only record of file names.
   */
  @VisibleForTesting
  public static Iterator<MismatchRecord> classifyBatch(
      Iterator<MismatchRecord> records, Context ctx, DuplicateInstantWindow window,
      LongAccumulator sanctioned) {
    boolean failFast = ctx.validationMode != ValidationMode.EXHAUSTIVE;
    List<MismatchRecord> surviving = new ArrayList<>();
    while (records.hasNext()) {
      MismatchRecord record = records.next();
      Option<DuplicateInstantWindow.Reason> reason =
          window.sanctionReasonFor(record.getBaseInstantTime());
      if (reason.isPresent()) {
        // Dropped, not relabelled: adjudicating a possibly in-flight instant would confirm an
        // ordinary write as dangling. The accumulator and beacon are its only trace.
        sanctioned.add(1);
        BranchBeacon.hit(reason.get() == DuplicateInstantWindow.Reason.PREV_INFLIGHT
            ? Branch.DUP_SANCTIONED_PREV_INFLIGHT : Branch.DUP_SANCTIONED_AFT_EXCLUSIVE);
        continue;
      }
      BranchBeacon.hit(Branch.DUP_SURVIVED_FILTER_SET);
      surviving.add(record);
      if (ctx.failsOnTheExecutor() && surviving.size() >= MAX_SUSPECTS_LOGGED_PER_TASK) {
        // Enough to log and throw with; the rest of the batch cannot change the verdict.
        break;
      }
    }
    if (failFast && !surviving.isEmpty()) {
      logSuspects(ctx, surviving);
      if (ctx.failsOnTheExecutor()) {
        throw failFastFailure(ctx, surviving);
      }
    }
    return surviving.iterator();
  }

  /**
   * Logs up to {@link #MAX_SUSPECTS_LOGGED_PER_TASK} suspects, one line each with partition, file
   * group and file, so an operator can inspect them without a re-run. Runs on the executor before
   * the throw because with the default {@code --ignore-failed=false} the driver never receives a
   * record. Called only in fail-fast; exhaustive writes {@code dangling_files/} instead.
   */
  private static void logSuspects(Context ctx, List<MismatchRecord> suspects) {
    int logged = Math.min(suspects.size(), MAX_SUSPECTS_LOGGED_PER_TASK);
    for (MismatchRecord record : suspects.subList(0, logged)) {
      LOG.error("Duplicate base file in table {}: partition={} fileGroup={} file={} reason={} "
              + "duplicateSetSize={} instant={} - {}", ctx.basePath, record.getPartition(),
          record.getFileGroupId(), record.getFileName(), record.getSuspectReason(),
          record.getDuplicateSetSize(), record.getInstantTime(), record.getDetail());
    }
    if (suspects.size() > logged) {
      LOG.error("This task of table {} holds {} duplicate base file(s), more than the {} logged "
          + "above", ctx.basePath, suspects.size(), logged);
    }
    LOG.error("Metadata table validation (fail-fast) found {} duplicate base file(s) in table {}. "
            + "Fail-fast writes no report; re-run with --validation-mode exhaustive for a "
            + "machine-readable list and for the timeline adjudication of each one",
        suspects.size(), ctx.basePath);
  }

  /** The failure fail-fast throws; it names the first suspect because there is no report. */
  private static HoodieValidationException failFastFailure(Context ctx,
                                                           List<MismatchRecord> sample) {
    MismatchRecord first = sample.get(0);
    return new HoodieValidationException(String.format(
        "Validation of %s failed for table: %s. File slice %s of partition %s holds %d base files "
            + "and no in-flight write explains them; %s is one of them, written by instant %s. This "
            + "task named %d such file(s) before stopping; fail-fast does not classify the rest of "
            + "the table, so re-run with --validation-mode exhaustive for the full list and the "
            + "timeline adjudication of each one",
        DUPLICATE_BASE_FILES_LABEL, ctx.basePath, first.getFileGroupId(), first.getPartition(),
        first.getDuplicateSetSize(), first.getFileName(), first.getInstantTime(), sample.size()));
  }

  /**
   * Phase 1 over one task's batch: one raw listing per partition in rounds of
   * {@code --partition-threads}, returning individual suspects and summing {@code filesSeen}.
   *
   * <p>Runs even when the dimension is disabled ({@link #detectDuplicates} checks
   * {@code ctx.enabled}), so a recompute can still make the run inconclusive, which is the
   * conservative direction.
   */
  private static Iterator<MismatchRecord> detectBatch(
      Iterator<String> partitionIterator, Context ctx, LongAccumulator listingTasks) {
    // First, before anything can throw: a task that failed and was retried must still count.
    listingTasks.add(1);
    List<String> batch = new ArrayList<>();
    partitionIterator.forEachRemaining(batch::add);
    List<MismatchRecord> taskSuspects = new ArrayList<>();
    // LongAccumulator is not thread-safe and the pool shares ctx's instances, so sum locally and
    // add once per task.
    AtomicLong taskValidationMs = new AtomicLong();
    AtomicLong taskFilesSeen = new AtomicLong();
    try (CloseableExecutor partitionPool =
             CloseableExecutor.fixedPool(Math.min(ctx.partitionThreads, batch.size()))) {
      int nextPartition = 0;
      while (nextPartition < batch.size()) {
        List<String> round =
            batch.subList(nextPartition, Math.min(batch.size(), nextPartition + ctx.partitionThreads));
        nextPartition += round.size();
        taskSuspects.addAll(
            runRound(round, partitionPool.service(), ctx, taskValidationMs, taskFilesSeen));
      }
    } finally {
      // In the finally so failed partitions still contribute their time.
      ctx.listingMs.add(taskValidationMs.get());
      ctx.filesSeen.add(taskFilesSeen.get());
    }
    return taskSuspects.iterator();
  }

  /** Checks one round's partitions concurrently and waits for all of them. */
  private static List<MismatchRecord> runRound(
      List<String> round, ExecutorService partitionPool, Context ctx,
      AtomicLong taskValidationMs, AtomicLong taskFilesSeen) {
    List<Future<List<MismatchRecord>>> futures = new ArrayList<>();
    for (String partitionPath : round) {
      futures.add(partitionPool.submit(
          () -> detectInOnePartition(partitionPath, ctx, taskValidationMs, taskFilesSeen)));
    }
    List<MismatchRecord> roundSuspects = new ArrayList<>();
    for (Future<List<MismatchRecord>> future : futures) {
      try {
        roundSuspects.addAll(future.get());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HoodieException("Interrupted while validating partitions of " + ctx.basePath, e);
      } catch (ExecutionException e) {
        throw ValidationFailures.classifyFailure(e);
      }
    }
    return roundSuspects;
  }

  /**
   * One partition's duplicate-base-file listing.
   *
   * <p>Must not catch: a listing failure is a {@link HoodieIOException} (a check that did not run),
   * and catching it here would promote it into a {@link HoodieValidationException}.
   */
  private static List<MismatchRecord> detectInOnePartition(
      String partitionPath, Context ctx, AtomicLong taskValidationMs, AtomicLong taskFilesSeen) {
    long partitionStartMs = System.currentTimeMillis();
    List<MismatchRecord> suspects = detectDuplicates(ctx, partitionPath, taskFilesSeen);
    // Not in a finally: a failed listing's time is dropped rather than wrapping the call in a try,
    // which the javadoc forbids.
    taskValidationMs.addAndGet(System.currentTimeMillis() - partitionStartMs);
    return suspects;
  }

  /**
   * The duplicate-base-file suspects in one partition, from a raw storage listing (the file system
   * view's latest slices collapse a duplicate set to one member). Runs in both modes; sanctioning
   * and adjudication happen later.
   *
   * <p>A partition that cannot be listed throws {@link HoodieIOException}, never a validation
   * failure and never an empty result, so an unlistable table cannot report success and alerts as
   * infrastructure, even under {@code --ignore-failed}. {@code --validate-duplicate-base-files=false}
   * is the escape hatch.
   *
   * @param filesSeen every file the listing considered is added here, findings and agreements alike
   */
  public static List<MismatchRecord> detectDuplicates(Context ctx, String partitionPath,
                                                      AtomicLong filesSeen) {
    // Before the listing, so off costs nothing rather than listing and discarding.
    if (!ctx.enabled) {
      return Collections.emptyList();
    }
    try {
      StoragePath fullPartitionPath = partitionPath.isEmpty()
          ? new StoragePath(ctx.basePath) : new StoragePath(ctx.basePath, partitionPath);
      // A different population from the FS-MDT diff's filesSeen, so each dimension keeps its own
      // denominator.
      List<StoragePathInfo> partitionFiles =
          FSUtils.getAllDataFilesInPartition(ctx.metaClient.getStorage(), fullPartitionPath);
      filesSeen.addAndGet(partitionFiles.size());
      return DuplicateBaseFileDetector.detectSuspects(partitionFiles, partitionPath);
    } catch (IOException e) {
      // Neither absorbed nor promoted to a validation failure; see the javadoc.
      throw new HoodieIOException("Failed to list partition " + partitionPath + " of table "
          + ctx.basePath + " while checking for dangling-file suspects", e);
    }
  }

  /**
   * Everything phase 2 needs on the executor. The archival cutoff is read once on the driver so no
   * two executors disagree about it. A {@link ExecutorMetaClientFactory} ships instead of a meta
   * client, which would drag its active timeline along. Accumulators are not here because a
   * broadcast is shared per executor and would not report per task.
   */
  @VisibleForTesting
  static final class AdjudicationContext implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ExecutorMetaClientFactory metaClients;
    /**
     * Earliest write commit on {@link #tPrevD}, derived on the driver from the same object so the
     * cutoff cannot disagree with the timeline.
     */
    private final String earliestActiveWriteCommit;
    /**
     * {@code T_prevD} as the driver read it, shipped whole and narrowed on the executor (a
     * pre-filtered timeline deserializes unusable). Every surviving suspect is completed at
     * {@code T_prevD} or absent from both reads, so {@code T_aftD} is not needed.
     */
    private final HoodieTimeline tPrevD;

    AdjudicationContext(ExecutorMetaClientFactory metaClients,
                        String earliestActiveWriteCommit, HoodieTimeline tPrevD) {
      this.metaClients = metaClients;
      this.earliestActiveWriteCommit = earliestActiveWriteCommit;
      this.tPrevD = tPrevD;
    }

    /** Exposed only for the serialization round-trip test; see TestPhaseTwoPayloadSerialization. */
    @VisibleForTesting
    ExecutorMetaClientFactory getMetaClients() {
      return metaClients;
    }

    /** Exposed only for the serialization round-trip test; see TestPhaseTwoPayloadSerialization. */
    @VisibleForTesting
    HoodieTimeline getTPrevD() {
      return tPrevD;
    }
  }

  /**
   * Phase 2 over one shuffle partition. The {@link DanglingAdjudicator} is built here because it is
   * not {@link Serializable}, and the archive is read once per task for the whole instant batch.
   */
  @VisibleForTesting
  static Iterator<MismatchRecord> adjudicateBatch(
      Iterator<MismatchRecord> records, Broadcast<JavaSerializedPayload<AdjudicationContext>> ctxBroadcast,
      LongAccumulator referenced, LongAccumulator notSettled) {
    // Read once per task rather than per use: the value is deserialized once per executor and shared.
    return adjudicateBatch(records, ctxBroadcast.value().get(), referenced, notSettled);
  }

  @VisibleForTesting
  static Iterator<MismatchRecord> adjudicateBatch(
      Iterator<MismatchRecord> records, AdjudicationContext ctx,
      LongAccumulator referenced, LongAccumulator notSettled) {
    List<MismatchRecord> batch = new ArrayList<>();
    records.forEachRemaining(batch::add);
    if (batch.isEmpty()) {
      return Collections.emptyIterator();
    }
    // Narrowed here: the timeline's instant reader is transient, so it cannot be filtered earlier.
    HoodieTimeline completedActive = ctx.tPrevD.getCommitsTimeline().filterCompletedInstants();

    // Bucket first, so the archive batch holds only instants the active timeline cannot speak for.
    Set<String> archiveTargets = new HashSet<>();
    for (MismatchRecord record : batch) {
      String writingInstant = record.getInstantTime();
      if (writingInstant != null
          && InstantBuckets.pokesArchive(writingInstant, ctx.earliestActiveWriteCommit)) {
        archiveTargets.add(writingInstant);
      }
    }

    // The one archived pass of this task, projected per file group so no record is judged on
    // another group's names.
    ArchivedInstantWorkingSet<ArchivedCommitPaths> archived = null;
    if (!archiveTargets.isEmpty()) {
      // Storage-only client for the archive. Invariant: nothing reachable here may call
      // getActiveTimeline() on it, which would build an active timeline on the executor.
      HoodieTableMetaClient client = ctx.metaClients.newStorageOnlyClient();
      // Load-bearing gate: the v2 resolver's warmUp throws by design. On v2 every archive-bucket
      // record refuses.
      if (ArchiveTimelineInstantBatchResolvers.readsV1Archive(client)) {
        archived = ArchiveTimelineInstantBatchResolvers
            .forTable(client, new CommitPathProjector(client.getBasePath(), wantedGroups(batch)))
            .warmUp(archiveTargets);
      }
    }

    DanglingAdjudicator adjudicator =
        new DanglingAdjudicator(completedActive, ctx.earliestActiveWriteCommit, archived);
    List<MismatchRecord> confirmed = new ArrayList<>();
    long referencedHere = 0;
    long notSettledHere = 0;
    for (MismatchRecord record : batch) {
      DanglingAdjudicator.Outcome outcome = adjudicator.adjudicate(record.getPartition(),
          record.getFileGroupId(), record.getFileName(), record.getInstantTime());
      switch (outcome) {
        case DANGLING_NO_COMPLETED_COMMIT:
        case DANGLING_COMMIT_DOES_NOT_NAME_IT:
          confirmed.add(asDanglingFile(record, outcome));
          break;
        case REFERENCED:
          referencedHere++;
          break;
        default:
          notSettledHere++;
          break;
      }
    }
    referenced.add(referencedHere);
    notSettled.add(notSettledHere);
    return confirmed.iterator();
  }

  /** Every file group the batch names, which is the scope the archived projection is bounded to. */
  private static Set<HoodieFileGroupId> wantedGroups(List<MismatchRecord> batch) {
    Set<HoodieFileGroupId> groups = new HashSet<>();
    for (MismatchRecord record : batch) {
      if (record.getPartition() != null && record.getFileGroupId() != null) {
        groups.add(new HoodieFileGroupId(record.getPartition(), record.getFileGroupId()));
      }
    }
    return groups;
  }

  /**
   * A suspect proven unreferenced, as the record a mitigation job acts on. The adjudication outcome
   * is cleared because {@code DANGLING_FILES} already carries it; everything else, including
   * {@code failsValidation}, is copied unchanged.
   */
  @VisibleForTesting
  public static MismatchRecord asDanglingFile(MismatchRecord record,
                                              DanglingAdjudicator.Outcome outcome) {
    return copyOf(record)
        .withPattern(MismatchPattern.DANGLING_FILES)
        .withDetail("proven unreferenced (" + outcome + "), promoted out of " + record.getPattern()
            + "; " + record.getDetail())
        .withAdjudicationOutcome(null)
        .build();
  }

  /**
   * A builder pre-loaded with every field of {@code record}, instant source included, so a caller
   * only states what changes.
   */
  private static MismatchRecord.Builder copyOf(MismatchRecord record) {
    MismatchRecord.Builder builder = MismatchRecord.newBuilder()
        .withPartition(record.getPartition())
        .withPattern(record.getPattern())
        .withFileGroupId(record.getFileGroupId())
        .withBaseInstantTime(record.getBaseInstantTime())
        .withFileName(record.getFileName())
        .withFileType(record.getFileType())
        .withSizeBytes(record.getSizeBytes())
        .withSizes(record.getFsSizeBytes(), record.getMdtSizeBytes())
        .withSuspectReason(record.getSuspectReason())
        .withDuplicateSet(record.getDuplicateSetSize(), record.getExpectedFileName())
        .withFailsValidation(record.isFailsValidation())
        .withDetail(record.getDetail())
        .withAdjudicationOutcome(record.getAdjudicationOutcome());
    if (record.getInstantSource() == MismatchRecord.InstantSource.UNRESOLVED) {
      return builder.withUnresolvedInstant(record.getInstantUnresolvedReason());
    }
    return builder.withInstant(record.getInstantTime(), record.getInstantSource());
  }
}
