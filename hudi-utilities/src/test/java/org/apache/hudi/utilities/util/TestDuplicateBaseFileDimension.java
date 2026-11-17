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

import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.util.archive.ArchiveTimelineInstantBatchResolver;
import org.apache.hudi.utilities.util.archive.ArchiveTimelineInstantBatchResolvers;
import org.apache.hudi.utilities.util.archive.ArchivedInstantWorkingSet;

import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

/**
 * The classify stage and the driver-side pieces of dimension A's phase 2: what the inflight window
 * does to a suspect in {@code classifyBatch}, where each mode raises its validation failure, and
 * what a proven suspect becomes. The Spark job itself is covered end to end by the functional suite.
 */
class TestDuplicateBaseFileDimension {

  private static final String PARTITION = "2024/01/01";

  /** The promotion says what proved it and where it came from, and carries no stale outcome. */
  @Test
  void promotionRecordsTheOutcomeThatProvedIt() {
    MismatchRecord promoted = DuplicateBaseFileDimension.asDanglingFile(
        suspect(PARTITION, "fg1", "1005", "fg1_0-1-0_1005.parquet"),
        DanglingAdjudicator.Outcome.DANGLING_COMMIT_DOES_NOT_NAME_IT);

    assertEquals(MismatchPattern.DANGLING_FILES, promoted.getPattern());
    assertEquals("fg1_0-1-0_1005.parquet", promoted.getFileName());
    assertEquals("1005", promoted.getInstantTime());
    assertNull(promoted.getAdjudicationOutcome(),
        "DANGLING_FILES carries the answer in its pattern; a stale outcome would contradict it");
    assertTrue(promoted.getDetail().contains("DANGLING_COMMIT_DOES_NOT_NAME_IT"));
  }

  /**
   * A suspect the inflight window sanctions still bumps the run-header count -- that count is the
   * mechanism by which a sanctioned suspect is accounted for at all -- but it must not come back out
   * of {@code classifyBatch}: carrying it downstream just to filter it again later is the waste this
   * task removes.
   */
  @Test
  void sanctionedSuspectIsCountedButNotEmitted() {
    LongAccumulator sanctioned = new LongAccumulator();
    List<MismatchRecord> records = Arrays.asList(
        suspect(PARTITION, "fg1", "200", "fg1_0-1-0_200.parquet"));

    List<MismatchRecord> emitted = classify(records, windowSanctioning("200"), sanctioned);

    assertTrue(emitted.isEmpty(), "a sanctioned suspect must not be emitted");
    assertEquals(1L, sanctioned.value());
  }

  /**
   * A sanctioned suspect must not survive into what the exhaustive document is written from -- not
   * as a SANCTIONED record, not at all. Mixed with a surviving suspect so the assertion is about the
   * real contents rather than a count a careless implementation could satisfy by other means.
   */
  @Test
  void sanctionedSuspectIsAbsentFromWhatSurvivesClassification() {
    LongAccumulator sanctioned = new LongAccumulator();
    List<MismatchRecord> records = Arrays.asList(
        suspect(PARTITION, "fg1", "200", "fg1_0-1-0_200.parquet"),
        suspect(PARTITION, "fg2", "999", "fg2_0-1-0_999.parquet"));

    List<MismatchRecord> emitted = classify(records, windowSanctioning("200"), sanctioned);

    assertEquals(Collections.singletonList("fg2_0-1-0_999.parquet"), fileNamesOf(emitted),
        "only the surviving suspect crosses to phase 2");
    assertEquals(1L, sanctioned.value());
  }

  /** A suspect the window does not sanction is emitted unchanged and still fails validation. */
  @Test
  void survivingSuspectIsStillEmittedAndStillFails() {
    LongAccumulator sanctioned = new LongAccumulator();
    MismatchRecord record = suspect(PARTITION, "fg1", "999", "fg1_0-1-0_999.parquet");

    List<MismatchRecord> emitted =
        classify(Arrays.asList(record), windowSanctioning("200"), sanctioned);

    assertEquals(1, emitted.size());
    assertSame(record, emitted.get(0), "a surviving suspect passes through unchanged");
    assertTrue(emitted.get(0).isFailsValidation());
    assertEquals(0L, sanctioned.value());
  }

  /**
   * A batch whose suspects are ALL sanctioned emits nothing and, in fail-fast, raises nothing: a
   * sanctioned record is a write racing the run, not a finding.
   */
  @Test
  void batchWhoseSuspectsAreAllSanctionedRaisesNothing() {
    LongAccumulator sanctioned = new LongAccumulator();
    List<MismatchRecord> records = Arrays.asList(
        suspect(PARTITION, "fg1", "200", "fg1_0-1-0_200.parquet"),
        suspect(PARTITION, "fg1", "200", "fg1_0-2-0_200.parquet"));

    assertTrue(classifyIn(ValidationMode.FAIL_FAST, false, records, windowSanctioning("200"),
        sanctioned).isEmpty());
    assertEquals(2L, sanctioned.value());
  }

  // ---------------------------------------------------------------------------------------------
  //  The two modes' verdicts: fail-fast throws where it stands, exhaustive hands one back
  // ---------------------------------------------------------------------------------------------

  /**
   * <b>Fail-fast raises on the executor, from the task that holds the survivor</b>, rather than
   * classifying the rest of the table first. The message names the file, because a fail-fast run
   * writes no report and there is nowhere else for an operator to look.
   */
  @Test
  void failFastThrowsFromTheTaskThatFindsASurvivor() {
    LongAccumulator sanctioned = new LongAccumulator();
    List<MismatchRecord> records = Arrays.asList(
        suspect(PARTITION, "fg1", "200", "fg1_0-1-0_200.parquet"),
        suspect(PARTITION, "fg2", "999", "fg2_0-1-0_999.parquet"));

    HoodieValidationException thrown = assertThrows(HoodieValidationException.class,
        () -> classifyIn(ValidationMode.FAIL_FAST, false, records, windowSanctioning("200"),
            sanctioned));

    assertTrue(thrown.getMessage().contains("fg2_0-1-0_999.parquet"),
        "the exception has to name a file: " + thrown.getMessage());
    assertTrue(thrown.getMessage().contains(PARTITION), thrown.getMessage());
    assertEquals(1L, sanctioned.value(),
        "the sanctioned suspect ahead of it is still explained rather than raised on");
  }

  /**
   * <b>The throw waits for a sample and no longer.</b> A task holding far more survivors than the
   * log bound stops at the bound: an operator gets names to go and look at, and the task does not
   * classify a damaged table's every duplicate to produce an answer already decided.
   */
  @Test
  void failFastStopsOnceItHoldsItsSample() {
    List<MismatchRecord> records = new ArrayList<>();
    for (int i = 0; i < 120; i++) {
      records.add(suspect(PARTITION, "fg" + i, "999", "fg" + i + "_0-1-0_999.parquet"));
    }

    HoodieValidationException thrown = assertThrows(HoodieValidationException.class,
        () -> classifyIn(ValidationMode.FAIL_FAST, false, records, windowSanctioning("200"),
            new LongAccumulator()));

    assertTrue(thrown.getMessage().contains("named 50 such file(s)"),
        "the task stops at the sample bound rather than classifying all 120: " + thrown.getMessage());
  }

  /**
   * <b>{@code --ignore-failed} keeps the executor from throwing</b>, and every survivor then travels
   * to the driver for the count the verdict is read off. Unchanged in kind from before this task:
   * that flag has always governed fail-fast alone.
   */
  @Test
  void failFastWithIgnoreFailedCollectsInsteadOfThrowing() {
    LongAccumulator sanctioned = new LongAccumulator();
    List<MismatchRecord> records = Arrays.asList(
        suspect(PARTITION, "fg1", "999", "fg1_0-1-0_999.parquet"),
        suspect(PARTITION, "fg2", "999", "fg2_0-1-0_999.parquet"));

    List<MismatchRecord> emitted = classifyIn(ValidationMode.FAIL_FAST, true, records,
        windowSanctioning("200"), sanctioned);

    assertEquals(2, emitted.size(), "nothing is dropped and nothing is raised on the executor");
  }

  /** Exhaustive mode never throws from the classify stage, however much it finds. */
  @Test
  void exhaustiveModeNeverThrowsFromClassify() {
    List<MismatchRecord> records = Arrays.asList(
        suspect(PARTITION, "fg1", "999", "fg1_0-1-0_999.parquet"),
        suspect(PARTITION, "fg2", "999", "fg2_0-1-0_999.parquet"));

    assertEquals(2, classifyIn(ValidationMode.EXHAUSTIVE, false, records,
        windowSanctioning("200"), new LongAccumulator()).size());
  }

  /**
   * <b>The end of phase 2 is where an exhaustive run's verdict is decided</b>: anything that
   * survived classification is a failure, and it is <em>returned</em> rather than thrown so that the
   * caller can write the reports a mitigation job reads before raising it.
   */
  @Test
  void exhaustiveRunWithSurvivorsHandsBackOneDriverSideFailure() {
    LongAccumulator filesSeen = new LongAccumulator();
    filesSeen.add(200L);

    DuplicateBaseFileDimension.Result result = DuplicateBaseFileDimension.exhaustiveResult(
        exhaustiveContext(filesSeen),
        Arrays.asList(suspect(PARTITION, "fg1", "1005", "fg1_0-1-0_1005.parquet"),
            suspect(PARTITION, "fg1", "1005", "fg1_0-2-0_1005.parquet")),
        Collections.emptyList(), 0L, 0L, 0L);

    assertTrue(result.getFailure().isPresent(), "two survivors fail the run");
    assertTrue(result.getFailure().get().getMessage().contains("2 file(s)"),
        result.getFailure().get().getMessage());
    assertEquals(2L, result.getSuspectCount());
    assertEquals(200L, result.getFilesSeen(),
        "the denominator a delete list is sized against comes off the listing accumulator");
  }

  /** Nothing survived, so there is no failure to hand back and the run passes this dimension. */
  @Test
  void exhaustiveRunWithNoSurvivorsHandsBackNoFailure() {
    DuplicateBaseFileDimension.Result result = DuplicateBaseFileDimension.exhaustiveResult(
        exhaustiveContext(new LongAccumulator()), Collections.emptyList(),
        Collections.emptyList(), 0L, 0L, 3L);

    assertFalse(result.getFailure().isPresent());
    assertEquals(0L, result.getSuspectCount());
    assertEquals(3L, result.getSanctionedCount(),
        "a sanctioned suspect is explained, not a finding: it must not fail the run");
  }

  /**
   * The suspects a run reports are sorted.
   *
   * <p>They arrive in reverse order on purpose: they come off an RDD in whatever order the tasks
   * finished in, and two runs over an unchanged table have to produce byte-identical documents.
   */
  @Test
  void exhaustiveResultSortsTheSuspects() {
    List<MismatchRecord> records = new ArrayList<>();
    for (int i = 4; i >= 0; i--) {
      records.add(suspect(PARTITION, "fg1", "1005", "fg1_0-" + i + "-0_1005.parquet"));
    }

    DuplicateBaseFileDimension.Result result = DuplicateBaseFileDimension.exhaustiveResult(
        exhaustiveContext(new LongAccumulator()), records, Collections.emptyList(), 0L, 0L, 0L);

    assertEquals(Arrays.asList("fg1_0-0-0_1005.parquet", "fg1_0-1-0_1005.parquet",
            "fg1_0-2-0_1005.parquet", "fg1_0-3-0_1005.parquet", "fg1_0-4-0_1005.parquet"),
        fileNamesOf(result.getSuspects()),
        "the written order must not depend on the order the records were collected in");
  }

  /**
   * A suspect naming no instant is unadjudicable -- {@code runPhaseTwo} keeps it out of the shuffle
   * because the balancer requires non-null keys -- but it is still a finding, so it still reaches
   * the reported set and still fails the run.
   */
  @Test
  void suspectNamingNoInstantStillReachesTheReportAndStillFails() {
    DuplicateBaseFileDimension.Result result = DuplicateBaseFileDimension.exhaustiveResult(
        exhaustiveContext(new LongAccumulator()),
        Arrays.asList(suspect(PARTITION, "fg3", null, "fg3_0-1-0_unresolved.parquet")),
        Collections.emptyList(), 0L, 0L, 0L);

    assertEquals(Collections.singletonList("fg3_0-1-0_unresolved.parquet"),
        fileNamesOf(result.getSuspects()));
    assertTrue(result.getFailure().isPresent());
  }

  // ---------------------------------------------------------------------------------------------
  //  Phase 2 over one shuffle partition: the archive is read once, or not at all
  // ---------------------------------------------------------------------------------------------

  /**
   * <b>One archived pass for the whole task, whatever the batch's shape.</b>
   *
   * <p>The batch spans two file groups and three instants, all older than the active timeline's
   * cutoff. The previous implementation grouped by file group and warmed each group separately, so
   * this batch cost two passes; the resolver now takes the whole instant batch and the projection is
   * bounded by the file groups the batch names, so it costs one. Per-instant and per-file-group
   * archived reads are what stopped this class of job finishing on the tables it exists for, so the
   * pass count is the property, not an implementation detail.
   *
   * <p>The single call is also asserted to have been handed <em>every</em> target: a resolver called
   * once with a third of the batch would satisfy a bare count and leave two instants unasked.
   */
  @Test
  void archiveBucketInstantIsResolvedOnceForTheWholeTask(@TempDir java.nio.file.Path dir)
      throws Exception {
    List<MismatchRecord> batch = Arrays.asList(
        suspect(PARTITION, "fg1", "1001", "fg1_0-1-0_1001.parquet"),
        suspect(PARTITION, "fg1", "1002", "fg1_0-1-0_1002.parquet"),
        suspect(PARTITION, "fg2", "1003", "fg2_0-1-0_1003.parquet"));
    AtomicInteger passes = new AtomicInteger();
    List<Set<String>> asked = new ArrayList<>();
    ArchiveTimelineInstantBatchResolver<ArchivedCommitPaths> resolver = targets -> {
      passes.incrementAndGet();
      asked.add(new HashSet<>(targets));
      return ArchivedInstantWorkingSet.allNotFound(targets);
    };

    List<MismatchRecord> confirmed;
    try (MockedStatic<ArchiveTimelineInstantBatchResolvers> resolvers =
             Mockito.mockStatic(ArchiveTimelineInstantBatchResolvers.class)) {
      resolvers.when(() -> ArchiveTimelineInstantBatchResolvers.readsV1Archive(any()))
          .thenReturn(true);
      resolvers.when(() -> ArchiveTimelineInstantBatchResolvers
              .<Map<HoodieFileGroupId, Set<String>>>forTable(any(), any()))
          .thenReturn(resolver);

      confirmed = adjudicate(batch, dir, HoodieTableVersion.SIX);

      resolvers.verify(() -> ArchiveTimelineInstantBatchResolvers
          .<Map<HoodieFileGroupId, Set<String>>>forTable(any(), any()));
    }

    assertEquals(1, passes.get(), "the whole task reads the archive once, not once per file group");
    assertEquals(Collections.singletonList(new HashSet<>(Arrays.asList("1001", "1002", "1003"))),
        asked, "and that one pass is asked about every instant of the batch");
    assertEquals(Arrays.asList("fg1_0-1-0_1001.parquet", "fg1_0-1-0_1002.parquet",
        "fg2_0-1-0_1003.parquet"), fileNamesOf(confirmed),
        "a proven absence condemns every member, which is what makes the pass worth bounding");
  }

  /**
   * <b>A v2-layout table constructs no resolver at all, and every archive-bucket record refuses.</b>
   *
   * <p>The gate is {@code ArchiveTimelineInstantBatchResolvers.readsV1Archive}, and it is load
   * bearing in a way a refusal assertion alone would not show:
   * {@code V2ArchiveTimelineInstantBatchResolver.warmUp} throws {@code UnsupportedOperationException}
   * by design, so an implementation that built one and called it would fail this task outright
   * rather than refuse politely. Both halves are therefore asserted at once — the call returns, and
   * what it returns is a refusal.
   */
  @Test
  void v2TableNeverConstructsAResolverAndRefusesTheArchiveBucket(@TempDir java.nio.file.Path dir)
      throws Exception {
    List<MismatchRecord> batch = Collections.singletonList(
        suspect(PARTITION, "fg1", "1001", "fg1_0-1-0_1001.parquet"));

    LongAccumulator referenced = new LongAccumulator();
    LongAccumulator notSettled = new LongAccumulator();
    List<MismatchRecord> confirmed = new ArrayList<>();
    BranchBeacons.assertTakes(Branch.DAJ_ARCHIVE_UNPROVEN, () -> {
      try {
        DuplicateBaseFileDimension.adjudicateBatch(batch.iterator(),
            adjudicationContext(dir, HoodieTableVersion.EIGHT), referenced, notSettled)
            .forEachRemaining(confirmed::add);
      } catch (IOException e) {
        throw new java.io.UncheckedIOException(e);
      }
    });

    assertTrue(confirmed.isEmpty(),
        "nothing on a v2 table may be condemned from an archive that was never read");
    assertEquals(0L, referenced.value());
    assertEquals(1L, notSettled.value());
  }

  /** Runs {@code adjudicateBatch} over {@code batch} against a real table of {@code version}. */
  private static List<MismatchRecord> adjudicate(List<MismatchRecord> batch,
                                                 java.nio.file.Path dir,
                                                 HoodieTableVersion version) throws IOException {
    List<MismatchRecord> confirmed = new ArrayList<>();
    DuplicateBaseFileDimension.adjudicateBatch(batch.iterator(),
        adjudicationContext(dir, version), new LongAccumulator(), new LongAccumulator())
        .forEachRemaining(confirmed::add);
    return confirmed;
  }

  /**
   * A phase-2 context over a real, empty table of {@code version} — real because the executor builds
   * its own storage-only meta client from it — whose {@code T_prevD} holds one completed commit at
   * {@code 2000}, so every suspect older than that lands in the archive bucket.
   */
  private static DuplicateBaseFileDimension.AdjudicationContext adjudicationContext(
      java.nio.file.Path dir, HoodieTableVersion version) throws IOException {
    HoodieTableMetaClient metaClient;
    try {
      metaClient = HoodieTestUtils.init(
          dir.toAbsolutePath().toString(), HoodieTableType.COPY_ON_WRITE, version);
    } catch (Exception e) {
      throw new IOException("could not initialise the fixture table", e);
    }
    HoodieTimeline completed = completedTimelineOver("2000");
    HoodieTimeline commits = Mockito.mock(HoodieTimeline.class);
    HoodieTimeline tPrevD = Mockito.mock(HoodieTimeline.class);
    Mockito.when(tPrevD.getCommitsTimeline()).thenReturn(commits);
    Mockito.when(commits.filterCompletedInstants()).thenReturn(completed);
    return new DuplicateBaseFileDimension.AdjudicationContext(
        ExecutorMetaClientFactory.from(metaClient),
        InstantBuckets.earliestCompletedWriteCommit(completed), tPrevD);
  }

  private static HoodieTimeline completedTimelineOver(String... instantTimes) {
    List<HoodieInstant> instants = new ArrayList<>();
    for (String time : instantTimes) {
      HoodieInstant instant = Mockito.mock(HoodieInstant.class);
      Mockito.when(instant.getAction()).thenReturn(HoodieTimeline.COMMIT_ACTION);
      Mockito.when(instant.requestedTime()).thenReturn(time);
      instants.add(instant);
    }
    HoodieTimeline timeline = Mockito.mock(HoodieTimeline.class);
    Mockito.when(timeline.getInstants()).thenReturn(instants);
    return timeline;
  }

  private static List<String> fileNamesOf(List<MismatchRecord> records) {
    List<String> fileNames = new ArrayList<>();
    for (MismatchRecord record : records) {
      fileNames.add(record.getFileName());
    }
    return fileNames;
  }

  /** Runs {@code classifyBatch} over {@code records} in exhaustive mode, which never throws. */
  private static List<MismatchRecord> classify(
      List<MismatchRecord> records, DuplicateInstantWindow window, LongAccumulator sanctioned) {
    return classifyIn(ValidationMode.EXHAUSTIVE, false, records, window, sanctioned);
  }

  /** Runs {@code classifyBatch} over {@code records} and returns what it emits, as a list. */
  private static List<MismatchRecord> classifyIn(
      ValidationMode mode, boolean ignoreFailed, List<MismatchRecord> records,
      DuplicateInstantWindow window, LongAccumulator sanctioned) {
    DuplicateBaseFileDimension.Context ctx = new DuplicateBaseFileDimension.Context(
        mode, null, null, "s3://table", 1, ignoreFailed, true,
        new LongAccumulator(), new LongAccumulator());
    Iterator<MismatchRecord> classified =
        DuplicateBaseFileDimension.classifyBatch(records.iterator(), ctx, window, sanctioned);
    List<MismatchRecord> result = new ArrayList<>();
    classified.forEachRemaining(result::add);
    return result;
  }

  /** An exhaustive context carrying {@code filesSeen} and nothing else a verdict reads. */
  private static DuplicateBaseFileDimension.Context exhaustiveContext(LongAccumulator filesSeen) {
    return new DuplicateBaseFileDimension.Context(ValidationMode.EXHAUSTIVE, null, null,
        "s3://table", 1, false, true, new LongAccumulator(), filesSeen);
  }

  /** A window that sanctions {@code instantTime} as {@code PREV_INFLIGHT} and nothing else. */
  private static DuplicateInstantWindow windowSanctioning(String instantTime) {
    HoodieTimeline tPrevD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, instantTime));
    HoodieTimeline tAftD = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, instantTime));
    return DuplicateInstantWindow.between(tPrevD, tAftD);
  }

  private static MismatchRecord suspect(String partition, String fileGroupId, String instantTime,
                                        String fileName) {
    return MismatchRecord.newBuilder()
        .withPartition(partition)
        .withPattern(MismatchPattern.DANGLING_FILE_SUSPECTS)
        .withFileGroupId(fileGroupId)
        .withBaseInstantTime(instantTime)
        .withFileName(fileName)
        .withFileType(MismatchRecord.FileType.BASE)
        .withInstant(instantTime, MismatchRecord.InstantSource.FILE_NAME)
        .withSuspectReason(MismatchRecord.SuspectReason.DUPLICATE_BASE_FILE)
        .withDuplicateSet(2, null)
        .withFailsValidation(true)
        .withDetail("file slice holds 2 base files")
        .build();
  }
}
