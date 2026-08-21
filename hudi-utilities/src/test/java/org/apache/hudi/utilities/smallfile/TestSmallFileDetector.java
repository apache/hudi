/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.smallfile;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pure unit tests for {@link SmallFileDetector}. No Spark, no on-disk Hudi table — every input
 * (per-partition stats, timeline instants) is fabricated so we can pin exact boundary behavior at
 * the qualifying gate, the 50 MB threshold, the tier percentages, and the young-table SKIPPED
 * gate. Integration coverage on real tables lives in {@code TestTableSizeStats}.
 */
class TestSmallFileDetector {

  // ---- Config.Builder ----

  @Test
  void configBuilderProducesDefaults() {
    SmallFileDetector.Config cfg = SmallFileDetector.Config.defaults();
    assertEquals(SmallFileDetector.Config.DEFAULT_MIN_FILES_PER_PARTITION, cfg.getMinFilesPerPartition());
    assertEquals(SmallFileDetector.Config.DEFAULT_THRESHOLD_BYTES, cfg.getThresholdBytes());
    assertEquals(SmallFileDetector.Config.DEFAULT_MODERATE_PCT, cfg.getModeratePct());
    assertEquals(SmallFileDetector.Config.DEFAULT_SEVERE_PCT, cfg.getSeverePct());
    assertEquals(SmallFileDetector.Config.DEFAULT_MIN_TABLE_COMMITS, cfg.getMinTableCommits());
  }

  @Test
  void configBuilderRejectsModerateAboveSevere() {
    // Guards against a silent misconfiguration where MODERATE would swallow SEVERE.
    assertThrows(IllegalArgumentException.class,
        () -> SmallFileDetector.Config.builder().moderatePct(0.5).severePct(0.3).build());
  }

  // ---- per-partition classification: qualifying gate ----

  @Test
  void notQualifyingWhenFileCountBelowMin() {
    SmallFileDetector.Config cfg = SmallFileDetector.Config.defaults();
    // 4 files @ 1 byte each — well below threshold but doesn't clear the file-count gate.
    SmallFileDetector.PartitionStats p = new SmallFileDetector.PartitionStats("p", 4, 4);
    assertEquals(false, SmallFileDetector.isQualifying(p, cfg));
    assertEquals(false, SmallFileDetector.isFlagged(p, cfg));
  }

  @Test
  void qualifyingAtExactlyMinFiles() {
    // Inclusive gate: fileCount == MIN_FILES qualifies, aligned with the micro-partition detector.
    SmallFileDetector.PartitionStats p = new SmallFileDetector.PartitionStats("p", 5, 5);
    assertEquals(true, SmallFileDetector.isQualifying(p, SmallFileDetector.Config.defaults()));
  }

  // ---- per-partition classification: flagged threshold ----

  @Test
  void flaggedWhenAvgStrictlyBelowThreshold() {
    SmallFileDetector.Config cfg = SmallFileDetector.Config.defaults();
    long fifty = SmallFileDetector.Config.DEFAULT_THRESHOLD_BYTES;
    // 5 files, total (5 * 50MB) - 1 → integer avg = 50MB - 1 → strictly below → flagged.
    SmallFileDetector.PartitionStats p = new SmallFileDetector.PartitionStats("p", 5, 5 * fifty - 1);
    assertEquals(true, SmallFileDetector.isFlagged(p, cfg));
  }

  @Test
  void notFlaggedWhenAvgExactlyAtThreshold() {
    SmallFileDetector.Config cfg = SmallFileDetector.Config.defaults();
    long fifty = SmallFileDetector.Config.DEFAULT_THRESHOLD_BYTES;
    // 5 files, total 5 * 50MB → avg exactly 50MB → strict '<' → not flagged.
    SmallFileDetector.PartitionStats p = new SmallFileDetector.PartitionStats("p", 5, 5 * fifty);
    assertEquals(true, SmallFileDetector.isQualifying(p, cfg));
    assertEquals(false, SmallFileDetector.isFlagged(p, cfg));
  }

  @Test
  void notFlaggedWhenAvgAboveThresholdEvenIfQualifying() {
    SmallFileDetector.Config cfg = SmallFileDetector.Config.defaults();
    long fifty = SmallFileDetector.Config.DEFAULT_THRESHOLD_BYTES;
    SmallFileDetector.PartitionStats p = new SmallFileDetector.PartitionStats("p", 5, 6 * fifty);
    assertEquals(false, SmallFileDetector.isFlagged(p, cfg));
  }

  @Test
  void avgFileSizeIsZeroWhenNoFiles() {
    // Guards the fileCount == 0 divide-by-zero branch.
    assertEquals(0, new SmallFileDetector.PartitionStats("p", 0, 0).avgFileSize());
  }

  // ---- classifyVerdict: SKIPPED gate takes precedence ----

  @Test
  void skippedWhenCommitCountBelowMin() {
    SmallFileDetector.Config cfg = SmallFileDetector.Config.defaults();
    // 100/100 flagged, but only 9 commits → SKIPPED wins over SEVERE. The young-table gate
    // is the whole reason it exists — beats every other classification.
    assertEquals(SmallFileDetector.Verdict.SKIPPED,
        SmallFileDetector.classifyVerdict(9, 100, 100, cfg));
    assertEquals(SmallFileDetector.Verdict.SKIPPED,
        SmallFileDetector.classifyVerdict(0, 0, 0, cfg));
  }

  @Test
  void cleanWhenNoQualifyingPartitions() {
    assertEquals(SmallFileDetector.Verdict.CLEAN,
        SmallFileDetector.classifyVerdict(10, 0, 0, SmallFileDetector.Config.defaults()));
  }

  // ---- classifyVerdict: tier boundaries ----

  @Test
  void moderateAtExactlyModeratePct() {
    // 10 / 100 = 0.10 = defaultModeratePct — inclusive lower bound.
    assertEquals(SmallFileDetector.Verdict.MODERATE,
        SmallFileDetector.classifyVerdict(10, 100, 10, SmallFileDetector.Config.defaults()));
  }

  @Test
  void moderateJustBelowSeverePct() {
    // 29 / 100 = 0.29 — moderate band, not severe.
    assertEquals(SmallFileDetector.Verdict.MODERATE,
        SmallFileDetector.classifyVerdict(10, 100, 29, SmallFileDetector.Config.defaults()));
  }

  @Test
  void severeAtAndAboveSeverePct() {
    // exactly 0.30, plus a case where the ratio is 1.0 to guard the top of the range.
    SmallFileDetector.Config cfg = SmallFileDetector.Config.defaults();
    assertEquals(SmallFileDetector.Verdict.SEVERE, SmallFileDetector.classifyVerdict(10, 10, 3, cfg));
    assertEquals(SmallFileDetector.Verdict.SEVERE, SmallFileDetector.classifyVerdict(10, 100, 30, cfg));
    assertEquals(SmallFileDetector.Verdict.SEVERE, SmallFileDetector.classifyVerdict(10, 4, 4, cfg));
  }

  @Test
  void cleanBelowModeratePct() {
    // 9 / 100 = 0.09 → below the MODERATE gate → CLEAN.
    assertEquals(SmallFileDetector.Verdict.CLEAN,
        SmallFileDetector.classifyVerdict(10, 100, 9, SmallFileDetector.Config.defaults()));
  }

  // ---- countIngestCommits: action filter ----

  @Test
  void countIngestCommitsSkipsNonIngestActions() {
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    HoodieTimeline completed = mock(HoodieTimeline.class);
    when(timeline.filterCompletedInstants()).thenReturn(completed);
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(instant(HoodieTimeline.COMMIT_ACTION));
    instants.add(instant(HoodieTimeline.DELTA_COMMIT_ACTION));
    // Clean/rollback/compaction/savepoint — all non-ingest, must be excluded.
    instants.add(instant(HoodieTimeline.CLEAN_ACTION));
    instants.add(instant(HoodieTimeline.ROLLBACK_ACTION));
    instants.add(instant(HoodieTimeline.COMPACTION_ACTION));
    instants.add(instant(HoodieTimeline.SAVEPOINT_ACTION));
    when(completed.getInstantsAsStream()).thenAnswer(inv -> instants.stream());

    assertEquals(2, SmallFileDetector.countIngestCommits(timeline));
  }

  @Test
  void countIngestCommitsExcludesClusteringReplaceCommits() throws Exception {
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    HoodieTimeline completed = mock(HoodieTimeline.class);
    when(timeline.filterCompletedInstants()).thenReturn(completed);
    // Distinct timestamps so HoodieInstant.equals distinguishes them for the stub matcher.
    HoodieInstant realIngest =
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, "20240101000000001",
            InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieInstant clustering =
        new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, "20240101000000002",
            InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    List<HoodieInstant> instants = Arrays.asList(realIngest, clustering);
    when(completed.getInstantsAsStream()).thenAnswer(inv -> instants.stream());

    HoodieCommitMetadata overwriteMeta = new HoodieCommitMetadata();
    overwriteMeta.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    HoodieCommitMetadata clusterMeta = new HoodieCommitMetadata();
    clusterMeta.setOperationType(WriteOperationType.CLUSTER);
    when(timeline.readInstantContent(eq(realIngest), eq(HoodieCommitMetadata.class))).thenReturn(overwriteMeta);
    when(timeline.readInstantContent(eq(clustering), eq(HoodieCommitMetadata.class))).thenReturn(clusterMeta);

    // Only the INSERT_OVERWRITE replacecommit counts; clustering is skipped.
    assertEquals(1, SmallFileDetector.countIngestCommits(timeline));
  }

  @Test
  void countIngestCommitsSkipsUnreadableReplaceCommitBody() throws Exception {
    // A failed deserialize should undercount rather than fail the whole run.
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    HoodieTimeline completed = mock(HoodieTimeline.class);
    when(timeline.filterCompletedInstants()).thenReturn(completed);
    HoodieInstant bad = instant(HoodieTimeline.REPLACE_COMMIT_ACTION);
    when(completed.getInstantsAsStream()).thenAnswer(inv -> Collections.singletonList(bad).stream());
    when(timeline.readInstantContent(eq(bad), eq(HoodieCommitMetadata.class))).thenThrow(new RuntimeException("corrupt"));

    assertEquals(0, SmallFileDetector.countIngestCommits(timeline));
  }

  // ---- countTotalIngestCommits: archived short-circuit ----

  @Test
  void countTotalIngestCommitsSkipsArchivedWhenActiveClearsGate() {
    // Build the timeline mock first — Mockito breaks if nested when(...) calls run inside another
    // when(...).thenReturn chain.
    HoodieActiveTimeline active = activeTimelineWithIngests(12);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenReturn(active);

    // Active alone (12) is >= gate (10) → archived never consulted (perf-critical on mature tables).
    assertEquals(12L, SmallFileDetector.countTotalIngestCommits(metaClient, SmallFileDetector.Config.defaults()));
    verify(metaClient, never()).getArchivedTimeline();
  }

  @Test
  void countTotalIngestCommitsConsultsArchivedWhenActiveUnderGate() {
    HoodieActiveTimeline active = activeTimelineWithIngests(3);
    HoodieArchivedTimeline archived = archivedTimelineWithIngests(8);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenReturn(active);
    when(metaClient.getArchivedTimeline()).thenReturn(archived);

    // active (3) + archived (8) = 11 — total across both timelines.
    assertEquals(11L, SmallFileDetector.countTotalIngestCommits(metaClient, SmallFileDetector.Config.defaults()));
    verify(metaClient).getArchivedTimeline();
  }

  @Test
  void countTotalIngestCommitsReturnsZeroOnFailure() {
    // A count failure must not fail the caller — it falls back to 0, which classifyVerdict then
    // maps to SKIPPED (the safe "no signal" default).
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenThrow(new RuntimeException("boom"));
    assertEquals(0L, SmallFileDetector.countTotalIngestCommits(metaClient, SmallFileDetector.Config.defaults()));
  }

  // ---- run: end-to-end wiring ----

  @Test
  void runReturnsSevereWhenAllPartitionsFlaggedAndCommitsClearGate() {
    HoodieActiveTimeline active = activeTimelineWithIngests(15);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenReturn(active);

    long fifty = SmallFileDetector.Config.DEFAULT_THRESHOLD_BYTES;
    List<SmallFileDetector.PartitionStats> parts = Arrays.asList(
        new SmallFileDetector.PartitionStats("p1", 5, 5 * 10 * 1024 * 1024L),   // avg 10MB, flagged
        new SmallFileDetector.PartitionStats("p2", 5, 5 * 20 * 1024 * 1024L),   // avg 20MB, flagged
        new SmallFileDetector.PartitionStats("p3", 5, 5 * (fifty - 1)));         // just under, flagged

    SmallFileDetector.Result r = SmallFileDetector.run(metaClient, parts, SmallFileDetector.Config.defaults());
    assertEquals(SmallFileDetector.Verdict.SEVERE, r.getVerdict());
    assertEquals(3L, r.getQualifyingPartitions());
    assertEquals(3L, r.getFlaggedPartitions());
    assertEquals(1.0, r.getFlaggedPct());
    assertEquals(15L, r.getTableIngestCommitCount());
  }

  @Test
  void runReturnsSkippedWhenCommitsUnderGateEvenIfEveryPartitionFlagged() {
    HoodieActiveTimeline active = activeTimelineWithIngests(3);
    HoodieArchivedTimeline archived = archivedTimelineWithIngests(2);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenReturn(active);
    when(metaClient.getArchivedTimeline()).thenReturn(archived);

    List<SmallFileDetector.PartitionStats> parts = Arrays.asList(
        new SmallFileDetector.PartitionStats("p1", 5, 5 * 1024L),
        new SmallFileDetector.PartitionStats("p2", 5, 5 * 1024L));
    SmallFileDetector.Result r = SmallFileDetector.run(metaClient, parts, SmallFileDetector.Config.defaults());
    assertEquals(SmallFileDetector.Verdict.SKIPPED, r.getVerdict());
    assertEquals(5L, r.getTableIngestCommitCount()); // 3 active + 2 archived
  }

  @Test
  void runReturnsCleanWhenNoPartitionQualifies() {
    HoodieActiveTimeline active = activeTimelineWithIngests(20);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getActiveTimeline()).thenReturn(active);

    // 4 partitions, each with 4 files (< MIN_FILES_PER_PARTITION=5) — none qualifies.
    List<SmallFileDetector.PartitionStats> parts = Arrays.asList(
        new SmallFileDetector.PartitionStats("p1", 4, 4),
        new SmallFileDetector.PartitionStats("p2", 4, 4),
        new SmallFileDetector.PartitionStats("p3", 4, 4),
        new SmallFileDetector.PartitionStats("p4", 4, 4));
    SmallFileDetector.Result r = SmallFileDetector.run(metaClient, parts, SmallFileDetector.Config.defaults());
    assertEquals(SmallFileDetector.Verdict.CLEAN, r.getVerdict());
    assertEquals(0L, r.getQualifyingPartitions());
    assertEquals(0L, r.getFlaggedPartitions());
  }

  // ---- helpers ----

  private static HoodieInstant instant(String action) {
    return new HoodieInstant(HoodieInstant.State.COMPLETED, action, "20240101000000000",
        InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
  }

  private static HoodieActiveTimeline activeTimelineWithIngests(int count) {
    HoodieActiveTimeline active = mock(HoodieActiveTimeline.class);
    HoodieTimeline completed = mock(HoodieTimeline.class);
    when(active.filterCompletedInstants()).thenReturn(completed);
    List<HoodieInstant> instants = IntStream.range(0, count)
        .mapToObj(i -> instant(HoodieTimeline.COMMIT_ACTION))
        .collect(java.util.stream.Collectors.toList());
    when(completed.getInstantsAsStream()).thenAnswer(inv -> instants.stream());
    return active;
  }

  private static HoodieArchivedTimeline archivedTimelineWithIngests(int count) {
    HoodieArchivedTimeline archived = mock(HoodieArchivedTimeline.class);
    HoodieTimeline completed = mock(HoodieTimeline.class);
    when(archived.filterCompletedInstants()).thenReturn(completed);
    List<HoodieInstant> instants = IntStream.range(0, count)
        .mapToObj(i -> instant(HoodieTimeline.DELTA_COMMIT_ACTION))
        .collect(java.util.stream.Collectors.toList());
    when(completed.getInstantsAsStream()).thenAnswer(inv -> instants.stream());
    return archived;
  }
}
