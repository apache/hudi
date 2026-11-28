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

import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantGeneratorV2;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link PendingServiceState#from} end to end through the real read path --
 * {@code CleanerUtils.getCleanerPlan} for clean plans (which reads via {@code
 * metaClient.getActiveTimeline()}, so that timeline is a mocked {@link HoodieActiveTimeline} rather
 * than the plain fake used elsewhere in this suite for the timeline handed to {@code from()} itself),
 * and {@code activeTimeline.readRollbackPlan} for rollback plans, which reads bytes directly off the
 * timeline passed to {@code from()}. Not just the {@link PendingServiceState#forTesting()} facade used
 * by {@link TestPendingServiceReconciler} -- this is the storage-reading path the reconciler never
 * touches itself, and it is the one place this class ever reads a real plan.
 */
class TestPendingServiceState {

  @Test
  void fromNormalisesRequestedCleanPlanFileNamesToBareNamesPerPartition() throws Exception {
    String partition = "2026/08/15";
    HoodieCleanerPlan cleanerPlan = HoodieCleanerPlan.newBuilder()
        .setPolicy("KEEP_LATEST_COMMITS")
        .setVersion(2)
        .setFilePathsToBeDeletedPerPartition(Collections.singletonMap(partition, Arrays.asList(
            HoodieCleanFileInfo.newBuilder().setFilePath("fg1_0-1-2_100.parquet").setIsBootstrapBaseFile(false).build(),
            HoodieCleanFileInfo.newBuilder()
                .setFilePath("s3://bucket/tbl/" + partition + "/fg2_0-1-2_100.parquet")
                .setIsBootstrapBaseFile(false).build())))
        .build();
    HoodieInstant cleanInstant = TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "100");
    HoodieTimeline activeTimeline = TestTimelines.of(cleanInstant);
    HoodieTableMetaClient metaClient = mockMetaClientReadingCleanerPlan(cleanInstant, cleanerPlan);

    PendingServiceState pending = PendingServiceState.from(activeTimeline, metaClient);

    assertEquals(new HashSet<>(Arrays.asList("fg1_0-1-2_100.parquet", "fg2_0-1-2_100.parquet")),
        pending.cleanPlannedFiles(partition));
    assertFalse(pending.readFailedFor(partition));
  }

  @Test
  void fromNormalisesRequestedRollbackPlanFileNamesAndTargetInstant() {
    String partition = "2026/08/16";
    HoodieRollbackRequest rollbackRequest = HoodieRollbackRequest.newBuilder()
        .setPartitionPath(partition)
        .setFileId("fg1")
        .setLatestBaseInstant("100")
        .setFilesToBeDeleted(Collections.singletonList("s3://bucket/tbl/" + partition + "/fg1_0-1-2_100.parquet"))
        .setLogBlocksToBeDeleted(Collections.singletonMap(".fg1_100.log.1_0-1-2", 100L))
        .build();
    HoodieRollbackPlan rollbackPlan = HoodieRollbackPlan.newBuilder()
        .setInstantToRollback(HoodieInstantInfo.newBuilder().setCommitTime("100").setAction("commit").build())
        .setRollbackRequests(Collections.singletonList(rollbackRequest))
        .setVersion(2)
        .build();
    HoodieInstant rollbackInstant = TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, "200");
    HoodieTimeline activeTimeline = TestTimelines.withContent(
        Collections.singletonMap(rollbackInstant, TestTimelines.serialize(rollbackPlan, HoodieRollbackPlan.class)),
        rollbackInstant);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getInstantGenerator()).thenReturn(new InstantGeneratorV2());

    PendingServiceState pending = PendingServiceState.from(activeTimeline, metaClient);

    assertEquals(new HashSet<>(Arrays.asList("fg1_0-1-2_100.parquet", ".fg1_100.log.1_0-1-2")),
        pending.rollbackPlannedFiles(partition));
    assertEquals(Collections.singleton("100"), pending.plannedRollbackInstants());
    assertFalse(pending.readFailedFor(partition));
  }

  @Test
  void fromResolvesAnInflightRollbackToItsRequestedPlanFileNamesAndTargetInstant() {
    // The plan content lives only in the .rollback.requested file: transitionRollbackRequestedToInflight
    // writes .rollback.inflight with no content, so reading directly off the enumerated INFLIGHT
    // instant would find nothing. from() must resolve to the requested-state instant first, mirroring
    // RollbackUtils#getRollbackPlan. If that resolve step were removed, this test would fail: the
    // fake timeline below stubs content only under the REQUESTED instant, not the INFLIGHT one.
    String partition = "2026/08/18";
    HoodieRollbackRequest rollbackRequest = HoodieRollbackRequest.newBuilder()
        .setPartitionPath(partition)
        .setFileId("fg1")
        .setLatestBaseInstant("500")
        .setFilesToBeDeleted(Collections.singletonList("s3://bucket/tbl/" + partition + "/fg1_0-1-2_500.parquet"))
        .setLogBlocksToBeDeleted(Collections.singletonMap(".fg1_500.log.1_0-1-2", 100L))
        .build();
    HoodieRollbackPlan rollbackPlan = HoodieRollbackPlan.newBuilder()
        .setInstantToRollback(HoodieInstantInfo.newBuilder().setCommitTime("500").setAction("commit").build())
        .setRollbackRequests(Collections.singletonList(rollbackRequest))
        .setVersion(2)
        .build();
    HoodieInstant inflightRollbackInstant = TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, "500");
    HoodieInstant requestedRollbackInstant = TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, "500");
    HoodieTimeline activeTimeline = TestTimelines.withContent(
        Collections.singletonMap(requestedRollbackInstant, TestTimelines.serialize(rollbackPlan, HoodieRollbackPlan.class)),
        inflightRollbackInstant);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getInstantGenerator()).thenReturn(new InstantGeneratorV2());

    PendingServiceState pending = PendingServiceState.from(activeTimeline, metaClient);

    assertEquals(new HashSet<>(Arrays.asList("fg1_0-1-2_500.parquet", ".fg1_500.log.1_0-1-2")),
        pending.rollbackPlannedFiles(partition));
    assertEquals(Collections.singleton("500"), pending.plannedRollbackInstants());
    // Scoped, not global: this inflight rollback resolving correctly must not blanket the table --
    // an unrelated partition must stay clean.
    assertFalse(pending.readFailedFor(partition));
    assertFalse(pending.readFailedFor("some/other/partition"));
  }

  @Test
  void fromNormalisesAnInflightCleanPlanFileNamesToBareNamesPerPartition() throws Exception {
    // CleanerUtils#getCleanRequestInstant already resolves a non-requested clean instant to its
    // requested form before reading (CleanerUtils.java:169-174), unlike the rollback path this suite
    // just added coverage for -- so this pins the clean path already had this right, rather than
    // leaving it as an unverified assumption.
    String partition = "2026/08/19";
    HoodieCleanerPlan cleanerPlan = HoodieCleanerPlan.newBuilder()
        .setPolicy("KEEP_LATEST_COMMITS")
        .setVersion(2)
        .setFilePathsToBeDeletedPerPartition(Collections.singletonMap(partition, Collections.singletonList(
            HoodieCleanFileInfo.newBuilder().setFilePath("fg1_0-1-2_600.parquet").setIsBootstrapBaseFile(false).build())))
        .build();
    HoodieInstant inflightCleanInstant = TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "600");
    HoodieInstant requestedCleanInstant = TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "600");
    HoodieTimeline activeTimeline = TestTimelines.of(inflightCleanInstant);
    byte[] bytes = TestTimelines.serialize(cleanerPlan, HoodieCleanerPlan.class);
    HoodieActiveTimeline mockActiveTimeline = Mockito.mock(HoodieActiveTimeline.class);
    Mockito.when(mockActiveTimeline.readCleanerPlan(requestedCleanInstant)).thenReturn(
        TimelineMetadataUtils.deserializeAvroMetadata(new ByteArrayInputStream(bytes), HoodieCleanerPlan.class));
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);
    Mockito.when(metaClient.getInstantGenerator()).thenReturn(new InstantGeneratorV2());

    PendingServiceState pending = PendingServiceState.from(activeTimeline, metaClient);

    assertEquals(Collections.singleton("fg1_0-1-2_600.parquet"), pending.cleanPlannedFiles(partition));
    assertFalse(pending.readFailedFor(partition));
    assertFalse(pending.readFailedFor("some/other/partition"));
  }

  @Test
  void fromRecordsARequestedOnlyReadFailureWhenAStillRequestedCleanPlanCannotBeParsedAtAll() throws Exception {
    // A .requested-only plan that fails to parse may simply not be fully flushed yet, so it is
    // weaker evidence than a hard failure: it must never answer readFailedFor, only the dedicated
    // requested-only accessor -- see PendingServiceReconciler#reconcile.
    HoodieInstant cleanInstant = TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "300");
    HoodieTimeline activeTimeline = TestTimelines.of(cleanInstant);
    HoodieActiveTimeline mockActiveTimeline = Mockito.mock(HoodieActiveTimeline.class);
    Mockito.when(mockActiveTimeline.readCleanerPlan(cleanInstant)).thenThrow(new IOException("corrupt plan"));
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);

    PendingServiceState pending = PendingServiceState.from(activeTimeline, metaClient);

    assertFalse(pending.readFailedFor("2026/08/15"));
    assertFalse(pending.readFailedFor("some/other/partition"));
    assertTrue(pending.hasRequestedOnlyPlanReadFailure());
  }

  @Test
  void fromRecordsAnUnattributedReadFailureWhenAnInflightCleanPlanCannotBeParsedAtAll() throws Exception {
    // The read fails before any partition inside the plan is known, and the plan has already been
    // promoted past .requested -- that race is over, so this is genuine corruption. The failure
    // cannot be scoped to a specific partition either, so it must fall back to "yes" for every
    // partition asked about. CleanerUtils#getCleanerPlan always resolves to the REQUESTED-state
    // instant before reading, whatever state was enumerated, so the stub must target that one.
    HoodieInstant cleanInstant = TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "300");
    HoodieInstant requestedInstant = TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "300");
    HoodieTimeline activeTimeline = TestTimelines.of(cleanInstant);
    HoodieActiveTimeline mockActiveTimeline = Mockito.mock(HoodieActiveTimeline.class);
    Mockito.when(mockActiveTimeline.readCleanerPlan(requestedInstant)).thenThrow(new IOException("corrupt plan"));
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);
    Mockito.when(metaClient.getInstantGenerator()).thenReturn(new InstantGeneratorV2());

    PendingServiceState pending = PendingServiceState.from(activeTimeline, metaClient);

    assertTrue(pending.readFailedFor("2026/08/15"));
    assertTrue(pending.readFailedFor("some/other/partition"));
    assertFalse(pending.hasRequestedOnlyPlanReadFailure());
  }

  @Test
  void fromIgnoresCompletedCleanAndRollbackInstantsBecauseOnlyPendingPlansMatter() {
    // A completed clean/rollback has already been accounted for by ServiceActivityDelta if it changed
    // between T_prev and T_aft; PendingServiceState only ever covers what a requested or inflight
    // plan (i.e. filterInflightsAndRequested) had already planned. Neither instant here has any
    // content stubbed, so this also proves neither is even read: filterInflightsAndRequested()
    // must exclude both up front.
    HoodieInstant completedClean = TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "400");
    HoodieInstant completedRollback = TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "410");
    HoodieTimeline activeTimeline = TestTimelines.of(completedClean, completedRollback);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);

    PendingServiceState pending = PendingServiceState.from(activeTimeline, metaClient);

    assertTrue(pending.cleanPlannedFiles("any/partition").isEmpty());
    assertTrue(pending.rollbackPlannedFiles("any/partition").isEmpty());
    assertTrue(pending.plannedRollbackInstants().isEmpty());
    assertFalse(pending.readFailedFor("any/partition"));
  }

  /**
   * The gap {@link PendingServiceState#across} exists to close: a clean <b>scheduled during the
   * run</b>.
   *
   * <p>It is absent from {@code T_prev} entirely, so the prev walk cannot see it, and it is still
   * pending at {@code T_aft}, so it never completed and {@code dlt}'s completed-clean loop skips
   * it. Its plan nonetheless names files it is deleting right now, and a file it has already
   * removed is a difference the run will observe. Reading only {@code T_prev} left that population
   * with nothing to explain it.
   */
  @Test
  void acrossCoversACleanScheduledDuringTheRun() throws Exception {
    String partition = "2026/08/17";
    HoodieCleanerPlan cleanerPlan = HoodieCleanerPlan.newBuilder()
        .setPolicy("KEEP_LATEST_COMMITS")
        .setVersion(2)
        .setFilePathsToBeDeletedPerPartition(Collections.singletonMap(partition,
            Collections.singletonList(HoodieCleanFileInfo.newBuilder()
                .setFilePath("fg1_0-1-2_100.parquet").setIsBootstrapBaseFile(false).build())))
        .build();
    HoodieInstant scheduledDuringTheRun =
        TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "300");
    // T_prev holds nothing pending at all; the clean appears only in T_aft.
    HoodieTimeline prev = TestTimelines.of();
    HoodieTimeline aft = TestTimelines.of(scheduledDuringTheRun);
    HoodieTableMetaClient metaClient =
        mockMetaClientReadingCleanerPlan(scheduledDuringTheRun, cleanerPlan);

    assertTrue(PendingServiceState.from(prev, metaClient).cleanPlannedFiles(partition).isEmpty(),
        "fixture check: the prev read alone sees nothing, which is the gap");

    PendingServiceState pending = PendingServiceState.across(prev, aft, metaClient);

    assertEquals(Collections.singleton("fg1_0-1-2_100.parquet"),
        pending.cleanPlannedFiles(partition));
  }

  /**
   * A clean pending at BOTH reads is reduced once, not twice.
   *
   * <p>The common case for a long-running clean, and the one that would silently double this
   * walk's storage reads. Asserted on the read itself rather than on the result, because the union
   * is idempotent and would look identical either way.
   */
  @Test
  void acrossReadsAServicePendingAtBothReadsOnlyOnce() throws Exception {
    String partition = "2026/08/18";
    HoodieCleanerPlan cleanerPlan = HoodieCleanerPlan.newBuilder()
        .setPolicy("KEEP_LATEST_COMMITS")
        .setVersion(2)
        .setFilePathsToBeDeletedPerPartition(Collections.singletonMap(partition,
            Collections.singletonList(HoodieCleanFileInfo.newBuilder()
                .setFilePath("fg1_0-1-2_100.parquet").setIsBootstrapBaseFile(false).build())))
        .build();
    // Requested at T_prev, inflight at T_aft: one service, two instant objects, one plan.
    HoodieInstant requested =
        TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, "400");
    HoodieInstant inflight =
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, "400");
    HoodieTableMetaClient metaClient = mockMetaClientReadingCleanerPlan(requested, cleanerPlan);

    PendingServiceState pending = PendingServiceState.across(
        TestTimelines.of(requested), TestTimelines.of(inflight), metaClient);

    assertEquals(Collections.singleton("fg1_0-1-2_100.parquet"),
        pending.cleanPlannedFiles(partition));
    // The aft walk must not have opened the plan again under the inflight instant.
    Mockito.verify(metaClient.getActiveTimeline(), Mockito.times(1)).readCleanerPlan(requested);
    Mockito.verify(metaClient.getActiveTimeline(), Mockito.never()).readCleanerPlan(inflight);
  }

  /**
   * A clean that had already completed before the run is never opened.
   *
   * <p>The bound this class must hold: the reconciliation's population is the run's delta, and
   * reducing every clean the table has ever performed would be an unbounded read against storage
   * for evidence that cannot bear on a file the run just observed.
   */
  @Test
  void acrossNeverReadsAServiceThatCompletedBeforeTheRun() {
    HoodieInstant longSinceCompleted =
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "50");
    HoodieActiveTimeline mockActiveTimeline = Mockito.mock(HoodieActiveTimeline.class);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);

    PendingServiceState pending = PendingServiceState.across(
        TestTimelines.of(longSinceCompleted), TestTimelines.of(longSinceCompleted), metaClient);

    assertTrue(pending.cleanPlannedFiles("any/partition").isEmpty());
    assertFalse(pending.readFailedFor("any/partition"));
    Mockito.verifyNoInteractions(mockActiveTimeline);
  }

  /**
   * {@code CleanerUtils.getCleanerPlan} always reads through {@code metaClient.getActiveTimeline()},
   * which returns {@link HoodieActiveTimeline} -- a different, richer type than the plain {@link
   * HoodieTimeline} {@code from()} itself walks to enumerate instants. A mock lets the real avro
   * bytes still be exercised through {@link TimelineMetadataUtils#deserializeAvroMetadata}, the same
   * deserialisation call the production {@code readCleanerPlan} default method makes.
   */
  private static HoodieTableMetaClient mockMetaClientReadingCleanerPlan(HoodieInstant cleanInstant,
                                                                        HoodieCleanerPlan plan) throws Exception {
    byte[] bytes = TestTimelines.serialize(plan, HoodieCleanerPlan.class);
    HoodieActiveTimeline mockActiveTimeline = Mockito.mock(HoodieActiveTimeline.class);
    Mockito.when(mockActiveTimeline.readCleanerPlan(cleanInstant)).thenReturn(
        TimelineMetadataUtils.deserializeAvroMetadata(new ByteArrayInputStream(bytes), HoodieCleanerPlan.class));
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(metaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);
    return metaClient;
  }
}
