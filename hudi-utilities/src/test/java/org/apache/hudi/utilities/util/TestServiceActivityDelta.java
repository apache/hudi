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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestServiceActivityDelta {

  private static final InstantStateIndex EMPTY_PREV = InstantStateIndex.from(TestTimelines.of());
  private static final HoodieTableMetaClient META_CLIENT = Mockito.mock(HoodieTableMetaClient.class);

  /**
   * The delta tracks clean and rollback activity and nothing else, so an ordinary commit that
   * landed during the run does not register as activity.
   *
   * <p>"What is instant X at T_aft" is not a question for this class; it goes to an
   * {@link InstantStateIndex} built from the aft timeline -- see
   * {@code TestFileDiffAdjudicator}'s aft-resolution cases, where the departure and savepoint
   * properties are pinned.
   */
  @Test
  void anOrdinaryCommitIsNotServiceActivity() {
    HoodieTimeline aft = TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "200"));

    ServiceActivityDelta dlt = ServiceActivityDelta.between(EMPTY_PREV, aft, META_CLIENT);

    assertTrue(dlt.noServiceActivity(),
        "a commit is not a clean, a rollback or a restore, and must not read as one");
    assertTrue(dlt.rolledBackInstants().isEmpty());
    assertTrue(dlt.fileNamesFreedByClean("2026/08/15").isEmpty());
  }

  @Test
  void normalisesRollbackFileNamesToBareNames() {
    // Rollback metadata mixes forms: MarkerBasedRollbackStrategy:189-190 stores a bare name,
    // :166 an absolute path, failedDeleteFiles full paths. Comparing unnormalised silently
    // matches nothing, which reads as a clean table.
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting()
        .withRollbackFreedFile("2026/08/15", "s3://bucket/tbl/2026/08/15/.fg1_100.log.1_0-1-2")
        .withRollbackFreedFile("2026/08/15", ".fg2_100.log.1_0-1-2")
        .build();

    assertEquals(new HashSet<>(Arrays.asList(".fg1_100.log.1_0-1-2", ".fg2_100.log.1_0-1-2")),
        dlt.fileNamesFreedByRollback("2026/08/15"));
  }

  @Test
  void keepsCleanAndRollbackSetsApartPerPartition() {
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting()
        .withCleanFreedFile("2026/08/15", "fg1_0-1-2_100.parquet")
        .withCleanFreedFile("2026/08/16", "fg9_0-1-2_100.parquet")
        .build();

    assertTrue(dlt.fileNamesFreedByClean("2026/08/15").contains("fg1_0-1-2_100.parquet"));
    assertFalse(dlt.fileNamesFreedByClean("2026/08/15").contains("fg9_0-1-2_100.parquet"));
    assertTrue(dlt.fileNamesFreedByRollback("2026/08/15").isEmpty());
  }

  @Test
  void recordsThatAPlanCouldNotBeRead() {
    ServiceActivityDelta dlt = ServiceActivityDelta.forTesting().withReadFailure("2026/08/15").build();

    assertTrue(dlt.readFailed());
    assertTrue(dlt.readFailedFor("2026/08/15"));
    assertFalse(dlt.readFailedFor("2026/08/16"));
  }

  @Test
  void betweenNormalisesMixedAbsoluteAndBareRollbackFileNamesToBareNames() {
    // Real rollback metadata mixes forms across these three fields depending on which code wrote
    // the entry. Comparing an unnormalised form against a file listing silently matches nothing,
    // which reads as a clean table -- this drives between() end to end through the real avro
    // deserialisation path, not just the forTesting() facade.
    String partition = "2026/08/15";
    HoodieRollbackPartitionMetadata partitionMetadata = HoodieRollbackPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setSuccessDeleteFiles(Collections.emptyList())
        .setFailedDeleteFiles(Collections.singletonList("s3://bucket/tbl/" + partition + "/fileA.parquet"))
        .setRollbackLogFiles(Collections.singletonMap("fileB.log.1_0-1-2", 100L))
        .setLogFilesFromFailedCommit(
            Collections.singletonMap("s3://bucket/tbl/" + partition + "/fileC.log.1_0-1-2", 200L))
        .build();
    HoodieRollbackMetadata rollbackMetadata = HoodieRollbackMetadata.newBuilder()
        .setStartRollbackTime("300")
        .setTimeTakenInMillis(10L)
        .setTotalFilesDeleted(3)
        .setCommitsRollback(Collections.singletonList("250"))
        .setPartitionMetadata(Collections.singletonMap(partition, partitionMetadata))
        .setVersion(2)
        .setInstantsRollback(Collections.emptyList())
        .build();
    HoodieInstant rollbackInstant = TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "300");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(rollbackInstant, TestTimelines.serialize(rollbackMetadata, HoodieRollbackMetadata.class)),
        rollbackInstant);

    ServiceActivityDelta dlt = ServiceActivityDelta.between(EMPTY_PREV, aft, META_CLIENT);

    assertEquals(new HashSet<>(Arrays.asList("fileA.parquet", "fileB.log.1_0-1-2", "fileC.log.1_0-1-2")),
        dlt.fileNamesFreedByRollback(partition));
    assertEquals(Collections.singleton("250"), dlt.rolledBackInstants());
    assertFalse(dlt.readFailed());
  }

  @Test
  void betweenNormalisesCleanFailedDeleteFileNamesToBareNames() {
    String partition = "2026/08/16";
    HoodieCleanPartitionMetadata cleanPartitionMetadata = HoodieCleanPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setPolicy("KEEP_LATEST_COMMITS")
        .setDeletePathPatterns(Collections.emptyList())
        .setSuccessDeleteFiles(Collections.emptyList())
        .setFailedDeleteFiles(Arrays.asList(
            "fg1_0-1-2_100.parquet", "s3://bucket/tbl/" + partition + "/fg2_0-1-2_100.parquet"))
        .setIsPartitionDeleted(false)
        .build();
    HoodieCleanMetadata cleanMetadata = HoodieCleanMetadata.newBuilder()
        .setStartCleanTime("400")
        .setTimeTakenInMillis(5L)
        .setTotalFilesDeleted(2)
        .setEarliestCommitToRetain("100")
        .setLastCompletedCommitTimestamp("390")
        .setPartitionMetadata(Collections.singletonMap(partition, cleanPartitionMetadata))
        .setVersion(2)
        .build();
    HoodieInstant cleanInstant = TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "400");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(cleanInstant, TestTimelines.serialize(cleanMetadata, HoodieCleanMetadata.class)),
        cleanInstant);

    ServiceActivityDelta dlt = ServiceActivityDelta.between(EMPTY_PREV, aft, META_CLIENT);

    assertEquals(new HashSet<>(Arrays.asList("fg1_0-1-2_100.parquet", "fg2_0-1-2_100.parquet")),
        dlt.fileNamesFreedByClean(partition));
    assertFalse(dlt.readFailed());
  }

  @Test
  void betweenAbsorbsACompletedCleansSuccessfulDeletesAsWellAsItsFailedOnes() {
    // The bug this pins: absorbing only failedDeleteFiles fails an undamaged table. The MDT view is
    // warmed at T_prev; a clean completes during phase 1 and deletes a file that is in the LATEST
    // file slice (partition deletion, or a file group replaced by a clustering or insert_overwrite
    // landing during the run); the executor lists the partition afterwards, so the file is
    // MDT_EXCLUSIVE with its writing instant COMPLETED at both reads. "The file no longer exists"
    // is not the absence of a finding on that side -- it IS the finding, and only the clean's
    // successful deletes withdraw it.
    String partition = "2026/08/18";
    HoodieCleanPartitionMetadata cleanPartitionMetadata = HoodieCleanPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setPolicy("KEEP_LATEST_COMMITS")
        // deletePathPatterns is exactly successDeleteFiles + failedDeleteFiles for every
        // non-bootstrap file (CleanActionExecutor:117-118 appends the same bare name to the pattern
        // list and to one of the two result lists), so it is populated here as a real clean would
        // populate it and is deliberately not read by ServiceActivityDelta.
        .setDeletePathPatterns(Arrays.asList(
            "fgDeleted_0-1-2_100.parquet", "fgKept_0-1-2_100.parquet"))
        .setSuccessDeleteFiles(Collections.singletonList(
            "s3://bucket/tbl/" + partition + "/fgDeleted_0-1-2_100.parquet"))
        .setFailedDeleteFiles(Collections.singletonList("fgKept_0-1-2_100.parquet"))
        .setIsPartitionDeleted(false)
        .build();
    HoodieInstant cleanInstant = TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "700");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(cleanInstant,
            TestTimelines.serialize(cleanMetadataOf(partition, cleanPartitionMetadata, "700"),
                HoodieCleanMetadata.class)),
        cleanInstant);

    ServiceActivityDelta dlt = ServiceActivityDelta.between(EMPTY_PREV, aft, META_CLIENT);

    assertEquals(new HashSet<>(Arrays.asList("fgDeleted_0-1-2_100.parquet", "fgKept_0-1-2_100.parquet")),
        dlt.fileNamesFreedByClean(partition));
    assertFalse(dlt.readFailed());
  }

  @Test
  void betweenIgnoresACleanThatHadAlreadyCompletedBeforeTPrev() {
    // Section 6.7: "A rollback that completed before T_prev is in neither set and deliberately not
    // consulted: its files were already deleted or already reconciled into MDT, so a survivor is a
    // genuine finding." The same holds for a clean. Reading the whole active window instead made
    // every orphan of every service still inside the archival horizon SANCTIONED, which withdrew
    // one of the two detections this validator exists for.
    String partition = "2026/08/19";
    HoodieCleanPartitionMetadata cleanPartitionMetadata = HoodieCleanPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setPolicy("KEEP_LATEST_COMMITS")
        .setDeletePathPatterns(Collections.emptyList())
        .setSuccessDeleteFiles(Collections.singletonList("fgOld_0-1-2_100.parquet"))
        .setFailedDeleteFiles(Collections.singletonList("fgStillThere_0-1-2_100.parquet"))
        .setIsPartitionDeleted(false)
        .build();
    HoodieInstant cleanInstant = TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.CLEAN_ACTION, "800");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(cleanInstant,
            TestTimelines.serialize(cleanMetadataOf(partition, cleanPartitionMetadata, "800"),
                HoodieCleanMetadata.class)),
        cleanInstant);
    // Completed at T_prev as well, so the run changed nothing about it.
    InstantStateIndex prev = InstantStateIndex.from(TestTimelines.of(cleanInstant));

    ServiceActivityDelta dlt = ServiceActivityDelta.between(prev, aft, META_CLIENT);

    assertTrue(dlt.fileNamesFreedByClean(partition).isEmpty());
    assertFalse(dlt.readFailed());
  }

  @Test
  void betweenIgnoresARollbackThatHadAlreadyCompletedBeforeTPrev() {
    String partition = "2026/08/20";
    HoodieRollbackPartitionMetadata partitionMetadata = HoodieRollbackPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setSuccessDeleteFiles(Collections.emptyList())
        .setFailedDeleteFiles(Collections.singletonList("fileA.parquet"))
        .build();
    HoodieRollbackMetadata rollbackMetadata = HoodieRollbackMetadata.newBuilder()
        .setStartRollbackTime("900")
        .setTimeTakenInMillis(10L)
        .setTotalFilesDeleted(1)
        .setCommitsRollback(Collections.singletonList("850"))
        .setPartitionMetadata(Collections.singletonMap(partition, partitionMetadata))
        .setVersion(2)
        .setInstantsRollback(Collections.emptyList())
        .build();
    HoodieInstant rollbackInstant = TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "900");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(rollbackInstant, TestTimelines.serialize(rollbackMetadata, HoodieRollbackMetadata.class)),
        rollbackInstant);
    InstantStateIndex prev = InstantStateIndex.from(TestTimelines.of(rollbackInstant));

    ServiceActivityDelta dlt = ServiceActivityDelta.between(prev, aft, META_CLIENT);

    assertTrue(dlt.rolledBackInstants().isEmpty());
    assertTrue(dlt.fileNamesFreedByRollback(partition).isEmpty());
    assertFalse(dlt.readFailed());
  }

  @Test
  void betweenStillAbsorbsARollbackThatCompletedDuringTheRun() {
    // The other side of the scoping rule, and the one that must not regress: a rollback pending at
    // T_prev and completed at T_aft is exactly what dlt exists to carry.
    String partition = "2026/08/21";
    HoodieRollbackPartitionMetadata partitionMetadata = HoodieRollbackPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setSuccessDeleteFiles(Collections.emptyList())
        .setFailedDeleteFiles(Collections.singletonList("fileA.parquet"))
        .build();
    HoodieRollbackMetadata rollbackMetadata = HoodieRollbackMetadata.newBuilder()
        .setStartRollbackTime("950")
        .setTimeTakenInMillis(10L)
        .setTotalFilesDeleted(1)
        .setCommitsRollback(Collections.singletonList("940"))
        .setPartitionMetadata(Collections.singletonMap(partition, partitionMetadata))
        .setVersion(2)
        .setInstantsRollback(Collections.emptyList())
        .build();
    HoodieInstant completedRollback =
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "950");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(completedRollback, TestTimelines.serialize(rollbackMetadata, HoodieRollbackMetadata.class)),
        completedRollback);
    InstantStateIndex prev = InstantStateIndex.from(TestTimelines.of(
        TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, "950")));

    ServiceActivityDelta dlt = ServiceActivityDelta.between(prev, aft, META_CLIENT);

    assertEquals(Collections.singleton("940"), dlt.rolledBackInstants());
    assertEquals(Collections.singleton("fileA.parquet"), dlt.fileNamesFreedByRollback(partition));
  }

  @Test
  void betweenIgnoresARestoreThatHadAlreadyCompletedBeforeTPrev() {
    String partition = "2026/08/22";
    HoodieRollbackPartitionMetadata partitionMetadata = HoodieRollbackPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setSuccessDeleteFiles(Collections.emptyList())
        .setFailedDeleteFiles(Collections.singletonList("fileR.parquet"))
        .build();
    HoodieRollbackMetadata inner = HoodieRollbackMetadata.newBuilder()
        .setStartRollbackTime("1000")
        .setTimeTakenInMillis(10L)
        .setTotalFilesDeleted(1)
        .setCommitsRollback(Collections.singletonList("990"))
        .setPartitionMetadata(Collections.singletonMap(partition, partitionMetadata))
        .setVersion(2)
        .setInstantsRollback(Collections.emptyList())
        .build();
    HoodieRestoreMetadata restoreMetadata = HoodieRestoreMetadata.newBuilder()
        .setStartRestoreTime("1000")
        .setTimeTakenInMillis(20L)
        .setInstantsToRollback(Collections.singletonList("990"))
        .setHoodieRestoreMetadata(
            Collections.singletonMap("990", Collections.singletonList(inner)))
        .setVersion(2)
        .setRestoreInstantInfo(Collections.emptyList())
        .build();
    HoodieInstant restoreInstant =
        TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.RESTORE_ACTION, "1000");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(restoreInstant, TestTimelines.serialize(restoreMetadata, HoodieRestoreMetadata.class)),
        restoreInstant);
    InstantStateIndex prev = InstantStateIndex.from(TestTimelines.of(restoreInstant));

    ServiceActivityDelta dlt = ServiceActivityDelta.between(prev, aft, META_CLIENT);

    assertTrue(dlt.rolledBackInstants().isEmpty());
    assertTrue(dlt.fileNamesFreedByRollback(partition).isEmpty());
    assertFalse(dlt.readFailed());
  }

  private static HoodieCleanMetadata cleanMetadataOf(String partition,
                                                     HoodieCleanPartitionMetadata partitionMetadata,
                                                     String startCleanTime) {
    return HoodieCleanMetadata.newBuilder()
        .setStartCleanTime(startCleanTime)
        .setTimeTakenInMillis(5L)
        .setTotalFilesDeleted(2)
        .setEarliestCommitToRetain("100")
        .setLastCompletedCommitTimestamp("090")
        .setPartitionMetadata(Collections.singletonMap(partition, partitionMetadata))
        .setVersion(2)
        .build();
  }

  @Test
  void betweenToleratesNullRollbackLogFileMapsAndAnEmptyCommitsRollback() {
    // rollbackLogFiles / logFilesFromFailedCommit are genuinely nullable (default null in the
    // .avsc) and left unset here. commitsRollback has no null/default option in the schema at all --
    // avro cannot encode a null value for it (a non-nullable array field), so an empty list is the
    // schema-legal stand-in for "nothing rolled back" exercising the same null-guard in
    // absorbRollbackPartitions / the between() commitsRollback check.
    String partition = "2026/08/17";
    HoodieRollbackPartitionMetadata partitionMetadata = HoodieRollbackPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setSuccessDeleteFiles(Collections.emptyList())
        .setFailedDeleteFiles(Collections.emptyList())
        .build();
    HoodieRollbackMetadata rollbackMetadata = HoodieRollbackMetadata.newBuilder()
        .setStartRollbackTime("500")
        .setTimeTakenInMillis(1L)
        .setTotalFilesDeleted(0)
        .setCommitsRollback(Collections.emptyList())
        .setPartitionMetadata(Collections.singletonMap(partition, partitionMetadata))
        .setVersion(2)
        .setInstantsRollback(Collections.emptyList())
        .build();
    HoodieInstant rollbackInstant = TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "500");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(rollbackInstant, TestTimelines.serialize(rollbackMetadata, HoodieRollbackMetadata.class)),
        rollbackInstant);

    ServiceActivityDelta dlt = ServiceActivityDelta.between(EMPTY_PREV, aft, META_CLIENT);

    assertTrue(dlt.rolledBackInstants().isEmpty());
    assertTrue(dlt.fileNamesFreedByRollback(partition).isEmpty());
    assertFalse(dlt.readFailed());
  }

  @Test
  void readFailedForIsConservativeWhenAFailureCannotBeAttributedToAPartition() {
    // The rollback metadata read itself fails (garbage content), before any partition inside it is
    // known. readFailurePartitions can name nothing, so readFailedFor must fall back to "yes" for
    // every partition asked about, rather than silently reporting them all clean.
    HoodieInstant rollbackInstant = TestTimelines.instant(HoodieInstant.State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, "600");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(rollbackInstant, "not a valid avro payload".getBytes(StandardCharsets.UTF_8)),
        rollbackInstant);

    ServiceActivityDelta dlt = ServiceActivityDelta.between(EMPTY_PREV, aft, META_CLIENT);

    assertTrue(dlt.readFailed());
    assertTrue(dlt.readFailedFor("2026/08/15"));
    assertTrue(dlt.readFailedFor("some/other/partition"));
  }

  @Test
  void betweenRecordsARequestedOnlyReadFailureWhenAPendingRollbackPlanCannotBeParsed() {
    // A .rollback.requested-only plan that fails to parse may simply not be fully flushed yet, so
    // it is weaker evidence than a hard failure: it must never answer readFailedFor, only the
    // dedicated requested-only accessor -- see PendingServiceReconciler#reconcile.
    HoodieInstant rollbackInstant = TestTimelines.instant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, "700");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(rollbackInstant, "not a valid avro payload".getBytes(StandardCharsets.UTF_8)),
        rollbackInstant);

    ServiceActivityDelta dlt = ServiceActivityDelta.between(EMPTY_PREV, aft, META_CLIENT);

    assertFalse(dlt.readFailedFor("2026/08/15"));
    assertFalse(dlt.readFailedFor("some/other/partition"));
    assertTrue(dlt.hasRequestedOnlyPlanReadFailure());
  }

  @Test
  void betweenRecordsAnUnattributedReadFailureWhenAnInflightRollbackPlanCannotBeParsed() {
    // The plan has already been promoted past .requested -- that race is over, so a plan that
    // still cannot be read at that point is genuine corruption, not a possible write in progress.
    HoodieInstant rollbackInstant = TestTimelines.instant(HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION, "700");
    HoodieTimeline aft = TestTimelines.withContent(
        Collections.singletonMap(rollbackInstant, "not a valid avro payload".getBytes(StandardCharsets.UTF_8)),
        rollbackInstant);

    ServiceActivityDelta dlt = ServiceActivityDelta.between(EMPTY_PREV, aft, META_CLIENT);

    assertTrue(dlt.readFailedFor("2026/08/15"));
    assertTrue(dlt.readFailedFor("some/other/partition"));
    assertFalse(dlt.hasRequestedOnlyPlanReadFailure());
  }
}
