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

package org.apache.hudi.utils;

import static org.apache.hudi.common.util.CleanerUtils.convertCleanMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.utils.MetadataConversionUtils;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMetadataConversionUtils extends HoodieCommonTestHarness {

  @BeforeEach
  void init() throws IOException {
    initMetaClient();
  }

  @Test
  void testCompletedClean() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCleanMetadata(newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.CLEAN_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getHoodieCleanMetadata().getStartCleanTime(), newCommitTime);
  }

  @Test
  void testCleanerPlan() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCleanMetadata(newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.CLEAN_ACTION, newCommitTime), metaClient);
    assertEquals(State.INFLIGHT.toString(), metaEntry.getActionState());
    assertEquals(2, metaEntry.getHoodieCleanerPlan().getVersion());
  }

  @Test
  void testCompletedReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE, true, false);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getHoodieReplaceCommitMetadata().getOperationType(), WriteOperationType.INSERT_OVERWRITE.toString());
  }

  @Test
  void testEmptyCompletedReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE, true, true);
    HoodieInstant hoodieInstant = new HoodieInstant(State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(hoodieInstant, metaClient);

    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getTimestamp());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    archivedMetaWrapper.setStateTransitionTime(hoodieInstant.getStateTransitionTime());
    archivedMetaWrapper.setActionType(ActionType.replacecommit.name());
    assertEquals(archivedMetaWrapper, metaEntry);
  }

  @Test
  void testEmptyRequestedReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE_TABLE, false, true);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
            new HoodieInstant(State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.REQUESTED.toString());
    assertNull(metaEntry.getHoodieRequestedReplaceMetadata());
  }

  @Test
  void testEmptyInflightReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE_TABLE, true, true);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
            new HoodieInstant(State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.INFLIGHT.toString());
    assertNull(metaEntry.getHoodieInflightReplaceMetadata());
  }

  @Test
  void testNonEmptyInflightReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE_TABLE, false, false);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.INFLIGHT.toString());
    assertEquals(metaEntry.getHoodieInflightReplaceMetadata().getOperationType(), WriteOperationType.INSERT_OVERWRITE_TABLE.name());
  }

  @Test
  void testCompletedCommit() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCommitMetadata(newCommitTime, false);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getHoodieCommitMetadata().getOperationType(), WriteOperationType.INSERT.toString());
  }

  @Test
  void testEmptyCompletedCommit() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCommitMetadata(newCommitTime, true);
    HoodieInstant hoodieInstant = new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(hoodieInstant, metaClient);

    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getTimestamp());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    archivedMetaWrapper.setStateTransitionTime(hoodieInstant.getStateTransitionTime());
    archivedMetaWrapper.setActionType(ActionType.commit.name());
    assertEquals(archivedMetaWrapper, metaEntry);
  }

  @Test
  void testCompletedDeltaCommit() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createDeltaCommitMetadata(newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
            new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getActionType(), HoodieTimeline.DELTA_COMMIT_ACTION);
  }

  @Test
  void testCompletedRollback() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createRollbackMetadata(newCommitTime, false);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getHoodieRollbackMetadata().getStartRollbackTime(), newCommitTime);
  }

  @Test
  void testEmptyCompletedRollback() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createRollbackMetadata(newCommitTime, true);
    HoodieInstant hoodieInstant = new HoodieInstant(State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(hoodieInstant, metaClient);

    HoodieArchivedMetaEntry archivedMetaWrapper = new HoodieArchivedMetaEntry();
    archivedMetaWrapper.setCommitTime(hoodieInstant.getTimestamp());
    archivedMetaWrapper.setActionState(hoodieInstant.getState().name());
    archivedMetaWrapper.setStateTransitionTime(hoodieInstant.getStateTransitionTime());
    archivedMetaWrapper.setActionType(ActionType.rollback.name());
    assertEquals(archivedMetaWrapper, metaEntry);
  }

  @Test
  void testCompletedCompaction() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCompactionMetadata(newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getHoodieCommitMetadata().getOperationType(), WriteOperationType.COMPACT.toString());
  }

  @Test
  void testInflightCompaction() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCompactionPlanMetadata(newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, newCommitTime), metaClient);
    assertEquals(State.REQUESTED.toString(), metaEntry.getActionState());
    assertEquals(2, metaEntry.getHoodieCompactionPlan().getVersion());
  }

  @Test
  void testInflightLogCompaction() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createLogCompactionPlanMetadata(newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.LOG_COMPACTION_ACTION, newCommitTime), metaClient);
    assertEquals(State.REQUESTED.toString(), metaEntry.getActionState());
    assertEquals(2, metaEntry.getHoodieCompactionPlan().getVersion());
  }
  
  @Test
  void testSavepoint() throws Exception {
    String instantTime = HoodieTestTable.makeNewCommitTime();
    HoodieSavepointMetadata savepointMetadata = new HoodieSavepointMetadata();
    long time = System.currentTimeMillis();
    savepointMetadata.setSavepointedAt(time);
    savepointMetadata.setSavepointedBy("user");
    savepointMetadata.setVersion(1);
    savepointMetadata.setPartitionMetadata(Collections.emptyMap());
    savepointMetadata.setComments("");
    HoodieTestTable.of(metaClient)
        .addSavepoint(instantTime, savepointMetadata);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        new HoodieInstant(State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, instantTime), metaClient);
    assertEquals(State.COMPLETED.toString(), metaEntry.getActionState());
    assertEquals(time, metaEntry.getHoodieSavePointMetadata().getSavepointedAt());
    assertEquals("user", metaEntry.getHoodieSavePointMetadata().getSavepointedBy());
  }

  @Test
  void testConvertCommitMetadata() {
    HoodieCommitMetadata hoodieCommitMetadata = new HoodieCommitMetadata();
    hoodieCommitMetadata.setOperationType(WriteOperationType.INSERT);
    org.apache.hudi.avro.model.HoodieCommitMetadata expectedCommitMetadata = MetadataConversionUtils
        .convertCommitMetadata(hoodieCommitMetadata);
    assertEquals(expectedCommitMetadata.getOperationType(), WriteOperationType.INSERT.toString());
  }

  private void createCompactionMetadata(String instantTime) throws Exception {
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.setOperationType(WriteOperationType.COMPACT);
    commitMetadata.setCompacted(true);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createCompactionPlanMetadata(String instantTime) throws Exception {
    HoodieCompactionPlan plan = new HoodieCompactionPlan();
    plan.setVersion(2);
    HoodieTestTable.of(metaClient)
        .addRequestedCompaction(instantTime, plan);
  }
  
  private void createLogCompactionPlanMetadata(String instantTime) throws Exception {
    HoodieCompactionPlan plan = new HoodieCompactionPlan();
    plan.setVersion(2);
    HoodieTestTable.of(metaClient)
        .addRequestedLogCompaction(instantTime, plan);
  }

  private void createRollbackMetadata(String instantTime, boolean isEmpty) throws Exception {
    HoodieRollbackMetadata rollbackMetadata = new HoodieRollbackMetadata();
    rollbackMetadata.setCommitsRollback(Arrays.asList(instantTime));
    rollbackMetadata.setStartRollbackTime(instantTime);
    HoodieRollbackPartitionMetadata rollbackPartitionMetadata = new HoodieRollbackPartitionMetadata();
    rollbackPartitionMetadata.setPartitionPath("p1");
    rollbackPartitionMetadata.setSuccessDeleteFiles(Arrays.asList("f1"));
    rollbackPartitionMetadata.setFailedDeleteFiles(new ArrayList<>());
    rollbackPartitionMetadata.setRollbackLogFiles(new HashMap<>());
    Map<String, HoodieRollbackPartitionMetadata> partitionMetadataMap = new HashMap<>();
    partitionMetadataMap.put("p1", rollbackPartitionMetadata);
    rollbackMetadata.setPartitionMetadata(partitionMetadataMap);
    rollbackMetadata.setInstantsRollback(Arrays.asList(new HoodieInstantInfo("1", HoodieTimeline.COMMIT_ACTION)));
    HoodieTestTable.of(metaClient)
        .addRollback(instantTime, rollbackMetadata, null);
    HoodieTestTable.of(metaClient)
        .addRollbackCompleted(instantTime, rollbackMetadata, isEmpty);
  }

  private void createCommitMetadata(String instantTime, boolean isEmpty) throws Exception {
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, isEmpty ? Option.empty() : Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createDeltaCommitMetadata(String instantTime) throws Exception {
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";
    HoodieTestTable.of(metaClient)
            .addDeltaCommit(instantTime)
            .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createReplace(String instantTime, WriteOperationType writeOperationType, boolean isClustering, boolean isEmpty)
          throws Exception {
    String fileId1 = "file-1";
    String fileId2 = "file-2";

    // create replace instant to mark fileId1 as deleted
    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    Map<String, List<String>> partitionFileIds = new HashMap<>();
    partitionFileIds.put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, Arrays.asList(fileId2));
    replaceMetadata.setPartitionToReplaceFileIds(partitionFileIds);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId("file-1");
    replaceMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, writeStat);
    replaceMetadata.setOperationType(writeOperationType);
    // some cases requestedReplaceMetadata will be null
    // e.g. insert_overwrite_table or insert_overwrite without clustering
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = null;
    HoodieCommitMetadata inflightReplaceMetadata = null;
    if (isClustering) {
      requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
      requestedReplaceMetadata.setOperationType(writeOperationType.name());
      HoodieClusteringPlan clusteringPlan = new HoodieClusteringPlan();
      HoodieClusteringGroup clusteringGroup = new HoodieClusteringGroup();
      HoodieSliceInfo sliceInfo = new HoodieSliceInfo();
      clusteringGroup.setSlices(Arrays.asList(sliceInfo));
      clusteringPlan.setInputGroups(Arrays.asList(clusteringGroup));
      requestedReplaceMetadata.setClusteringPlan(clusteringPlan);
      requestedReplaceMetadata.setVersion(TimelineLayoutVersion.CURR_VERSION);
    } else {
      // inflightReplaceMetadata will be null in clustering but not null
      // in insert_overwrite or insert_overwrite_table
      inflightReplaceMetadata = new HoodieCommitMetadata();
      inflightReplaceMetadata.setOperationType(writeOperationType);
      inflightReplaceMetadata.setCompacted(false);
    }
    HoodieTestTable.of(metaClient)
        .addReplaceCommit(instantTime, Option.ofNullable(requestedReplaceMetadata), Option.ofNullable(inflightReplaceMetadata), isEmpty ? Option.empty() : Option.of(replaceMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createCleanMetadata(String instantTime) throws IOException {
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""),
        "", "", new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, new HashMap<>(), new ArrayList<>(), Collections.emptyMap());
    HoodieCleanStat cleanStats = new HoodieCleanStat(
        HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
        HoodieTestUtils.DEFAULT_PARTITION_PATHS[new Random().nextInt(HoodieTestUtils.DEFAULT_PARTITION_PATHS.length)],
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        instantTime,
        "");
    HoodieCleanMetadata cleanMetadata = convertCleanMetadata(instantTime, Option.of(0L), Collections.singletonList(cleanStats), Collections.emptyMap());
    HoodieTestTable.of(metaClient).addClean(instantTime, cleanerPlan, cleanMetadata);
  }
}
