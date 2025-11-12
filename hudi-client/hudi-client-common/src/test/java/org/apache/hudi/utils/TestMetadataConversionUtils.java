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

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieLSMTimelineInstant;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.MetadataConversionUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanV2MigrationHandler;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.deserializeAvroMetadata;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.apache.hudi.common.util.CleanerUtils.convertCleanMetadata;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestMetadataConversionUtils extends HoodieCommonTestHarness {

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
  }

  @Test
  public void testCompletedClean() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCleanMetadata(newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.CLEAN_ACTION, newCommitTime), metaClient);
    assertEquals(State.COMPLETED.toString(), metaEntry.getActionState());
    assertEquals(newCommitTime, metaEntry.getHoodieCleanMetadata().getStartCleanTime());
  }

  @Test
  public void testCompletedReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE, true);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getHoodieReplaceCommitMetadata().getOperationType(), WriteOperationType.INSERT_OVERWRITE.toString());
  }

  @Test
  public void testEmptyRequestedReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE_TABLE, false);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.REQUESTED.toString());
    assertNull(metaEntry.getHoodieRequestedReplaceMetadata());
  }

  @Test
  public void testEmptyInflightReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE_TABLE, true);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.INFLIGHT.toString());
    assertNull(metaEntry.getHoodieInflightReplaceMetadata());
  }

  @Test
  public void testNonEmptyInflightReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE_TABLE, false);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.INFLIGHT.toString());
    assertEquals(metaEntry.getHoodieInflightReplaceMetadata().getOperationType(), WriteOperationType.INSERT_OVERWRITE_TABLE.name());
  }

  @Test
  public void testCompletedCommit() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCommitMetadata(newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getHoodieCommitMetadata().getOperationType(), WriteOperationType.INSERT.toString());
  }

  @Test
  public void testCompletedDeltaCommit() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createDeltaCommitMetadata(newCommitTime);
    // Need to get the actual completed instant with the exact completion time from the timeline.
    HoodieInstant completedDeltaCommitInstant = metaClient.getActiveTimeline().getInstantsAsStream().filter(s -> s.isCompleted() && s.requestedTime().equals(newCommitTime))
        .findFirst().get();
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(completedDeltaCommitInstant, metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getActionType(), HoodieTimeline.DELTA_COMMIT_ACTION);
  }

  @Test
  public void testCompletedRollback() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createRollbackMetadata(newCommitTime);
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.ROLLBACK_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getHoodieRollbackMetadata().getStartRollbackTime(), newCommitTime);
  }

  @Test
  public void testCompletedCompaction() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCompactionMetadata(newCommitTime);
    // Creating compaction commits would initialize timeline(see HoodieTestTable.addRequestedCompaction)
    // Because of HUDI-6885, now HUDI have to iterator instants to get the correct completion time.
    // So we have to reload here to get all commits written before.
    metaClient.reloadActiveTimeline();
    HoodieArchivedMetaEntry metaEntry = MetadataConversionUtils.createMetaWrapper(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, newCommitTime), metaClient);
    assertEquals(metaEntry.getActionState(), State.COMPLETED.toString());
    assertEquals(metaEntry.getHoodieCommitMetadata().getOperationType(), WriteOperationType.COMPACT.toString());
  }

  @Test
  public void testConvertCommitMetadata() {
    HoodieCommitMetadata hoodieCommitMetadata = new HoodieCommitMetadata();
    hoodieCommitMetadata.setOperationType(WriteOperationType.INSERT);
    org.apache.hudi.avro.model.HoodieCommitMetadata expectedCommitMetadata = MetadataConversionUtils.convertCommitMetadataToAvro(hoodieCommitMetadata);
    assertEquals(expectedCommitMetadata.getOperationType(), WriteOperationType.INSERT.toString());
  }

  // -------------------------------------------------------------------------
  //  BEGIN: test cases for HoodieArchivedInstant conversion.
  // -------------------------------------------------------------------------

  @Test
  public void testArchivedClean() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCleanMetadata(newCommitTime);
    // test conversion to archived instant
    HoodieLSMTimelineInstant archived = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime), metaClient);
    assertEquals(newCommitTime, archived.getInstantTime());
    assertEquals(HoodieTimeline.CLEAN_ACTION, archived.getAction());
    assertDoesNotThrow(() -> CleanerUtils.getCleanerMetadata(metaClient, new ByteArrayInputStream(archived.getMetadata().array())));
    assertDoesNotThrow(() -> deserializeAvroMetadata(new ByteArrayInputStream(archived.getPlan().array()),  HoodieCleanerPlan.class));
  }

  @Test
  public void testArchivedReplace() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE, true);
    // test conversion to archived instant
    HoodieLSMTimelineInstant archived = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime), metaClient);
    assertEquals(newCommitTime, archived.getInstantTime());
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, archived.getAction());
    assertDoesNotThrow(() -> deserializeAvroMetadata(new ByteArrayInputStream(archived.getPlan().array()), HoodieRequestedReplaceMetadata.class));
  }

  @Test
  public void testArchivedInsertOverwriteWithoutClustering() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE, false);
    // test conversion to archived instant
    HoodieLSMTimelineInstant archived = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime), metaClient);
    assertEquals(newCommitTime, archived.getInstantTime());
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, archived.getAction());
    assertDoesNotThrow(() -> metaClient.getCommitMetadataSerDe().deserialize(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, newCommitTime),
            new ByteArrayInputStream(archived.getPlan().array()), () -> false, HoodieCommitMetadata.class),
        "Insert overwrite without clustering should have a plan");

    String newCommitTime2 = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime2, WriteOperationType.INSERT_OVERWRITE_TABLE, false);
    // test conversion to archived instant
    HoodieLSMTimelineInstant archived2 = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime2), metaClient);
    assertEquals(newCommitTime2, archived2.getInstantTime());
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, archived2.getAction());
    assertDoesNotThrow(() -> metaClient.getCommitMetadataSerDe().deserialize(
            INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, newCommitTime2),
            new ByteArrayInputStream(archived2.getPlan().array()), () -> false, HoodieCommitMetadata.class),
        "Insert overwrite table without clustering should have a plan");
  }

  @Test
  public void testArchivedInsertOverwriteWithClustering() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime, WriteOperationType.INSERT_OVERWRITE, true);
    // test conversion to archived instant
    HoodieLSMTimelineInstant archived = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime), metaClient);
    assertEquals(newCommitTime, archived.getInstantTime());
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, archived.getAction());
    assertDoesNotThrow(() -> deserializeAvroMetadata(new ByteArrayInputStream(archived.getPlan().array()), HoodieRequestedReplaceMetadata.class));

    String newCommitTime2 = HoodieTestTable.makeNewCommitTime();
    createReplace(newCommitTime2, WriteOperationType.INSERT_OVERWRITE_TABLE, true);
    // test conversion to archived instant
    HoodieLSMTimelineInstant archived2 = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime2), metaClient);
    assertEquals(newCommitTime2, archived2.getInstantTime());
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, archived2.getAction());
    assertDoesNotThrow(() -> deserializeAvroMetadata(new ByteArrayInputStream(archived2.getPlan().array()), HoodieRequestedReplaceMetadata.class));
  }

  @Test
  public void testArchivedCommit() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCommitMetadata(newCommitTime);
    HoodieLSMTimelineInstant archived = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime), metaClient);
    assertEquals(newCommitTime, archived.getInstantTime());
    assertEquals(HoodieTimeline.COMMIT_ACTION, archived.getAction());
    assertDoesNotThrow(() -> metaClient.getCommitMetadataSerDe().deserialize(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, newCommitTime),
        new ByteArrayInputStream(archived.getMetadata().array()),
        () -> false, HoodieCommitMetadata.class));
  }

  @Test
  public void testArchivedDeltaCommit() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createDeltaCommitMetadata(newCommitTime);
    HoodieLSMTimelineInstant archived = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime), metaClient);
    assertEquals(newCommitTime, archived.getInstantTime());
    assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, archived.getAction());
    assertNull(archived.getMetadata());
  }

  @Test
  public void testArchivedRollback() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createRollbackMetadata(newCommitTime);
    HoodieLSMTimelineInstant archived = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime), metaClient);
    assertEquals(newCommitTime, archived.getInstantTime());
    assertEquals(HoodieTimeline.ROLLBACK_ACTION, archived.getAction());
    assertDoesNotThrow(() -> deserializeAvroMetadata(new ByteArrayInputStream(archived.getMetadata().array()), HoodieRollbackMetadata.class));
    assertNull(archived.getPlan());
  }

  @Test
  public void testArchivedCompaction() throws Exception {
    String newCommitTime = HoodieTestTable.makeNewCommitTime();
    createCompactionMetadata(newCommitTime);
    HoodieLSMTimelineInstant archived = MetadataConversionUtils.createLSMTimelineInstant(getActiveInstant(newCommitTime), metaClient);
    assertEquals(newCommitTime, archived.getInstantTime());
    assertEquals(HoodieTimeline.COMMIT_ACTION, archived.getAction());
    assertDoesNotThrow(() -> metaClient.getCommitMetadataSerDe().deserialize(
        INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, newCommitTime),
        new ByteArrayInputStream(archived.getMetadata().array()),
        () -> false, HoodieCommitMetadata.class));
    assertDoesNotThrow(() -> CompactionUtils.getCompactionPlan(metaClient, new ByteArrayInputStream(archived.getPlan().array())));
  }

  // -------------------------------------------------------------------------
  //  END: test cases for HoodieArchivedInstant conversion.
  // -------------------------------------------------------------------------

  private void createCompactionMetadata(String instantTime) throws Exception {
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.setOperationType(WriteOperationType.COMPACT);
    commitMetadata.setCompacted(true);
    HoodieTestTable.of(metaClient)
        .addCompaction(instantTime, commitMetadata)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createRollbackMetadata(String instantTime) throws Exception {
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
        .addRollbackCompleted(instantTime, rollbackMetadata, false);
  }

  private void createCommitMetadata(String instantTime) throws Exception {
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata))
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createDeltaCommitMetadata(String instantTime) throws Exception {
    String fileId1 = "file-" + instantTime + "-1";
    String fileId2 = "file-" + instantTime + "-2";
    HoodieTestTable.of(metaClient)
        .addDeltaCommit(instantTime)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createReplace(String instantTime, WriteOperationType writeOperationType, Boolean isClustering)
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
        .addReplaceCommit(instantTime, Option.ofNullable(requestedReplaceMetadata), Option.ofNullable(inflightReplaceMetadata), replaceMetadata)
        .withBaseFilesInPartition(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, fileId1, fileId2);
  }

  private void createCleanMetadata(String instantTime) throws IOException {
    HoodieCleanFileInfo fileInfo = new HoodieCleanFileInfo("file1", false);
    HoodieCleanerPlan cleanerPlan = new HoodieCleanerPlan(new HoodieActionInstant("", "", ""),
        "", "", new HashMap<>(), CleanPlanV2MigrationHandler.VERSION, Collections.singletonMap("key", Collections.singletonList(fileInfo)), new ArrayList<>(), Collections.EMPTY_MAP);
    HoodieCleanStat cleanStats = new HoodieCleanStat(
        HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS,
        HoodieTestUtils.DEFAULT_PARTITION_PATHS[new Random().nextInt(HoodieTestUtils.DEFAULT_PARTITION_PATHS.length)],
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        instantTime,
        "");
    HoodieCleanMetadata cleanMetadata = convertCleanMetadata(instantTime, Option.of(0L), Collections.singletonList(cleanStats), Collections.EMPTY_MAP);
    HoodieTestTable.of(metaClient).addClean(instantTime, cleanerPlan, cleanMetadata);
  }

  private ActiveAction getActiveInstant(String instantTime) {
    HoodieActiveTimeline rawActiveTimeline = TIMELINE_FACTORY.createActiveTimeline(metaClient, false);
    List<HoodieInstant> instants = rawActiveTimeline.getInstantsAsStream().filter(instant -> instant.requestedTime().equals(instantTime)).collect(Collectors.toList());
    return ActiveAction.fromInstants(instants);
  }
}
