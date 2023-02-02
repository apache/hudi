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

package org.apache.hudi.common.table;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.SAVEPOINT_ACTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link TimelineUtils}.
 */
public class TestTimelineUtils extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testGetPartitionsWithReplaceCommits() throws IOException {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue(activeCommitTimeline.empty());

    String ts1 = "1";
    String replacePartition = "2021/01/01";
    String newFilePartition = "2021/01/02";
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, ts1);
    activeTimeline.createNewInstant(instant1);
    // create replace metadata only with replaced file Ids (no new files created)
    activeTimeline.saveAsComplete(instant1,
        Option.of(getReplaceCommitMetadata(basePath, ts1, replacePartition, 2,
            newFilePartition, 0, Collections.emptyMap(), WriteOperationType.CLUSTER)));
    metaClient.reloadActiveTimeline();

    List<String> partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsAfter("0", 10));
    assertEquals(1, partitions.size());
    assertEquals(replacePartition, partitions.get(0));

    String ts2 = "2";
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, ts2);
    activeTimeline.createNewInstant(instant2);
    // create replace metadata only with replaced file Ids (no new files created)
    activeTimeline.saveAsComplete(instant2,
        Option.of(getReplaceCommitMetadata(basePath, ts2, replacePartition, 0,
            newFilePartition, 3, Collections.emptyMap(), WriteOperationType.CLUSTER)));
    metaClient.reloadActiveTimeline();
    partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsAfter("1", 10));
    assertEquals(1, partitions.size());
    assertEquals(newFilePartition, partitions.get(0));

    partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsAfter("0", 10));
    assertEquals(2, partitions.size());
    assertTrue(partitions.contains(replacePartition));
    assertTrue(partitions.contains(newFilePartition));
  }

  @Test
  public void testGetPartitions() throws IOException {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue(activeCommitTimeline.empty());

    String olderPartition = "0"; // older partitions that is modified by all cleans
    for (int i = 1; i <= 5; i++) {
      String ts = i + "";
      HoodieInstant instant = new HoodieInstant(true, COMMIT_ACTION, ts);
      activeTimeline.createNewInstant(instant);
      activeTimeline.saveAsComplete(instant, Option.of(getCommitMetadata(basePath, ts, ts, 2, Collections.emptyMap())));

      HoodieInstant cleanInstant = new HoodieInstant(true, CLEAN_ACTION, ts);
      activeTimeline.createNewInstant(cleanInstant);
      activeTimeline.saveAsComplete(cleanInstant, getCleanMetadata(olderPartition, ts));
    }

    metaClient.reloadActiveTimeline();

    // verify modified partitions included cleaned data
    List<String> partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsAfter("1", 10));
    assertEquals(5, partitions.size());
    assertEquals(partitions, Arrays.asList("0", "2", "3", "4", "5"));

    partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsInRange("1", "4"));
    assertEquals(4, partitions.size());
    assertEquals(partitions, Arrays.asList("0", "2", "3", "4"));

    // verify only commit actions
    partitions = TimelineUtils.getWrittenPartitions(metaClient.getActiveTimeline().findInstantsAfter("1", 10));
    assertEquals(4, partitions.size());
    assertEquals(partitions, Arrays.asList("2", "3", "4", "5"));

    partitions = TimelineUtils.getWrittenPartitions(metaClient.getActiveTimeline().findInstantsInRange("1", "4"));
    assertEquals(3, partitions.size());
    assertEquals(partitions, Arrays.asList("2", "3", "4"));
  }

  @Test
  public void testGetPartitionsUnPartitioned() throws IOException {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue(activeCommitTimeline.empty());

    String partitionPath = "";
    for (int i = 1; i <= 5; i++) {
      String ts = i + "";
      HoodieInstant instant = new HoodieInstant(true, COMMIT_ACTION, ts);
      activeTimeline.createNewInstant(instant);
      activeTimeline.saveAsComplete(instant, Option.of(getCommitMetadata(basePath, partitionPath, ts, 2, Collections.emptyMap())));

      HoodieInstant cleanInstant = new HoodieInstant(true, CLEAN_ACTION, ts);
      activeTimeline.createNewInstant(cleanInstant);
      activeTimeline.saveAsComplete(cleanInstant, getCleanMetadata(partitionPath, ts));
    }

    metaClient.reloadActiveTimeline();

    // verify modified partitions included cleaned data
    List<String> partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsAfter("1", 10));
    assertTrue(partitions.isEmpty());

    partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsInRange("1", "4"));
    assertTrue(partitions.isEmpty());
  }

  @Test
  public void testRestoreInstants() throws Exception {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue(activeCommitTimeline.empty());

    for (int i = 1; i <= 5; i++) {
      String ts = i + "";
      HoodieInstant instant = new HoodieInstant(true, HoodieTimeline.RESTORE_ACTION, ts);
      activeTimeline.createNewInstant(instant);
      activeTimeline.saveAsComplete(instant, Option.of(getRestoreMetadata(basePath, ts, ts, 2, COMMIT_ACTION)));
    }

    metaClient.reloadActiveTimeline();

    // verify modified partitions included cleaned data
    List<String> partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsAfter("1", 10));
    assertEquals(partitions, Arrays.asList("2", "3", "4", "5"));

    partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsInRange("1", "4"));
    assertEquals(partitions, Arrays.asList("2", "3", "4"));
  }

  @Test
  public void testGetExtraMetadata() throws Exception {
    String extraMetadataKey = "test_key";
    String extraMetadataValue1 = "test_value1";
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue(activeCommitTimeline.empty());
    assertFalse(TimelineUtils.getExtraMetadataFromLatest(metaClient, extraMetadataKey).isPresent());

    String ts = "0";
    HoodieInstant instant = new HoodieInstant(true, COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(instant);
    activeTimeline.saveAsComplete(instant, Option.of(getCommitMetadata(basePath, ts, ts, 2, Collections.emptyMap())));

    ts = "1";
    instant = new HoodieInstant(true, COMMIT_ACTION, ts);
    activeTimeline.createNewInstant(instant);
    Map<String, String> extraMetadata = new HashMap<>();
    extraMetadata.put(extraMetadataKey, extraMetadataValue1);
    activeTimeline.saveAsComplete(instant, Option.of(getCommitMetadata(basePath, ts, ts, 2, extraMetadata)));

    metaClient.reloadActiveTimeline();

    // verify modified partitions included cleaned data
    verifyExtraMetadataLatestValue(extraMetadataKey, extraMetadataValue1, false);
    assertFalse(TimelineUtils.getExtraMetadataFromLatest(metaClient, "unknownKey").isPresent());

    // verify adding clustering commit doesn't change behavior of getExtraMetadataFromLatest
    String ts2 = "2";
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, ts2);
    activeTimeline.createNewInstant(instant2);
    String newValueForMetadata = "newValue2";
    extraMetadata.put(extraMetadataKey, newValueForMetadata);
    activeTimeline.saveAsComplete(instant2,
        Option.of(getReplaceCommitMetadata(basePath, ts2, "p2", 0,
            "p2", 3, extraMetadata, WriteOperationType.CLUSTER)));
    metaClient.reloadActiveTimeline();

    verifyExtraMetadataLatestValue(extraMetadataKey, extraMetadataValue1, false);
    verifyExtraMetadataLatestValue(extraMetadataKey, newValueForMetadata, true);
    assertFalse(TimelineUtils.getExtraMetadataFromLatest(metaClient, "unknownKey").isPresent());

    Map<String, Option<String>> extraMetadataEntries = TimelineUtils.getAllExtraMetadataForKey(metaClient, extraMetadataKey);
    assertEquals(3, extraMetadataEntries.size());
    assertFalse(extraMetadataEntries.get("0").isPresent());
    assertTrue(extraMetadataEntries.get("1").isPresent());
    assertEquals(extraMetadataValue1, extraMetadataEntries.get("1").get());
    assertTrue(extraMetadataEntries.get("2").isPresent());
    assertEquals(newValueForMetadata, extraMetadataEntries.get("2").get());
  }

  @Test
  public void testGetCommitsTimelineAfter() throws IOException {
    // Should only load active timeline
    String startTs = "010";
    HoodieTableMetaClient mockMetaClient = prepareMetaClient(
        Arrays.asList(
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "009"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "010"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "011"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "012")),
        Arrays.asList(new HoodieInstant(COMPLETED, COMMIT_ACTION, "001"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "002")),
        startTs
    );
    verifyTimeline(
        Arrays.asList(
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "011"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "012")),
        TimelineUtils.getCommitsTimelineAfter(mockMetaClient, startTs));
    verify(mockMetaClient, never()).getArchivedTimeline(any());

    // Should load both archived and active timeline
    startTs = "001";
    mockMetaClient = prepareMetaClient(
        Arrays.asList(
            new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "009"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "010"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "011"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "012")),
        Arrays.asList(new HoodieInstant(COMPLETED, COMMIT_ACTION, "001"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "002")),
        startTs
    );
    verifyTimeline(
        Arrays.asList(
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "002"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "010"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "011"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "012")),
        TimelineUtils.getCommitsTimelineAfter(mockMetaClient, startTs));
    verify(mockMetaClient, times(1)).getArchivedTimeline(any());
  }

  private HoodieTableMetaClient prepareMetaClient(
      List<HoodieInstant> activeInstants,
      List<HoodieInstant> archivedInstants,
      String startTs
  ) throws IOException {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieArchivedTimeline mockArchivedTimeline = mock(HoodieArchivedTimeline.class);
    when(mockMetaClient.scanHoodieInstantsFromFileSystem(any(), eq(true)))
        .thenReturn(activeInstants);
    HoodieActiveTimeline activeTimeline = new HoodieActiveTimeline(mockMetaClient);
    when(mockMetaClient.getActiveTimeline())
        .thenReturn(activeTimeline);
    when(mockMetaClient.getArchivedTimeline(any()))
        .thenReturn(mockArchivedTimeline);
    HoodieDefaultTimeline mergedTimeline = new HoodieDefaultTimeline(
        archivedInstants.stream()
            .filter(instant -> instant.getTimestamp().compareTo(startTs) >= 0),
        i -> Option.empty())
        .mergeTimeline(activeTimeline);
    when(mockArchivedTimeline.mergeTimeline(eq(activeTimeline)))
        .thenReturn(mergedTimeline);

    return mockMetaClient;
  }

  public void verifyTimeline(List<HoodieInstant> expectedInstants, HoodieTimeline timeline) {
    assertEquals(
        expectedInstants.stream().sorted().collect(Collectors.toList()),
        timeline.getInstants().stream().sorted().collect(Collectors.toList())
    );
  }

  @Test
  public void testGetEarliestInstantForMetadataArchival() throws IOException {
    // Empty timeline
    assertEquals(
        Option.empty(),
        TimelineUtils.getEarliestInstantForMetadataArchival(
            prepareActiveTimeline(new ArrayList<>()), false));

    // Earlier request clean action before commits
    assertEquals(
        Option.of(new HoodieInstant(REQUESTED, CLEAN_ACTION, "003")),
        TimelineUtils.getEarliestInstantForMetadataArchival(
            prepareActiveTimeline(
                Arrays.asList(
                    new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "001"),
                    new HoodieInstant(COMPLETED, CLEAN_ACTION, "002"),
                    new HoodieInstant(REQUESTED, CLEAN_ACTION, "003"),
                    new HoodieInstant(COMPLETED, COMMIT_ACTION, "010"),
                    new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "011"))), false));

    // No inflight instants
    assertEquals(
        Option.of(new HoodieInstant(COMPLETED, COMMIT_ACTION, "010")),
        TimelineUtils.getEarliestInstantForMetadataArchival(
            prepareActiveTimeline(
                Arrays.asList(
                    new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "001"),
                    new HoodieInstant(COMPLETED, CLEAN_ACTION, "002"),
                    new HoodieInstant(COMPLETED, CLEAN_ACTION, "003"),
                    new HoodieInstant(COMPLETED, COMMIT_ACTION, "010"),
                    new HoodieInstant(COMPLETED, REPLACE_COMMIT_ACTION, "011"))), false));

    // Rollbacks only
    assertEquals(
        Option.of(new HoodieInstant(INFLIGHT, ROLLBACK_ACTION, "003")),
        TimelineUtils.getEarliestInstantForMetadataArchival(
            prepareActiveTimeline(
                Arrays.asList(
                    new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "001"),
                    new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "002"),
                    new HoodieInstant(INFLIGHT, ROLLBACK_ACTION, "003"))), false));

    assertEquals(
        Option.empty(),
        TimelineUtils.getEarliestInstantForMetadataArchival(
            prepareActiveTimeline(
                Arrays.asList(
                    new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "001"),
                    new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "002"),
                    new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "003"))), false));

    // With savepoints
    HoodieActiveTimeline timeline = prepareActiveTimeline(
        Arrays.asList(
            new HoodieInstant(COMPLETED, ROLLBACK_ACTION, "001"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "003"),
            new HoodieInstant(COMPLETED, SAVEPOINT_ACTION, "003"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "010"),
            new HoodieInstant(COMPLETED, COMMIT_ACTION, "011")));
    assertEquals(
        Option.of(new HoodieInstant(COMPLETED, COMMIT_ACTION, "003")),
        TimelineUtils.getEarliestInstantForMetadataArchival(timeline, false));
    assertEquals(
        Option.of(new HoodieInstant(COMPLETED, COMMIT_ACTION, "010")),
        TimelineUtils.getEarliestInstantForMetadataArchival(timeline, true));
  }

  private HoodieActiveTimeline prepareActiveTimeline(
      List<HoodieInstant> activeInstants) throws IOException {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockMetaClient.scanHoodieInstantsFromFileSystem(any(), eq(true)))
        .thenReturn(activeInstants);
    return new HoodieActiveTimeline(mockMetaClient);
  }

  private void verifyExtraMetadataLatestValue(String extraMetadataKey, String expected, boolean includeClustering) {
    final Option<String> extraLatestValue;
    if (includeClustering) {
      extraLatestValue = TimelineUtils.getExtraMetadataFromLatestIncludeClustering(metaClient, extraMetadataKey);
    } else {
      extraLatestValue = TimelineUtils.getExtraMetadataFromLatest(metaClient, extraMetadataKey);
    }
    assertTrue(extraLatestValue.isPresent());
    assertEquals(expected, extraLatestValue.get());
  }

  private byte[] getRestoreMetadata(String basePath, String partition, String commitTs, int count, String actionType) throws IOException {
    List<HoodieRollbackMetadata> rollbackM = new ArrayList<>();
    rollbackM.add(getRollbackMetadataInstance(basePath, partition, commitTs, count, actionType));
    List<HoodieInstant> rollbackInstants = new ArrayList<>();
    rollbackInstants.add(new HoodieInstant(false, commitTs, actionType));
    HoodieRestoreMetadata metadata = TimelineMetadataUtils.convertRestoreMetadata(commitTs, 200, rollbackInstants,
        CollectionUtils.createImmutableMap(commitTs, rollbackM));
    return TimelineMetadataUtils.serializeRestoreMetadata(metadata).get();
  }

  private HoodieRollbackMetadata getRollbackMetadataInstance(String basePath, String partition, String commitTs, int count, String actionType) {
    List<String> deletedFiles = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      deletedFiles.add("file-" + i);
    }
    List<HoodieInstant> rollbacks = new ArrayList<>();
    rollbacks.add(new HoodieInstant(false, actionType, commitTs));

    HoodieRollbackStat rollbackStat = new HoodieRollbackStat(partition, deletedFiles, Collections.emptyList(), Collections.emptyMap());
    List<HoodieRollbackStat> rollbackStats = new ArrayList<>();
    rollbackStats.add(rollbackStat);
    return TimelineMetadataUtils.convertRollbackMetadata(commitTs, Option.empty(), rollbacks, rollbackStats);
  }

  private byte[] getCommitMetadata(String basePath, String partition, String commitTs, int count, Map<String, String> extraMetadata)
      throws IOException {
    HoodieCommitMetadata commit = new HoodieCommitMetadata();
    for (int i = 1; i <= count; i++) {
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setFileId(i + "");
      stat.setPartitionPath(Paths.get(basePath, partition).toString());
      stat.setPath(commitTs + "." + i + metaClient.getTableConfig().getBaseFileFormat().getFileExtension());
      commit.addWriteStat(partition, stat);
    }
    for (Map.Entry<String, String> extraEntries : extraMetadata.entrySet()) {
      commit.addMetadata(extraEntries.getKey(), extraEntries.getValue());
    }
    return commit.toJsonString().getBytes(StandardCharsets.UTF_8);
  }

  private byte[] getReplaceCommitMetadata(String basePath, String commitTs, String replacePartition, int replaceCount,
                                          String newFilePartition, int newFileCount, Map<String, String> extraMetadata,
                                          WriteOperationType operationType)
      throws IOException {
    HoodieReplaceCommitMetadata commit = new HoodieReplaceCommitMetadata();
    commit.setOperationType(operationType);
    for (int i = 1; i <= newFileCount; i++) {
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setFileId(i + "");
      stat.setPartitionPath(Paths.get(basePath, newFilePartition).toString());
      stat.setPath(commitTs + "." + i + metaClient.getTableConfig().getBaseFileFormat().getFileExtension());
      commit.addWriteStat(newFilePartition, stat);
    }
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    if (replaceCount > 0) {
      partitionToReplaceFileIds.put(replacePartition, new ArrayList<>());
    }
    for (int i = 1; i <= replaceCount; i++) {
      partitionToReplaceFileIds.get(replacePartition).add(FSUtils.createNewFileIdPfx());
    }
    commit.setPartitionToReplaceFileIds(partitionToReplaceFileIds);
    for (Map.Entry<String, String> extraEntries : extraMetadata.entrySet()) {
      commit.addMetadata(extraEntries.getKey(), extraEntries.getValue());
    }
    return commit.toJsonString().getBytes(StandardCharsets.UTF_8);
  }

  private Option<byte[]> getCleanMetadata(String partition, String time) throws IOException {
    Map<String, HoodieCleanPartitionMetadata> partitionToFilesCleaned = new HashMap<>();
    List<String> filesDeleted = new ArrayList<>();
    filesDeleted.add("file-" + partition + "-" + time + "1");
    filesDeleted.add("file-" + partition + "-" + time + "2");
    HoodieCleanPartitionMetadata partitionMetadata = HoodieCleanPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
        .setFailedDeleteFiles(Collections.emptyList())
        .setDeletePathPatterns(Collections.emptyList())
        .setSuccessDeleteFiles(filesDeleted)
        .build();
    partitionToFilesCleaned.putIfAbsent(partition, partitionMetadata);
    HoodieCleanMetadata cleanMetadata = HoodieCleanMetadata.newBuilder()
        .setVersion(1)
        .setTimeTakenInMillis(100)
        .setTotalFilesDeleted(1)
        .setStartCleanTime(time)
        .setEarliestCommitToRetain(time)
        .setLastCompletedCommitTimestamp("")
        .setPartitionMetadata(partitionToFilesCleaned).build();

    return TimelineMetadataUtils.serializeCleanMetadata(cleanMetadata);
  }

}
