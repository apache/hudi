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
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTimelineUtils extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @Test
  public void testGetPartitions() throws IOException {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue(activeCommitTimeline.empty());

    String olderPartition = "0"; // older partitions that is modified by all cleans
    for (int i = 1; i <= 5; i++) {
      String ts = i + "";
      HoodieInstant instant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, ts);
      activeTimeline.createNewInstant(instant);
      activeTimeline.saveAsComplete(instant, Option.of(getCommitMetadata(basePath, ts, ts, 2)));

      HoodieInstant cleanInstant = new HoodieInstant(true, HoodieTimeline.CLEAN_ACTION, ts);
      activeTimeline.createNewInstant(cleanInstant);
      activeTimeline.saveAsComplete(cleanInstant, getCleanMetadata(olderPartition, ts));
    }

    metaClient.reloadActiveTimeline();

    // verify modified partitions included cleaned data
    List<String> partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsAfter("1", 10));
    assertEquals(5, partitions.size());
    assertEquals(partitions, Arrays.asList(new String[]{"0", "2", "3", "4", "5"}));

    partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsInRange("1", "4"));
    assertEquals(4, partitions.size());
    assertEquals(partitions, Arrays.asList(new String[]{"0", "2", "3", "4"}));

    // verify only commit actions
    partitions = TimelineUtils.getPartitionsWritten(metaClient.getActiveTimeline().findInstantsAfter("1", 10));
    assertEquals(4, partitions.size());
    assertEquals(partitions, Arrays.asList(new String[]{"2", "3", "4", "5"}));

    partitions = TimelineUtils.getPartitionsWritten(metaClient.getActiveTimeline().findInstantsInRange("1", "4"));
    assertEquals(3, partitions.size());
    assertEquals(partitions, Arrays.asList(new String[]{"2", "3", "4"}));
  }

  @Test
  public void testGetPartitionsUnpartitioned() throws IOException {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline activeCommitTimeline = activeTimeline.getCommitTimeline();
    assertTrue(activeCommitTimeline.empty());

    String partitionPath = "";
    for (int i = 1; i <= 5; i++) {
      String ts = i + "";
      HoodieInstant instant = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, ts);
      activeTimeline.createNewInstant(instant);
      activeTimeline.saveAsComplete(instant, Option.of(getCommitMetadata(basePath, partitionPath, ts, 2)));

      HoodieInstant cleanInstant = new HoodieInstant(true, HoodieTimeline.CLEAN_ACTION, ts);
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
      activeTimeline.saveAsComplete(instant, Option.of(getRestoreMetadata(basePath, ts, ts, 2)));
    }

    metaClient.reloadActiveTimeline();

    // verify modified partitions included cleaned data
    List<String> partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsAfter("1", 10));
    assertEquals(partitions, Arrays.asList(new String[]{"2", "3", "4", "5"}));

    partitions = TimelineUtils.getAffectedPartitions(metaClient.getActiveTimeline().findInstantsInRange("1", "4"));
    assertEquals(partitions, Arrays.asList(new String[]{"2", "3", "4"}));
  }

  private byte[] getRestoreMetadata(String basePath, String partition, String commitTs, int count) throws IOException {
    HoodieRestoreMetadata metadata = new HoodieRestoreMetadata();
    List<HoodieRollbackMetadata> rollbackM = new ArrayList<>();
    rollbackM.add(getRollbackMetadataInstance(basePath, partition, commitTs, count));
    metadata.setHoodieRestoreMetadata(CollectionUtils.createImmutableMap(commitTs, rollbackM));
    List<String> rollbackInstants = new ArrayList<>();
    rollbackInstants.add(commitTs);
    metadata.setInstantsToRollback(rollbackInstants);
    metadata.setStartRestoreTime(commitTs);
    return TimelineMetadataUtils.serializeRestoreMetadata(metadata).get();
  }

  private HoodieRollbackMetadata getRollbackMetadataInstance(String basePath, String partition, String commitTs, int count) {
    List<String> deletedFiles = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      deletedFiles.add("file-" + i);
    }
    List<String> rollbacks = new ArrayList<>();
    rollbacks.add(commitTs);

    HoodieRollbackStat rollbackStat = new HoodieRollbackStat(partition, deletedFiles, Collections.emptyList(), Collections.emptyMap());
    List<HoodieRollbackStat> rollbackStats = new ArrayList<>();
    rollbackStats.add(rollbackStat);
    return TimelineMetadataUtils.convertRollbackMetadata(commitTs, Option.empty(), rollbacks, rollbackStats);
  }

  private byte[] getCommitMetadata(String basePath, String partition, String commitTs, int count)
      throws IOException {
    HoodieCommitMetadata commit = new HoodieCommitMetadata();
    for (int i = 1; i <= count; i++) {
      HoodieWriteStat stat = new HoodieWriteStat();
      stat.setFileId(i + "");
      stat.setPartitionPath(Paths.get(basePath, partition).toString());
      stat.setPath(commitTs + "." + i + ".parquet");
      commit.addWriteStat(partition, stat);
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
        .setPartitionMetadata(partitionToFilesCleaned).build();

    return TimelineMetadataUtils.serializeCleanMetadata(cleanMetadata);
  }

}