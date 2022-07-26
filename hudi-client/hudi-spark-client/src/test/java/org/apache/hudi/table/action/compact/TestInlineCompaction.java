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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.config.HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestInlineCompaction extends CompactionTestBase {

  private HoodieWriteConfig getConfigForInlineCompaction(int maxDeltaCommits, int maxDeltaTime, CompactionTriggerStrategy inlineCompactionType) {
    return getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(true)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommits)
            .withMaxDeltaSecondsBeforeCompaction(maxDeltaTime)
            .withInlineCompactionTriggerStrategy(inlineCompactionType).build())
        .build();
  }

  @Test
  public void testCompactionIsNotScheduledEarly() throws Exception {
    // Given: make two commits
    HoodieWriteConfig cfg = getConfigForInlineCompaction(3, 60, CompactionTriggerStrategy.NUM_COMMITS);
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(HoodieActiveTimeline.createNewInstantTime(), 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      List<String> instants = IntStream.range(0, 2).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();

      // Then: ensure no compaction is executed since there are only 2 delta commits
      assertEquals(2, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
    }
  }

  @Test
  public void testSuccessfulCompactionBasedOnNumCommits() throws Exception {
    // Given: make three commits
    HoodieWriteConfig cfg = getConfigForInlineCompaction(3, 60, CompactionTriggerStrategy.NUM_COMMITS);
    List<String> instants = IntStream.range(0, 2).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());

    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(instants.get(0), 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());

      // third commit, that will trigger compaction
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      String finalInstant = HoodieActiveTimeline.createNewInstantTime();
      createNextDeltaCommit(finalInstant, dataGen.generateUpdates(finalInstant, 100), writeClient, metaClient, cfg, false);

      // Then: ensure the file slices are compacted as per policy
      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      assertEquals(4, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
      assertEquals(HoodieTimeline.COMMIT_ACTION, metaClient.getActiveTimeline().lastInstant().get().getAction());
      String compactionTime = metaClient.getActiveTimeline().lastInstant().get().getTimestamp();
      assertFalse(WriteMarkersFactory.get(cfg.getMarkersType(), HoodieSparkTable.create(cfg, context), compactionTime).doesMarkerDirExist());
    }
  }

  @Test
  public void testSuccessfulCompactionBasedOnNumAfterCompactionRequest() throws Exception {
    // Given: make 4 commits
    HoodieWriteConfig cfg = getConfigForInlineCompaction(4, 60, CompactionTriggerStrategy.NUM_COMMITS_AFTER_REQUEST);

    List<String> instants = IntStream.range(0, 3).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());

    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(instants.get(0), 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());

      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();

      cfg.setValue(INLINE_COMPACT_NUM_DELTA_COMMITS,"1");
      String requestInstant = HoodieActiveTimeline.createNewInstantTime();
      // add one compaction request
      scheduleCompaction(requestInstant, writeClient, cfg);
      metaClient.getActiveTimeline().reload();
      assertEquals(metaClient.getActiveTimeline().getInstants().filter(hoodieInstant -> hoodieInstant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)
            && hoodieInstant.getState() == HoodieInstant.State.REQUESTED).count(), 1);
      // can not add request second time
      requestInstant = HoodieActiveTimeline.createNewInstantTime();
      try {
        scheduleCompaction(requestInstant, writeClient, cfg);
        Assertions.fail();
      } catch (AssertionError error) {
        //should be here
      }
      cfg.setValue(INLINE_COMPACT_NUM_DELTA_COMMITS,"4");

      // this commit won't generate another compaction request only trigger last compaction request
      String finalInstant = HoodieActiveTimeline.createNewInstantTime();
      createNextDeltaCommit(finalInstant, dataGen.generateUpdates(finalInstant, 100), writeClient, metaClient, cfg, false);

      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();

      assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().filter(instant -> instant.getAction().equals(HoodieTimeline.COMMIT_ACTION))
            .countInstants(), 1);
      assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().filterPendingCompactionTimeline().countInstants(), 0);
      assertEquals(HoodieTimeline.DELTA_COMMIT_ACTION, metaClient.getActiveTimeline().lastInstant().get().getAction());
    }
  }

  @Test
  public void testSuccessfulCompactionBasedOnTime() throws Exception {
    // Given: make one commit
    HoodieWriteConfig cfg = getConfigForInlineCompaction(5, 10, CompactionTriggerStrategy.TIME_ELAPSED);

    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      String instantTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 10);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, Arrays.asList(instantTime), records, cfg, true, new ArrayList<>());

      // after 10s, that will trigger compaction
      String finalInstant = HoodieActiveTimeline.createNewInstantTime(10000);
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      createNextDeltaCommit(finalInstant, dataGen.generateUpdates(finalInstant, 100), writeClient, metaClient, cfg, false);

      // Then: ensure the file slices are compacted as per policy
      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      assertEquals(3, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
      assertEquals(HoodieTimeline.COMMIT_ACTION, metaClient.getActiveTimeline().lastInstant().get().getAction());
    }
  }

  @Test
  public void testSuccessfulCompactionBasedOnNumOrTime() throws Exception {
    // Given: make three commits
    HoodieWriteConfig cfg = getConfigForInlineCompaction(3, 60, CompactionTriggerStrategy.NUM_OR_TIME);
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(HoodieActiveTimeline.createNewInstantTime(), 10);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      List<String> instants = IntStream.range(0, 2).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());
      // Then: trigger the compaction because reach 3 commits.
      String finalInstant = HoodieActiveTimeline.createNewInstantTime();
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      createNextDeltaCommit(finalInstant, dataGen.generateUpdates(finalInstant, 10), writeClient, metaClient, cfg, false);

      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      assertEquals(4, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
      // 4th commit, that will trigger compaction because reach the time elapsed
      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      finalInstant = HoodieActiveTimeline.createNewInstantTime(60000);
      createNextDeltaCommit(finalInstant, dataGen.generateUpdates(finalInstant, 10), writeClient, metaClient, cfg, false);

      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      assertEquals(6, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
    }
  }

  @Test
  public void testSuccessfulCompactionBasedOnNumAndTime() throws Exception {
    // Given: make three commits
    HoodieWriteConfig cfg = getConfigForInlineCompaction(3, 20, CompactionTriggerStrategy.NUM_AND_TIME);
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(HoodieActiveTimeline.createNewInstantTime(), 10);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      List<String> instants = IntStream.range(0, 3).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();

      // Then: ensure no compaction is executed since there are only 3 delta commits
      assertEquals(3, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
      // 4th commit, that will trigger compaction
      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      String finalInstant = HoodieActiveTimeline.createNewInstantTime(20000);
      createNextDeltaCommit(finalInstant, dataGen.generateUpdates(finalInstant, 10), writeClient, metaClient, cfg, false);

      metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      assertEquals(5, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
    }
  }

  @Test
  public void testCompactionRetryOnFailureBasedOnNumCommits() throws Exception {
    // Given: two commits, schedule compaction and its failed/in-flight
    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
    List<String> instants = IntStream.range(0, 2).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());
    String instantTime2;
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(instants.get(0), 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());
      // Schedule compaction instant2, make it in-flight (simulates inline compaction failing)
      instantTime2 = HoodieActiveTimeline.createNewInstantTime();
      scheduleCompaction(instantTime2, writeClient, cfg);
      moveCompactionFromRequestedToInflight(instantTime2, cfg);
    }

    // When: a third commit happens
    HoodieWriteConfig inlineCfg = getConfigForInlineCompaction(2, 60, CompactionTriggerStrategy.NUM_COMMITS);
    String instantTime3 = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(inlineCfg)) {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      createNextDeltaCommit(instantTime3, dataGen.generateUpdates(instantTime3, 100), writeClient, metaClient, inlineCfg, false);
    }

    // Then: 1 delta commit is done, the failed compaction is retried
    metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
    assertEquals(4, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
    assertEquals(instantTime2, metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().firstInstant().get().getTimestamp());
  }

  @Test
  public void testCompactionRetryOnFailureBasedOnTime() throws Exception {
    // Given: two commits, schedule compaction and its failed/in-flight
    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxDeltaSecondsBeforeCompaction(5)
            .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.TIME_ELAPSED).build())
        .build();
    String instantTime;
    List<String> instants = IntStream.range(0, 2).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(instants.get(0), 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());
      // Schedule compaction instantTime, make it in-flight (simulates inline compaction failing)
      instantTime = HoodieActiveTimeline.createNewInstantTime(10000);
      scheduleCompaction(instantTime, writeClient, cfg);
      moveCompactionFromRequestedToInflight(instantTime, cfg);
    }

    // When: commit happens after 10s
    HoodieWriteConfig inlineCfg = getConfigForInlineCompaction(5, 10, CompactionTriggerStrategy.TIME_ELAPSED);
    String instantTime2;
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(inlineCfg)) {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      instantTime2 = HoodieActiveTimeline.createNewInstantTime();
      createNextDeltaCommit(instantTime2, dataGen.generateUpdates(instantTime2, 10), writeClient, metaClient, inlineCfg, false);
    }

    // Then: 1 delta commit is done, the failed compaction is retried
    metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
    assertEquals(4, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
    assertEquals(instantTime, metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().firstInstant().get().getTimestamp());
  }

  @Test
  public void testCompactionRetryOnFailureBasedOnNumAndTime() throws Exception {
    // Given: two commits, schedule compaction and its failed/in-flight
    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxDeltaSecondsBeforeCompaction(1)
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.NUM_AND_TIME).build())
        .build();
    String instantTime;
    List<String> instants = IntStream.range(0, 2).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(instants.get(0), 10);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());
      // Schedule compaction instantTime, make it in-flight (simulates inline compaction failing)
      instantTime = HoodieActiveTimeline.createNewInstantTime();
      scheduleCompaction(instantTime, writeClient, cfg);
      moveCompactionFromRequestedToInflight(instantTime, cfg);
    }

    // When: a third commit happens
    HoodieWriteConfig inlineCfg = getConfigForInlineCompaction(3, 20, CompactionTriggerStrategy.NUM_OR_TIME);
    String instantTime2;
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(inlineCfg)) {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
      instantTime2 = HoodieActiveTimeline.createNewInstantTime();
      createNextDeltaCommit(instantTime2, dataGen.generateUpdates(instantTime2, 10), writeClient, metaClient, inlineCfg, false);
    }

    // Then: 1 delta commit is done, the failed compaction is retried
    metaClient = HoodieTableMetaClient.builder().setConf(hadoopConf).setBasePath(cfg.getBasePath()).build();
    assertEquals(4, metaClient.getActiveTimeline().getWriteTimeline().countInstants());
    assertEquals(instantTime, metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().firstInstant().get().getTimestamp());
  }
}
