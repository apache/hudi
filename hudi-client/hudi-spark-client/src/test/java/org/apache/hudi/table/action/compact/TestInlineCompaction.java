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
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestInlineCompaction extends CompactionTestBase {

  private HoodieWriteConfig getConfigForInlineCompaction(int maxDeltaCommits, int maxDeltaTime) {
    return getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(true)
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommits)
            .withMaxDeltaTimeBeforeCompaction(maxDeltaTime)
            .withMaxDeltaTimeBeforeCompactionEnabled(true).build())
        .build();
  }

  @Test
  public void testCompactionIsNotScheduledEarly() throws Exception {
    // Given: make two commits
    HoodieWriteConfig cfg = getConfigForInlineCompaction(3, 60);
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(HoodieActiveTimeline.createNewInstantTime(), 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      List<String> instants = IntStream.range(0, 2).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());

      // Then: ensure no compaction is executedm since there are only 2 delta commits
      assertEquals(2, metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().countInstants());
    }
  }

  @Test
  public void testSuccessfulCompactionBasedOnNumCommits() throws Exception {
    // Given: make three commits
    HoodieWriteConfig cfg = getConfigForInlineCompaction(3, 60);
    List<String> instants = IntStream.range(0, 2).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());

    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(instants.get(0), 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());

      // third commit, that will trigger compaction
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      String finalInstant = HoodieActiveTimeline.createNewInstantTime();
      createNextDeltaCommit(finalInstant, dataGen.generateUpdates(finalInstant, 100), writeClient, metaClient, cfg, false);

      // Then: ensure the file slices are compacted as per policy
      metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      assertEquals(4, metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().countInstants());
      assertEquals(HoodieTimeline.COMMIT_ACTION, metaClient.getActiveTimeline().lastInstant().get().getAction());
    }
  }

  @Test
  public void testSuccessfulCompactionBasedOnTime() throws Exception {
    // Given: make one commit
    HoodieWriteConfig cfg = getConfigForInlineCompaction(5, 10);

    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      String instantTime = HoodieActiveTimeline.createNewInstantTime();
      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 10);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, Arrays.asList(instantTime), records, cfg, true, new ArrayList<>());

      // after 10s, that will trigger compaction
      Thread.sleep(10000);
      String finalInstant = HoodieActiveTimeline.createNewInstantTime();
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      createNextDeltaCommit(finalInstant, dataGen.generateUpdates(finalInstant, 100), writeClient, metaClient, cfg, false);

      // Then: ensure the file slices are compacted as per policy
      metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      assertEquals(3, metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().countInstants());
      assertEquals(HoodieTimeline.COMMIT_ACTION, metaClient.getActiveTimeline().lastInstant().get().getAction());
    }
  }

  @Test
  public void testCompactionRetryOnFailureBasedOnNumCommits() throws Exception {
    // Given: two commits, schedule compaction and its failed/in-flight
    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
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
    HoodieWriteConfig inlineCfg = getConfigForInlineCompaction(2, 60);
    String instantTime3 = HoodieActiveTimeline.createNewInstantTime();
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(inlineCfg)) {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      createNextDeltaCommit(instantTime3, dataGen.generateUpdates(instantTime3, 100), writeClient, metaClient, inlineCfg, false);
    }

    // Then: 1 delta commit is done, the failed compaction is retried
    metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    assertEquals(4, metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().countInstants());
    assertEquals(instantTime2, metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().firstInstant().get().getTimestamp());
  }

  @Test
  public void testCompactionRetryOnFailureBasedOnTime() throws Exception {
    // Given: two commits, schedule compaction and its failed/in-flight
    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false).withMaxDeltaTimeBeforeCompaction(5)
            .withMaxDeltaTimeBeforeCompactionEnabled(true).build())
        .build();
    String instantTime;
    List<String> instants = IntStream.range(0, 2).mapToObj(i -> HoodieActiveTimeline.createNewInstantTime()).collect(Collectors.toList());
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(instants.get(0), 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());
      // Schedule compaction instantTime, make it in-flight (simulates inline compaction failing)
      Thread.sleep(10000);
      instantTime = HoodieActiveTimeline.createNewInstantTime();
      scheduleCompaction(instantTime, writeClient, cfg);
      moveCompactionFromRequestedToInflight(instantTime, cfg);
    }

    // When: commit happens after 10s
    HoodieWriteConfig inlineCfg = getConfigForInlineCompaction(5, 10);
    String instantTime2;
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(inlineCfg)) {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      instantTime2 = HoodieActiveTimeline.createNewInstantTime();
      createNextDeltaCommit(instantTime2, dataGen.generateUpdates(instantTime2, 10), writeClient, metaClient, inlineCfg, false);
    }

    // Then: 1 delta commit is done, the failed compaction is retried
    metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    assertEquals(4, metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().countInstants());
    assertEquals(instantTime, metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().firstInstant().get().getTimestamp());
  }
}
