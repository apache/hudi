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
import org.apache.hudi.common.util.CollectionUtils;
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

  private HoodieWriteConfig getConfigForInlineCompaction(int maxDeltaCommits) {
    return getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(true).withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommits).build())
        .build();
  }

  @Test
  public void testCompactionIsNotScheduledEarly() throws Exception {
    // Given: make two commits
    HoodieWriteConfig cfg = getConfigForInlineCompaction(3);
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts("000", 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, Arrays.asList("000", "001"), records, cfg, true, new ArrayList<>());
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());

      // Then: ensure no compaction is executedm since there are only 2 delta commits
      assertEquals(2, metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().countInstants());
    }
  }

  @Test
  public void testSuccessfulCompaction() throws Exception {
    // Given: make three commits
    HoodieWriteConfig cfg = getConfigForInlineCompaction(3);
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
  public void testCompactionRetryOnFailure() throws Exception {
    // Given: two commits, schedule compaction and its failed/in-flight
    HoodieWriteConfig cfg = getConfigBuilder(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .build();
    List<String> instants = CollectionUtils.createImmutableList("000", "001");
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(cfg)) {
      List<HoodieRecord> records = dataGen.generateInserts(instants.get(0), 100);
      HoodieReadClient readClient = getHoodieReadClient(cfg.getBasePath());
      runNextDeltaCommits(writeClient, readClient, instants, records, cfg, true, new ArrayList<>());
      // Schedule compaction 002, make it in-flight (simulates inline compaction failing)
      scheduleCompaction("002", writeClient, cfg);
      moveCompactionFromRequestedToInflight("002", cfg);
    }

    // When: a third commit happens
    HoodieWriteConfig inlineCfg = getConfigForInlineCompaction(2);
    try (SparkRDDWriteClient<?> writeClient = getHoodieWriteClient(inlineCfg)) {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
      createNextDeltaCommit("003", dataGen.generateUpdates("003", 100), writeClient, metaClient, inlineCfg, false);
    }

    // Then: 1 delta commit is done, the failed compaction is retried
    metaClient = new HoodieTableMetaClient(hadoopConf, cfg.getBasePath());
    assertEquals(4, metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().countInstants());
    assertEquals("002", metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants().firstInstant().get().getTimestamp());
  }
}
