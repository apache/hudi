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

package org.apache.hudi.table.functional;

import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.TestCleaner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCleanPlanExecutor extends TestCleaner {

  /**
   * Tests cleaning service based on number of hours retained.
   */
  @ParameterizedTest
  @MethodSource("argumentsForTestKeepLatestCommits")
  public void testKeepXHoursWithCleaning(boolean simulateFailureRetry, boolean enableIncrementalClean, boolean enableBootstrapSourceClean) throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
            .withMetadataConfig(HoodieMetadataConfig.newBuilder().withAssumeDatePartitioning(true).build())
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                    .withIncrementalCleaningMode(enableIncrementalClean)
                    .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER)
                    .withCleanBootstrapBaseFileEnabled(enableBootstrapSourceClean)
                    .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS).cleanerNumHoursRetained(2).build())
            .build();

    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String p0 = "2020/01/01";
    String p1 = "2020/01/02";
    Map<String, List<BootstrapFileMapping>> bootstrapMapping = enableBootstrapSourceClean ? generateBootstrapIndexAndSourceData(p0, p1) : null;

    String file1P0C0 = enableBootstrapSourceClean ? bootstrapMapping.get(p0).get(0).getFileId()
            : UUID.randomUUID().toString();
    String file1P1C0 = enableBootstrapSourceClean ? bootstrapMapping.get(p1).get(0).getFileId()
            : UUID.randomUUID().toString();
    Instant instant = Instant.now();
    ZonedDateTime commitDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault());
    int minutesForFirstCommit = 150;
    String firstCommitTs = HoodieActiveTimeline.formatDate(Date.from(commitDateTime.minusMinutes(minutesForFirstCommit).toInstant()));
    testTable.addInflightCommit(firstCommitTs).withBaseFilesInPartition(p0, file1P0C0).withBaseFilesInPartition(p1, file1P1C0);

    HoodieCommitMetadata commitMetadata = generateCommitMetadata(firstCommitTs,
            Collections.unmodifiableMap(new HashMap<String, List<String>>() {
              {
                put(p0, CollectionUtils.createImmutableList(file1P0C0));
                put(p1, CollectionUtils.createImmutableList(file1P1C0));
              }
            })
    );
    metaClient.getActiveTimeline().saveAsComplete(
            new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, firstCommitTs),
            Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    metaClient = HoodieTableMetaClient.reload(metaClient);

    List<HoodieCleanStat> hoodieCleanStatsOne = runCleaner(config, simulateFailureRetry);
    assertEquals(0, hoodieCleanStatsOne.size(), "Must not scan any partitions and clean any files");
    assertTrue(testTable.baseFileExists(p0, firstCommitTs, file1P0C0));
    assertTrue(testTable.baseFileExists(p1, firstCommitTs, file1P1C0));

    // make next commit, with 1 insert & 1 update per partition
    int minutesForSecondCommit = 90;
    String secondCommitTs = HoodieActiveTimeline.formatDate(Date.from(commitDateTime.minusMinutes(minutesForSecondCommit).toInstant()));
    Map<String, String> partitionAndFileId002 = testTable.addInflightCommit(secondCommitTs).getFileIdsWithBaseFilesInPartitions(p0, p1);
    String file2P0C1 = partitionAndFileId002.get(p0);
    String file2P1C1 = partitionAndFileId002.get(p1);
    testTable.forCommit(secondCommitTs).withBaseFilesInPartition(p0, file1P0C0).withBaseFilesInPartition(p1, file1P1C0);
    commitMetadata = generateCommitMetadata(secondCommitTs, new HashMap<String, List<String>>() {
      {
        put(p0, CollectionUtils.createImmutableList(file1P0C0, file2P0C1));
        put(p1, CollectionUtils.createImmutableList(file1P1C0, file2P1C1));
      }
    });
    metaClient.getActiveTimeline().saveAsComplete(
            new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, secondCommitTs),
            Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    List<HoodieCleanStat> hoodieCleanStatsTwo = runCleaner(config, simulateFailureRetry);
    assertEquals(2, hoodieCleanStatsTwo.size(), "Should clean one file each from both the partitions");
    assertTrue(testTable.baseFileExists(p0, secondCommitTs, file2P0C1));
    assertTrue(testTable.baseFileExists(p1, secondCommitTs, file2P1C1));
    assertTrue(testTable.baseFileExists(p0, secondCommitTs, file1P0C0));
    assertTrue(testTable.baseFileExists(p1, secondCommitTs, file1P1C0));
    assertFalse(testTable.baseFileExists(p0, firstCommitTs, file1P0C0));
    assertFalse(testTable.baseFileExists(p1, firstCommitTs, file1P1C0));
  }
}
