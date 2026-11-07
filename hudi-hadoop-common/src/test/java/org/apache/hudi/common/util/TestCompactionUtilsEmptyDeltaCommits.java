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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link CompactionUtils#filterOutEmptyDeltaCommits} and the archival retention used when the compaction trigger
 * ignores empty delta commits.
 */
class TestCompactionUtilsEmptyDeltaCommits extends HoodieCommonTestHarness {

  @BeforeEach
  void setUp() {
    initPath();
  }

  @Test
  void testFilterOutEmptyDeltaCommitsOnDataTable() throws Exception {
    HoodieTableMetaClient dataMetaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    HoodieTestTable.of(dataMetaClient)
        .addDeltaCommit("001", commitMetadata(10, 0))
        .addDeltaCommit("002", new HoodieCommitMetadata())
        .addDeltaCommit("003", commitMetadata(0, 5))
        .addDeltaCommit("004", new HoodieCommitMetadata());
    dataMetaClient.reloadActiveTimeline();

    HoodieTimeline deltaCommits = dataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants();
    assertEquals(Arrays.asList("001", "003"), requestedTimes(CompactionUtils.filterOutEmptyDeltaCommits(dataMetaClient, deltaCommits)));
  }

  @Test
  void testFilterOutEmptyDeltaCommitsOnMetadataTableUsesDataTableCommits() throws Exception {
    HoodieTableMetaClient dataMetaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieTestTable.of(dataMetaClient)
        .addCommit("001", Option.of(commitMetadata(10, 0)))
        .addCommit("002", Option.of(new HoodieCommitMetadata()));

    // Every metadata table delta commit writes a files partition record, so only the data table commit can tell.
    // 003 has no data table commit (as for a clean or a rollback) and has to count.
    HoodieTableMetaClient metadataMetaClient =
        HoodieTestUtils.init(basePath + "/" + HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH, HoodieTableType.MERGE_ON_READ);
    HoodieTestTable.of(metadataMetaClient)
        .addDeltaCommit("001", commitMetadata(1, 0))
        .addDeltaCommit("002", commitMetadata(1, 0))
        .addDeltaCommit("003", commitMetadata(1, 0));
    metadataMetaClient.reloadActiveTimeline();

    HoodieTimeline deltaCommits = metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants();
    assertEquals(Arrays.asList("001", "003"), requestedTimes(CompactionUtils.filterOutEmptyDeltaCommits(metadataMetaClient, deltaCommits)));
  }

  @Test
  void testEarliestInstantToRetainKeepsAllDeltaCommitsWhenIgnoringEmptyCommits() throws Exception {
    HoodieTableMetaClient dataMetaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    HoodieTestTable testTable = HoodieTestTable.of(dataMetaClient);
    for (String instant : Arrays.asList("001", "002", "003", "004", "005")) {
      testTable.addDeltaCommit(instant, new HoodieCommitMetadata());
    }
    dataMetaClient.reloadActiveTimeline();

    assertEquals("004", CompactionUtils.getEarliestInstantToRetainForCompaction(
        dataMetaClient.getActiveTimeline(), 2, false).get().requestedTime());
    assertEquals("001", CompactionUtils.getEarliestInstantToRetainForCompaction(
        dataMetaClient.getActiveTimeline(), 2, true).get().requestedTime());
  }

  private static HoodieCommitMetadata commitMetadata(long numWrites, long numDeletes) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath("p1");
    stat.setFileId("f1");
    stat.setNumWrites(numWrites);
    stat.setNumDeletes(numDeletes);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addWriteStat("p1", stat);
    return metadata;
  }

  private static List<String> requestedTimes(HoodieTimeline timeline) {
    return timeline.getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toList());
  }
}
