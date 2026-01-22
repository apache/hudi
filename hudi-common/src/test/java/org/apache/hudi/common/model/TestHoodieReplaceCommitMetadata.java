/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.model;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.model.TestHoodieCommitMetadata.verifyMetadataFieldNames;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieReplaceCommitMetadata}.
 */
public class TestHoodieReplaceCommitMetadata {

  private static final List<String> EXPECTED_FIELD_NAMES = Arrays.asList(
      "partitionToWriteStats", "partitionToReplaceFileIds", "compacted", "extraMetadata", "operationType");

  @Test
  public void verifyFieldNamesInReplaceCommitMetadata() throws IOException {
    List<HoodieWriteStat> fakeHoodieWriteStats = HoodieTestUtils.generateFakeHoodieWriteStat(10);
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    fakeHoodieWriteStats.forEach(stat -> {
      commitMetadata.addWriteStat(stat.getPartitionPath(), stat);
      commitMetadata.addReplaceFileId(stat.getPartitionPath(), stat.getFileId());
    });
    verifyMetadataFieldNames(commitMetadata, EXPECTED_FIELD_NAMES);
  }

  @Test
  public void testGetWritePartitionPathsWithExistingFileGroupsModified_OnlyReplaceFileIds() {
    // Partitions from partitionToReplaceFileIds should be included even if there are no write stats
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();

    commitMetadata.addReplaceFileId("partition1", "replacedFile1");
    commitMetadata.addReplaceFileId("partition2", "replacedFile2");

    Set<String> result = commitMetadata.getWritePartitionPathsWithExistingFileGroupsModified();
    Set<String> expected = new HashSet<>(Arrays.asList("partition1", "partition2"));
    assertEquals(expected, result, "Result should contain partitions from replaceFileIds");
  }

  @Test
  public void testGetWritePartitionPathsWithExistingFileGroupsModified_OnlyInsertsWithReplaceFileIds() {
    // Even if partitionToWriteStats has only insert stats, partitions from partitionToReplaceFileIds should be included
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();

    HoodieWriteStat insertStat = createWriteStat("partition1", "file1", "null");
    commitMetadata.addWriteStat("partition1", insertStat);
    commitMetadata.addReplaceFileId("partition2", "replacedFile2");

    Set<String> result = commitMetadata.getWritePartitionPathsWithExistingFileGroupsModified();
    Set<String> expected = new HashSet<>(Arrays.asList("partition2"));
    assertEquals(expected, result, "Result should only contain partition with replaced files");
  }

  @Test
  public void testGetWritePartitionPathsWithExistingFileGroupsModified_UpdatesAndReplaceFileIds() {
    // Should include both partitions with updates and partitions with replaceFileIds
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();

    HoodieWriteStat updateStat = createWriteStat("partition1", "file1", "20240101120000");
    commitMetadata.addWriteStat("partition1", updateStat);
    commitMetadata.addReplaceFileId("partition2", "replacedFile2");

    Set<String> result = commitMetadata.getWritePartitionPathsWithExistingFileGroupsModified();
    Set<String> expected = new HashSet<>(Arrays.asList("partition1", "partition2"));
    assertEquals(expected, result, "Result should contain both update and replace partitions");
  }

  @Test
  public void testGetWritePartitionPathsWithExistingFileGroupsModified_OverlappingPartitions() {
    // When the same partition has both update stats and replaceFileIds, it should appear only once
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();

    HoodieWriteStat updateStat = createWriteStat("partition1", "file1", "20240101120000");
    commitMetadata.addWriteStat("partition1", updateStat);
    commitMetadata.addReplaceFileId("partition1", "replacedFile1");

    Set<String> result = commitMetadata.getWritePartitionPathsWithExistingFileGroupsModified();
    Set<String> expected = new HashSet<>(Arrays.asList("partition1"));
    assertEquals(expected, result, "Result should contain partition only once");
  }

  @Test
  public void testGetWritePartitionPathsWithExistingFileGroupsModified_EmptyReplaceMetadata() {
    // When metadata is empty, result should be empty
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();

    Set<String> result = commitMetadata.getWritePartitionPathsWithExistingFileGroupsModified();
    assertTrue(result.isEmpty(), "Result should be empty for empty metadata");
  }

  @Test
  public void testGetWritePartitionPathsWithExistingFileGroupsModified_MixedScenario() {
    // Complex scenario: inserts in p1, updates in p2, replaced files in p3, both updates and replaces in p4
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();

    // partition1: only inserts (should NOT be included)
    HoodieWriteStat insertStat1 = createWriteStat("partition1", "file1", "null");
    commitMetadata.addWriteStat("partition1", insertStat1);

    // partition2: updates (should be included)
    HoodieWriteStat updateStat = createWriteStat("partition2", "file2", "20240101120000");
    commitMetadata.addWriteStat("partition2", updateStat);

    // partition3: replaced files only (should be included)
    commitMetadata.addReplaceFileId("partition3", "replacedFile3");

    // partition4: both updates and replaced files (should be included once)
    HoodieWriteStat updateStat4 = createWriteStat("partition4", "file4", "20240101130000");
    commitMetadata.addWriteStat("partition4", updateStat4);
    commitMetadata.addReplaceFileId("partition4", "replacedFile4");

    Set<String> result = commitMetadata.getWritePartitionPathsWithExistingFileGroupsModified();
    Set<String> expected = new HashSet<>(Arrays.asList("partition2", "partition3", "partition4"));
    assertEquals(expected, result, "Result should contain only partitions with modified file groups");
  }

  /**
   * Helper method to create a HoodieWriteStat with specified partition, fileId, and prevCommit.
   */
  private HoodieWriteStat createWriteStat(String partitionPath, String fileId, String prevCommit) {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath(partitionPath);
    writeStat.setFileId(fileId);
    writeStat.setPrevCommit(prevCommit);
    writeStat.setPath(partitionPath + "/" + fileId + ".parquet");
    writeStat.setNumWrites(100);
    writeStat.setNumDeletes(0);
    return writeStat;
  }
}
