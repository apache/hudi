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
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCommitUtils {

  @Test
  public void testCommitMetadataCreation() {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    writeStats.add(createWriteStat("p1", "f1"));
    writeStats.add(createWriteStat("p2", "f2"));
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIds = new ArrayList<>();
    replacedFileIds.add("f0");
    partitionToReplaceFileIds.put("p1", replacedFileIds);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(writeStats, partitionToReplaceFileIds,
        Option.empty(),
        WriteOperationType.INSERT,
        TRIP_SCHEMA,
        HoodieTimeline.DELTA_COMMIT_ACTION);

    assertFalse(commitMetadata instanceof HoodieReplaceCommitMetadata);
    assertEquals(2, commitMetadata.getPartitionToWriteStats().size());
    assertEquals("f1", commitMetadata.getPartitionToWriteStats().get("p1").get(0).getFileId());
    assertEquals("f2", commitMetadata.getPartitionToWriteStats().get("p2").get(0).getFileId());
    assertEquals(WriteOperationType.INSERT, commitMetadata.getOperationType());
    assertEquals(TRIP_SCHEMA, commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY));
  }

  @Test
  public void testReplaceMetadataCreation() {
    List<HoodieWriteStat> writeStats = new ArrayList<>();
    writeStats.add(createWriteStat("p1", "f1"));
    writeStats.add(createWriteStat("p2", "f2"));

    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIds = new ArrayList<>();
    replacedFileIds.add("f0");
    partitionToReplaceFileIds.put("p1", replacedFileIds);
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(writeStats, partitionToReplaceFileIds,
        Option.empty(),
        WriteOperationType.INSERT,
        TRIP_SCHEMA,
        HoodieTimeline.REPLACE_COMMIT_ACTION);

    assertTrue(commitMetadata instanceof HoodieReplaceCommitMetadata);
    HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
    assertEquals(1, replaceCommitMetadata.getPartitionToReplaceFileIds().size());
    assertEquals("f0", replaceCommitMetadata.getPartitionToReplaceFileIds().get("p1").get(0));
    assertEquals(2, commitMetadata.getPartitionToWriteStats().size());
    assertEquals("f1", commitMetadata.getPartitionToWriteStats().get("p1").get(0).getFileId());
    assertEquals("f2", commitMetadata.getPartitionToWriteStats().get("p2").get(0).getFileId());
    assertEquals(WriteOperationType.INSERT, commitMetadata.getOperationType());
    assertEquals(TRIP_SCHEMA, commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY));
  }

  private HoodieWriteStat createWriteStat(String partition, String fileId) {
    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setPartitionPath(partition);
    writeStat1.setFileId(fileId);
    return writeStat1;
  }
}