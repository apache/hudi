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

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.model.TestHoodieCommitMetadata.verifyMetadataFieldNames;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieReplaceCommitMetadata}.
 */
class TestHoodieReplaceCommitMetadata extends HoodieCommonTestHarness {

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
  void handleNullPartitionInSerialization() throws Exception {
    initMetaClient();
    HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, "1");
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    timeline.createNewInstant(commitInstant);
    timeline.transitionRequestedToInflight(commitInstant, Option.empty());
    HoodieInstant completeCommitInstant = new HoodieInstant(true, commitInstant.getAction(), commitInstant.getTimestamp());
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setPath("/path1");
    writeStat1.setPrevCommit("001");
    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setPath("/path2");
    writeStat2.setPrevCommit("001");
    commitMetadata.addWriteStat(null, writeStat1);
    String partition1 = "partition1";
    commitMetadata.addWriteStat(partition1, writeStat2);
    commitMetadata.addReplaceFileId(null, "fileId1");
    commitMetadata.addReplaceFileId(partition1, "fileId2");
    timeline.saveAsComplete(completeCommitInstant, Option.of(commitMetadata));

    HoodieReplaceCommitMetadata expected = new HoodieReplaceCommitMetadata();
    expected.setOperationType(WriteOperationType.INSERT_OVERWRITE);
    expected.addWriteStat(partition1, writeStat2);
    expected.addReplaceFileId(partition1, "fileId2");
    assertEquals(expected, timeline.deserializeInstantContent(timeline.reload().lastInstant().get(), HoodieReplaceCommitMetadata.class));
  }
}
