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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie commit metadata {@link HoodieCommitMetadata}.
 */
public class TestHoodieCommitMetadata extends HoodieCommonTestHarness {

  private static final List<String> EXPECTED_FIELD_NAMES = Arrays.asList(
      "partitionToWriteStats", "compacted", "extraMetadata", "operationType");

  public static void verifyMetadataFieldNames(
      HoodieCommitMetadata commitMetadata, List<String> expectedFieldNameList)
      throws IOException {
    String serializedCommitMetadata = JsonUtils.getObjectMapper().writeValueAsString(commitMetadata);
    List<String> actualFieldNameList = CollectionUtils.toStream(
            JsonUtils.getObjectMapper().readTree(serializedCommitMetadata).fieldNames())
        .collect(Collectors.toList());
    assertEquals(
        expectedFieldNameList.stream().sorted().collect(Collectors.toList()),
        actualFieldNameList.stream().sorted().collect(Collectors.toList())
    );
  }

  @Test
  public void verifyFieldNamesInCommitMetadata() throws IOException {
    List<HoodieWriteStat> fakeHoodieWriteStats = HoodieTestUtils.generateFakeHoodieWriteStat(10);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    fakeHoodieWriteStats.forEach(stat -> commitMetadata.addWriteStat(stat.getPartitionPath(), stat));
    verifyMetadataFieldNames(commitMetadata, EXPECTED_FIELD_NAMES);
  }

  @Test
  public void testPerfStatPresenceInHoodieMetadata() throws Exception {

    List<HoodieWriteStat> fakeHoodieWriteStats = HoodieTestUtils.generateFakeHoodieWriteStat(100);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    fakeHoodieWriteStats.forEach(stat -> commitMetadata.addWriteStat(stat.getPartitionPath(), stat));
    assertTrue(commitMetadata.getTotalCreateTime() > 0);
    assertTrue(commitMetadata.getTotalUpsertTime() > 0);
    assertTrue(commitMetadata.getTotalScanTime() > 0);
    assertTrue(commitMetadata.getTotalLogFilesCompacted() > 0);

    // Serialize and deserialize
    initMetaClient();
    HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    timeline.createNewInstant(commitInstant);
    timeline.transitionRequestedToInflight(commitInstant, Option.empty());
    HoodieInstant completeCommitInstant = new HoodieInstant(true, commitInstant.getAction(), commitInstant.getTimestamp());
    timeline.saveAsComplete(completeCommitInstant, Option.of(commitMetadata));

    HoodieCommitMetadata metadata = timeline.deserializeInstantContent(timeline.reload().lastInstant().get(), HoodieCommitMetadata.class);
    assertTrue(commitMetadata.getTotalCreateTime() > 0);
    assertTrue(commitMetadata.getTotalUpsertTime() > 0);
    assertTrue(commitMetadata.getTotalScanTime() > 0);
    assertTrue(metadata.getTotalLogFilesCompacted() > 0);
  }

  @Test
  public void testCompatibilityWithoutOperationType() throws Exception {
    initMetaClient();
    HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    timeline.createNewInstant(commitInstant);
    timeline.transitionRequestedToInflight(commitInstant, Option.empty());
    HoodieInstant completeCommitInstant = new HoodieInstant(true, commitInstant.getAction(), commitInstant.getTimestamp());

    // test compatibility of old version file
    String serializedCommitMetadata =
        FileIOUtils.readAsUTFString(TestHoodieCommitMetadata.class.getResourceAsStream("/old-version.commit"));
    timeline.saveAsComplete(completeCommitInstant, Option.of(outputStream -> outputStream.write(serializedCommitMetadata.getBytes(StandardCharsets.UTF_8))));

    HoodieCommitMetadata metadata = timeline.deserializeInstantContent(timeline.reload().lastInstant().get(), HoodieCommitMetadata.class);
    assertSame(metadata.getOperationType(), WriteOperationType.UNKNOWN);

    // test operate type
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    assertSame(commitMetadata.getOperationType(), WriteOperationType.INSERT);

    HoodieInstant commitInstant2 = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "2");
    timeline.createNewInstant(commitInstant2);
    timeline.transitionRequestedToInflight(commitInstant2, Option.empty());
    HoodieInstant completeCommitInstant2 = new HoodieInstant(true, commitInstant2.getAction(), commitInstant2.getTimestamp());
    // test serialized
    timeline.saveAsComplete(completeCommitInstant2, Option.of(commitMetadata));
    metadata = timeline.deserializeInstantContent(timeline.reload().lastInstant().get(), HoodieCommitMetadata.class);
    assertSame(metadata.getOperationType(), WriteOperationType.INSERT);
  }

  @Test
  void handleNullPartitionInSerialization() throws Exception {
    initMetaClient();
    HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "1");
    HoodieActiveTimeline timeline = new HoodieActiveTimeline(metaClient);
    timeline.createNewInstant(commitInstant);
    timeline.transitionRequestedToInflight(commitInstant, Option.empty());
    HoodieInstant completeCommitInstant = new HoodieInstant(true, commitInstant.getAction(), commitInstant.getTimestamp());
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.UPSERT);
    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setPath("/path1");
    writeStat1.setPrevCommit("001");
    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setPath("/path2");
    writeStat2.setPrevCommit("001");
    commitMetadata.addWriteStat(null, writeStat1);
    commitMetadata.addWriteStat("partition1", writeStat2);
    timeline.saveAsComplete(completeCommitInstant, Option.of(commitMetadata));

    HoodieCommitMetadata expected = new HoodieCommitMetadata();
    expected.setOperationType(WriteOperationType.UPSERT);
    expected.addWriteStat("partition1", writeStat2);
    assertEquals(expected, timeline.deserializeInstantContent(timeline.reload().lastInstant().get(), HoodieCommitMetadata.class));
  }
}
