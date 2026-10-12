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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRollbackOrphanDetector extends HoodieRollbackTestBase {

  private static final String FAILED_INSTANT = "20260101000000000";
  private static final String OTHER_INSTANT  = "20260102000000000";
  private static final String PARTITION = "2026/01/01";
  private static final String FILE_ID = "11111111-2222-3333-4444-555555555555-0";

  private final RollbackOrphanDetector detector = new RollbackOrphanDetector();

  @Test
  public void modeOff_returnsEmpty_evenWithFailedDeleteFiles() throws IOException {
    HoodieInstant rollback = stubRollbackInstant(rollbackMetadata(Arrays.asList("foo.parquet")));
    assertTrue(detector.detectOrphans(table, rollback, RollbackOrphanDetector.Mode.OFF).isEmpty());
    assertFalse(detector.hasOrphans(table, rollback, RollbackOrphanDetector.Mode.OFF));
  }

  @Test
  public void modeLight_emptyFailedList_returnsEmpty() throws IOException {
    HoodieInstant rollback = stubRollbackInstant(rollbackMetadata(Collections.emptyList()));
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(timeline.getInstantsAsStream()).thenReturn(java.util.stream.Stream.empty());
    assertTrue(detector.detectOrphans(table, rollback, RollbackOrphanDetector.Mode.LIGHT).isEmpty());
  }

  @Test
  public void modeLight_returnsFailedDeleteFiles_filteredByCompletedTimeline() throws IOException {
    String orphanName = orphanBaseName(FILE_ID, FAILED_INSTANT);
    String legitName = orphanBaseName(FILE_ID, OTHER_INSTANT);
    List<String> failed = Arrays.asList(orphanName, legitName);
    HoodieInstant rollback = stubRollbackInstant(rollbackMetadata(failed));
    stubCompletedInstants(OTHER_INSTANT);

    Set<String> orphans = detector.detectOrphans(table, rollback, RollbackOrphanDetector.Mode.LIGHT);
    assertEquals(1, orphans.size());
    assertTrue(orphans.iterator().next().endsWith(orphanName));
  }

  @Test
  public void modeThorough_findsLateLandingFile_notInFailedDeleteList() throws IOException {
    StoragePath orphan = createBaseFileToRollback(PARTITION, FILE_ID, FAILED_INSTANT);
    HoodieInstant rollback = stubRollbackInstant(
        rollbackMetadataWithPartition(Collections.emptyList(), PARTITION));
    stubCompletedInstants();

    Set<String> orphans = detector.detectOrphans(table, rollback, RollbackOrphanDetector.Mode.THOROUGH);
    assertEquals(1, orphans.size());
    assertTrue(orphans.iterator().next().endsWith(orphan.getName()));
  }

  @Test
  public void modeThorough_skipsLegitimateCommittedFile_evenWhenFilenameMatches() throws IOException {
    createBaseFileToRollback(PARTITION, FILE_ID, OTHER_INSTANT);
    HoodieInstant rollback = stubRollbackInstant(
        rollbackMetadataWithPartition(Collections.emptyList(), PARTITION));
    stubCompletedInstants(OTHER_INSTANT);

    Set<String> orphans = detector.detectOrphans(table, rollback, RollbackOrphanDetector.Mode.THOROUGH);
    assertTrue(orphans.isEmpty(), "Legitimate committed file must not be flagged as orphan");
  }

  // ---- helpers ----

  private HoodieInstant stubRollbackInstant(HoodieRollbackMetadata meta) throws IOException {
    HoodieInstant rollback = mock(HoodieInstant.class);
    when(rollback.getAction()).thenReturn(HoodieTimeline.ROLLBACK_ACTION);
    when(rollback.isCompleted()).thenReturn(true);
    when(rollback.requestedTime()).thenReturn("20260101010101010");
    when(timeline.readRollbackMetadata(any(HoodieInstant.class))).thenReturn(meta);
    return rollback;
  }

  private void stubCompletedInstants(String... instantTimes) {
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    List<HoodieInstant> instants = new ArrayList<>();
    for (String t : instantTimes) {
      HoodieInstant i = mock(HoodieInstant.class);
      when(i.requestedTime()).thenReturn(t);
      instants.add(i);
    }
    when(timeline.getInstantsAsStream()).thenReturn(instants.stream());
  }

  private HoodieRollbackMetadata rollbackMetadata(List<String> failedDeleteFiles) {
    return rollbackMetadataWithPartition(failedDeleteFiles, PARTITION);
  }

  private HoodieRollbackMetadata rollbackMetadataWithPartition(List<String> failedDeleteFiles, String partition) {
    HoodieRollbackPartitionMetadata pm = HoodieRollbackPartitionMetadata.newBuilder()
        .setPartitionPath(partition)
        .setSuccessDeleteFiles(Collections.emptyList())
        .setFailedDeleteFiles(new ArrayList<>(failedDeleteFiles))
        .build();
    Map<String, HoodieRollbackPartitionMetadata> pmMap = new HashMap<>();
    pmMap.put(partition, pm);
    return HoodieRollbackMetadata.newBuilder()
        .setStartRollbackTime("20260101010101010")
        .setTimeTakenInMillis(0L)
        .setTotalFilesDeleted(0)
        .setCommitsRollback(Collections.singletonList(FAILED_INSTANT))
        .setPartitionMetadata(pmMap)
        .build();
  }

  private static String orphanBaseName(String fileId, String instantTime) {
    return fileId + "_1-2-3_" + instantTime + ".parquet";
  }
}
