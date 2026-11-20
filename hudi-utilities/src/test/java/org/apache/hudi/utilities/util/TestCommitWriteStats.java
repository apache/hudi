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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

/**
 * The write-stat lookup used by the metadata-table validator.
 *
 * <p>These cases pin the three answers apart, because the validator distinguishes "the commit named
 * someone else" (a proven orphan) from "I could not find out" (not adjudicated).
 */
class TestCommitWriteStats {

  private static final String PARTITION = "2026/08/12";
  private static final String FILE_ID = "fg1";
  private static final String INSTANT_TIME = "1005";

  private static HoodieWriteStat writeStat(String fileId, String path) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(fileId);
    stat.setPath(path);
    return stat;
  }

  private static HoodieTimeline timelineNaming(List<HoodieWriteStat> stats) throws IOException {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (HoodieWriteStat stat : stats) {
      metadata.addWriteStat(PARTITION, stat);
    }
    HoodieTimeline timeline = Mockito.mock(HoodieTimeline.class);
    Mockito.when(timeline.readCommitMetadata(any())).thenReturn(metadata);
    return timeline;
  }

  private static HoodieInstant commitInstant() {
    HoodieInstant instant = Mockito.mock(HoodieInstant.class);
    Mockito.when(instant.getAction()).thenReturn(HoodieTimeline.COMMIT_ACTION);
    Mockito.when(instant.requestedTime()).thenReturn(INSTANT_TIME);
    return instant;
  }

  @Test
  void oneRecordedPathIsReturned() throws IOException {
    HoodieTimeline timeline = timelineNaming(Collections.singletonList(
        writeStat(FILE_ID, PARTITION + "/fg1_0-1-0_1005.parquet")));

    List<String> paths = CommitWriteStats.getFilePathsAddedForInstant(
        timeline, new HoodieFileGroupId(PARTITION, FILE_ID), commitInstant());

    assertEquals(Collections.singletonList(PARTITION + "/fg1_0-1-0_1005.parquet"), paths);
  }

  @Test
  void pathsForOtherFileGroupsAreExcluded() throws IOException {
    HoodieTimeline timeline = timelineNaming(Arrays.asList(
        writeStat(FILE_ID, PARTITION + "/fg1_0-1-0_1005.parquet"),
        writeStat("fg2", PARTITION + "/fg2_0-1-0_1005.parquet")));

    List<String> paths = CommitWriteStats.getFilePathsAddedForInstant(
        timeline, new HoodieFileGroupId(PARTITION, FILE_ID), commitInstant());

    assertEquals(Collections.singletonList(PARTITION + "/fg1_0-1-0_1005.parquet"), paths);
  }

  @Test
  void severalRecordedPathsAreAllReturnedRatherThanCollapsed() throws IOException {
    HoodieTimeline timeline = timelineNaming(Arrays.asList(
        writeStat(FILE_ID, PARTITION + "/fg1_0-1-0_1005.parquet"),
        writeStat(FILE_ID, PARTITION + "/fg1_0-2-0_1005.parquet")));

    List<String> paths = CommitWriteStats.getFilePathsAddedForInstant(
        timeline, new HoodieFileGroupId(PARTITION, FILE_ID), commitInstant());

    assertEquals(2, paths.size(),
        "the shared lookup reports what it found; deciding that two is unusable is the caller's "
            + "policy, and the two callers decide it differently");
  }

  @Test
  void anUnreadableCommitThrowsRatherThanReadingAsNamingNothing() throws IOException {
    HoodieTimeline timeline = Mockito.mock(HoodieTimeline.class);
    Mockito.when(timeline.readCommitMetadata(any())).thenThrow(new IOException("archive gone"));

    assertThrows(IOException.class, () -> CommitWriteStats.getFilePathsAddedForInstant(
        timeline, new HoodieFileGroupId(PARTITION, FILE_ID), commitInstant()));
  }

  @Test
  void partitionTheCommitNeverTouchedReturnsEmptyRatherThanThrowing() throws IOException {
    // HoodieCommitMetadata.getWriteStats(String) is a bare map lookup and hands back null for an
    // absent key, which is also what a partition-path spelling mismatch looks like. Without a guard
    // that surfaces as an NPE, and a caller catching it reports "the commit could not be read" --
    // indistinguishable from a genuine I/O failure, and it hides the case from whatever policy the
    // caller applies to a commit that recorded nothing.
    HoodieTimeline timeline = timelineNaming(Collections.singletonList(
        writeStat(FILE_ID, PARTITION + "/fg1_0-1-0_1005.parquet")));

    List<String> paths = CommitWriteStats.getFilePathsAddedForInstant(
        timeline, new HoodieFileGroupId("2026/08/13", FILE_ID), commitInstant());

    assertTrue(paths.isEmpty());
  }

  @Test
  void anActionThatRecordsNoFilesReturnsEmpty() throws IOException {
    HoodieInstant clean = Mockito.mock(HoodieInstant.class);
    Mockito.when(clean.getAction()).thenReturn(HoodieTimeline.CLEAN_ACTION);

    List<String> paths = CommitWriteStats.getFilePathsAddedForInstant(
        Mockito.mock(HoodieTimeline.class), new HoodieFileGroupId(PARTITION, FILE_ID), clean);

    assertTrue(paths.isEmpty());
  }
}
