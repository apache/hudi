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

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.apache.hudi.common.model.HoodieTestUtils.generateFakeHoodieWriteStat;
import static org.apache.hudi.table.HoodieCopyOnWriteTable.averageBytesPerRecord;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieRecordSizing {

  private static List<HoodieInstant> setupHoodieInstants() {
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts1"));
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts2"));
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts3"));
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts4"));
    instants.add(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "ts5"));
    Collections.reverse(instants);
    return instants;
  }

  private static List<HoodieWriteStat> generateCommitStatWith(int totalRecordsWritten, int totalBytesWritten) {
    List<HoodieWriteStat> writeStatsList = generateFakeHoodieWriteStat(5);
    // clear all record and byte stats except for last entry.
    for (int i = 0; i < writeStatsList.size() - 1; i++) {
      HoodieWriteStat writeStat = writeStatsList.get(i);
      writeStat.setNumWrites(0);
      writeStat.setTotalWriteBytes(0);
    }
    HoodieWriteStat lastWriteStat = writeStatsList.get(writeStatsList.size() - 1);
    lastWriteStat.setTotalWriteBytes(totalBytesWritten);
    lastWriteStat.setNumWrites(totalRecordsWritten);
    return writeStatsList;
  }

  private static HoodieCommitMetadata generateCommitMetadataWith(int totalRecordsWritten, int totalBytesWritten) {
    List<HoodieWriteStat> fakeHoodieWriteStats = generateCommitStatWith(totalRecordsWritten, totalBytesWritten);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    fakeHoodieWriteStats.forEach(stat -> commitMetadata.addWriteStat(stat.getPartitionPath(), stat));
    return commitMetadata;
  }

  /*
   * This needs to be a stack so we test all cases when either/both recordsWritten ,bytesWritten is zero before a non
   * zero averageRecordSize can be computed.
   */
  private static LinkedList<Option<byte[]>> generateCommitMetadataList() throws IOException {
    LinkedList<Option<byte[]>> commits = new LinkedList<>();
    // First commit with non zero records and bytes
    commits.push(Option.of(generateCommitMetadataWith(2000, 10000).toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Second commit with non zero records and bytes
    commits.push(Option.of(generateCommitMetadataWith(1500, 7500).toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Third commit with both zero records and zero bytes
    commits.push(Option.of(generateCommitMetadataWith(0, 0).toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Fourth commit with zero records
    commits.push(Option.of(generateCommitMetadataWith(0, 1500).toJsonString().getBytes(StandardCharsets.UTF_8)));
    // Fifth commit with zero bytes
    commits.push(Option.of(generateCommitMetadataWith(2500, 0).toJsonString().getBytes(StandardCharsets.UTF_8)));
    return commits;
  }

  @Test
  public void testAverageBytesPerRecordForNonEmptyCommitTimeLine() throws Exception {
    HoodieTimeline commitTimeLine = mock(HoodieTimeline.class);
    when(commitTimeLine.empty()).thenReturn(false);
    when(commitTimeLine.getReverseOrderedInstants()).thenReturn(setupHoodieInstants().stream());
    LinkedList<Option<byte[]>> commits = generateCommitMetadataList();
    when(commitTimeLine.getInstantDetails(any(HoodieInstant.class))).thenAnswer(invocationOnMock -> commits.pop());
    long expectAvgSize = (long) Math.ceil((1.0 * 7500) / 1500);
    long actualAvgSize = averageBytesPerRecord(commitTimeLine, 1234);
    assertEquals(expectAvgSize, actualAvgSize);
  }

  @Test
  public void testAverageBytesPerRecordForEmptyCommitTimeLine() {
    HoodieTimeline commitTimeLine = mock(HoodieTimeline.class);
    when(commitTimeLine.empty()).thenReturn(true);
    long expectAvgSize = 2345;
    long actualAvgSize = averageBytesPerRecord(commitTimeLine, 2345);
    assertEquals(expectAvgSize, actualAvgSize);
  }
}
