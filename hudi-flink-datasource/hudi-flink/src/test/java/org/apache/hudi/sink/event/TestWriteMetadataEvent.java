/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.event;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieWriteStat;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link WriteMetadataEvent}.
 */
public class TestWriteMetadataEvent {

  @Test
  void testMergeWithRescaleMergesEventsAcrossTasks() {
    WriteMetadataEvent event = newEvent(0, "001", "par1", false, "file-1");
    WriteMetadataEvent other = newEvent(1, "002", "par2", true, "file-2");

    WriteMetadataEvent merged = event.mergeWithRescale(other);

    assertSame(event, merged);
    assertEquals("002", merged.getInstantTime());
    assertTrue(merged.isLastBatch());
    assertEquals(2, merged.getWriteStatuses().size());
    assertEquals(Set.of("par1", "par2"), merged.getWriteStatuses().stream()
        .map(writeStatus -> writeStatus.getStat().getPartitionPath())
        .collect(Collectors.toSet()));
  }

  private static WriteMetadataEvent newEvent(
      int taskId,
      String instant,
      String partitionPath,
      boolean lastBatch,
      String fileId) {
    WriteStatus writeStatus = new WriteStatus();
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath(partitionPath);
    writeStat.setFileId(fileId);
    writeStat.setPath(partitionPath + "/" + fileId);
    writeStatus.setStat(writeStat);

    return WriteMetadataEvent.builder()
        .taskID(taskId)
        .checkpointId(1L)
        .instantTime(instant)
        .writeStatus(Collections.singletonList(writeStatus))
        .lastBatch(lastBatch)
        .build();
  }
}
