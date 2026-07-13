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

package org.apache.hudi.sink.utils;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.utils.EventBuffers.EventBuffer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link EventBuffersStateSerializer}. */
public class TestEventBuffersStateSerializer {

  @Test
  void testRoundTripPreservesMapAndEventArrayLayout() throws Exception {
    WriteMetadataEvent dataEvent = event(0, 7L, "007", false);
    WriteMetadataEvent indexEvent = event(1, 7L, "007", true);
    Map<Long, Pair<String, EventBuffer>> expected = new LinkedHashMap<>();
    expected.put(7L, Pair.of("007", new EventBuffer(
        new WriteMetadataEvent[] {dataEvent, null},
        new WriteMetadataEvent[] {null, indexEvent})));
    expected.put(3L, Pair.of("003", new EventBuffer(
        new WriteMetadataEvent[0], new WriteMetadataEvent[0])));

    Map<Long, Pair<String, ?>> actual = EventBuffersStateSerializer.deserialize(
        EventBuffersStateSerializer.serialize(expected));

    assertEquals(Arrays.asList(3L, 7L), new ArrayList<>(actual.keySet()));
    assertEventBuffer(actual.get(7L), "007", dataEvent, indexEvent);
    EventBuffer emptyBuffer = assertInstanceOf(EventBuffer.class, actual.get(3L).getRight());
    assertEquals(0, emptyBuffer.getDataWriteEventBuffer().length);
    assertEquals(0, emptyBuffer.getIndexWriteEventBuffer().length);
  }

  @Test
  void testRestoresJavaSerializedEventBufferState() throws Exception {
    WriteMetadataEvent dataEvent = event(0, 5L, "005", false);
    WriteMetadataEvent indexEvent = event(1, 5L, "005", true);
    Map<Long, Pair<String, EventBuffer>> legacyState = Collections.singletonMap(
        5L, Pair.of("005", new EventBuffer(
            new WriteMetadataEvent[] {dataEvent, null},
            new WriteMetadataEvent[] {null, indexEvent})));

    Map<Long, Pair<String, ?>> actual = EventBuffersStateSerializer.deserialize(
        SerializationUtils.serialize(legacyState));

    assertEventBuffer(actual.get(5L), "005", dataEvent, indexEvent);
  }

  @Test
  void testRestoresJavaSerializedLegacyEventArrayState() throws Exception {
    WriteMetadataEvent dataEvent = event(0, 1L, "001", false);
    Map<Long, Pair<String, WriteMetadataEvent[]>> legacyState = Collections.singletonMap(
        1L, Pair.of("001", new WriteMetadataEvent[] {dataEvent, null}));

    Map<Long, Pair<String, ?>> actual = EventBuffersStateSerializer.deserialize(
        SerializationUtils.serialize(legacyState));

    WriteMetadataEvent[] events = assertInstanceOf(
        WriteMetadataEvent[].class, actual.get(1L).getRight());
    assertEquals(2, events.length);
    assertEvent(dataEvent, events[0]);
    assertNull(events[1]);
  }

  @Test
  void testRejectsUnsupportedVersionAndTrailingBytes() throws Exception {
    byte[] bytes = EventBuffersStateSerializer.serialize(Collections.emptyMap());
    byte[] unsupportedVersion = Arrays.copyOf(bytes, bytes.length);
    ByteBuffer.wrap(unsupportedVersion).putInt(Integer.BYTES, 2);

    IOException versionError = assertThrows(
        IOException.class, () -> EventBuffersStateSerializer.deserialize(unsupportedVersion));
    assertEquals("Unsupported event buffers state version: 2", versionError.getMessage());

    byte[] trailingBytes = Arrays.copyOf(bytes, bytes.length + 1);
    IOException trailingBytesError = assertThrows(
        IOException.class, () -> EventBuffersStateSerializer.deserialize(trailingBytes));
    assertEquals("Unexpected trailing bytes in event buffers state", trailingBytesError.getMessage());
  }

  private static void assertEventBuffer(
      Pair<String, ?> actual,
      String expectedInstant,
      WriteMetadataEvent expectedDataEvent,
      WriteMetadataEvent expectedIndexEvent) {
    assertEquals(expectedInstant, actual.getLeft());
    EventBuffer eventBuffer = assertInstanceOf(EventBuffer.class, actual.getRight());
    assertEquals(2, eventBuffer.getDataWriteEventBuffer().length);
    assertEvent(expectedDataEvent, eventBuffer.getDataWriteEventBuffer()[0]);
    assertNull(eventBuffer.getDataWriteEventBuffer()[1]);
    assertEquals(2, eventBuffer.getIndexWriteEventBuffer().length);
    assertNull(eventBuffer.getIndexWriteEventBuffer()[0]);
    assertEvent(expectedIndexEvent, eventBuffer.getIndexWriteEventBuffer()[1]);
  }

  private static void assertEvent(WriteMetadataEvent expected, WriteMetadataEvent actual) {
    assertEquals(expected.getTaskID(), actual.getTaskID());
    assertEquals(expected.getCheckpointId(), actual.getCheckpointId());
    assertEquals(expected.getInstantTime(), actual.getInstantTime());
    assertEquals(expected.isLastBatch(), actual.isLastBatch());
    assertEquals(expected.isMetadataTable(), actual.isMetadataTable());
    assertEquals(1, actual.getWriteStatuses().size());
    WriteStatus status = actual.getWriteStatuses().get(0);
    assertEquals(expected.isMetadataTable(), status.isMetadataTable());
    assertEquals(11L, status.getTotalRecords());
    assertEquals("file-1", status.getFileId());
    assertEquals("partition-a", status.getPartitionPath());
    assertEquals("partition-a/file-1.parquet", status.getStat().getPath());
  }

  private static WriteMetadataEvent event(
      int taskId, long checkpointId, String instant, boolean isMetadataTable) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId("file-1");
    stat.setPartitionPath("partition-a");
    stat.setPath("partition-a/file-1.parquet");
    stat.setNumWrites(11L);

    WriteStatus status = new WriteStatus(false, 0D, isMetadataTable);
    status.setFileId("file-1");
    status.setPartitionPath("partition-a");
    status.setTotalRecords(11L);
    status.setStat(stat);

    return WriteMetadataEvent.builder()
        .taskID(taskId)
        .checkpointId(checkpointId)
        .instantTime(instant)
        .writeStatus(Collections.singletonList(status))
        .lastBatch(true)
        .metadataTable(isMetadataTable)
        .build();
  }
}
