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

import org.apache.hudi.sink.event.WriteMetadataEvent;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link EventBuffers}.
 */
public class TestEventBuffers {

  @Test
  void testPendingInstantsBeforeIncludeBuffersWithoutEvents() {
    EventBuffers buffers = EventBuffers.getInstance(new Configuration(), 1);
    assertTrue(buffers.getPendingInstantsBefore(3L).isEmpty());

    buffers.initNewEventBuffer(1L, "001");
    assertEquals(Collections.singletonList("001"), buffers.getPendingInstantsBefore(2L));
    assertFalse(buffers.nonEmpty());
    assertTrue(buffers.getPendingInstantsBefore(1L).isEmpty());

    buffers.addEventToBuffer(WriteMetadataEvent.builder()
        .taskID(0)
        .checkpointId(1L)
        .instantTime("001")
        .writeStatus(Collections.emptyList())
        .lastBatch(true)
        .build());
    assertEquals(Collections.singletonList("001"), buffers.getPendingInstantsBefore(2L));
    assertTrue(buffers.nonEmpty());

    buffers.initNewEventBuffer(2L, "002");
    assertEquals(Arrays.asList("001", "002"), buffers.getPendingInstantsBefore(3L));
    buffers.reset(1L);
    assertTrue(buffers.getPendingInstantsBefore(2L).isEmpty());
    assertEquals(Collections.singletonList("002"), buffers.getPendingInstantsBefore(3L));
    assertFalse(buffers.nonEmpty());

    buffers.reset(2L);
    assertTrue(buffers.getPendingInstantsBefore(3L).isEmpty());
  }
}
