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

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link EventBuffers}.
 */
public class TestEventBuffers {

  @Test
  void testPendingInstantsIncludeBuffersWithoutEvents() {
    EventBuffers buffers = EventBuffers.getInstance(new Configuration(), 1);
    assertFalse(buffers.hasPendingInstants());

    buffers.initNewEventBuffer(1L, "001");
    assertTrue(buffers.hasPendingInstants());
    assertFalse(buffers.nonEmpty());

    buffers.addEventToBuffer(WriteMetadataEvent.builder()
        .taskID(0)
        .checkpointId(1L)
        .instantTime("001")
        .writeStatus(Collections.emptyList())
        .lastBatch(true)
        .build());
    assertTrue(buffers.hasPendingInstants());
    assertTrue(buffers.nonEmpty());

    buffers.initNewEventBuffer(2L, "002");
    buffers.reset(1L);
    assertTrue(buffers.hasPendingInstants());
    assertFalse(buffers.nonEmpty());

    buffers.reset(2L);
    assertFalse(buffers.hasPendingInstants());
  }
}
