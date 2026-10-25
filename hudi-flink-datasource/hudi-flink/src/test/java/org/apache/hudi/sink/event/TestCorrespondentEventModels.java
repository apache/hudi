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

import org.apache.hudi.exception.HoodieException;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestCorrespondentEventModels {

  @Test
  void testCorrespondentFactoryAndRequestResponseModels() {
    OperatorID operatorId = new OperatorID();
    TaskOperatorEventGateway gateway = mock(TaskOperatorEventGateway.class);
    Correspondent correspondent = Correspondent.getInstance(operatorId, gateway);
    assertSame(operatorId, correspondent.getOperatorID());
    assertSame(gateway, correspondent.getGateway());

    assertEquals(9L, Correspondent.InstantTimeRequest.getInstance(9L).getCheckpointId());
    assertEquals("001", Correspondent.InstantTimeResponse.getInstance("001").getInstant());
    assertNull(Correspondent.InstantTimeResponse.getInstance(null).getInstant());
    assertNotNull(Correspondent.InflightInstantsRequest.getInstance());

    HashMap<Long, String> instants = new HashMap<>();
    instants.put(9L, "001");
    assertSame(instants,
        Correspondent.InflightInstantsResponse.getInstance(instants).getInflightInstants());
  }

  @Test
  void testCommitAckFactoryAndAccessors() {
    CommitAckEvent event = CommitAckEvent.getInstance(11L);
    assertEquals(11L, event.getCheckpointId());
    event.setCheckpointId(12L);
    assertEquals(12L, event.getCheckpointId());
  }

  @Test
  void testInstantRequestPollsUntilReady() {
    AtomicInteger requestCount = new AtomicInteger();
    Correspondent correspondent = new Correspondent() {
      @Override
      protected InstantTimeResponse fetchInstantTimeResponse(long checkpointId) {
        return InstantTimeResponse.getInstance(requestCount.getAndIncrement() == 0 ? null : "001");
      }
    };

    assertEquals("001", correspondent.requestInstantTime(9L, 10_000L));
    assertEquals(2, requestCount.get());
  }

  @Test
  void testInstantRequestTimeoutDoesNotRetry() {
    AtomicInteger requestCount = new AtomicInteger();
    Correspondent correspondent = new Correspondent() {
      @Override
      protected InstantTimeResponse fetchInstantTimeResponse(long checkpointId) {
        requestCount.incrementAndGet();
        return InstantTimeResponse.getInstance(null);
      }
    };

    HoodieException error = assertThrows(HoodieException.class, () -> correspondent.requestInstantTime(9L, 0L));
    assertEquals("Timeout waiting for the instant time from the coordinator for checkpoint 9", error.getMessage());
    assertEquals(1, requestCount.get());
  }

  @Test
  void testInstantPollingPreservesInterrupt() {
    Correspondent correspondent = new Correspondent() {
      @Override
      protected InstantTimeResponse fetchInstantTimeResponse(long checkpointId) {
        return InstantTimeResponse.getInstance(null);
      }
    };

    Thread.currentThread().interrupt();
    try {
      HoodieException error = assertThrows(HoodieException.class, () -> correspondent.requestInstantTime(9L, 10_000L));
      assertInstanceOf(InterruptedException.class, error.getCause());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void testInstantRequestFailureIsNotRetried() {
    AtomicInteger requestCount = new AtomicInteger();
    Correspondent correspondent = new Correspondent() {
      @Override
      protected InstantTimeResponse fetchInstantTimeResponse(long checkpointId) throws Exception {
        requestCount.incrementAndGet();
        throw new IOException("request failed");
      }
    };

    HoodieException error = assertThrows(
        HoodieException.class, () -> correspondent.requestInstantTime(9L, 10_000L));
    assertEquals("request failed", error.getCause().getMessage());
    assertEquals(1, requestCount.get());
  }
}
