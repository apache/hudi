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

package org.apache.hudi.common.util.queue;

import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that a Disruptor consumer failure reaches {@link DisruptorExecutor#execute()}
 * even when production and {@link HoodieConsumer#finish()} complete normally.
 */
public class TestDisruptorExecutorConsumerFailure {

  @Test
  @Timeout(30)
  public void testExecutePropagatesStoredConsumerFailureAfterFinish() {
    IllegalStateException consumeFailure = new IllegalStateException("consume failed");
    AtomicBoolean finished = new AtomicBoolean(false);
    HoodieConsumer<String, String> consumer = new HoodieConsumer<String, String>() {
      @Override
      public void consume(String record) {
        throw consumeFailure;
      }

      @Override
      public String finish() {
        finished.set(true);
        return "finished";
      }
    };

    DisruptorExecutor<String, String, String> executor = new DisruptorExecutor<>(
        8,
        Collections.singletonList("row").iterator(),
        consumer,
        record -> record,
        WaitStrategyFactory.DEFAULT_STRATEGY,
        () -> { });

    try {
      HoodieException thrown = assertThrows(HoodieException.class, executor::execute);
      assertSame(consumeFailure, thrown.getCause());
      assertTrue(finished.get(), "finish() must still release consumer resources");
    } finally {
      executor.shutdownNow();
    }
  }
}
