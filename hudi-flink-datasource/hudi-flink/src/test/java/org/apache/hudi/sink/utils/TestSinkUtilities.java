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

import org.apache.hudi.exception.HoodieException;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSinkUtilities {

  @Test
  void testNaturalOrderImplementationsUseConstantKeys() {
    GenericRowData first = GenericRowData.of(1);
    GenericRowData second = GenericRowData.of(2);
    assertEquals(0, new NaturalOrderRecordComparator().compare(first, second));

    NaturalOrderKeyComputer keyComputer = new NaturalOrderKeyComputer();
    MemorySegment firstSegment = MemorySegmentFactory.wrap(new byte[] {42});
    MemorySegment secondSegment = MemorySegmentFactory.wrap(new byte[] {24});
    keyComputer.putKey(first, firstSegment, 0);

    assertEquals(0, firstSegment.get(0));
    assertEquals(0, keyComputer.compareKey(firstSegment, 0, secondSegment, 0));
    keyComputer.swapKey(firstSegment, 0, secondSegment, 0);
    assertEquals(0, firstSegment.get(0));
    assertEquals(24, secondSegment.get(0));
    assertEquals(1, keyComputer.getNumKeyBytes());
    assertTrue(keyComputer.isKeyFullyDetermines());
    assertFalse(keyComputer.invertKey());
  }

  @Test
  void testOperatorIdGenerationIsDeterministicAndUidSpecific() {
    OperatorID first = OperatorIDGenerator.fromUid("writer");

    assertEquals(first, OperatorIDGenerator.fromUid("writer"));
    assertNotEquals(first, OperatorIDGenerator.fromUid("committer"));
  }

  @Test
  void testSamplingActionRunsAtEachConfiguredBoundary() {
    SamplingActionExecutor executor = new SamplingActionExecutor(3);
    AtomicInteger invocations = new AtomicInteger();

    for (int i = 0; i < 7; i++) {
      executor.runIfNecessary(invocations::incrementAndGet);
    }

    assertEquals(2, invocations.get());
  }

  @Test
  void testExplicitClassloaderThreadFactoryConfiguresSingleThread() throws Exception {
    ClassLoader classLoader = new ClassLoader() { };
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread.UncaughtExceptionHandler handler = (thread, throwable) -> failure.set(throwable);
    ExplicitClassloaderThreadFactory factory =
        new ExplicitClassloaderThreadFactory("coordinator", classLoader, handler);
    RuntimeException expected = new RuntimeException("expected");

    Thread thread = factory.newThread(() -> {
      throw expected;
    });
    assertEquals("coordinator", thread.getName());
    assertSame(classLoader, thread.getContextClassLoader());
    assertSame(handler, thread.getUncaughtExceptionHandler());
    thread.start();
    thread.join();

    assertSame(expected, failure.get());
    assertThrows(Error.class, () -> factory.newThread(() -> { }));
  }

  @Test
  void testTimeWaitBuilderAndTimeout() {
    assertThrows(NullPointerException.class, () -> TimeWait.builder().build());

    TimeWait wait = TimeWait.builder()
        .action("checkpoint acknowledgement")
        .timeout(1)
        .interval(1)
        .build();
    wait.waitFor();
    wait.waitFor();

    HoodieException timeout = assertThrows(HoodieException.class, wait::waitFor);
    assertTrue(timeout.getMessage().contains("checkpoint acknowledgement"));
  }

  @Test
  void testTimeWaitWrapsInterruptionAndPreservesCause() {
    TimeWait wait = TimeWait.builder().action("interruptible action").interval(1).build();
    Thread.currentThread().interrupt();
    try {
      HoodieException exception = assertThrows(HoodieException.class, wait::waitFor);
      assertTrue(exception.getCause() instanceof InterruptedException);
      assertTrue(exception.getMessage().contains("interruptible action"));
    } finally {
      assertFalse(Thread.currentThread().isInterrupted());
    }
  }
}
