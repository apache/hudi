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

package org.apache.hudi.common.util;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSamplingLogger {

  @Test
  public void testShouldLogAtInfoWithFrequency5() {
    Logger mockLogger = mock(Logger.class);
    SamplingLogger samplingLogger = new SamplingLogger(mockLogger, 5);

    // Calls 1-4 should return false (DEBUG)
    assertFalse(samplingLogger.shouldLogAtInfo());
    assertFalse(samplingLogger.shouldLogAtInfo());
    assertFalse(samplingLogger.shouldLogAtInfo());
    assertFalse(samplingLogger.shouldLogAtInfo());

    // Call 5 should return true (INFO)
    assertTrue(samplingLogger.shouldLogAtInfo());

    // Calls 6-9 should return false (DEBUG)
    assertFalse(samplingLogger.shouldLogAtInfo());
    assertFalse(samplingLogger.shouldLogAtInfo());
    assertFalse(samplingLogger.shouldLogAtInfo());
    assertFalse(samplingLogger.shouldLogAtInfo());

    // Call 10 should return true (INFO)
    assertTrue(samplingLogger.shouldLogAtInfo());
  }

  @Test
  public void testShouldLogAtInfoWithFrequency1() {
    Logger mockLogger = mock(Logger.class);
    SamplingLogger samplingLogger = new SamplingLogger(mockLogger, 1);

    // Every call should return true (always INFO)
    assertTrue(samplingLogger.shouldLogAtInfo());
    assertTrue(samplingLogger.shouldLogAtInfo());
    assertTrue(samplingLogger.shouldLogAtInfo());
  }

  @Test
  public void testLogInfoOrDebugWithSupplierOnlyEvaluatesWhenNeeded() {
    Logger mockLogger = mock(Logger.class);
    when(mockLogger.isDebugEnabled()).thenReturn(false);

    SamplingLogger samplingLogger = new SamplingLogger(mockLogger, 5);
    AtomicInteger supplierCallCount = new AtomicInteger(0);

    for (int i = 0; i < 10; i++) {
      samplingLogger.logInfoOrDebug("Test message {}", () -> {
        supplierCallCount.incrementAndGet();
        return new Object[]{"arg"};
      });
    }

    assertEquals(2, supplierCallCount.get());
  }

  @Test
  public void testLogInfoOrDebugWithSupplierEvaluatesForDebugWhenEnabled() {
    Logger mockLogger = mock(Logger.class);
    when(mockLogger.isDebugEnabled()).thenReturn(true);

    SamplingLogger samplingLogger = new SamplingLogger(mockLogger, 5);
    AtomicInteger supplierCallCount = new AtomicInteger(0);

    for (int i = 0; i < 10; i++) {
      samplingLogger.logInfoOrDebug("Test message {}", () -> {
        supplierCallCount.incrementAndGet();
        return new Object[]{"arg"};
      });
    }

    assertEquals(10, supplierCallCount.get());
  }
}
