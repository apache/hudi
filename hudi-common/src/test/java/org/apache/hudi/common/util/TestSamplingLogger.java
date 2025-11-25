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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TestSamplingLogger {

  @Test
  void testShouldLogAtInfoWithFrequency5() {
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
  void testShouldLogAtInfoWithFrequency1() {
    Logger mockLogger = mock(Logger.class);
    SamplingLogger samplingLogger = new SamplingLogger(mockLogger, 1);

    // Every call should return true (always INFO)
    assertTrue(samplingLogger.shouldLogAtInfo());
    assertTrue(samplingLogger.shouldLogAtInfo());
    assertTrue(samplingLogger.shouldLogAtInfo());
  }

  @Test
  void testLogInfoOrDebugCallsCorrectMethod() {
    Logger mockLogger = mock(Logger.class);
    SamplingLogger samplingLogger = new SamplingLogger(mockLogger, 5);

    // Make 10 calls
    for (int i = 0; i < 10; i++) {
      samplingLogger.logInfoOrDebug("Test message {}", i);
    }

    // Should have called info() 2 times (at calls 5 and 10)
    verify(mockLogger, times(2)).info(anyString(), any(Object[].class));

    // Should have called debug() 8 times (calls 1-4 and 6-9)
    verify(mockLogger, times(8)).debug(anyString(), any(Object[].class));
  }

  @Test
  void testLogInfoOrDebugWithMultipleArgs() {
    Logger mockLogger = mock(Logger.class);
    SamplingLogger samplingLogger = new SamplingLogger(mockLogger, 2);

    // Call 1 - should be DEBUG
    samplingLogger.logInfoOrDebug("Message with {} and {}", "arg1", "arg2");
    verify(mockLogger, times(1)).debug("Message with {} and {}", new Object[]{"arg1", "arg2"});

    // Call 2 - should be INFO
    samplingLogger.logInfoOrDebug("Message with {} and {}", "arg3", "arg4");
    verify(mockLogger, times(1)).info("Message with {} and {}", new Object[]{"arg3", "arg4"});
  }
}
