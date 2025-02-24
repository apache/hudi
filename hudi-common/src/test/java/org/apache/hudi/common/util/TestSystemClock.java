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

package org.apache.hudi.common.util;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestSystemClock {

  @Test
  void testNow() {
    Instant instant = Instant.now();
    try (MockedStatic<Instant> instantMock = Mockito.mockStatic(Instant.class)) {
      instantMock.when(Instant::now).thenReturn(instant);
      SystemClock clock = new SystemClock();
      assertEquals(instant, clock.now());
    }
  }

  @Test
  void testCurrentEpoch() {
    Instant instant = Instant.now();
    try (MockedStatic<Instant> instantMock = Mockito.mockStatic(Instant.class)) {
      instantMock.when(Instant::now).thenReturn(instant);
      SystemClock clock = new SystemClock();
      assertEquals(instant.toEpochMilli(), clock.currentEpoch());
    }
  }
}
