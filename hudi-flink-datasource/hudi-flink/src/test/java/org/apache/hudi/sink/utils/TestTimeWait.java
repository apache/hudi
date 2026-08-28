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

package org.apache.hudi.sink.utils;

import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link TimeWait}. */
class TestTimeWait {

  @Test
  void testBuilderRequiresAction() {
    assertThrows(NullPointerException.class, () -> TimeWait.builder().build());
  }

  @Test
  void testWaitAndTimeout() {
    TimeWait timeWait = TimeWait.builder()
        .timeout(1)
        .interval(2)
        .action("test action")
        .build();

    assertDoesNotThrow(timeWait::waitFor);
    HoodieException exception = assertThrows(HoodieException.class, timeWait::waitFor);
    assertTrue(exception.getMessage().contains("test action"));
  }

  @Test
  void testInterruptedWaitIsWrapped() {
    TimeWait timeWait = TimeWait.builder()
        .interval(1)
        .action("interruptible action")
        .build();

    Thread.currentThread().interrupt();
    try {
      HoodieException exception = assertThrows(HoodieException.class, timeWait::waitFor);
      assertTrue(exception.getMessage().contains("interruptible action"));
      assertTrue(exception.getCause() instanceof InterruptedException);
    } finally {
      Thread.interrupted();
    }
  }
}
