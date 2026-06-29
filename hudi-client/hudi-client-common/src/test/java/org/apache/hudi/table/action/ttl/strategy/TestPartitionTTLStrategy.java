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

package org.apache.hudi.table.action.ttl.strategy;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPartitionTTLStrategy {

  private static final long MILLIS_PER_DAY = 24L * 3600L * 1000L;

  private KeepByTimeStrategy newStrategy(int daysRetain) {
    HoodieTable hoodieTable = mock(HoodieTable.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(hoodieTable.getConfig()).thenReturn(writeConfig);
    when(writeConfig.getPartitionTTLStrategyDaysRetain()).thenReturn(daysRetain);
    return new KeepByTimeStrategy(hoodieTable, "");
  }

  /**
   * Verify that ttlInMilis is computed in long arithmetic, so values that would
   * overflow a 32-bit int multiplication (daysRetain > 24) still yield the
   * correct positive millisecond value.
   *
   * <p>Without the {@code (long)} cast on {@code getPartitionTTLStrategyDaysRetain()},
   * the expression {@code daysRetain * 1000 * 3600 * 24} is evaluated as int and
   * overflows once daysRetain * 86_400_000 exceeds Integer.MAX_VALUE
   * (i.e., for any daysRetain >= 25).
   */
  @Test
  public void testKeepByTimeStrategyTTLInMilis() {
    // Small value: no overflow either way, sanity check.
    assertEquals(1L * MILLIS_PER_DAY, newStrategy(1).ttlInMilis);

    // Largest value that does NOT overflow int: 24 * 86_400_000 = 2_073_600_000.
    assertEquals(24L * MILLIS_PER_DAY, newStrategy(24).ttlInMilis);

    // 25 days: 25 * 86_400_000 = 2_160_000_000, exceeds Integer.MAX_VALUE.
    // With the (long) cast the result is correct; without it the int expression
    // would overflow to a negative value.
    int days = 25;
    long expected = (long) days * MILLIS_PER_DAY;
    assertEquals(expected, newStrategy(days).ttlInMilis);
    assertTrue(newStrategy(days).ttlInMilis > 0,
        "ttlInMilis must stay positive for daysRetain beyond the int-overflow boundary");
    // Guard against regression: the unfixed int expression would produce this
    // (overflowed) value. The fixed code must NOT match it.
    int overflowed = days * 1000 * 3600 * 24;
    assertEquals(-2_134_967_296, overflowed, "sanity: confirm the int expression overflows for 25 days");
    assertTrue(newStrategy(days).ttlInMilis != overflowed,
        "ttlInMilis must not equal the int-overflowed value");

    // A clearly large value (e.g., 365 days) to ensure long arithmetic holds.
    assertEquals(365L * MILLIS_PER_DAY, newStrategy(365).ttlInMilis);

    // Zero / disabled.
    assertEquals(0L, newStrategy(0).ttlInMilis);
  }
}
