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

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link KeepByTimeStrategy}.
 */
public class TestKeepByTimeStrategy {

  /**
   * Regression test: when there are no candidate partitions to evaluate,
   * the strategy must short-circuit and return an empty result instead of
   * handing a parallelism of 0 to the engine, which would surface as:
   *   java.lang.IllegalArgumentException: Positive number of partitions required
   * from ParallelCollectionRDD.slice on the Spark path.
   */
  @Test
  public void testGetExpiredPartitionsForTimeStrategy_emptyInput_returnsEmptyWithoutTouchingEngine() {
    HoodieTable hoodieTable = mock(HoodieTable.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(hoodieTable.getConfig()).thenReturn(writeConfig);
    when(writeConfig.getPartitionTTLStrategyDaysRetain()).thenReturn(10);

    KeepByTimeStrategy strategy = new KeepByTimeStrategy(hoodieTable, "20240101000000000");

    List<String> expired = strategy.getExpiredPartitionsForTimeStrategy(Collections.emptyList());

    assertTrue(expired.isEmpty(), "Empty candidate list should yield no expired partitions");
    // Crucial: we must never reach the engine map call with parallelism=0.
    verify(hoodieTable, never()).getContext();
  }
}
