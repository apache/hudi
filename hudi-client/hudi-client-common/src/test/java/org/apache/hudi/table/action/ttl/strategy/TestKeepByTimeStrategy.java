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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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

  /**
   * When the candidate partition count exceeds the configured max parallelism,
   * the stats collection must be capped at the configured value.
   */
  @Test
  public void testStatsParallelism_cappedByConfiguredMax() {
    int configuredMax = 500;
    List<String> partitions = IntStream.range(0, 1000)
        .mapToObj(i -> "p" + i)
        .collect(Collectors.toList());

    int captured = captureStatsParallelism(configuredMax, partitions);

    assertEquals(configuredMax, captured,
        "Parallelism should be capped at the configured max when partitions exceed it");
  }

  /**
   * When the candidate partition count is below the configured max parallelism,
   * the effective parallelism should be the partition count (avoid over-provisioning).
   */
  @Test
  public void testStatsParallelism_boundedByPartitionCount() {
    int configuredMax = 500;
    List<String> partitions = IntStream.range(0, 50)
        .mapToObj(i -> "p" + i)
        .collect(Collectors.toList());

    int captured = captureStatsParallelism(configuredMax, partitions);

    assertEquals(partitions.size(), captured,
        "Parallelism should equal the partition count when it is below the configured max");
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private int captureStatsParallelism(int configuredMax, List<String> partitions) {
    HoodieTable hoodieTable = mock(HoodieTable.class);
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    when(hoodieTable.getConfig()).thenReturn(writeConfig);
    when(writeConfig.getPartitionTTLStrategyDaysRetain()).thenReturn(10);
    when(writeConfig.getPartitionTTLStatsMaxParallelism()).thenReturn(configuredMax);
    when(hoodieTable.getContext()).thenReturn(context);
    // Return an empty result so no expired partitions are produced; we only care
    // about the parallelism argument handed to the engine.
    when(context.map(any(List.class), any(SerializableFunction.class), anyInt()))
        .thenReturn(Collections.emptyList());

    KeepByTimeStrategy strategy = new KeepByTimeStrategy(hoodieTable, "20240101000000000");
    strategy.getExpiredPartitionsForTimeStrategy(partitions);

    ArgumentCaptor<Integer> parallelismCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(context).map(any(List.class), any(SerializableFunction.class), parallelismCaptor.capture());
    return parallelismCaptor.getValue();
  }
}
