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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.config.HoodieTTLConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link PartitionTTLStrategyType}.
 */
public class TestPartitionTTLStrategyType {

  @Test
  public void resolvesKeepByTimeFromType() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTTLConfig.PARTITION_TTL_STRATEGY_TYPE,
        PartitionTTLStrategyType.KEEP_BY_TIME.name());

    assertEquals(PartitionTTLStrategyType.KEEP_BY_TIME.getClassName(),
        PartitionTTLStrategyType.getPartitionTTLStrategyClassName(config));
  }

  @Test
  public void resolvesKeepByCreationTimeFromType() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTTLConfig.PARTITION_TTL_STRATEGY_TYPE,
        PartitionTTLStrategyType.KEEP_BY_CREATION_TIME.name());

    assertEquals(PartitionTTLStrategyType.KEEP_BY_CREATION_TIME.getClassName(),
        PartitionTTLStrategyType.getPartitionTTLStrategyClassName(config));
  }

  @Test
  public void resolvesKeepByEventTimeFromType() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTTLConfig.PARTITION_TTL_STRATEGY_TYPE,
        PartitionTTLStrategyType.KEEP_BY_EVENT_TIME.name());

    assertEquals(PartitionTTLStrategyType.KEEP_BY_EVENT_TIME.getClassName(),
        PartitionTTLStrategyType.getPartitionTTLStrategyClassName(config));
  }

  @Test
  public void throwsOnUnknownType() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTTLConfig.PARTITION_TTL_STRATEGY_TYPE, "NOT_A_REAL_TYPE");

    assertThrows(IllegalArgumentException.class,
        () -> PartitionTTLStrategyType.getPartitionTTLStrategyClassName(config));
  }
}
