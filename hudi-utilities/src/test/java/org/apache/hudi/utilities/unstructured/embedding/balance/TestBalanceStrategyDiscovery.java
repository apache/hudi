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

package org.apache.hudi.utilities.unstructured.embedding.balance;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** The size and type aware prep strategy is the default, and the hash strategy can be selected. */
class TestBalanceStrategyDiscovery {

  private static final String STRATEGY = "org.apache.hudi.utilities.unstructured.embedding.balance.HashBalancingStrategy";

  @Test
  void defaultIsSizeTypeAware() {
    assertInstanceOf(SizeTypeAwareBalancingStrategy.class, RowBalancer.strategyFrom(new TypedProperties()));
  }

  @Test
  void hashStrategyCanBeSelected() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.BALANCE_STRATEGY_CLASS.key(), STRATEGY);
    assertEquals(STRATEGY, RowBalancer.strategyFrom(props).getClass().getName());
  }
}
