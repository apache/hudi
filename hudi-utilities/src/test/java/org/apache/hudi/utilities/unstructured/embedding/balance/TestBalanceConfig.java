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

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The size- and type-aware strategy's knobs: the per-group byte budget per task ({@link SizeTypeAwareBalancingStrategy#maxBytesPerTask}),
 * the warning list of budget keys naming no configured group, the parallelism P each group's floor uses
 * ({@link BalanceParallelism}), and which strategy a job gets.
 */
class TestBalanceConfig {

  private static final long MIB = 1024L * 1024L;

  private static String budgetKey(String group) {
    return EmbeddingTableConfig.BALANCE_PREFIX + group + EmbeddingTableConfig.BALANCE_MAX_BYTES_PER_TASK_SUFFIX;
  }

  @Test
  void sizeTypeDefaultAndOptionalHashStrategy() {
    assertEquals(SizeTypeAwareBalancingStrategy.class, RowBalancer.strategyFrom(new TypedProperties()).getClass());
    assertEquals(SizeTypeAwareBalancingStrategy.class.getName(), EmbeddingTableConfig.BALANCE_STRATEGY_CLASS.defaultValue());
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.BALANCE_STRATEGY_CLASS.key(), HashBalancingStrategy.class.getName());
    assertEquals(HashBalancingStrategy.class, RowBalancer.strategyFrom(props).getClass());
  }

  @Test
  void budgetsDefaultPerGroup() {
    TypedProperties props = new TypedProperties();
    assertEquals(256 * MIB, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, BalanceGroups.PDF));
    assertEquals(2048 * MIB, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, BalanceGroups.IMAGE));
    assertEquals(1024 * MIB, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, BalanceGroups.VIDEO));
    assertEquals(256 * MIB, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, "com.example.X"));
  }

  @Test
  void budgetsConfigured() {
    TypedProperties props = new TypedProperties();
    props.setProperty(budgetKey(BalanceGroups.VIDEO), "2147483648");
    props.setProperty(EmbeddingTableConfig.BALANCE_DEFAULT_MAX_BYTES_PER_TASK.key(), "1");
    props.setProperty(budgetKey("com.example.Y"), "7");
    assertEquals(2048 * MIB, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, BalanceGroups.VIDEO));
    assertEquals(1L, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, "com.example.X"));
    assertEquals(7L, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, "com.example.Y"));
    assertEquals(256 * MIB, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, BalanceGroups.PDF));
  }

  @Test
  void blankBudgetKeyCountsAsUnset() {
    TypedProperties props = new TypedProperties();
    props.setProperty(budgetKey(BalanceGroups.VIDEO), " ");
    props.setProperty(budgetKey("com.example.X"), "");
    props.setProperty(EmbeddingTableConfig.BALANCE_DEFAULT_MAX_BYTES_PER_TASK.key(), "3");
    assertEquals(1024 * MIB, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, BalanceGroups.VIDEO));
    assertEquals(3L, SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, "com.example.X"));
  }

  @Test
  void budgetsMustBePositive() {
    for (String value : new String[] {"0", "-5", "lots"}) {
      TypedProperties props = new TypedProperties();
      props.setProperty(budgetKey(BalanceGroups.PDF), value);
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
          () -> SizeTypeAwareBalancingStrategy.maxBytesPerTask(props, BalanceGroups.PDF));
      assertTrue(e.getMessage().contains(budgetKey(BalanceGroups.PDF)), e.getMessage());
      TypedProperties fallback = new TypedProperties();
      fallback.setProperty(EmbeddingTableConfig.BALANCE_DEFAULT_MAX_BYTES_PER_TASK.key(), value);
      e = assertThrows(IllegalArgumentException.class,
          () -> SizeTypeAwareBalancingStrategy.maxBytesPerTask(fallback, "com.example.X"));
      assertTrue(e.getMessage().contains(EmbeddingTableConfig.BALANCE_DEFAULT_MAX_BYTES_PER_TASK.key()), e.getMessage());
    }
  }

  @Test
  void unknownBudgetGroups() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        "com.example.TxtPreprocessor");
    for (String group : new String[] {BalanceGroups.PDF, "default",
        "com.example.TxtPreprocessor"}) {
      props.setProperty(budgetKey(group), "1");
    }
    BalanceGroups groups = BalanceGroups.from(props);
    assertEquals(Collections.emptyList(), SizeTypeAwareBalancingStrategy.unknownBudgetGroups(props, groups));
    props.setProperty(budgetKey("pfd"), "1");
    assertEquals(Collections.singletonList("pfd"), SizeTypeAwareBalancingStrategy.unknownBudgetGroups(props, groups));
  }

}
