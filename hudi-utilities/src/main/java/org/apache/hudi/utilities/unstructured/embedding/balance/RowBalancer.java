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
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Entry point of prep balancing: loads the configured {@link RowBalancingStrategy} and applies it to the batch, one row per
 * source version (sha, modification time, size), blob bytes included.
 */
public final class RowBalancer {

  private RowBalancer() {
  }

  /**
   * @return the strategy {@code hoodie.unstructured.embedding.prep.balance.strategy.class} names, default
   *     {@link HashBalancingStrategy}
   */
  public static RowBalancingStrategy strategyFrom(TypedProperties props) {
    return ReflectionUtils.loadClass(ConfigUtils.getStringWithAltKeys(props, EmbeddingTableConfig.BALANCE_STRATEGY_CLASS,
        HashBalancingStrategy.class.getName()));
  }

  /**
   * Balances {@code blobRows} with {@code strategy} ({@link RowBalancingStrategy#balance}).
   *
   * @return the balanced rows; close it once the rows are consumed
   */
  public static BalancedRows balance(Dataset<Row> blobRows, RowBalancingStrategy strategy, TypedProperties props) {
    return strategy.balance(blobRows, props);
  }
}
