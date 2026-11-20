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

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Spreads rows over a fixed number of Spark partitions by a hash of their key columns, with one shuffle and nothing else: no
 * projection, no sort, no persisted plan and no join. It is Spark's hash partitioning, {@code pmod(murmur3(keys), P)}, and so:
 * <ul>
 *   <li><b>Deterministic.</b> A row's partition is a function of its own key column values and P alone. Murmur3 over the values is
 *   stable across JVMs, task attempts, input partitioning and input order, so a retried or recomputed task reads the same rows:
 *   none is duplicated or missed.</li>
 *   <li><b>Null-safe.</b> A null key column is skipped by the hash; the row is kept, never dropped.</li>
 *   <li><b>Exactly P partitions.</b> The partition count is user specified, which adaptive query execution never coalesces.</li>
 *   <li><b>Balanced in expectation.</b> Partition row counts are binomial(N, 1/P), not an exact floor or ceiling of N/P.</li>
 * </ul>
 */
public final class HashSpread {

  private HashSpread() {
  }

  /**
   * @return {@code rows} hash partitioned into exactly {@code numPartitions} partitions by {@code keys}
   * @throws IllegalArgumentException if {@code numPartitions} is not positive or no key is given
   */
  public static Dataset<Row> of(Dataset<Row> rows, int numPartitions, Column... keys) {
    ValidationUtils.checkArgument(numPartitions > 0, () -> "The number of partitions must be positive, got " + numPartitions);
    ValidationUtils.checkArgument(keys.length > 0, "At least one key column is needed");
    return rows.repartition(numPartitions, keys);
  }
}
