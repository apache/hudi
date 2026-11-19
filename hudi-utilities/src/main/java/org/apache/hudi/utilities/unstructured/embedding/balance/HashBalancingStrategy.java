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

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Spreads the batch's rows over P Spark partitions by a hash of their source version key (sha, modification time, size), with
 * one shuffle of the rows ({@link HashSpread}); P = {@code hoodie.unstructured.embedding.prep.balance.num.partitions}, default
 * {@code spark.default.parallelism}. A row's partition depends on its own version and P alone, never on the batch's partitioning
 * or order, so a recomputed partition reads the same rows. Partition row counts are balanced in expectation (binomial(N, 1/P)),
 * not exactly equal. There is no assignment to persist, so the returned {@link BalancedRows} has none.
 */
public class HashBalancingStrategy implements RowBalancingStrategy {

  private static final long serialVersionUID = 1L;

  @Override
  public BalancedRows balance(Dataset<Row> blobRows, TypedProperties writerProps) {
    int numPartitions = EmbeddingTableConfig.from(writerProps)
        .balanceNumPartitions(blobRows.sparkSession().sparkContext().defaultParallelism());
    Column[] key = BalancingAssignment.KEY_COLUMNS.stream().map(functions::col).toArray(Column[]::new);
    return new BalancedRows(HashSpread.of(blobRows, numPartitions, key).javaRDD());
  }
}
