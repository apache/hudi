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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * Decides which Spark task preprocesses which source row: the pluggable prep balancing SPI, selected by
 * {@code hoodie.unstructured.embedding.prep.balance.strategy.class} ({@link RowBalancer#strategyFrom}).
 *
 * <p>A strategy takes the incremental batch, blob bytes included, and returns its rows spread over the prep tasks. It either
 * shuffles the rows itself ({@link HashBalancingStrategy}: one hash shuffle by the source version key), or extends
 * {@link AssignmentBalancingStrategy} and supplies an explicit (source version, partition) plan computed from the batch's
 * metadata, which the base class applies exactly ({@link SizeTypeAwareBalancingStrategy}). Either way a row's task must depend
 * on the batch's versions alone, never on its partitioning or order, so a recomputed task reads the same rows.
 * Implementations need a public no-arg constructor.
 */
public interface RowBalancingStrategy extends Serializable {

  /**
   * The batch may be EMPTY, so an implementation must return no rows for it, never fail on it.
   *
   * @param blobRows    the incremental batch, every column, one row per source version, possibly empty
   * @param writerProps the job's properties
   * @return every input row exactly once, whole, in the Spark partition of the task that prepares it; the caller closes it once
   *     the rows are consumed
   */
  BalancedRows balance(Dataset<Row> blobRows, TypedProperties writerProps);
}
