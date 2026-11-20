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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A {@link Transformer} that writes files for the instant of the round, and so is given that instant.
 *
 * <p>A transformer chain with one makes the streamer start the round's commit (REQUESTED) once the source returned data, and move
 * it to INFLIGHT right before the instant-aware transformer runs ({@link #apply(JavaSparkContext, SparkSession, Dataset,
 * TypedProperties, String, Runnable)}), so every file the transformer writes for the instant follows the write protocol: its
 * CREATE marker first, then the file, under an inflight instant, and a rollback of the instant removes it. The round's write then
 * takes the instant that is already INFLIGHT. A chain without one runs as before: no instant exists while it transforms.
 *
 * <p>Files the transformer writes are not in the commit's write stats, so they survive the commit only under the table's meta
 * folder; marker reconciliation deletes them anywhere else.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface InstantAwareTransformer extends Transformer {

  /**
   * Transforms the source dataset for the round that writes to {@code instantTime}.
   *
   * @param jsc          JavaSparkContext
   * @param sparkSession Spark Session
   * @param rowDataset   Source DataSet
   * @param properties   Config properties
   * @param instantTime  the INFLIGHT instant of the round
   * @return Transformed Dataset
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties, String instantTime);

  /**
   * Transforms the source dataset for the round that writes to {@code instantTime}, which is still REQUESTED: {@code toInflight}
   * moves it to INFLIGHT, and must run before this transformer writes any file for the instant (running it again does nothing).
   * The default moves it at once and calls {@link #apply(JavaSparkContext, SparkSession, Dataset, TypedProperties, String)}.
   *
   * @param toInflight moves the instant to INFLIGHT
   * @return Transformed Dataset
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties,
                             String instantTime, Runnable toInflight) {
    toInflight.run();
    return apply(jsc, sparkSession, rowDataset, properties, instantTime);
  }

  /**
   * Whether this transformer writes files for the round's instant, so the streamer starts the instant before it. True; a
   * transformer that wraps others (a chain) answers for what it wraps.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default boolean needsInstant() {
    return true;
  }
}
