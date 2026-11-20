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
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Transform source to target dataset before writing.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public interface Transformer extends AutoCloseable {

  /** Releases driver resources owned by this transformer when the streamer stops or rebuilds it. */
  @Override
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default void close() {
  }

  /**
   * Transform source RDD to target RDD.
   *
   * @param jsc JavaSparkContext
   * @param sparkSession Spark Session
   * @param rowDataset Source DataSet
   * @param properties Config properties
   * @return Transformed Dataset
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties);

  /**
   * Invoked on the driver after a durable table commit and the source's commit callback, in a round that starts its instant before
   * the transformer ({@link InstantAwareTransformer#needsInstant()}); any other round runs as stock and is not called back. The
   * default performs no action. A failure stops post-commit processing; the completed table commit is retained.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default void onCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime) {
  }

  /**
   * Invoked on the driver inside the data-table commit transaction, after the write and before the instant completes; a failure
   * aborts the commit, so nothing of the round becomes durable. It is invoked in every round with a transformer, including a
   * round that is not called back by {@link #onCommit}, so work staged here must not depend on {@link #onCommit} to be published
   * or cleaned up. The default performs no action. Implementations must not acquire
   * the table lock again: the commit already holds it when the writer's configuration requires one (a single writer without a
   * lock provider commits unlocked). To refuse the round, throw
   * {@link org.apache.hudi.utilities.exception.HoodiePreCommitRefusalException}: the streamer then rolls the refused instant back
   * before it rethrows. Any other failure also aborts the commit, but leaves the instant to the failed-writes cleaner.
   *
   * @param jsc          the job's JavaSparkContext
   * @param sparkSession the job's SparkSession
   * @param properties   the commit's write config properties, a copy this call may change
   * @param instantTime  the instant being committed
   * @param metaClient   the meta client of the table being committed, as the write client's commit sees it. Its timeline is
   *                     loaded before the commit takes the lock and is not reloaded inside it (conflict resolution uses a table
   *                     of its own), so an implementation must reload it before reading the timeline.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default void onPreCommit(JavaSparkContext jsc, SparkSession sparkSession, TypedProperties properties, String instantTime,
                           HoodieTableMetaClient metaClient) {
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
    Dataset<Row> emptyDataset = sparkSession.createDataFrame(sparkSession.emptyDataFrame().rdd(), incomingStruct);
    Dataset<Row> transformedDataset = this.apply(jsc, sparkSession, emptyDataset, properties);
    return transformedDataset.schema();
  }
}
