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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Properties;

/**
 * What a source requires of the Hudi Streamer job that reads it, applied when the job starts and before the source is built.
 * A source has a contract when it is registered in {@link SourceContracts}; every other source gets {@link SourceContracts#NONE},
 * under which the streamer behaves as without contracts.
 */
public interface SourceContract {

  /** Returns the job's settings, rejecting conflicts without changing the caller's properties. */
  TypedProperties prepareJob(TypedProperties props);

  /** Whether these unchanged properties already passed this contract's job preparation. */
  default boolean isPreparedJob(TypedProperties props) {
    return false;
  }

  /** Opaque properties to persist when this source creates a new table. */
  default Properties tableProperties() {
    return new Properties();
  }

  /**
   * Returns the transformers to run, including any the source requires. {@code configured} is the job's configured chain, and
   * null (or empty) when the job configures none; a null or empty result runs no transformer.
   */
  default List<String> transformerChain(List<String> configured) {
    return configured;
  }

  /**
   * Checks the target table before the streamer's own table validation; throws to refuse the job. The default accepts every
   * table. It may run more than once for one job (the streamer service and its sync each check), and the sync also checks a
   * table it has just created.
   */
  default void checkTable(String basePath, HoodieTableConfig tableConfig) {
  }

  /**
   * Checks the job before its source is built or any input is read; throws to refuse the job. Called only for a source with a
   * contract. The default accepts every job.
   *
   * @param chain        the transformer chain the job runs, from {@link #transformerChain(List)}; empty when there is none
   * @param props        the prepared job properties, with the target base path set
   * @param targetSchema the target schema of the job's schema provider, when it gives one
   * @param tableSchema  the table's latest writer schema without meta columns; empty for a new table, one with no commit, or
   *                     when the schema could not be read
   */
  default void checkJob(JavaSparkContext jsc, SparkSession spark, List<String> chain, TypedProperties props,
                        Option<HoodieSchema> targetSchema, Option<StructType> tableSchema) {
  }
}
