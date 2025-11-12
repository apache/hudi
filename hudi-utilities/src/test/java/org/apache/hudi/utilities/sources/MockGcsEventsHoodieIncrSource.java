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
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A mock implementation of GcsEventsHoodieIncrSource used for testing StreamSync functionality.
 * This class simulates different checkpoint and data fetch scenarios to test the checkpoint handling
 * and data ingestion behavior of the StreamSync class.
 */
public class MockGcsEventsHoodieIncrSource extends GcsEventsHoodieIncrSource {

  public MockGcsEventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext jsc,
      SparkSession spark,
      SchemaProvider schemaProvider,
      HoodieIngestionMetrics metrics) {
    super(props, jsc, spark,
        new CloudDataFetcher(props, jsc, spark, metrics),
        new QueryRunner(spark, props),
        new DefaultStreamContext(schemaProvider, Option.empty())
    );
  }

  public MockGcsEventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext jsc,
      SparkSession spark,
      HoodieIngestionMetrics metrics,
      StreamContext streamContext) {
    super(props, jsc, spark,
        new CloudDataFetcher(props, jsc, spark, metrics),
        new QueryRunner(spark, props),
        streamContext
    );
  }

  /**
   * Overrides the fetchNextBatch method to simulate different test scenarios based on configuration.
   *
   * @param lastCheckpoint Option containing the last checkpoint
   * @param sourceLimit maximum number of records to fetch
   * @return Pair containing Option<Dataset<Row>> and Checkpoint
   */
  @Override
  public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    CheckpointValidator.validateCheckpointOption(lastCheckpoint, props);
    return DummyOperationExecutor.executeDummyOperation(lastCheckpoint, sourceLimit, props);
  }
}
