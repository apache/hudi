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
 * A mock implementation of S3EventsHoodieIncrSource used for testing StreamSync functionality.
 * This class simulates different checkpoint and data fetch scenarios to test the checkpoint handling
 * and data ingestion behavior of the StreamSync class.
 */
public class MockS3EventsHoodieIncrSource extends S3EventsHoodieIncrSource {
  
  /**
   * Constructs a new MockS3EventsHoodieIncrSource with the specified parameters.
   *
   * @param props TypedProperties containing configuration properties
   * @param sparkContext JavaSparkContext instance
   * @param sparkSession SparkSession instance
   * @param schemaProvider SchemaProvider for the source
   * @param metrics HoodieIngestionMetrics instance
   */
  public MockS3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider,
      HoodieIngestionMetrics metrics) {
    this(props, sparkContext, sparkSession, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props, sparkContext, sparkSession, metrics), new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public MockS3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      HoodieIngestionMetrics metrics,
      StreamContext streamContext) {
    this(props, sparkContext, sparkSession, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props, sparkContext, sparkSession, metrics), streamContext);
  }

  MockS3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      QueryRunner queryRunner,
      CloudDataFetcher cloudDataFetcher,
      StreamContext streamContext) {
    super(props, sparkContext, sparkSession, queryRunner, cloudDataFetcher, streamContext);
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
