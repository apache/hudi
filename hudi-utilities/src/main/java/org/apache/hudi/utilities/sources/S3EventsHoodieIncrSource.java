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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectIncrCheckpoint;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.QueryInfo;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon.Type.S3;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getHollowCommitHandleMode;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getMissingCheckpointStrategy;

/**
 * This source will use the S3 events meta information from hoodie table generate by {@link S3EventsSource}.
 */
public class S3EventsHoodieIncrSource extends HoodieIncrSource {

  private static final Logger LOG = LoggerFactory.getLogger(S3EventsHoodieIncrSource.class);
  private final String srcPath;
  private final int numInstantsPerFetch;
  private final IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy;
  private final QueryRunner queryRunner;
  private final CloudDataFetcher cloudDataFetcher;

  private final Option<SchemaProvider> schemaProvider;

  private final Option<SnapshotLoadQuerySplitter> snapshotLoadQuerySplitter;

  public S3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider,
      HoodieIngestionMetrics metrics) {
    this(props, sparkContext, sparkSession, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props, sparkSession, metrics), new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public S3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      HoodieIngestionMetrics metrics,
      StreamContext streamContext) {
    this(props, sparkContext, sparkSession, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props, sparkSession, metrics), streamContext);
  }

  S3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      QueryRunner queryRunner,
      CloudDataFetcher cloudDataFetcher,
      StreamContext streamContext) {
    super(props, sparkContext, sparkSession, streamContext);
    checkRequiredConfigProperties(props, Collections.singletonList(HOODIE_SRC_BASE_PATH));
    this.srcPath = getStringWithAltKeys(props, HOODIE_SRC_BASE_PATH);
    this.numInstantsPerFetch = getIntWithAltKeys(props, NUM_INSTANTS_PER_FETCH);
    this.missingCheckpointStrategy = getMissingCheckpointStrategy(props);
    this.queryRunner = queryRunner;
    this.cloudDataFetcher = cloudDataFetcher;
    this.schemaProvider = Option.ofNullable(streamContext.getSchemaProvider());
    this.snapshotLoadQuerySplitter = SnapshotLoadQuerySplitter.getInstance(props);
  }

  @Override
  protected Option<Checkpoint> translateCheckpoint(Option<Checkpoint> lastCheckpoint) {
    if (lastCheckpoint.isPresent()) {
      ValidationUtils.checkArgument(lastCheckpoint.get() instanceof StreamerCheckpointV1,
          "For S3EventsHoodieIncrSource, only StreamerCheckpointV1, i.e., requested time-based "
              + "checkpoint, is supported. Checkpoint provided is: " + lastCheckpoint.get());
    }
    return lastCheckpoint;
  }

  @Override
  public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint = CloudObjectIncrCheckpoint.fromString(lastCheckpoint);
    HollowCommitHandling handlingMode = getHollowCommitHandleMode(props);
    QueryInfo queryInfo =
        IncrSourceHelper.generateQueryInfo(
            sparkContext, srcPath, numInstantsPerFetch,
            Option.of(new StreamerCheckpointV1(cloudObjectIncrCheckpoint.getCommit())),
            missingCheckpointStrategy, handlingMode,
            HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            CloudObjectsSelectorCommon.S3_OBJECT_KEY,
            CloudObjectsSelectorCommon.S3_OBJECT_SIZE, true,
            Option.ofNullable(cloudObjectIncrCheckpoint.getKey()));
    LOG.info("Querying S3 with:{}, queryInfo:{}", cloudObjectIncrCheckpoint, queryInfo);

    if (isNullOrEmpty(cloudObjectIncrCheckpoint.getKey()) && queryInfo.areStartAndEndInstantsEqual()) {
      LOG.info("Already caught up. No new data to process");
      return Pair.of(Option.empty(), new StreamerCheckpointV1(queryInfo.getEndInstant()));
    }
    return cloudDataFetcher.fetchPartitionedSource(S3, cloudObjectIncrCheckpoint, this.sourceProfileSupplier, queryRunner.run(queryInfo, snapshotLoadQuerySplitter), this.schemaProvider, sourceLimit);
  }
}
