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
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectIncrCheckpoint;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.QueryInfo;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.config.CloudSourceConfig.ENABLE_EXISTS_CHECK;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH;
import static org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX;
import static org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig.S3_INCR_ENABLE_EXISTS_CHECK;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon.getCloudObjectMetadataPerPartition;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getHollowCommitHandleMode;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getMissingCheckpointStrategy;

/**
 * This source will use the S3 events meta information from hoodie table generate by {@link S3EventsSource}.
 */
public class S3EventsHoodieIncrSource extends HoodieIncrSource {

  private static final Logger LOG = LoggerFactory.getLogger(S3EventsHoodieIncrSource.class);
  private final String srcPath;
  private final int numInstantsPerFetch;
  private final boolean checkIfFileExists;
  private final IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy;
  private final QueryRunner queryRunner;
  private final CloudDataFetcher cloudDataFetcher;

  private final Option<SchemaProvider> schemaProvider;

  private final Option<SnapshotLoadQuerySplitter> snapshotLoadQuerySplitter;

  public static class Config {
    // control whether we do existence check for files before consuming them
    @Deprecated
    static final String ENABLE_EXISTS_CHECK = S3_INCR_ENABLE_EXISTS_CHECK.key();
    @Deprecated
    static final Boolean DEFAULT_ENABLE_EXISTS_CHECK = S3_INCR_ENABLE_EXISTS_CHECK.defaultValue();

    @Deprecated
    static final String S3_FS_PREFIX = S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key();

    /**
     * {@link #SPARK_DATASOURCE_OPTIONS} is json string, passed to the reader while loading dataset.
     * Example Hudi Streamer conf
     * - --hoodie-conf hoodie.streamer.source.s3incr.spark.datasource.options={"header":"true","encoding":"UTF-8"}
     */
    @Deprecated
    public static final String SPARK_DATASOURCE_OPTIONS = S3EventsHoodieIncrSourceConfig.SPARK_DATASOURCE_OPTIONS.key();
  }

  public S3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    this(props, sparkContext, sparkSession, schemaProvider, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props));
  }

  public S3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider,
      QueryRunner queryRunner,
      CloudDataFetcher cloudDataFetcher) {
    super(props, sparkContext, sparkSession, schemaProvider);
    checkRequiredConfigProperties(props, Collections.singletonList(HOODIE_SRC_BASE_PATH));
    this.srcPath = getStringWithAltKeys(props, HOODIE_SRC_BASE_PATH);
    this.numInstantsPerFetch = getIntWithAltKeys(props, NUM_INSTANTS_PER_FETCH);
    this.checkIfFileExists = getBooleanWithAltKeys(props, ENABLE_EXISTS_CHECK);
    this.missingCheckpointStrategy = getMissingCheckpointStrategy(props);
    this.queryRunner = queryRunner;
    this.cloudDataFetcher = cloudDataFetcher;
    this.schemaProvider = Option.ofNullable(schemaProvider);
    this.snapshotLoadQuerySplitter = SnapshotLoadQuerySplitter.getInstance(props);
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCheckpoint, long sourceLimit) {
    CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint = CloudObjectIncrCheckpoint.fromString(lastCheckpoint);
    HollowCommitHandling handlingMode = getHollowCommitHandleMode(props);
    QueryInfo queryInfo =
        IncrSourceHelper.generateQueryInfo(
            sparkContext, srcPath, numInstantsPerFetch,
            Option.of(cloudObjectIncrCheckpoint.getCommit()),
            missingCheckpointStrategy, handlingMode,
            HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            CloudObjectsSelectorCommon.S3_OBJECT_KEY,
            CloudObjectsSelectorCommon.S3_OBJECT_SIZE, true,
            Option.ofNullable(cloudObjectIncrCheckpoint.getKey()));
    LOG.info("Querying S3 with:" + cloudObjectIncrCheckpoint + ", queryInfo:" + queryInfo);

    if (isNullOrEmpty(cloudObjectIncrCheckpoint.getKey()) && queryInfo.areStartAndEndInstantsEqual()) {
      LOG.warn("Already caught up. No new data to process");
      return Pair.of(Option.empty(), queryInfo.getEndInstant());
    }
    Pair<QueryInfo, Dataset<Row>> queryInfoDatasetPair = queryRunner.run(queryInfo, snapshotLoadQuerySplitter);
    queryInfo = queryInfoDatasetPair.getLeft();
    Dataset<Row> filteredSourceData = queryInfoDatasetPair.getRight().filter(
        CloudObjectsSelectorCommon.generateFilter(CloudObjectsSelectorCommon.Type.S3, props));

    LOG.info("Adjusting end checkpoint:" + queryInfo.getEndInstant() + " based on sourceLimit :" + sourceLimit);
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> checkPointAndDataset =
        IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
            filteredSourceData, sourceLimit, queryInfo, cloudObjectIncrCheckpoint);
    if (!checkPointAndDataset.getRight().isPresent()) {
      LOG.info("Empty source, returning endpoint:" + checkPointAndDataset.getLeft());
      return Pair.of(Option.empty(), checkPointAndDataset.getLeft().toString());
    }
    LOG.info("Adjusted end checkpoint :" + checkPointAndDataset.getLeft());

    String s3FS = getStringWithAltKeys(props, S3_FS_PREFIX, true).toLowerCase();
    String s3Prefix = s3FS + "://";

    // Create S3 paths
    StorageConfiguration<Configuration> storageConf = HadoopFSUtils.getStorageConfWithCopy(sparkContext.hadoopConfiguration());
    List<CloudObjectMetadata> cloudObjectMetadata = checkPointAndDataset.getRight().get()
        .select(CloudObjectsSelectorCommon.S3_BUCKET_NAME,
                CloudObjectsSelectorCommon.S3_OBJECT_KEY,
                CloudObjectsSelectorCommon.S3_OBJECT_SIZE)
        .distinct()
        .mapPartitions(getCloudObjectMetadataPerPartition(s3Prefix, storageConf, checkIfFileExists), Encoders.kryo(CloudObjectMetadata.class))
        .collectAsList();
    LOG.info("Total number of files to process :" + cloudObjectMetadata.size());

    Option<Dataset<Row>> datasetOption = cloudDataFetcher.getCloudObjectDataDF(sparkSession, cloudObjectMetadata, props, schemaProvider);
    return Pair.of(datasetOption, checkPointAndDataset.getLeft().toString());
  }
}
