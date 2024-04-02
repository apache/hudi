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
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectIncrCheckpoint;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.MissingCheckpointStrategy;
import org.apache.hudi.utilities.sources.helpers.QueryInfo;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.sources.helpers.gcs.GcsObjectMetadataFetcher;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
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
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.generateQueryInfo;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getHollowCommitHandleMode;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getMissingCheckpointStrategy;

/**
 * An incremental source that detects new data in a source table containing metadata about GCS files,
 * downloads the actual content of these files from GCS and stores them as records into a destination table.
 * <p>
 * You should set spark.driver.extraClassPath in spark-defaults.conf to
 * look like below WITHOUT THE NEWLINES (or give the equivalent as CLI options if in cluster mode):
 * (mysql-connector at the end is only needed if Hive Sync is enabled and Mysql is used for Hive Metastore).
 * <p>
 * absolute_path_to/protobuf-java-3.21.1.jar:absolute_path_to/failureaccess-1.0.1.jar:
 * absolute_path_to/31.1-jre/guava-31.1-jre.jar:
 * absolute_path_to/mysql-connector-java-8.0.30.jar
 * <p>
 * This class can be invoked via spark-submit as follows. There's a bunch of optional hive sync flags at the end.
 * $ bin/spark-submit \
 * --packages com.google.cloud:google-cloud-pubsub:1.120.0 \
 * --packages com.google.cloud.bigdataoss:gcs-connector:hadoop2-2.2.7 \
 * --driver-memory 4g \
 * --executor-memory 4g \
 * --class org.apache.hudi.utilities.streamer.HoodieStreamer \
 * absolute_path_to/hudi-utilities-bundle_2.12-0.13.0-SNAPSHOT.jar \
 * --source-class org.apache.hudi.utilities.sources.GcsEventsHoodieIncrSource \
 * --op INSERT \
 * --hoodie-conf hoodie.streamer.source.hoodieincr.file.format="parquet" \
 * --hoodie-conf hoodie.streamer.source.cloud.data.select.file.extension="jsonl" \
 * --hoodie-conf hoodie.streamer.source.cloud.data.datafile.format="json" \
 * --hoodie-conf hoodie.streamer.source.cloud.data.select.relpath.prefix="country" \
 * --hoodie-conf hoodie.streamer.source.cloud.data.ignore.relpath.prefix="blah" \
 * --hoodie-conf hoodie.streamer.source.cloud.data.ignore.relpath.substring="blah" \
 * --hoodie-conf hoodie.datasource.write.recordkey.field=id \
 * --hoodie-conf hoodie.datasource.write.partitionpath.field= \
 * --filter-dupes \
 * --hoodie-conf hoodie.datasource.write.insert.drop.duplicates=true \
 * --hoodie-conf hoodie.combine.before.insert=true \
 * --source-ordering-field id \
 * --table-type COPY_ON_WRITE \
 * --target-base-path file:\/\/\/absolute_path_to/data-gcs \
 * --target-table gcs_data \
 * --continuous \
 * --source-limit 100 \
 * --min-sync-interval-seconds 60 \
 * --hoodie-conf hoodie.streamer.source.hoodieincr.path=file:\/\/\/absolute_path_to/meta-gcs \
 * --hoodie-conf hoodie.streamer.source.hoodieincr.missing.checkpoint.strategy=READ_UPTO_LATEST_COMMIT \
 * --enable-hive-sync \
 * --hoodie-conf hoodie.datasource.hive_sync.database=default \
 * --hoodie-conf hoodie.datasource.hive_sync.table=gcs_data
 */
public class GcsEventsHoodieIncrSource extends HoodieIncrSource {

  private final String srcPath;
  private final boolean checkIfFileExists;
  private final int numInstantsPerFetch;

  private final MissingCheckpointStrategy missingCheckpointStrategy;
  private final GcsObjectMetadataFetcher gcsObjectMetadataFetcher;
  private final CloudDataFetcher gcsObjectDataFetcher;
  private final QueryRunner queryRunner;
  private final Option<SchemaProvider> schemaProvider;
  private final Option<SnapshotLoadQuerySplitter> snapshotLoadQuerySplitter;

  private static final Logger LOG = LoggerFactory.getLogger(GcsEventsHoodieIncrSource.class);

  public GcsEventsHoodieIncrSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark,
                                   SchemaProvider schemaProvider) {

    this(props, jsc, spark, schemaProvider,
        new GcsObjectMetadataFetcher(props),
        new CloudDataFetcher(props),
        new QueryRunner(spark, props)
    );
  }

  GcsEventsHoodieIncrSource(TypedProperties props, JavaSparkContext jsc, SparkSession spark,
                            SchemaProvider schemaProvider, GcsObjectMetadataFetcher gcsObjectMetadataFetcher, CloudDataFetcher gcsObjectDataFetcher, QueryRunner queryRunner) {
    super(props, jsc, spark, schemaProvider);

    checkRequiredConfigProperties(props, Collections.singletonList(HOODIE_SRC_BASE_PATH));
    srcPath = getStringWithAltKeys(props, HOODIE_SRC_BASE_PATH);
    missingCheckpointStrategy = getMissingCheckpointStrategy(props);
    numInstantsPerFetch = getIntWithAltKeys(props, NUM_INSTANTS_PER_FETCH);
    checkIfFileExists = getBooleanWithAltKeys(props, ENABLE_EXISTS_CHECK);

    this.gcsObjectMetadataFetcher = gcsObjectMetadataFetcher;
    this.gcsObjectDataFetcher = gcsObjectDataFetcher;
    this.queryRunner = queryRunner;
    this.schemaProvider = Option.ofNullable(schemaProvider);
    this.snapshotLoadQuerySplitter = SnapshotLoadQuerySplitter.getInstance(props);

    LOG.info("srcPath: " + srcPath);
    LOG.info("missingCheckpointStrategy: " + missingCheckpointStrategy);
    LOG.info("numInstantsPerFetch: " + numInstantsPerFetch);
    LOG.info("checkIfFileExists: " + checkIfFileExists);
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCheckpoint, long sourceLimit) {
    CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint = CloudObjectIncrCheckpoint.fromString(lastCheckpoint);
    HollowCommitHandling handlingMode = getHollowCommitHandleMode(props);

    QueryInfo queryInfo = generateQueryInfo(
        sparkContext, srcPath, numInstantsPerFetch,
        Option.of(cloudObjectIncrCheckpoint.getCommit()),
        missingCheckpointStrategy, handlingMode, HoodieRecord.COMMIT_TIME_METADATA_FIELD,
        CloudObjectsSelectorCommon.GCS_OBJECT_KEY,
        CloudObjectsSelectorCommon.GCS_OBJECT_SIZE, true,
        Option.ofNullable(cloudObjectIncrCheckpoint.getKey()));
    LOG.info("Querying GCS with:" + cloudObjectIncrCheckpoint + " and queryInfo:" + queryInfo);

    if (isNullOrEmpty(cloudObjectIncrCheckpoint.getKey()) && queryInfo.areStartAndEndInstantsEqual()) {
      LOG.info("Source of file names is empty. Returning empty result and endInstant: "
          + queryInfo.getStartInstant());
      return Pair.of(Option.empty(), queryInfo.getStartInstant());
    }

    Pair<QueryInfo, Dataset<Row>> queryInfoDatasetPair = queryRunner.run(queryInfo, snapshotLoadQuerySplitter);
    Dataset<Row> filteredSourceData = gcsObjectMetadataFetcher.applyFilter(queryInfoDatasetPair.getRight());
    queryInfo = queryInfoDatasetPair.getLeft();
    LOG.info("Adjusting end checkpoint:" + queryInfo.getEndInstant() + " based on sourceLimit :" + sourceLimit);
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> checkPointAndDataset =
        IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
            filteredSourceData, sourceLimit, queryInfo, cloudObjectIncrCheckpoint);
    if (!checkPointAndDataset.getRight().isPresent()) {
      LOG.info("Empty source, returning endpoint:" + queryInfo.getEndInstant());
      return Pair.of(Option.empty(), queryInfo.getEndInstant());
    }
    LOG.info("Adjusted end checkpoint :" + checkPointAndDataset.getLeft());

    Pair<Option<Dataset<Row>>, String> extractedCheckPointAndDataset = extractData(queryInfo, checkPointAndDataset.getRight().get());
    return Pair.of(extractedCheckPointAndDataset.getLeft(), checkPointAndDataset.getLeft().toString());
  }

  private Pair<Option<Dataset<Row>>, String> extractData(QueryInfo queryInfo, Dataset<Row> cloudObjectMetadataDF) {
    List<CloudObjectMetadata> cloudObjectMetadata = gcsObjectMetadataFetcher.getGcsObjectMetadata(sparkContext, cloudObjectMetadataDF, checkIfFileExists);
    LOG.info("Total number of files to process :" + cloudObjectMetadata.size());
    Option<Dataset<Row>> fileDataRows = gcsObjectDataFetcher.getCloudObjectDataDF(sparkSession, cloudObjectMetadata, props, schemaProvider);
    return Pair.of(fileDataRows, queryInfo.getEndInstant());
  }
}
