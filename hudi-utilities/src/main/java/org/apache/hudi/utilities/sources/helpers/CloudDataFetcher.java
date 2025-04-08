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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_MAX_FILE_SIZE;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.CloudSourceConfig.DATAFILE_FORMAT;
import static org.apache.hudi.utilities.config.CloudSourceConfig.ENABLE_EXISTS_CHECK;
import static org.apache.hudi.utilities.config.CloudSourceConfig.SOURCE_MAX_BYTES_PER_PARTITION;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.SOURCE_FILE_FORMAT;

/**
 * Connects to S3/GCS from Spark and downloads data from a given list of files.
 * Assumes SparkContext is already configured.
 */
public class CloudDataFetcher implements Serializable {

  private static final String EMPTY_STRING = "";

  private transient TypedProperties props;
  private transient JavaSparkContext sparkContext;
  private transient SparkSession sparkSession;
  private transient CloudObjectsSelectorCommon cloudObjectsSelectorCommon;

  private static final Logger LOG = LoggerFactory.getLogger(CloudDataFetcher.class);

  private static final long serialVersionUID = 1L;

  private final HoodieIngestionMetrics metrics;

  public CloudDataFetcher(TypedProperties props, JavaSparkContext jsc, SparkSession sparkSession, HoodieIngestionMetrics metrics) {
    this(props, jsc, sparkSession, metrics, new CloudObjectsSelectorCommon(props));
  }

  public CloudDataFetcher(TypedProperties props, JavaSparkContext jsc, SparkSession sparkSession, HoodieIngestionMetrics metrics, CloudObjectsSelectorCommon cloudObjectsSelectorCommon) {
    this.props = props;
    this.sparkContext = jsc;
    this.sparkSession = sparkSession;
    this.metrics = metrics;
    this.cloudObjectsSelectorCommon = cloudObjectsSelectorCommon;
  }

  public static String getFileFormat(TypedProperties props) {
    // This is to ensure backward compatibility where we were using the
    // config SOURCE_FILE_FORMAT for file format in previous versions.
    return StringUtils.isNullOrEmpty(getStringWithAltKeys(props, DATAFILE_FORMAT, EMPTY_STRING))
        ? getStringWithAltKeys(props, SOURCE_FILE_FORMAT, true)
        : getStringWithAltKeys(props, DATAFILE_FORMAT, EMPTY_STRING);
  }

  public Pair<Option<Dataset<Row>>, Checkpoint> fetchPartitionedSource(
      CloudObjectsSelectorCommon.Type cloudType,
      CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint,
      Option<SourceProfileSupplier> sourceProfileSupplier,
      Pair<QueryInfo, Dataset<Row>> queryInfoDatasetPair,
      Option<SchemaProvider> schemaProvider,
      long sourceLimit) {
    boolean isSourceProfileSupplierAvailable = sourceProfileSupplier.isPresent() && sourceProfileSupplier.get().getSourceProfile() != null;
    if (isSourceProfileSupplierAvailable) {
      LOG.debug("Using source limit from source profile sourceLimitFromConfig {} sourceLimitFromProfile {}", sourceLimit, sourceProfileSupplier.get().getSourceProfile().getMaxSourceBytes());
      sourceLimit = sourceProfileSupplier.get().getSourceProfile().getMaxSourceBytes();
    }

    QueryInfo queryInfo = queryInfoDatasetPair.getLeft();
    String filter = CloudObjectsSelectorCommon.generateFilter(cloudType, props);
    LOG.info("Adding filter string to Dataset: " + filter);
    Dataset<Row> filteredSourceData = queryInfoDatasetPair.getRight().filter(filter);

    LOG.info("Adjusting end checkpoint:" + queryInfo.getEndInstant() + " based on sourceLimit :" + sourceLimit);
    Pair<CloudObjectIncrCheckpoint, Option<Dataset<Row>>> checkPointAndDataset =
        IncrSourceHelper.filterAndGenerateCheckpointBasedOnSourceLimit(
            filteredSourceData, sourceLimit, queryInfo, cloudObjectIncrCheckpoint);
    if (!checkPointAndDataset.getRight().isPresent()) {
      LOG.info("Empty source, returning endpoint:" + checkPointAndDataset.getLeft());
      return Pair.of(Option.empty(), new StreamerCheckpointV1(checkPointAndDataset.getLeft().toString()));
    }
    LOG.info("Adjusted end checkpoint :" + checkPointAndDataset.getLeft());

    boolean checkIfFileExists = getBooleanWithAltKeys(props, ENABLE_EXISTS_CHECK);
    List<CloudObjectMetadata> cloudObjectMetadata = CloudObjectsSelectorCommon.getObjectMetadata(cloudType, sparkContext, checkPointAndDataset.getRight().get(), checkIfFileExists, props);
    LOG.info("Total number of files to process :" + cloudObjectMetadata.size());

    long bytesPerPartition = props.containsKey(SOURCE_MAX_BYTES_PER_PARTITION.key()) ? props.getLong(SOURCE_MAX_BYTES_PER_PARTITION.key()) :
        props.getLong(PARQUET_MAX_FILE_SIZE.key(), Long.parseLong(PARQUET_MAX_FILE_SIZE.defaultValue()));
    int numSourcePartitions = 0;
    if (isSourceProfileSupplierAvailable) {
      long bytesPerPartitionFromProfile = (long) sourceProfileSupplier.get().getSourceProfile().getSourceSpecificContext();
      if (bytesPerPartitionFromProfile > 0) {
        LOG.debug("Using bytesPerPartition from source profile bytesPerPartitionFromConfig {} bytesPerPartitionFromProfile {}", bytesPerPartition, bytesPerPartitionFromProfile);
        bytesPerPartition = bytesPerPartitionFromProfile;
      }
      numSourcePartitions = sourceProfileSupplier.get().getSourceProfile().getSourcePartitions();
    }
    Option<Dataset<Row>> datasetOption = getCloudObjectDataDF(cloudObjectMetadata, schemaProvider, bytesPerPartition, numSourcePartitions);
    return Pair.of(datasetOption, new StreamerCheckpointV1(checkPointAndDataset.getLeft().toString()));
  }

  private Option<Dataset<Row>> getCloudObjectDataDF(List<CloudObjectMetadata> cloudObjectMetadata,
                                                    Option<SchemaProvider> schemaProviderOption,
                                                    long bytesPerPartition,
                                                    int numSourcePartitions) {
    long totalSize = 0;
    for (CloudObjectMetadata o : cloudObjectMetadata) {
      totalSize += o.getSize();
    }
    // inflate 10% for potential hoodie meta fields
    double totalSizeWithHoodieMetaFields = totalSize * 1.1;
    metrics.updateStreamerSourceBytesToBeIngestedInSyncRound(totalSize);
    int numPartitions = (int) Math.max(Math.ceil(totalSizeWithHoodieMetaFields / bytesPerPartition), 1);
    // If the number of source partitions is configured to be greater, then use it instead.
    if (numPartitions < numSourcePartitions) {
      numPartitions = numSourcePartitions;
    }
    metrics.updateStreamerSourceParallelism(numPartitions);
    return cloudObjectsSelectorCommon.loadAsDataset(sparkSession, cloudObjectMetadata, getFileFormat(props), schemaProviderOption, numPartitions);
  }
}
