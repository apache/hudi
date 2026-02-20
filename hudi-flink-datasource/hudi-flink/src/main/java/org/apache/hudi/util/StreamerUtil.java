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

package org.apache.hudi.util;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.model.CommitTimeFlinkRecordMerger;
import org.apache.hudi.client.model.EventTimeFlinkRecordMerger;
import org.apache.hudi.client.model.PartialUpdateFlinkRecordMerger;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.metadata.FlinkHoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.schema.FilebasedSchemaProvider;
import org.apache.hudi.sink.FlinkCheckpointClient;
import org.apache.hudi.sink.muttley.AthenaIngestionGateway;
import org.apache.hudi.sink.transform.ChainedTransformer;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.streamer.FlinkStreamerConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.orc.OrcFile;
import org.apache.parquet.hadoop.ParquetFileWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieFileFormat.HOODIE_LOG;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.table.HoodieTableConfig.TIMELINE_HISTORY_PATH;
import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;
import static org.apache.hudi.configuration.FlinkOptions.WRITE_FAIL_FAST;

/**
 * Utilities for Flink stream read and write.
 */
@Slf4j
public class StreamerUtil {

  public static final String FLINK_CHECKPOINT_ID = "flink_checkpoint_id";
  public static final String EMPTY_PARTITION_PATH = "";

  // Kafka offset metadata constants - using Flink-specific keys with Spark-compatible format
  public static final String HOODIE_METADATA_KEY = "HoodieMetadataKey";

  // Constants for Kafka offset formatting
  private static final String KAFKA_METADATA_PREFIX = "kafka_metadata";
  private static final String URL_ENCODED_COLON = "%3A";
  private static final String PARTITION_SEPARATOR = ";";

  public static TypedProperties appendKafkaProps(FlinkStreamerConfig config) {
    TypedProperties properties = getProps(config);
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.kafkaGroupId);
    return properties;
  }

  public static TypedProperties getProps(FlinkStreamerConfig cfg) {
    if (cfg.propsFilePath.isEmpty()) {
      return new TypedProperties();
    }
    return readConfig(
        HadoopConfigurations.getHadoopConf(cfg),
        new StoragePath(cfg.propsFilePath), cfg.configs).getProps();
  }

  public static TypedProperties buildProperties(List<String> props) {
    TypedProperties properties = DFSPropertiesConfiguration.getGlobalProps();
    props.forEach(x -> {
      String[] kv = x.split("=");
      ValidationUtils.checkArgument(kv.length == 2);
      properties.setProperty(kv[0], kv[1]);
    });
    return properties;
  }

  /**
   * Creates the metadata write client from the given write client for data table.
   */
  public static HoodieFlinkWriteClient createMetadataWriteClient(HoodieFlinkWriteClient dataWriteClient) {
    // Get the metadata writer from the table and use its write client
    Option<HoodieTableMetadataWriter> metadataWriterOpt =
        dataWriteClient.getHoodieTable().getMetadataWriter(null, true, true);
    ValidationUtils.checkArgument(metadataWriterOpt.isPresent(), "Failed to create the metadata writer");
    FlinkHoodieBackedTableMetadataWriter metadataWriter = (FlinkHoodieBackedTableMetadataWriter) metadataWriterOpt.get();
    return (HoodieFlinkWriteClient) metadataWriter.getWriteClient();
  }

  public static HoodieSchema getSourceSchema(org.apache.flink.configuration.Configuration conf) {
    if (conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH).isPresent()) {
      return new FilebasedSchemaProvider(conf).getSourceHoodieSchema();
    } else if (conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA).isPresent()) {
      final String schemaStr = conf.get(FlinkOptions.SOURCE_AVRO_SCHEMA);
      return HoodieSchema.parse(schemaStr);
    } else {
      final String errorMsg = String.format("Either option '%s' or '%s' "
              + "should be specified for avro schema deserialization",
          FlinkOptions.SOURCE_AVRO_SCHEMA_PATH.key(), FlinkOptions.SOURCE_AVRO_SCHEMA.key());
      throw new HoodieException(errorMsg);
    }
  }

  /**
   * Read config from properties file (`--props` option) and cmd line (`--hoodie-conf` option).
   */
  public static DFSPropertiesConfiguration readConfig(org.apache.hadoop.conf.Configuration hadoopConfig,
                                                      StoragePath cfgPath, List<String> overriddenProps) {
    DFSPropertiesConfiguration conf = new DFSPropertiesConfiguration(hadoopConfig, cfgPath);
    try {
      if (!overriddenProps.isEmpty()) {
        log.info("Adding overridden properties to file properties.");
        conf.addPropsFromStream(new BufferedReader(new StringReader(String.join("\n", overriddenProps))), cfgPath);
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Unexpected error adding config overrides", ioe);
    }

    return conf;
  }

  /**
   * Returns the payload config with given configuration.
   */
  public static HoodiePayloadConfig getPayloadConfig(Configuration conf) {
    return HoodiePayloadConfig.newBuilder()
        .withPayloadClass(conf.get(FlinkOptions.PAYLOAD_CLASS_NAME))
        .withPayloadOrderingFields(OptionsResolver.getOrderingFieldsStr(conf))
        .withPayloadEventTimeField(OptionsResolver.getOrderingFieldsStr(conf))
        .build();
  }

  /**
   * Returns the index config with given configuration.
   */
  public static HoodieIndexConfig getIndexConfig(Configuration conf) {
    return HoodieIndexConfig.newBuilder()
        .withIndexType(OptionsResolver.getIndexType(conf))
        .withBucketNum(String.valueOf(conf.get(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS)))
        .withRecordKeyField(conf.get(FlinkOptions.RECORD_KEY_FIELD))
        .withIndexKeyField(OptionsResolver.getIndexKeyField(conf))
        .withBucketIndexEngineType(OptionsResolver.getBucketEngineType(conf))
        .withEngineType(EngineType.FLINK)
        .build();
  }

  /**
   * Get the lockConfig if required, empty {@link Option} otherwise.
   */
  public static Option<HoodieLockConfig> getLockConfig(Configuration conf) {
    if (OptionsResolver.isLockRequired(conf) && !conf.containsKey(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key())) {
      // configure the fs lock provider by default
      return Option.of(HoodieLockConfig.newBuilder()
          .fromProperties(FileSystemBasedLockProvider.getLockConfig(conf.get(FlinkOptions.PATH)))
          .withConflictResolutionStrategy(OptionsResolver.getConflictResolutionStrategy(conf))
          .build());
    }

    return Option.empty();
  }

  /**
   * Returns the timeGenerator config with given configuration.
   */
  public static HoodieTimeGeneratorConfig getTimeGeneratorConfig(Configuration conf) {
    TypedProperties properties = flinkConf2TypedProperties(conf);
    // Set lock configure, which is needed in TimeGenerator.
    Option<HoodieLockConfig> lockConfig = getLockConfig(conf);
    if (lockConfig.isPresent()) {
      properties.putAll(lockConfig.get().getProps());
    }

    return HoodieTimeGeneratorConfig.newBuilder()
        .withPath(conf.get(FlinkOptions.PATH))
        .fromProperties(properties)
        .build();
  }

  /**
   * Converts the give {@link Configuration} to {@link TypedProperties}.
   * The default values are also set up.
   *
   * @param conf The flink configuration
   * @return a TypedProperties instance
   */
  public static TypedProperties flinkConf2TypedProperties(Configuration conf) {
    Configuration flatConf = FlinkOptions.flatOptions(conf);
    TypedProperties properties = new TypedProperties();
    // put all the set options
    flatConf.addAllToProperties(properties);
    // put all the default options
    for (ConfigOption<?> option : FlinkOptions.optionalOptions()) {
      if (!flatConf.contains(option) && option.hasDefaultValue()) {
        properties.put(option.key(), option.defaultValue());
      }
    }
    properties.put(HoodieTableConfig.TYPE.key(), conf.get(FlinkOptions.TABLE_TYPE));
    return properties;
  }

  public static void initTableFromClientIfNecessary(Configuration conf) {
    // Since Flink 2.0, the adaptive execution for batch job will generate job graph incrementally
    // for multiple stages (FLIP-469). And the write coordinator is initialized along with write
    // operator in the final stage, so hudi table should be initialized if necessary during the plan
    // compilation phase when adaptive execution is enabled.
    if (OptionsResolver.isIncrementalJobGraph(conf)
        || OptionsResolver.isPartitionLevelSimpleBucketIndex(conf)) {
      // init table, create if not exists.
      try {
        StreamerUtil.initTableIfNotExists(conf);
      } catch (IOException e) {
        throw new HoodieException("Failed to initialize table.", e);
      }
    }
  }

  /**
   * Initialize the table if it does not exist.
   *
   * @param conf the configuration
   * @throws IOException if errors happens when writing metadata
   */
  public static HoodieTableMetaClient initTableIfNotExists(Configuration conf) throws IOException {
    return initTableIfNotExists(conf, HadoopConfigurations.getHadoopConf(conf));
  }

  /**
   * Initialize the table if it does not exist.
   *
   * @param conf the configuration
   * @throws IOException if errors happens when writing metadata
   */
  public static HoodieTableMetaClient initTableIfNotExists(
      Configuration conf,
      org.apache.hadoop.conf.Configuration hadoopConf) throws IOException {
    final String basePath = conf.get(FlinkOptions.PATH);
    if (!tableExists(basePath, hadoopConf)) {
      HoodieTableMetaClient.newTableBuilder()
          .setTableCreateSchema(conf.get(FlinkOptions.SOURCE_AVRO_SCHEMA))
          .setTableType(conf.get(FlinkOptions.TABLE_TYPE))
          .setTableName(conf.get(FlinkOptions.TABLE_NAME))
          .setTableVersion(conf.get(FlinkOptions.WRITE_TABLE_VERSION))
          .setTableFormat(conf.get(FlinkOptions.WRITE_TABLE_FORMAT))
          .setRecordMergeMode(getMergeMode(conf))
          .setRecordMergeStrategyId(getMergeStrategyId(conf))
          .setPayloadClassName(getPayloadClass(conf))
          .setDatabaseName(conf.get(FlinkOptions.DATABASE_NAME))
          .setRecordKeyFields(conf.getString(FlinkOptions.RECORD_KEY_FIELD.key(), null))
          .setOrderingFields(OptionsResolver.getOrderingFieldsStr(conf))
          .setArchiveLogFolder(TIMELINE_HISTORY_PATH.defaultValue())
          .setPartitionFields(conf.getString(FlinkOptions.PARTITION_PATH_FIELD.key(), null))
          .setKeyGeneratorClassProp(
              conf.getOptional(FlinkOptions.KEYGEN_CLASS_NAME).orElse(SimpleAvroKeyGenerator.class.getName()))
          .setHiveStylePartitioningEnable(conf.get(FlinkOptions.HIVE_STYLE_PARTITIONING))
          .setUrlEncodePartitioning(conf.get(FlinkOptions.URL_ENCODE_PARTITIONING))
          .setCDCEnabled(conf.get(FlinkOptions.CDC_ENABLED))
          .setCDCSupplementalLoggingMode(conf.get(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE))
          .setPopulateMetaFields(OptionsResolver.isPopulateMetaFields(conf))
          .initTable(HadoopFSUtils.getStorageConfWithCopy(hadoopConf), basePath);
      log.info("Table initialized under base path {}", basePath);
    } else {
      log.info("Table [path={}, name={}] already exists, no need to initialize the table",
          basePath, conf.get(FlinkOptions.TABLE_NAME));
    }

    return StreamerUtil.createMetaClient(conf, hadoopConf);

    // Do not close the filesystem in order to use the CACHE,
    // some filesystems release the handles in #close method.
  }

  private static String getMergeStrategyId(Configuration conf) {
    return conf.getString(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key(), null);
  }

  private static String getPayloadClass(Configuration conf) {
    if (conf.contains(FlinkOptions.RECORD_MERGER_IMPLS)) {
      // check whether contains partial update merger class
      String mergerClasses = conf.get(FlinkOptions.RECORD_MERGER_IMPLS);
      if (mergerClasses.contains(PartialUpdateFlinkRecordMerger.class.getSimpleName())) {
        return PartialUpdateAvroPayload.class.getName();
      }
    }
    if (conf.contains(FlinkOptions.PAYLOAD_CLASS_NAME)) {
      return conf.get(FlinkOptions.PAYLOAD_CLASS_NAME);
    } else if (getMergeMode(conf) == RecordMergeMode.COMMIT_TIME_ORDERING) {
      return OverwriteWithLatestAvroPayload.class.getName();
    } else {
      // payload inferred in HoodieTableConfig for EVENT_TIME_ORDERING is DefaultHoodieRecordPayload,
      // but Flink use EventTimeAvroPayload, so return default value here if necessary
      return conf.get(FlinkOptions.PAYLOAD_CLASS_NAME);
    }
  }

  /**
   * Returns the merge mode.
   */
  public static RecordMergeMode getMergeMode(Configuration conf) {
    if (conf.contains(FlinkOptions.RECORD_MERGE_MODE)) {
      return RecordMergeMode.valueOf(conf.get(FlinkOptions.RECORD_MERGE_MODE));
    } else {
      return null;
    }
  }

  /**
   * Returns the merger classes.
   */
  public static String getMergerClasses(Configuration conf, RecordMergeMode mergeMode, String payloadClass) {
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        if (payloadClass.contains(PartialUpdateAvroPayload.class.getSimpleName())) {
          return PartialUpdateFlinkRecordMerger.class.getName();
        } else {
          return EventTimeFlinkRecordMerger.class.getName();
        }
      case COMMIT_TIME_ORDERING:
        return CommitTimeFlinkRecordMerger.class.getName();
      default:
        return conf.get(FlinkOptions.RECORD_MERGER_IMPLS);
    }
  }

  /**
   * Write Flink checkpoint id as extra metadata, if write.extra.metadata.enabled is true.
   *
   * @param conf Flink configuration
   * @param checkpointCommitMetadata commit metadata map
   * @param checkpointId flink checkpoint id
   */
  public static void addFlinkCheckpointIdIntoMetaData(
      Configuration conf,
      HashMap<String, String> checkpointCommitMetadata,
      long checkpointId) {
    if (conf.get(FlinkOptions.WRITE_EXTRA_METADATA_ENABLED)) {
      checkpointCommitMetadata.put(FLINK_CHECKPOINT_ID, String.valueOf(checkpointId));
    }
  }

  /**
   * Infers the merging behavior based on what the user sets (or doesn't set).
   *
   * @param conf Flink configuration
   * @return The correct merging behaviour: <merge_mode, payload_class, merge_strategy_id>
   */
  public static Triple<RecordMergeMode, String, String> inferMergingBehavior(Configuration conf) {
    String payloadClassName = getPayloadClass(conf);
    Map<String, String> mergeConf = HoodieTableConfig.inferMergingConfigsForV9TableCreation(
        getMergeMode(conf), payloadClassName, getMergeStrategyId(conf), OptionsResolver.getOrderingFieldsStr(conf), HoodieTableVersion.current());
    String mergeMode = mergeConf.get(HoodieTableConfig.RECORD_MERGE_MODE.key());
    String mergeStrategyId = mergeConf.get(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key());
    ValidationUtils.checkArgument(mergeMode != null, "Merge mode should not be null");
    return Triple.of(RecordMergeMode.valueOf(mergeMode), payloadClassName, mergeStrategyId);
  }

  /**
   * Returns whether the hoodie table exists under given path {@code basePath}.
   */
  public static boolean tableExists(String basePath, org.apache.hadoop.conf.Configuration hadoopConf) {
    // Hadoop FileSystem
    FileSystem fs = HadoopFSUtils.getFs(basePath, hadoopConf);
    try {
      return fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
          && fs.exists(new Path(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME), HoodieTableConfig.HOODIE_PROPERTIES_FILE));
    } catch (IOException e) {
      throw new HoodieException("Error while checking whether table exists under path:" + basePath, e);
    }
  }

  /**
   * Returns whether the hoodie partition exists under given table path {@code tablePath} and partition path {@code partitionPath}.
   *
   * @param tablePath     Base path of the table.
   * @param partitionPath The path of the partition.
   * @param hadoopConf    The hadoop configuration.
   */
  public static boolean partitionExists(String tablePath, String partitionPath, org.apache.hadoop.conf.Configuration hadoopConf) {
    // Hadoop FileSystem
    FileSystem fs = HadoopFSUtils.getFs(tablePath, hadoopConf);
    try {
      return fs.exists(new Path(tablePath, partitionPath));
    } catch (IOException e) {
      throw new HoodieException(String.format("Error while checking whether partition exists under table path [%s] and partition path [%s]", tablePath, partitionPath), e);
    }
  }

  /**
   * Generates the bucket ID using format {partition path}_{fileID}.
   */
  public static String generateBucketKey(String partitionPath, String fileId) {
    return new StringBuilder()
        .append(partitionPath)
        .append('_')
        .append(fileId)
        .toString();
  }

  /**
   * Creates the meta client for reader.
   *
   * <p>The streaming pipeline process is long-running, so empty table path is allowed,
   * the reader would then check and refresh the meta client.
   *
   * @see org.apache.hudi.source.StreamReadMonitoringFunction
   */
  public static HoodieTableMetaClient metaClientForReader(
      Configuration conf,
      org.apache.hadoop.conf.Configuration hadoopConf) {
    final String basePath = conf.get(FlinkOptions.PATH);
    if (conf.get(FlinkOptions.READ_AS_STREAMING) && !tableExists(basePath, hadoopConf)) {
      return null;
    } else {
      return createMetaClient(basePath, hadoopConf);
    }
  }

  /**
   * Creates the meta client.
   */
  public static HoodieTableMetaClient createMetaClient(String basePath, org.apache.hadoop.conf.Configuration hadoopConf) {
    return HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HadoopFSUtils.getStorageConfWithCopy(hadoopConf))
        .build();
  }

  /**
   * Creates the meta client.
   */
  public static HoodieTableMetaClient createMetaClient(Configuration conf) {
    return createMetaClient(conf, HadoopConfigurations.getHadoopConf(conf));
  }

  /**
   * Creates the meta client.
   */
  public static HoodieTableMetaClient createMetaClient(Configuration conf, org.apache.hadoop.conf.Configuration hadoopConf) {
    return HoodieTableMetaClient.builder()
        .setBasePath(conf.get(FlinkOptions.PATH))
        .setConf(HadoopFSUtils.getStorageConfWithCopy(hadoopConf))
        .setTimeGeneratorConfig(getTimeGeneratorConfig(conf))
        .build();
  }

  /**
   * Returns the table config or empty if the table does not exist.
   */
  public static Option<HoodieTableConfig> getTableConfig(String basePath, org.apache.hadoop.conf.Configuration hadoopConf) {
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, HadoopFSUtils.getStorageConf(hadoopConf));
    StoragePath metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    try {
      if (storage.exists(new StoragePath(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE))) {
        return Option.of(new HoodieTableConfig(storage, metaPath));
      }
    } catch (IOException e) {
      throw new HoodieIOException("Get table config error", e);
    }
    return Option.empty();
  }

  /**
   * Returns the median instant time between the given two instant time.
   */
  public static Option<String> medianInstantTime(String highVal, String lowVal) {
    long high = TimelineUtils.parseDateFromInstantTimeSafely(highVal)
            .orElseThrow(() -> new HoodieException("Get instant time diff with interval [" + highVal + "] error")).getTime();
    long low = TimelineUtils.parseDateFromInstantTimeSafely(lowVal)
            .orElseThrow(() -> new HoodieException("Get instant time diff with interval [" + lowVal + "] error")).getTime();
    ValidationUtils.checkArgument(high > low,
            "Instant [" + highVal + "] should have newer timestamp than instant [" + lowVal + "]");
    long median = low + (high - low) / 2;
    final String instantTime = TimelineUtils.formatDate(new Date(median));
    if (compareTimestamps(lowVal, GREATER_THAN_OR_EQUALS, instantTime)
            || compareTimestamps(highVal, LESSER_THAN_OR_EQUALS, instantTime)) {
      return Option.empty();
    }
    return Option.of(instantTime);
  }

  /**
   * Returns the time interval in seconds between the given instant time.
   */
  public static long instantTimeDiffSeconds(String newInstantTime, String oldInstantTime) {
    long newTimestamp = TimelineUtils.parseDateFromInstantTimeSafely(newInstantTime)
            .orElseThrow(() -> new HoodieException("Get instant time diff with interval [" + oldInstantTime + ", " + newInstantTime + "] error")).getTime();
    long oldTimestamp = TimelineUtils.parseDateFromInstantTimeSafely(oldInstantTime)
            .orElseThrow(() -> new HoodieException("Get instant time diff with interval [" + oldInstantTime + ", " + newInstantTime + "] error")).getTime();
    return (newTimestamp - oldTimestamp) / 1000;
  }

  public static Option<Transformer> createTransformer(List<String> classNames) throws IOException {
    try {
      List<Transformer> transformers = new ArrayList<>();
      for (String className : Option.ofNullable(classNames).orElse(Collections.emptyList())) {
        transformers.add(ReflectionUtils.loadClass(className));
      }
      return transformers.isEmpty() ? Option.empty() : Option.of(new ChainedTransformer(transformers));
    } catch (Throwable e) {
      throw new IOException("Could not load transformer class(es) " + classNames, e);
    }
  }

  /**
   * Returns whether the give file is in valid hoodie format.
   * For example, filtering out the empty or corrupt files.
   */
  public static boolean isValidFile(StoragePathInfo pathInfo) {
    final String extension = FSUtils.getFileExtension(pathInfo.getPath().toString());
    if (PARQUET.getFileExtension().equals(extension)) {
      return pathInfo.getLength() > ParquetFileWriter.MAGIC.length;
    }

    if (ORC.getFileExtension().equals(extension)) {
      return pathInfo.getLength() > OrcFile.MAGIC.length();
    }

    if (HOODIE_LOG.getFileExtension().equals(extension)) {
      return pathInfo.getLength() > HoodieLogFormat.MAGIC.length;
    }

    return pathInfo.getLength() > 0;
  }

  public static String getLastPendingInstant(HoodieTableMetaClient metaClient) {
    return getLastPendingInstant(metaClient, true);
  }

  public static String getLastPendingInstant(HoodieTableMetaClient metaClient, boolean reloadTimeline) {
    if (reloadTimeline) {
      metaClient.reloadActiveTimeline();
    }
    return metaClient.getCommitsTimeline().filterPendingExcludingCompaction()
        .lastInstant()
        .map(HoodieInstant::requestedTime)
        .orElse(null);
  }

  public static String getLastCompletedInstant(HoodieTableMetaClient metaClient) {
    return metaClient.getCommitsTimeline().filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::requestedTime)
        .orElse(null);
  }

  /**
   * Returns whether there are successful commits on the timeline.
   *
   * @param metaClient The meta client
   * @return true if there is any successful commit
   */
  public static boolean haveSuccessfulCommits(HoodieTableMetaClient metaClient) {
    return !metaClient.getCommitsTimeline().filterCompletedInstants().empty();
  }

  /**
   * Returns the max compaction memory in bytes with given conf.
   */
  public static long getMaxCompactionMemoryInBytes(Configuration conf) {
    return (long) conf.get(FlinkOptions.COMPACTION_MAX_MEMORY) * 1024 * 1024;
  }

  public static HoodieSchema getTableSchema(HoodieTableMetaClient metaClient, boolean includeMetadataFields) throws Exception {
    TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
    return schemaUtil.getTableSchema(includeMetadataFields);
  }

  public static HoodieSchema getLatestTableSchema(String path, org.apache.hadoop.conf.Configuration hadoopConf) {
    if (StringUtils.isNullOrEmpty(path) || !StreamerUtil.tableExists(path, hadoopConf)) {
      return null;
    }

    try {
      HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(path, hadoopConf);
      return getTableSchema(metaClient, false);
    } catch (Exception e) {
      log.error("Failed to resolve the latest table schema", e);
    }
    return null;
  }

  public static boolean fileExists(HoodieStorage storage, StoragePath path) {
    try {
      return storage.exists(path);
    } catch (IOException e) {
      throw new HoodieException("Exception while checking file " + path + " existence", e);
    }
  }

  /**
   * Returns whether the given instant is a data writing commit.
   *
   * @param tableType The table type
   * @param instant   The instant
   * @param timeline  The timeline
   */
  public static boolean isWriteCommit(HoodieTableType tableType, HoodieInstant instant, HoodieTimeline timeline) {
    return tableType == HoodieTableType.MERGE_ON_READ
        ? !instant.getAction().equals(HoodieTimeline.COMMIT_ACTION) // not a compaction
        : !ClusteringUtils.isCompletedClusteringInstant(instant, timeline);   // not a clustering
  }

  /**
   * Validate pre_combine key.
   */
  public static void checkOrderingFields(Configuration conf, List<String> fields) {
    String orderingFields = conf.get(FlinkOptions.ORDERING_FIELDS);
    if (null == orderingFields || !fields.contains(orderingFields)) {
      if (OptionsResolver.isDefaultHoodieRecordPayloadClazz(conf)) {
        // default payload force set of some columns existed in schema as ordering ones
        throw new HoodieValidationException("Option '" + FlinkOptions.ORDERING_FIELDS.key()
                + "' is required for payload class: " + DefaultHoodieRecordPayload.class.getName());
      }
      if (null == orderingFields) {
        // if there is no ordering fields we set them as no-precombine
        conf.set(FlinkOptions.ORDERING_FIELDS, FlinkOptions.NO_PRE_COMBINE);
      } else if (!orderingFields.equals(FlinkOptions.NO_PRE_COMBINE)) {
        // but if no-precombine was passed initially then we shouldn't fail here on schema check
        throw new HoodieValidationException("Field " + orderingFields + " does not exist in the table schema. "
                + "Please check '" + FlinkOptions.ORDERING_FIELDS.key() + "' option.");
      }
    }
  }

  /**
   * Validate keygen generator.
   */
  public static void checkKeygenGenerator(boolean isComplexHoodieKey, Configuration conf) {
    if (isComplexHoodieKey && FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.KEYGEN_CLASS_NAME)) {
      conf.set(FlinkOptions.KEYGEN_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
      log.info("Table option [{}] is reset to {} because record key or partition path has two or more fields",
          FlinkOptions.KEYGEN_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName());
    }
  }

  /**
   * @return HoodieMetadataConfig constructed from flink configuration.
   */
  public static HoodieMetadataConfig metadataConfig(org.apache.flink.configuration.Configuration conf) {
    Properties properties = new Properties();

    // set up metadata.enabled=true in table DDL to enable metadata listing
    properties.put(HoodieMetadataConfig.ENABLE.key(), conf.get(FlinkOptions.METADATA_ENABLED));

    return HoodieMetadataConfig.newBuilder().fromProperties(properties).build();
  }

  /**
   * Validate against the given list of write statuses.
   *
   * @param config          The Flink conf
   * @param currentInstant  The current instant
   * @param writeStatusList The write status list
   *
   * @throws HoodieException if the {code WRITE_FAIL_FAST} is set up as true and there are writing errors
   */
  public static void validateWriteStatus(
      Configuration config,
      String currentInstant,
      List<WriteStatus> writeStatusList) throws HoodieException {
    if (config.get(WRITE_FAIL_FAST)) {
      // It will early detect the write failures in each of task to prevent data loss caused by commit failure
      // after a checkpoint has been triggered.
      writeStatusList.stream().filter(ws -> !ws.getErrors().isEmpty()).findFirst().map(writeStatus -> {
        Map.Entry<HoodieKey, Throwable> entry = writeStatus.getErrors().entrySet().iterator().next();
        throw new HoodieException(String.format("Write failure occurs with hoodie key %s at Instant [%s] in append write function", entry.getKey(), currentInstant), entry.getValue());
      });
    }
  }

  /*
   * Add Kafka offset metadata to the checkpoint metadata.
   * Uses Flink-specific checkpoint key but same format as Spark for compatibility.
   *
   * @param conf Flink configuration
   * @param checkpointCommitMetadata commit metadata map
   * @param kafkaOffsetCheckpoint Kafka offset checkpoint string in Spark format: "HoodieMetadataKey" : "kafka_metadata%3Ahp-event-web%3A101:7583675434;kafka_metadata%3Ahp-event-web%3A222:7190059945;"
   */
  public static void addKafkaOffsetMetaData(
          Configuration conf,
          HashMap<String, String> checkpointCommitMetadata,
          String kafkaOffsetCheckpoint) {
    if (conf.get(FlinkOptions.WRITE_EXTRA_METADATA_ENABLED) && kafkaOffsetCheckpoint != null) {
      checkpointCommitMetadata.put(HOODIE_METADATA_KEY, kafkaOffsetCheckpoint);
    }
  }

  /**
   * Extracts Kafka offsets from checkpoint response.
   *
   * @param checkpointInfo The checkpoint info from service
   * @param checkpointId The checkpoint ID for logging
   * @return Map of partition ID to offset, or null if no valid offsets found
   */
  private static Map<Integer, Long> extractKafkaOffsets(
      AthenaIngestionGateway.CheckpointKafkaOffsetInfo checkpointInfo,
      long checkpointId) {

    List<AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo> kafkaOffsetsInfoList =
        checkpointInfo.getKafkaOffsetsInfo();

    if (kafkaOffsetsInfoList == null || kafkaOffsetsInfoList.isEmpty()) {
      log.warn("No Kafka offset information found in checkpoint response");
      return null;
    }

    // Verify we have exactly one topic
    if (kafkaOffsetsInfoList.size() > 1) {
      throw new IllegalStateException(
          String.format("Expected exactly one topic in checkpoint response, but found %d topics",
              kafkaOffsetsInfoList.size()));
    }

    // Get the single topic's offsets
    AthenaIngestionGateway.CheckpointKafkaOffsetInfo.KafkaOffsetsInfo kafkaOffsetInfo =
        kafkaOffsetsInfoList.get(0);

    // Validate offset info structure
    if (kafkaOffsetInfo.getOffsets() == null || kafkaOffsetInfo.getOffsets().getOffsets() == null) {
      log.warn("Kafka offset info has null offsets for checkpointId={}", checkpointId);
      return null;
    }

    Map<Integer, Long> partitionOffsets = kafkaOffsetInfo.getOffsets().getOffsets();

    // Validate partition offsets
    if (partitionOffsets.isEmpty()) {
      log.warn("No partition offsets found for checkpointId={}", checkpointId);
      return null;
    }

    // Validate offset values
    for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {
      if (entry.getKey() < 0 || entry.getValue() < 0) {
        log.error("Invalid partition ID {} for checkpointId={} or offset for offset={}",
                entry.getKey(), checkpointId, entry.getValue());
        return null;
      }
    }

    return partitionOffsets;
  }

  /**
   * Collects Kafka offset checkpoint from checkpoint service.
   *
   * <p>Important assumptions:
   * <ul>
   *   <li>Each Flink job processes only ONE Kafka topic</li>
   *   <li>Each checkpoint ID is associated with exactly ONE topic</li>
   *   <li>Each topic can have multiple partitions</li>
   * </ul>
   *
   * <p>Fails open - if any error occurs, returns null and allows commit to proceed without Kafka offsets.
   *
   * @param conf             The Flink configuration
   * @param checkpointId     The checkpoint ID
   * @param checkpointClient The checkpoint client (nullable)
   * @return Kafka offset checkpoint string in URL-encoded format for Hudi metadata,
   * e.g., "kafka_metadata%3Atopic-name%3A0:100;kafka_metadata%3Atopic-name%3A1:200"
   * where format is "kafka_metadata%3Atopic%3Apartition:offset" separated by semicolons.
   * Returns null if not available or on error
   * @throws IllegalStateException if more than one topic is found in the checkpoint response
   */
  public static String collectKafkaOffsetCheckpoint(Configuration conf, long checkpointId,
                                                    FlinkCheckpointClient checkpointClient) {
    // Extract topic and cluster names early
    String topicName = conf.contains(FlinkOptions.KAFKA_TOPIC_NAME)
        ? conf.get(FlinkOptions.KAFKA_TOPIC_NAME) : null;
    String clusterName = conf.contains(FlinkOptions.SOURCE_KAFKA_CLUSTER)
        ? conf.get(FlinkOptions.SOURCE_KAFKA_CLUSTER) : null;
    Map<Integer, Long> partitionOffsets = null;

    try {
      // Validate checkpoint ID
      if (checkpointId < 0) {
        log.warn("Invalid checkpointId={}, must be non-negative", checkpointId);
        return stringFy(topicName, clusterName, partitionOffsets);
      }

      // Check if required configurations are present
      if (!conf.contains(FlinkOptions.DC) || !conf.contains(FlinkOptions.ENV)
          || !conf.contains(FlinkOptions.JOB_NAME) || !conf.contains(FlinkOptions.KAFKA_TOPIC_NAME)) {
        log.debug("Kafka offset collection skipped - required configurations not set");
        return stringFy(topicName, clusterName, partitionOffsets);
      }

      // Extract configuration parameters
      String dc = conf.get(FlinkOptions.DC);
      String env = conf.get(FlinkOptions.ENV);
      String jobName = conf.get(FlinkOptions.JOB_NAME);
      String hadoopUser = conf.get(FlinkOptions.HADOOP_USER);
      String sourceCluster = conf.get(FlinkOptions.SOURCE_KAFKA_CLUSTER);
      String targetCluster = conf.get(FlinkOptions.TARGET_KAFKA_CLUSTER);
      String athenaService = conf.get(FlinkOptions.ATHENA_SERVICE);
      String callerService = conf.get(FlinkOptions.CALLER_SERVICE_NAME);
      String topicId = conf.get(FlinkOptions.TOPIC_ID);
      String serviceTier = conf.get(FlinkOptions.SERVICE_TIER);
      String serviceName = conf.get(FlinkOptions.SERVICE_NAME);

      log.info("Fetching Kafka offsets for checkpointId={}, topicId={}, dc={}, env={}, jobName={}",
          checkpointId, topicId, dc, env, jobName);

      // Create FlinkCheckpointClient if not provided
      if (checkpointClient == null) {
        checkpointClient = new FlinkCheckpointClient(callerService, athenaService);
      }

      // Build checkpoint request
      FlinkCheckpointClient.CheckpointRequest request = FlinkCheckpointClient.CheckpointRequest.builder()
          .dc(dc)
          .env(env)
          .checkpointId(checkpointId)
          .jobName(jobName)
          .hadoopUser(hadoopUser)
          .sourceCluster(sourceCluster)
          .targetCluster(targetCluster)
          .checkpointLookback(0)
          .topicOperatorIds(Collections.singletonMap(topicName, topicId))
          .serviceTier(serviceTier)
          .serviceName(serviceName)
          .build();

      // Fetch checkpoint info
      Option<AthenaIngestionGateway.CheckpointKafkaOffsetInfo> checkpointInfo =
          checkpointClient.getKafkaCheckpointsInfo(request);

      if (checkpointInfo.isPresent()) {
        AthenaIngestionGateway.CheckpointKafkaOffsetInfo offsetInfo = checkpointInfo.get();
        log.info("Successfully retrieved checkpoint info: checkpointId={}, checkpointTimestamp={}",
            offsetInfo.getCheckpointId(), offsetInfo.getCheckpointTimestamp());

        // Extract offsets using helper method
        partitionOffsets = extractKafkaOffsets(offsetInfo, checkpointId);
      } else {
        log.warn("No checkpoint info found for checkpointId={}", checkpointId);
      }
    } catch (Exception e) {
      // Swallow the exception and log error - fail open to allow commit to proceed
      log.error("Failed to collect Kafka offset checkpoint for checkpointId={}, proceeding without offsets",
          checkpointId, e);
    }

    // Single return point - always return stringified result
    String ret = stringFy(topicName, clusterName, partitionOffsets);
    log.info("Kafka offset checkpoint for checkpointId={}: {}", checkpointId, ret);
    return ret;
  }

  /**
   * Converts Kafka topic and partition offsets to a URL-encoded string format with cluster metadata.
   *
   * @param topic The Kafka topic name
   * @param cluster The Kafka cluster name
   * @param offsetMap Map of partition ID to offset
   * @return URL-encoded string containing both offset and cluster metadata,
   *         or empty string if offsetMap is empty or topic is null/empty
   */
  public static String stringFy(String topic, String cluster, Map<Integer, Long> offsetMap) {
    if (topic == null || topic.isEmpty()) {
      log.warn("Topic name is null or empty in stringFy");
      return "";
    }

    String offsets = "";

    // Create offset entries if offsetMap is not null or empty
    if (offsetMap != null && !offsetMap.isEmpty()) {
      offsets = offsetMap.entrySet().stream()
              .sorted(Map.Entry.comparingByKey())
              .map(entry -> String.format("%s%s%s%s%d:%d",
                      KAFKA_METADATA_PREFIX, URL_ENCODED_COLON,
                      topic, URL_ENCODED_COLON,
                      entry.getKey(), entry.getValue()))
              .collect(Collectors.joining(PARTITION_SEPARATOR));
    }

    // Add cluster metadata if cluster name is provided
    if (cluster != null && !cluster.isEmpty()) {
      String clusterMetadata = String.format("%s%skafka_cluster%s%s%s:%s",
              KAFKA_METADATA_PREFIX, URL_ENCODED_COLON,
              URL_ENCODED_COLON, topic, URL_ENCODED_COLON, cluster);

      // If we have offsets, append cluster metadata with separator
      // Otherwise, return just the cluster metadata
      return offsets.isEmpty() ? clusterMetadata : offsets + PARTITION_SEPARATOR + clusterMetadata;
    }

    return offsets;
  }
}
