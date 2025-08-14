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
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
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
import org.apache.hudi.schema.FilebasedSchemaProvider;
import org.apache.hudi.sink.transform.ChainedTransformer;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.streamer.FlinkStreamerConfig;

import org.apache.avro.Schema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.orc.OrcFile;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class StreamerUtil {

  private static final Logger LOG = LoggerFactory.getLogger(StreamerUtil.class);

  public static final String FLINK_CHECKPOINT_ID = "flink_checkpoint_id";

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

  public static Schema getSourceSchema(org.apache.flink.configuration.Configuration conf) {
    if (conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA_PATH).isPresent()) {
      return new FilebasedSchemaProvider(conf).getSourceSchema();
    } else if (conf.getOptional(FlinkOptions.SOURCE_AVRO_SCHEMA).isPresent()) {
      final String schemaStr = conf.get(FlinkOptions.SOURCE_AVRO_SCHEMA);
      return new Schema.Parser().parse(schemaStr);
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
        LOG.info("Adding overridden properties to file properties.");
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
        .withPayloadOrderingFields(conf.get(FlinkOptions.PRECOMBINE_FIELD))
        .withPayloadEventTimeField(conf.get(FlinkOptions.PRECOMBINE_FIELD))
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
          .setPreCombineFields(OptionsResolver.getPreCombineField(conf))
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
      LOG.info("Table initialized under base path {}", basePath);
    } else {
      LOG.info("Table [path={}, name={}] already exists, no need to initialize the table",
          basePath, conf.get(FlinkOptions.TABLE_NAME));
    }

    return StreamerUtil.createMetaClient(conf, hadoopConf);

    // Do not close the filesystem in order to use the CACHE,
    // some filesystems release the handles in #close method.
  }

  private static String getMergeStrategyId(Configuration conf) {
    if (conf.contains(FlinkOptions.RECORD_MERGER_IMPLS)) {
      return HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID;
    } else if (conf.contains(FlinkOptions.PAYLOAD_CLASS_NAME)) {
      // for the compatibility of legacy payload class configuration
      String payloadClass = conf.get(FlinkOptions.PAYLOAD_CLASS_NAME);
      if (payloadClass.contains(PartialUpdateAvroPayload.class.getSimpleName())) {
        return HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID;
      }
    }
    return null;
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
        return EventTimeFlinkRecordMerger.class.getName();
      case COMMIT_TIME_ORDERING:
        return CommitTimeFlinkRecordMerger.class.getName();
      default:
        if (payloadClass.contains(PartialUpdateAvroPayload.class.getSimpleName())) {
          return PartialUpdateFlinkRecordMerger.class.getName();
        } else {
          return conf.get(FlinkOptions.RECORD_MERGER_IMPLS);
        }
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
    return HoodieTableConfig.inferMergingConfigsForWrites(
        getMergeMode(conf), getPayloadClass(conf), getMergeStrategyId(conf), OptionsResolver.getPreCombineField(conf), HoodieTableVersion.EIGHT);
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
        return Option.of(new HoodieTableConfig(storage, metaPath, null, null, null));
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

  public static Schema getTableAvroSchema(HoodieTableMetaClient metaClient, boolean includeMetadataFields) throws Exception {
    TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
    return schemaUtil.getTableAvroSchema(includeMetadataFields);
  }

  public static Schema getLatestTableSchema(String path, org.apache.hadoop.conf.Configuration hadoopConf) {
    if (StringUtils.isNullOrEmpty(path) || !StreamerUtil.tableExists(path, hadoopConf)) {
      return null;
    }

    try {
      HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(path, hadoopConf);
      return getTableAvroSchema(metaClient, false);
    } catch (Exception e) {
      LOG.warn("Error while resolving the latest table schema", e);
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
  public static void checkPreCombineKey(Configuration conf, List<String> fields) {
    String preCombineField = conf.get(FlinkOptions.PRECOMBINE_FIELD);
    if (!fields.contains(preCombineField)) {
      if (OptionsResolver.isDefaultHoodieRecordPayloadClazz(conf)) {
        throw new HoodieValidationException("Option '" + FlinkOptions.PRECOMBINE_FIELD.key()
                + "' is required for payload class: " + DefaultHoodieRecordPayload.class.getName());
      }
      if (preCombineField.equals(FlinkOptions.PRECOMBINE_FIELD.defaultValue())) {
        conf.set(FlinkOptions.PRECOMBINE_FIELD, FlinkOptions.NO_PRE_COMBINE);
      } else if (!preCombineField.equals(FlinkOptions.NO_PRE_COMBINE)) {
        throw new HoodieValidationException("Field " + preCombineField + " does not exist in the table schema."
                + "Please check '" + FlinkOptions.PRECOMBINE_FIELD.key() + "' option.");
      }
    }
  }

  /**
   * Validate keygen generator.
   */
  public static void checkKeygenGenerator(boolean isComplexHoodieKey, Configuration conf) {
    if (isComplexHoodieKey && FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.KEYGEN_CLASS_NAME)) {
      conf.set(FlinkOptions.KEYGEN_CLASS_NAME, ComplexAvroKeyGenerator.class.getName());
      LOG.info("Table option [{}] is reset to {} because record key or partition path has two or more fields",
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
}
