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

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.schema.FilebasedSchemaProvider;
import org.apache.hudi.sink.transform.ChainedTransformer;
import org.apache.hudi.sink.transform.Transformer;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FileStatus;
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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieFileFormat.HOODIE_LOG;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.table.HoodieTableConfig.ARCHIVELOG_FOLDER;
import static org.apache.hudi.common.table.HoodieTableMetaClient.AUXILIARYFOLDER_NAME;

/**
 * Utilities for Flink stream read and write.
 */
public class StreamerUtil {

  private static final Logger LOG = LoggerFactory.getLogger(StreamerUtil.class);

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
        new Path(cfg.propsFilePath), cfg.configs).getProps();
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
  public static DFSPropertiesConfiguration readConfig(org.apache.hadoop.conf.Configuration hadoopConfig, Path cfgPath, List<String> overriddenProps) {
    DFSPropertiesConfiguration conf = new DFSPropertiesConfiguration(hadoopConfig, cfgPath);
    try {
      if (!overriddenProps.isEmpty()) {
        LOG.info("Adding overridden properties to file properties.");
        conf.addPropsFromStream(new BufferedReader(new StringReader(String.join("\n", overriddenProps))));
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Unexpected error adding config overrides", ioe);
    }

    return conf;
  }

  /**
   * Mainly used for tests.
   */
  public static HoodieWriteConfig getHoodieClientConfig(Configuration conf) {
    return getHoodieClientConfig(conf, false, false);
  }

  public static HoodieWriteConfig getHoodieClientConfig(Configuration conf, boolean loadFsViewStorageConfig) {
    return getHoodieClientConfig(conf, false, loadFsViewStorageConfig);
  }

  public static HoodieWriteConfig getHoodieClientConfig(
      Configuration conf,
      boolean enableEmbeddedTimelineService,
      boolean loadFsViewStorageConfig) {
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder()
            .withEngineType(EngineType.FLINK)
            .withPath(conf.getString(FlinkOptions.PATH))
            .combineInput(conf.getBoolean(FlinkOptions.PRE_COMBINE), true)
            .withWriteLogSuffix(conf.getString(FlinkOptions.WRITE_LOG_SUFFIX))
            .withMergeAllowDuplicateOnInserts(OptionsResolver.insertClustering(conf))
            .withClusteringConfig(
                HoodieClusteringConfig.newBuilder()
                    .withAsyncClustering(conf.getBoolean(FlinkOptions.CLUSTERING_ASYNC_ENABLED))
                    .withClusteringPlanStrategyClass(conf.getString(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLASS))
                    .withClusteringPlanPartitionFilterMode(
                        ClusteringPlanPartitionFilterMode.valueOf(conf.getString(FlinkOptions.CLUSTERING_PLAN_PARTITION_FILTER_MODE_NAME)))
                    .withClusteringTargetPartitions(conf.getInteger(FlinkOptions.CLUSTERING_TARGET_PARTITIONS))
                    .withClusteringMaxNumGroups(conf.getInteger(FlinkOptions.CLUSTERING_MAX_NUM_GROUPS))
                    .withClusteringTargetFileMaxBytes(conf.getInteger(FlinkOptions.CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES))
                    .withClusteringPlanSmallFileLimit(conf.getInteger(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT) * 1024 * 1024L)
                    .withClusteringSkipPartitionsFromLatest(conf.getInteger(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST))
                    .withAsyncClusteringMaxCommits(conf.getInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS))
                    .build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withAsyncClean(conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED))
                .retainCommits(conf.getInteger(FlinkOptions.CLEAN_RETAIN_COMMITS))
                .cleanerNumHoursRetained(conf.getInteger(FlinkOptions.CLEAN_RETAIN_HOURS))
                .retainFileVersions(conf.getInteger(FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS))
                // override and hardcode to 20,
                // actually Flink cleaning is always with parallelism 1 now
                .withCleanerParallelism(20)
                .withCleanerPolicy(HoodieCleaningPolicy.valueOf(conf.getString(FlinkOptions.CLEAN_POLICY)))
                .build())
            .withArchivalConfig(HoodieArchivalConfig.newBuilder()
                .archiveCommitsWith(conf.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS), conf.getInteger(FlinkOptions.ARCHIVE_MAX_COMMITS))
                .build())
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withTargetIOPerCompactionInMB(conf.getLong(FlinkOptions.COMPACTION_TARGET_IO))
                .withInlineCompactionTriggerStrategy(
                    CompactionTriggerStrategy.valueOf(conf.getString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY).toUpperCase(Locale.ROOT)))
                .withMaxNumDeltaCommitsBeforeCompaction(conf.getInteger(FlinkOptions.COMPACTION_DELTA_COMMITS))
                .withMaxDeltaSecondsBeforeCompaction(conf.getInteger(FlinkOptions.COMPACTION_DELTA_SECONDS))
                .build())
            .withMemoryConfig(
                HoodieMemoryConfig.newBuilder()
                    .withMaxMemoryMaxSize(
                        conf.getInteger(FlinkOptions.WRITE_MERGE_MAX_MEMORY) * 1024 * 1024L,
                        conf.getInteger(FlinkOptions.COMPACTION_MAX_MEMORY) * 1024 * 1024L
                    ).build())
            .forTable(conf.getString(FlinkOptions.TABLE_NAME))
            .withStorageConfig(HoodieStorageConfig.newBuilder()
                .logFileDataBlockMaxSize(conf.getInteger(FlinkOptions.WRITE_LOG_BLOCK_SIZE) * 1024 * 1024)
                .logFileMaxSize(conf.getLong(FlinkOptions.WRITE_LOG_MAX_SIZE) * 1024 * 1024)
                .parquetBlockSize(conf.getInteger(FlinkOptions.WRITE_PARQUET_BLOCK_SIZE) * 1024 * 1024)
                .parquetPageSize(conf.getInteger(FlinkOptions.WRITE_PARQUET_PAGE_SIZE) * 1024 * 1024)
                .parquetMaxFileSize(conf.getInteger(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE) * 1024 * 1024L)
                .build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                .enable(conf.getBoolean(FlinkOptions.METADATA_ENABLED))
                .withMaxNumDeltaCommitsBeforeCompaction(conf.getInteger(FlinkOptions.METADATA_COMPACTION_DELTA_COMMITS))
                .build())
            .withLockConfig(HoodieLockConfig.newBuilder()
                .withLockProvider(FileSystemBasedLockProvider.class)
                .withLockWaitTimeInMillis(2000L) // 2s
                .withFileSystemLockExpire(1) // 1 minute
                .withClientNumRetries(30)
                .withFileSystemLockPath(StreamerUtil.getAuxiliaryPath(conf))
                .build())
            .withPayloadConfig(getPayloadConfig(conf))
            .withEmbeddedTimelineServerEnabled(enableEmbeddedTimelineService)
            .withEmbeddedTimelineServerReuseEnabled(true) // make write client embedded timeline service singleton
            .withAutoCommit(false)
            .withAllowOperationMetadataField(conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED))
            .withProps(flinkConf2TypedProperties(conf))
            .withSchema(getSourceSchema(conf).toString());

    // do not configure cleaning strategy as LAZY until multi-writers is supported.
    HoodieWriteConfig writeConfig = builder.build();
    if (loadFsViewStorageConfig) {
      // do not use the builder to give a change for recovering the original fs view storage config
      FileSystemViewStorageConfig viewStorageConfig = ViewStorageProperties.loadFromProperties(conf.getString(FlinkOptions.PATH), conf);
      writeConfig.setViewStorageConfig(viewStorageConfig);
    }
    return writeConfig;
  }

  /**
   * Returns the payload config with given configuration.
   */
  public static HoodiePayloadConfig getPayloadConfig(Configuration conf) {
    return HoodiePayloadConfig.newBuilder()
        .withPayloadClass(conf.getString(FlinkOptions.PAYLOAD_CLASS_NAME))
        .withPayloadOrderingField(conf.getString(FlinkOptions.PRECOMBINE_FIELD))
        .withPayloadEventTimeField(conf.getString(FlinkOptions.PRECOMBINE_FIELD))
        .withPayloadClass(conf.getString(FlinkOptions.PAYLOAD_CLASS_NAME))
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
    Properties properties = new Properties();
    // put all the set options
    flatConf.addAllToProperties(properties);
    // put all the default options
    for (ConfigOption<?> option : FlinkOptions.optionalOptions()) {
      if (!flatConf.contains(option) && option.hasDefaultValue()) {
        properties.put(option.key(), option.defaultValue());
      }
    }
    return new TypedProperties(properties);
  }

  public static void checkRequiredProperties(TypedProperties props, List<String> checkPropNames) {
    checkPropNames.forEach(prop ->
        Preconditions.checkState(props.containsKey(prop), "Required property " + prop + " is missing"));
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
    final String basePath = conf.getString(FlinkOptions.PATH);
    if (!tableExists(basePath, hadoopConf)) {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.withPropertyBuilder()
          .setTableCreateSchema(conf.getString(FlinkOptions.SOURCE_AVRO_SCHEMA))
          .setTableType(conf.getString(FlinkOptions.TABLE_TYPE))
          .setTableName(conf.getString(FlinkOptions.TABLE_NAME))
          .setRecordKeyFields(conf.getString(FlinkOptions.RECORD_KEY_FIELD, null))
          .setPayloadClassName(conf.getString(FlinkOptions.PAYLOAD_CLASS_NAME))
          .setPreCombineField(OptionsResolver.getPreCombineField(conf))
          .setArchiveLogFolder(ARCHIVELOG_FOLDER.defaultValue())
          .setPartitionFields(conf.getString(FlinkOptions.PARTITION_PATH_FIELD, null))
          .setKeyGeneratorClassProp(
              conf.getOptional(FlinkOptions.KEYGEN_CLASS_NAME).orElse(SimpleAvroKeyGenerator.class.getName()))
          .setHiveStylePartitioningEnable(conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING))
          .setUrlEncodePartitioning(conf.getBoolean(FlinkOptions.URL_ENCODE_PARTITIONING))
          .setTimelineLayoutVersion(1)
          .initTable(hadoopConf, basePath);
      LOG.info("Table initialized under base path {}", basePath);
      return metaClient;
    } else {
      LOG.info("Table [{}/{}] already exists, no need to initialize the table",
          basePath, conf.getString(FlinkOptions.TABLE_NAME));
      return StreamerUtil.createMetaClient(basePath, hadoopConf);
    }
    // Do not close the filesystem in order to use the CACHE,
    // some filesystems release the handles in #close method.
  }

  /**
   * Returns whether the hoodie table exists under given path {@code basePath}.
   */
  public static boolean tableExists(String basePath, org.apache.hadoop.conf.Configuration hadoopConf) {
    // Hadoop FileSystem
    FileSystem fs = FSUtils.getFs(basePath, hadoopConf);
    try {
      return fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME));
    } catch (IOException e) {
      throw new HoodieException("Error while checking whether table exists under path:" + basePath, e);
    }
  }

  /**
   * Generates the bucket ID using format {partition path}_{fileID}.
   */
  public static String generateBucketKey(String partitionPath, String fileId) {
    return String.format("%s_%s", partitionPath, fileId);
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
    final String basePath = conf.getString(FlinkOptions.PATH);
    if (conf.getBoolean(FlinkOptions.READ_AS_STREAMING) && !tableExists(basePath, hadoopConf)) {
      return null;
    } else {
      return createMetaClient(basePath, hadoopConf);
    }
  }

  /**
   * Creates the meta client.
   */
  public static HoodieTableMetaClient createMetaClient(String basePath, org.apache.hadoop.conf.Configuration hadoopConf) {
    return HoodieTableMetaClient.builder().setBasePath(basePath).setConf(hadoopConf).build();
  }

  /**
   * Creates the meta client.
   */
  public static HoodieTableMetaClient createMetaClient(Configuration conf) {
    return createMetaClient(conf.getString(FlinkOptions.PATH), HadoopConfigurations.getHadoopConf(conf));
  }

  /**
   * Creates the Flink write client.
   *
   * <p>This expects to be used by client, the driver should start an embedded timeline server.
   */
  @SuppressWarnings("rawtypes")
  public static HoodieFlinkWriteClient createWriteClient(Configuration conf, RuntimeContext runtimeContext) {
    return createWriteClient(conf, runtimeContext, true);
  }

  /**
   * Creates the Flink write client.
   *
   * <p>This expects to be used by client, set flag {@code loadFsViewStorageConfig} to use
   * remote filesystem view storage config, or an in-memory filesystem view storage is used.
   */
  @SuppressWarnings("rawtypes")
  public static HoodieFlinkWriteClient createWriteClient(Configuration conf, RuntimeContext runtimeContext, boolean loadFsViewStorageConfig) {
    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(
            new SerializableConfiguration(HadoopConfigurations.getHadoopConf(conf)),
            new FlinkTaskContextSupplier(runtimeContext));

    HoodieWriteConfig writeConfig = getHoodieClientConfig(conf, loadFsViewStorageConfig);
    return new HoodieFlinkWriteClient<>(context, writeConfig);
  }

  /**
   * Creates the Flink write client.
   *
   * <p>This expects to be used by the driver, the client can then send requests for files view.
   *
   * <p>The task context supplier is a constant: the write token is always '0-1-0'.
   */
  @SuppressWarnings("rawtypes")
  public static HoodieFlinkWriteClient createWriteClient(Configuration conf) throws IOException {
    HoodieWriteConfig writeConfig = getHoodieClientConfig(conf, true, false);
    // build the write client to start the embedded timeline server
    final HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient<>(HoodieFlinkEngineContext.DEFAULT, writeConfig);
    // create the filesystem view storage properties for client
    final FileSystemViewStorageConfig viewStorageConfig = writeConfig.getViewStorageConfig();
    // rebuild the view storage config with simplified options.
    FileSystemViewStorageConfig rebuilt = FileSystemViewStorageConfig.newBuilder()
        .withStorageType(viewStorageConfig.getStorageType())
        .withRemoteServerHost(viewStorageConfig.getRemoteViewServerHost())
        .withRemoteServerPort(viewStorageConfig.getRemoteViewServerPort())
        .withRemoteTimelineClientTimeoutSecs(viewStorageConfig.getRemoteTimelineClientTimeoutSecs())
        .withRemoteTimelineClientRetry(viewStorageConfig.isRemoteTimelineClientRetryEnabled())
        .withRemoteTimelineClientMaxRetryNumbers(viewStorageConfig.getRemoteTimelineClientMaxRetryNumbers())
        .withRemoteTimelineInitialRetryIntervalMs(viewStorageConfig.getRemoteTimelineInitialRetryIntervalMs())
        .withRemoteTimelineClientMaxRetryIntervalMs(viewStorageConfig.getRemoteTimelineClientMaxRetryIntervalMs())
        .withRemoteTimelineClientRetryExceptions(viewStorageConfig.getRemoteTimelineClientRetryExceptions())
        .build();
    ViewStorageProperties.createProperties(conf.getString(FlinkOptions.PATH), rebuilt, conf);
    return writeClient;
  }

  /**
   * Returns the median instant time between the given two instant time.
   */
  public static Option<String> medianInstantTime(String highVal, String lowVal) {
    try {
      long high = HoodieActiveTimeline.parseDateFromInstantTime(highVal).getTime();
      long low = HoodieActiveTimeline.parseDateFromInstantTime(lowVal).getTime();
      ValidationUtils.checkArgument(high > low,
          "Instant [" + highVal + "] should have newer timestamp than instant [" + lowVal + "]");
      long median = low + (high - low) / 2;
      final String instantTime = HoodieActiveTimeline.formatDate(new Date(median));
      if (HoodieTimeline.compareTimestamps(lowVal, HoodieTimeline.GREATER_THAN_OR_EQUALS, instantTime)
          || HoodieTimeline.compareTimestamps(highVal, HoodieTimeline.LESSER_THAN_OR_EQUALS, instantTime)) {
        return Option.empty();
      }
      return Option.of(instantTime);
    } catch (ParseException e) {
      throw new HoodieException("Get median instant time with interval [" + lowVal + ", " + highVal + "] error", e);
    }
  }

  /**
   * Returns the time interval in seconds between the given instant time.
   */
  public static long instantTimeDiffSeconds(String newInstantTime, String oldInstantTime) {
    try {
      long newTimestamp = HoodieActiveTimeline.parseDateFromInstantTime(newInstantTime).getTime();
      long oldTimestamp = HoodieActiveTimeline.parseDateFromInstantTime(oldInstantTime).getTime();
      return (newTimestamp - oldTimestamp) / 1000;
    } catch (ParseException e) {
      throw new HoodieException("Get instant time diff with interval [" + oldInstantTime + ", " + newInstantTime + "] error", e);
    }
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
  public static boolean isValidFile(FileStatus fileStatus) {
    final String extension = FSUtils.getFileExtension(fileStatus.getPath().toString());
    if (PARQUET.getFileExtension().equals(extension)) {
      return fileStatus.getLen() > ParquetFileWriter.MAGIC.length;
    }

    if (ORC.getFileExtension().equals(extension)) {
      return fileStatus.getLen() > OrcFile.MAGIC.length();
    }

    if (HOODIE_LOG.getFileExtension().equals(extension)) {
      return fileStatus.getLen() > HoodieLogFormat.MAGIC.length;
    }

    return fileStatus.getLen() > 0;
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
        .map(HoodieInstant::getTimestamp)
        .orElse(null);
  }

  public static String getLastCompletedInstant(HoodieTableMetaClient metaClient) {
    return metaClient.getCommitsTimeline().filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::getTimestamp)
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
    return (long) conf.getInteger(FlinkOptions.COMPACTION_MAX_MEMORY) * 1024 * 1024;
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

  public static boolean fileExists(FileSystem fs, Path path) {
    try {
      return fs.exists(path);
    } catch (IOException e) {
      throw new HoodieException("Exception while checking file " + path + " existence", e);
    }
  }

  /**
   * Returns the auxiliary path.
   */
  public static String getAuxiliaryPath(Configuration conf) {
    return conf.getString(FlinkOptions.PATH) + Path.SEPARATOR + AUXILIARYFOLDER_NAME;
  }
}
