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
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.schema.FilebasedSchemaProvider;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_ARCHIVELOG_FOLDER_PROP;

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
        FSUtils.getFs(cfg.propsFilePath, getHadoopConf()),
        new Path(cfg.propsFilePath), cfg.configs).getConfig();
  }

  public static Schema getSourceSchema(FlinkStreamerConfig cfg) {
    return new FilebasedSchemaProvider(FlinkStreamerConfig.toFlinkConfig(cfg)).getSourceSchema();
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
  public static DFSPropertiesConfiguration readConfig(FileSystem fs, Path cfgPath, List<String> overriddenProps) {
    DFSPropertiesConfiguration conf;
    try {
      conf = new DFSPropertiesConfiguration(cfgPath.getFileSystem(fs.getConf()), cfgPath);
    } catch (Exception e) {
      conf = new DFSPropertiesConfiguration();
      LOG.warn("Unexpected error read props file at :" + cfgPath, e);
    }

    try {
      if (!overriddenProps.isEmpty()) {
        LOG.info("Adding overridden properties to file properties.");
        conf.addProperties(new BufferedReader(new StringReader(String.join("\n", overriddenProps))));
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Unexpected error adding config overrides", ioe);
    }

    return conf;
  }

  // Keep to avoid to much modifications.
  public static org.apache.hadoop.conf.Configuration getHadoopConf() {
    return FlinkClientUtil.getHadoopConf();
  }

  public static HoodieWriteConfig getHoodieClientConfig(Configuration conf) {
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder()
            .withEngineType(EngineType.FLINK)
            .withPath(conf.getString(FlinkOptions.PATH))
            .combineInput(conf.getBoolean(FlinkOptions.INSERT_DROP_DUPS), true)
            .withCompactionConfig(
                HoodieCompactionConfig.newBuilder()
                    .withPayloadClass(conf.getString(FlinkOptions.PAYLOAD_CLASS))
                    .withTargetIOPerCompactionInMB(conf.getLong(FlinkOptions.COMPACTION_TARGET_IO))
                    .withInlineCompactionTriggerStrategy(
                        CompactionTriggerStrategy.valueOf(conf.getString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY).toUpperCase(Locale.ROOT)))
                    .withMaxNumDeltaCommitsBeforeCompaction(conf.getInteger(FlinkOptions.COMPACTION_DELTA_COMMITS))
                    .withMaxDeltaSecondsBeforeCompaction(conf.getInteger(FlinkOptions.COMPACTION_DELTA_SECONDS))
                    .withAsyncClean(conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED))
                    .retainCommits(conf.getInteger(FlinkOptions.CLEAN_RETAIN_COMMITS))
                    // override and hardcode to 20,
                    // actually Flink cleaning is always with parallelism 1 now
                    .withCleanerParallelism(20)
                    .archiveCommitsWith(conf.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS), conf.getInteger(FlinkOptions.ARCHIVE_MAX_COMMITS))
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
                .logFileMaxSize(conf.getInteger(FlinkOptions.WRITE_LOG_MAX_SIZE) * 1024 * 1024)
                .build())
            .withEmbeddedTimelineServerReuseEnabled(true) // make write client embedded timeline service singleton
            .withAutoCommit(false)
            .withProps(flinkConf2TypedProperties(FlinkOptions.flatOptions(conf)));

    builder = builder.withSchema(getSourceSchema(conf).toString());
    return builder.build();
  }

  /**
   * Converts the give {@link Configuration} to {@link TypedProperties}.
   * The default values are also set up.
   *
   * @param conf The flink configuration
   * @return a TypedProperties instance
   */
  public static TypedProperties flinkConf2TypedProperties(Configuration conf) {
    Properties properties = new Properties();
    // put all the set up options
    conf.addAllToProperties(properties);
    // put all the default options
    for (ConfigOption<?> option : FlinkOptions.optionalOptions()) {
      if (!conf.contains(option) && option.hasDefaultValue()) {
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
  public static void initTableIfNotExists(Configuration conf) throws IOException {
    final String basePath = conf.getString(FlinkOptions.PATH);
    final org.apache.hadoop.conf.Configuration hadoopConf = StreamerUtil.getHadoopConf();
    // Hadoop FileSystem
    FileSystem fs = FSUtils.getFs(basePath, hadoopConf);
    if (!fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))) {
      HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(conf.getString(FlinkOptions.TABLE_TYPE))
          .setTableName(conf.getString(FlinkOptions.TABLE_NAME))
          .setPayloadClassName(conf.getString(FlinkOptions.PAYLOAD_CLASS))
          .setArchiveLogFolder(HOODIE_ARCHIVELOG_FOLDER_PROP.defaultValue())
          .setPartitionColumns(conf.getString(FlinkOptions.PARTITION_PATH_FIELD, null))
          .setPreCombineField(conf.getString(FlinkOptions.PRECOMBINE_FIELD))
          .setTimelineLayoutVersion(1)
          .initTable(hadoopConf, basePath);
      LOG.info("Table initialized under base path {}", basePath);
    } else {
      LOG.info("Table [{}/{}] already exists, no need to initialize the table",
          basePath, conf.getString(FlinkOptions.TABLE_NAME));
    }
    // Do not close the filesystem in order to use the CACHE,
    // some of the filesystems release the handles in #close method.
  }

  /**
   * Generates the bucket ID using format {partition path}_{fileID}.
   */
  public static String generateBucketKey(String partitionPath, String fileId) {
    return String.format("%s_%s", partitionPath, fileId);
  }

  /**
   * Returns whether needs to schedule the async compaction.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsAsyncCompaction(Configuration conf) {
    return conf.getString(FlinkOptions.TABLE_TYPE)
        .toUpperCase(Locale.ROOT)
        .equals(FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        && conf.getBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED);
  }

  /**
   * Returns whether needs to schedule the compaction plan.
   *
   * @param conf The flink configuration.
   */
  public static boolean needsScheduleCompaction(Configuration conf) {
    return conf.getString(FlinkOptions.TABLE_TYPE)
        .toUpperCase(Locale.ROOT)
        .equals(FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        && conf.getBoolean(FlinkOptions.COMPACTION_SCHEDULE_ENABLED);
  }

  /**
   * Creates the meta client.
   */
  public static HoodieTableMetaClient createMetaClient(Configuration conf) {
    return HoodieTableMetaClient.builder().setBasePath(conf.getString(FlinkOptions.PATH)).setConf(FlinkClientUtil.getHadoopConf()).build();
  }

  /**
   * Creates the Flink write client.
   */
  public static HoodieFlinkWriteClient createWriteClient(Configuration conf, RuntimeContext runtimeContext) {
    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(
            new SerializableConfiguration(getHadoopConf()),
            new FlinkTaskContextSupplier(runtimeContext));

    return new HoodieFlinkWriteClient<>(context, getHoodieClientConfig(conf));
  }

  /**
   * Return the median instant time between the given two instant time.
   */
  public static String medianInstantTime(String highVal, String lowVal) {
    try {
      long high = HoodieActiveTimeline.COMMIT_FORMATTER.parse(highVal).getTime();
      long low = HoodieActiveTimeline.COMMIT_FORMATTER.parse(lowVal).getTime();
      ValidationUtils.checkArgument(high > low,
          "Instant [" + highVal + "] should have newer timestamp than instant [" + lowVal + "]");
      long median = low + (high - low) / 2;
      return HoodieActiveTimeline.COMMIT_FORMATTER.format(new Date(median));
    } catch (ParseException e) {
      throw new HoodieException("Get median instant time with interval [" + lowVal + ", " + highVal + "] error", e);
    }
  }

  /**
   * Returns the time interval in seconds between the given instant time.
   */
  public static long instantTimeDiffSeconds(String newInstantTime, String oldInstantTime) {
    try {
      long newTimestamp = HoodieActiveTimeline.COMMIT_FORMATTER.parse(newInstantTime).getTime();
      long oldTimestamp = HoodieActiveTimeline.COMMIT_FORMATTER.parse(oldInstantTime).getTime();
      return (newTimestamp - oldTimestamp) / 1000;
    } catch (ParseException e) {
      throw new HoodieException("Get instant time diff with interval [" + oldInstantTime + ", " + newInstantTime + "] error", e);
    }
  }
}
