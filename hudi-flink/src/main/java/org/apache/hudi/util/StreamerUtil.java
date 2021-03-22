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

import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.TablePathUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.schema.FilebasedSchemaProvider;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
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
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

/**
 * Utilities for Flink stream read and write.
 */
public class StreamerUtil {
  private static final String DEFAULT_ARCHIVE_LOG_FOLDER = "archived";

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
    return new FilebasedSchemaProvider(FlinkOptions.fromStreamerConfig(cfg)).getSourceSchema();
  }

  public static Schema getSourceSchema(org.apache.flink.configuration.Configuration conf) {
    if (conf.getOptional(FlinkOptions.READ_AVRO_SCHEMA_PATH).isPresent()) {
      return new FilebasedSchemaProvider(conf).getSourceSchema();
    } else if (conf.getOptional(FlinkOptions.READ_AVRO_SCHEMA).isPresent()) {
      final String schemaStr = conf.get(FlinkOptions.READ_AVRO_SCHEMA);
      return new Schema.Parser().parse(schemaStr);
    } else {
      final String errorMsg = String.format("Either option '%s' or '%s' "
              + "should be specified for avro schema deserialization",
          FlinkOptions.READ_AVRO_SCHEMA_PATH.key(), FlinkOptions.READ_AVRO_SCHEMA.key());
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

  /**
   * Create a key generator class via reflection, passing in any configs needed.
   * <p>
   * If the class name of key generator is configured through the properties file, i.e., {@code props}, use the corresponding key generator class; otherwise, use the default key generator class
   * specified in {@code DataSourceWriteOptions}.
   */
  public static KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    String keyGeneratorClass = props.getString("hoodie.datasource.write.keygenerator.class",
        SimpleAvroKeyGenerator.class.getName());
    try {
      return (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, props);
    } catch (Throwable e) {
      throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
    }
  }

  /**
   * Create a key generator class via reflection, passing in any configs needed.
   * <p>
   * If the class name of key generator is configured through the properties file, i.e., {@code props}, use the corresponding key generator class; otherwise, use the default key generator class
   * specified in {@link FlinkOptions}.
   */
  public static KeyGenerator createKeyGenerator(Configuration conf) throws IOException {
    String keyGeneratorClass = conf.getString(FlinkOptions.KEYGEN_CLASS);
    try {
      return (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, flinkConf2TypedProperties(conf));
    } catch (Throwable e) {
      throw new IOException("Could not load key generator class " + keyGeneratorClass, e);
    }
  }

  /**
   * Create a payload class via reflection, passing in an ordering/precombine value.
   */
  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record, Comparable orderingVal)
      throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[] {GenericRecord.class, Comparable.class}, record, orderingVal);
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }

  /**
   * Create a payload class via reflection, do not ordering/precombine value.
   */
  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record)
      throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[] {Option.class}, Option.of(record));
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }

  public static HoodieWriteConfig getHoodieClientConfig(FlinkStreamerConfig conf) {
    return getHoodieClientConfig(FlinkOptions.fromStreamerConfig(conf));
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
                    .withInlineCompactionTriggerStrategy(
                        CompactionTriggerStrategy.valueOf(conf.getString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY).toUpperCase(Locale.ROOT)))
                    .withMaxNumDeltaCommitsBeforeCompaction(conf.getInteger(FlinkOptions.COMPACTION_DELTA_COMMITS))
                    .withMaxDeltaSecondsBeforeCompaction(conf.getInteger(FlinkOptions.COMPACTION_DELTA_SECONDS))
                    .build())
            .forTable(conf.getString(FlinkOptions.TABLE_NAME))
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
    for (ConfigOption<?> option : FlinkOptions.OPTIONAL_OPTIONS) {
      if (!conf.contains(option)) {
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
          .setArchiveLogFolder(DEFAULT_ARCHIVE_LOG_FOLDER)
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

  /** Generates the bucket ID using format {partition path}_{fileID}. */
  public static String generateBucketKey(String partitionPath, String fileId) {
    return String.format("%s_%s", partitionPath, fileId);
  }

  /** Returns whether the location represents an insert. */
  public static boolean isInsert(HoodieRecordLocation loc) {
    return Objects.equals(loc.getInstantTime(), "I");
  }

  public static String getTablePath(FileSystem fs, Path[] userProvidedPaths) throws IOException {
    LOG.info("Getting table path..");
    for (Path path : userProvidedPaths) {
      try {
        Option<Path> tablePath = TablePathUtils.getTablePath(fs, path);
        if (tablePath.isPresent()) {
          return tablePath.get().toString();
        }
      } catch (HoodieException he) {
        LOG.warn("Error trying to get table path from " + path.toString(), he);
      }
    }

    throw new TableNotFoundException("Unable to find a hudi table for the user provided paths.");
  }

  /**
   * Returns whether needs to schedule the async compaction.
   * @param conf The flink configuration.
   */
  public static boolean needsScheduleCompaction(Configuration conf) {
    return conf.getString(FlinkOptions.TABLE_TYPE)
        .toUpperCase(Locale.ROOT)
        .equals(FlinkOptions.TABLE_TYPE_MERGE_ON_READ)
        && conf.getBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED);
  }
}
