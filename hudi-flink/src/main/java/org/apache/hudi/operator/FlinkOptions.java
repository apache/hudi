/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.operator;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.streamer.FlinkStreamerConfig;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hoodie Flink config options.
 *
 * <p>It has the options for Hoodie table read and write. It also defines some utilities.
 */
public class FlinkOptions {
  private FlinkOptions() {
  }

  // ------------------------------------------------------------------------
  //  Base Options
  // ------------------------------------------------------------------------
  public static final ConfigOption<String> PATH = ConfigOptions
      .key("path")
      .stringType()
      .noDefaultValue()
      .withDescription("Base path for the target hoodie table."
          + "\nThe path would be created if it does not exist,\n"
          + "otherwise a Hoodie table expects to be initialized successfully");

  // ------------------------------------------------------------------------
  //  Read Options
  // ------------------------------------------------------------------------
  public static final ConfigOption<String> READ_SCHEMA_FILE_PATH = ConfigOptions
      .key("read.schema.file.path")
      .stringType()
      .noDefaultValue()
      .withDescription("Avro schema file path, the parsed schema is used for deserializing");

  // ------------------------------------------------------------------------
  //  Write Options
  // ------------------------------------------------------------------------
  public static final ConfigOption<String> TABLE_NAME = ConfigOptions
      .key(HoodieWriteConfig.TABLE_NAME)
      .stringType()
      .noDefaultValue()
      .withDescription("Table name to register to Hive metastore");

  public static final ConfigOption<String> TABLE_TYPE = ConfigOptions
      .key("write.table.type")
      .stringType()
      .defaultValue(HoodieTableType.COPY_ON_WRITE.name())
      .withDescription("Type of table to write, COPY_ON_WRITE (or) MERGE_ON_READ");

  public static final ConfigOption<String> OPERATION = ConfigOptions
      .key("write.operation")
      .stringType()
      .defaultValue("upsert")
      .withDescription("The write operation, that this write should do");

  public static final ConfigOption<String> PRECOMBINE_FIELD = ConfigOptions
      .key("write.precombine.field")
      .stringType()
      .defaultValue("ts")
      .withDescription("Field used in preCombining before actual write. When two records have the same\n"
          + "key value, we will pick the one with the largest value for the precombine field,\n"
          + "determined by Object.compareTo(..)");

  public static final ConfigOption<String> PAYLOAD_CLASS = ConfigOptions
      .key("write.payload.class")
      .stringType()
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDescription("Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.\n"
          + "This will render any value set for the option in-effective");

  /**
   * Flag to indicate whether to drop duplicates upon insert.
   * By default insert will accept duplicates, to gain extra performance.
   */
  public static final ConfigOption<Boolean> INSERT_DROP_DUPS = ConfigOptions
      .key("write.insert.drop.duplicates")
      .booleanType()
      .defaultValue(false)
      .withDescription("Flag to indicate whether to drop duplicates upon insert.\n"
          + "By default insert will accept duplicates, to gain extra performance");

  public static final ConfigOption<Integer> RETRY_TIMES = ConfigOptions
      .key("write.retry.times")
      .intType()
      .defaultValue(3)
      .withDescription("Flag to indicate how many times streaming job should retry for a failed checkpoint batch.\n"
          + "By default 3");

  public static final ConfigOption<Long> RETRY_INTERVAL_MS = ConfigOptions
      .key("write.retry.interval.ms")
      .longType()
      .defaultValue(2000L)
      .withDescription("Flag to indicate how long (by millisecond) before a retry should issued for failed checkpoint batch.\n"
          + "By default 2000 and it will be doubled by every retry");

  public static final ConfigOption<Boolean> IGNORE_FAILED = ConfigOptions
      .key("write.ignore.failed")
      .booleanType()
      .defaultValue(true)
      .withDescription("Flag to indicate whether to ignore any non exception error (e.g. writestatus error). within a checkpoint batch.\n"
          + "By default true (in favor of streaming progressing over data integrity)");

  public static final ConfigOption<String> RECORD_KEY_FIELD = ConfigOptions
      .key(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY)
      .stringType()
      .defaultValue("uuid")
      .withDescription("Record key field. Value to be used as the `recordKey` component of `HoodieKey`.\n"
          + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using "
          + "the dot notation eg: `a.b.c`");

  public static final ConfigOption<String> PARTITION_PATH_FIELD = ConfigOptions
      .key(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY)
      .stringType()
      .defaultValue("partition-path")
      .withDescription("Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`.\n"
          + "Actual value obtained by invoking .toString()");

  public static final ConfigOption<String> KEYGEN_CLASS = ConfigOptions
      .key(HoodieWriteConfig.KEYGENERATOR_CLASS_PROP)
      .stringType()
      .defaultValue(SimpleAvroKeyGenerator.class.getName())
      .withDescription("Key generator class, that implements will extract the key out of incoming record");

  public static final ConfigOption<Integer> WRITE_TASK_PARALLELISM = ConfigOptions
      .key("write.task.parallelism")
      .intType()
      .defaultValue(4)
      .withDescription("Parallelism of tasks that do actual write, default is 4");

  public static final ConfigOption<Double> WRITE_BATCH_SIZE = ConfigOptions
      .key("write.batch.size.MB")
      .doubleType()
      .defaultValue(128D) // 128MB
      .withDescription("Batch buffer size in MB to flush data into the underneath filesystem");

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  // Remember to update the set when adding new options.
  public static final List<ConfigOption<?>> OPTIONAL_OPTIONS = Arrays.asList(
      TABLE_TYPE, OPERATION, PRECOMBINE_FIELD, PAYLOAD_CLASS, INSERT_DROP_DUPS, RETRY_TIMES,
      RETRY_INTERVAL_MS, IGNORE_FAILED, RECORD_KEY_FIELD, PARTITION_PATH_FIELD, KEYGEN_CLASS
  );

  // Prefix for Hoodie specific properties.
  private static final String PROPERTIES_PREFIX = "properties.";

  /**
   * Transforms a {@code HoodieFlinkStreamer.Config} into {@code Configuration}.
   * The latter is more suitable for the table APIs. It reads all the properties
   * in the properties file (set by `--props` option) and cmd line options
   * (set by `--hoodie-conf` option).
   */
  @SuppressWarnings("unchecked, rawtypes")
  public static org.apache.flink.configuration.Configuration fromStreamerConfig(FlinkStreamerConfig config) {
    Map<String, String> propsMap = new HashMap<String, String>((Map) StreamerUtil.getProps(config));
    org.apache.flink.configuration.Configuration conf = fromMap(propsMap);

    conf.setString(FlinkOptions.PATH, config.targetBasePath);
    conf.setString(READ_SCHEMA_FILE_PATH, config.readSchemaFilePath);
    conf.setString(FlinkOptions.TABLE_NAME, config.targetTableName);
    // copy_on_write works same as COPY_ON_WRITE
    conf.setString(FlinkOptions.TABLE_TYPE, config.tableType.toUpperCase());
    conf.setString(FlinkOptions.OPERATION, config.operation.value());
    conf.setString(FlinkOptions.PRECOMBINE_FIELD, config.sourceOrderingField);
    conf.setString(FlinkOptions.PAYLOAD_CLASS, config.payloadClassName);
    conf.setBoolean(FlinkOptions.INSERT_DROP_DUPS, config.filterDupes);
    conf.setInteger(FlinkOptions.RETRY_TIMES, Integer.parseInt(config.instantRetryTimes));
    conf.setLong(FlinkOptions.RETRY_INTERVAL_MS, Long.parseLong(config.instantRetryInterval));
    conf.setBoolean(FlinkOptions.IGNORE_FAILED, config.commitOnErrors);
    conf.setString(FlinkOptions.RECORD_KEY_FIELD, config.recordKeyField);
    conf.setString(FlinkOptions.PARTITION_PATH_FIELD, config.partitionPathField);
    conf.setString(FlinkOptions.KEYGEN_CLASS, config.keygenClass);
    conf.setInteger(FlinkOptions.WRITE_TASK_PARALLELISM, config.writeTaskNum);

    return conf;
  }

  /**
   * Collects the config options that start with 'properties.' into a 'key'='value' list.
   */
  public static Map<String, String> getHoodieProperties(Map<String, String> options) {
    final Map<String, String> hoodieProperties = new HashMap<>();

    if (hasPropertyOptions(options)) {
      options.keySet().stream()
          .filter(key -> key.startsWith(PROPERTIES_PREFIX))
          .forEach(key -> {
            final String value = options.get(key);
            final String subKey = key.substring((PROPERTIES_PREFIX).length());
            hoodieProperties.put(subKey, value);
          });
    }
    return hoodieProperties;
  }

  /**
   * Collects all the config options, the 'properties.' prefix would be removed if the option key starts with it.
   */
  public static Configuration flatOptions(Configuration conf) {
    final Map<String, String> propsMap = new HashMap<>();

    conf.toMap().forEach((key, value) -> {
      final String subKey = key.startsWith(PROPERTIES_PREFIX)
          ? key.substring((PROPERTIES_PREFIX).length())
          : key;
      propsMap.put(subKey, value);
    });
    return fromMap(propsMap);
  }

  private static boolean hasPropertyOptions(Map<String, String> options) {
    return options.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
  }

  /** Creates a new configuration that is initialized with the options of the given map. */
  private static Configuration fromMap(Map<String, String> map) {
    final Configuration configuration = new Configuration();
    map.forEach(configuration::setString);
    return configuration;
  }
}
