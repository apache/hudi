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

package org.apache.hudi.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;

/**
 * Flink sql hiveSync related config.
 */
@ConfigClassProperty(name = "Flink HiveSync Options",
    groupName = ConfigGroups.Names.FLINK_SQL,
    description = "Flink jobs using the SQL can be configured through the options in WITH clause."
        + "Configurations that control flink hiveSync behavior on Hudi tables are listed below.")
public class FlinkHiveSyncOptions {

  public static final ConfigOption<Boolean> HIVE_SYNC_ENABLED = ConfigOptions
      .key("hive_sync.enable")
      .booleanType()
      .defaultValue(false)
      .withDescription("Asynchronously sync Hive meta to HMS, default false.");

  public static final ConfigOption<String> HIVE_SYNC_DB = ConfigOptions
      .key("hive_sync.db")
      .stringType()
      .defaultValue("default")
      .withDescription("Database name for hive sync, default 'default'.");

  public static final ConfigOption<String> HIVE_SYNC_TABLE = ConfigOptions
      .key("hive_sync.table")
      .stringType()
      .defaultValue("unknown")
      .withDescription("Table name for hive sync, default 'unknown'.");

  public static final ConfigOption<String> HIVE_SYNC_FILE_FORMAT = ConfigOptions
      .key("hive_sync.file_format")
      .stringType()
      .defaultValue("PARQUET")
      .withDescription("File format for hive sync, default 'PARQUET'.");

  public static final ConfigOption<String> HIVE_SYNC_MODE = ConfigOptions
      .key("hive_sync.mode")
      .stringType()
      .defaultValue("jdbc")
      .withDescription("Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql, default 'jdbc'.");

  public static final ConfigOption<String> HIVE_SYNC_USERNAME = ConfigOptions
      .key("hive_sync.username")
      .stringType()
      .defaultValue("hive")
      .withDescription("Username for hive sync, default 'hive'.");

  public static final ConfigOption<String> HIVE_SYNC_PASSWORD = ConfigOptions
      .key("hive_sync.password")
      .stringType()
      .defaultValue("hive")
      .withDescription("Password for hive sync, default 'hive'.");

  public static final ConfigOption<String> HIVE_SYNC_JDBC_URL = ConfigOptions
      .key("hive_sync.jdbc_url")
      .stringType()
      .defaultValue("jdbc:hive2://localhost:10000")
      .withDescription("Jdbc URL for hive sync, default 'jdbc:hive2://localhost:10000'.");

  public static final ConfigOption<String> HIVE_SYNC_METASTORE_URIS = ConfigOptions
      .key("hive_sync.metastore.uris")
      .stringType()
      .defaultValue("")
      .withDescription("Metastore uris for hive sync, default ''.");

  public static final ConfigOption<String> HIVE_SYNC_PARTITION_FIELDS = ConfigOptions
      .key("hive_sync.partition_fields")
      .stringType()
      .defaultValue("")
      .withDescription("Partition fields for hive sync, default ''.");

  public static final ConfigOption<String> HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME = ConfigOptions
      .key("hive_sync.partition_extractor_class")
      .stringType()
      .defaultValue(SlashEncodedDayPartitionValueExtractor.class.getCanonicalName())
      .withDescription("Tool to extract the partition value from HDFS path, "
          + "default 'SlashEncodedDayPartitionValueExtractor'.");

  public static final ConfigOption<Boolean> HIVE_SYNC_ASSUME_DATE_PARTITION = ConfigOptions
      .key("hive_sync.assume_date_partitioning")
      .booleanType()
      .defaultValue(false)
      .withDescription("Assume partitioning is yyyy/mm/dd, default false.");

  public static final ConfigOption<Boolean> HIVE_SYNC_USE_JDBC = ConfigOptions
      .key("hive_sync.use_jdbc")
      .booleanType()
      .defaultValue(true)
      .withDescription("Use JDBC when hive synchronization is enabled, default true");

  public static final ConfigOption<Boolean> HIVE_SYNC_AUTO_CREATE_DB = ConfigOptions
      .key("hive_sync.auto_create_db")
      .booleanType()
      .defaultValue(true)
      .withDescription("Auto create hive database if it does not exists, default true");

  public static final ConfigOption<Boolean> HIVE_SYNC_IGNORE_EXCEPTIONS = ConfigOptions
      .key("hive_sync.ignore_exceptions")
      .booleanType()
      .defaultValue(false)
      .withDescription("Ignore exceptions during hive synchronization, default false");

  public static final ConfigOption<Boolean> HIVE_SYNC_SKIP_RO_SUFFIX = ConfigOptions
      .key("hive_sync.skip_ro_suffix")
      .booleanType()
      .defaultValue(false)
      .withDescription("Skip the _ro suffix for Read optimized table when registering, default false");

  public static final ConfigOption<Boolean> HIVE_SYNC_SUPPORT_TIMESTAMP = ConfigOptions
      .key("hive_sync.support_timestamp")
      .booleanType()
      .defaultValue(false)
      .withDescription("INT64 with original type TIMESTAMP_MICROS is converted to hive timestamp type.\n"
          + "Disabled by default for backward compatibility.");
}
