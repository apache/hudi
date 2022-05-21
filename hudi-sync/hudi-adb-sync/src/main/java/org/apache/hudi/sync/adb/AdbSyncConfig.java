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

package org.apache.hudi.sync.adb;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.beust.jcommander.Parameter;

/**
 * Configs needed to sync data into Alibaba Cloud AnalyticDB(ADB).
 */
public class AdbSyncConfig extends HoodieSyncConfig {

  @Parameter(names = {"--user"}, description = "Adb username", required = true)
  public String adbUser;

  @Parameter(names = {"--pass"}, description = "Adb password", required = true)
  public String adbPass;

  @Parameter(names = {"--jdbc-url"}, description = "Adb jdbc connect url", required = true)
  public String jdbcUrl;

  @Parameter(names = {"--skip-ro-suffix"}, description = "Whether skip the `_ro` suffix for read optimized table when syncing")
  public Boolean skipROSuffix;

  @Parameter(names = {"--skip-rt-sync"}, description = "Whether skip the rt table when syncing")
  public Boolean skipRTSync;

  @Parameter(names = {"--hive-style-partitioning"}, description = "Whether use hive style partitioning, true if like the following style: field1=value1/field2=value2")
  public Boolean useHiveStylePartitioning;

  @Parameter(names = {"--support-timestamp"}, description = "If true, converts int64(timestamp_micros) to timestamp type")
  public Boolean supportTimestamp;

  @Parameter(names = {"--spark-datasource"}, description = "Whether sync this table as spark data source table")
  public Boolean syncAsSparkDataSourceTable;

  @Parameter(names = {"--table-properties"}, description = "Table properties, to support read hoodie table as datasource table", required = true)
  public String tableProperties;

  @Parameter(names = {"--serde-properties"}, description = "Serde properties, to support read hoodie table as datasource table", required = true)
  public String serdeProperties;

  @Parameter(names = {"--spark-schema-length-threshold"}, description = "The maximum length allowed in a single cell when storing additional schema information in Hive's metastore")
  public int sparkSchemaLengthThreshold;

  @Parameter(names = {"--db-location"}, description = "Database location")
  public String dbLocation;

  @Parameter(names = {"--auto-create-database"}, description = "Whether auto create adb database")
  public Boolean autoCreateDatabase = true;

  @Parameter(names = {"--skip-last-commit-time-sync"}, description = "Whether skip last commit time syncing")
  public Boolean skipLastCommitTimeSync = false;

  @Parameter(names = {"--drop-table-before-creation"}, description = "Whether drop table before creation")
  public Boolean dropTableBeforeCreation = false;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  public static final ConfigProperty<String> ADB_SYNC_USER = ConfigProperty
      .key("hoodie.datasource.adb.sync.username")
      .noDefaultValue()
      .withDocumentation("ADB username");

  public static final ConfigProperty<String> ADB_SYNC_PASS = ConfigProperty
      .key("hoodie.datasource.adb.sync.password")
      .noDefaultValue()
      .withDocumentation("ADB user password");

  public static final ConfigProperty<String> ADB_SYNC_JDBC_URL = ConfigProperty
      .key("hoodie.datasource.adb.sync.jdbc_url")
      .noDefaultValue()
      .withDocumentation("Adb jdbc connect url");

  public static final ConfigProperty<Boolean> ADB_SYNC_SKIP_RO_SUFFIX = ConfigProperty
      .key("hoodie.datasource.adb.sync.skip_ro_suffix")
      .defaultValue(true)
      .withDocumentation("Whether skip the `_ro` suffix for read optimized table when syncing");

  public static final ConfigProperty<Boolean> ADB_SYNC_SKIP_RT_SYNC = ConfigProperty
      .key("hoodie.datasource.adb.sync.skip_rt_sync")
      .defaultValue(true)
      .withDocumentation("Whether skip the rt table when syncing");

  public static final ConfigProperty<Boolean> ADB_SYNC_USE_HIVE_STYLE_PARTITIONING = ConfigProperty
      .key("hoodie.datasource.adb.sync.hive_style_partitioning")
      .defaultValue(false)
      .withDocumentation("Whether use hive style partitioning, true if like the following style: field1=value1/field2=value2");

  public static final ConfigProperty<Boolean> ADB_SYNC_SUPPORT_TIMESTAMP = ConfigProperty
      .key("hoodie.datasource.adb.sync.support_timestamp")
      .defaultValue(false)
      .withDocumentation("If true, converts int64(timestamp_micros) to timestamp type");

  public static final ConfigProperty<Boolean> ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE = ConfigProperty
      .key("hoodie.datasource.adb.sync.sync_as_spark_datasource")
      .defaultValue(true)
      .withDocumentation("Whether sync this table as spark data source table");

  public static final ConfigProperty<String> ADB_SYNC_TABLE_PROPERTIES = ConfigProperty
      .key("hoodie.datasource.adb.sync.table_properties")
      .noDefaultValue()
      .withDocumentation("Table properties, to support read hoodie table as datasource table");

  public static final ConfigProperty<String> ADB_SYNC_SERDE_PROPERTIES = ConfigProperty
      .key("hoodie.datasource.adb.sync.serde_properties")
      .noDefaultValue()
      .withDocumentation("Serde properties, to support read hoodie table as datasource table");

  public static final ConfigProperty<Integer> ADB_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD = ConfigProperty
      .key("hoodie.datasource.adb.sync.schema_string_length_threshold")
      .defaultValue(4000)
      .withDocumentation("The maximum length allowed in a single cell when storing additional schema information in Hive's metastore");

  public static final ConfigProperty<String> ADB_SYNC_DB_LOCATION = ConfigProperty
      .key("hoodie.datasource.adb.sync.db_location")
      .noDefaultValue()
      .withDocumentation("Database location");

  public static final ConfigProperty<Boolean> ADB_SYNC_AUTO_CREATE_DATABASE = ConfigProperty
      .key("hoodie.datasource.adb.sync.auto_create_database")
      .defaultValue(true)
      .withDocumentation("Whether auto create adb database");

  public static final ConfigProperty<Boolean> ADB_SYNC_SKIP_LAST_COMMIT_TIME_SYNC = ConfigProperty
      .key("hoodie.datasource.adb.sync.skip_last_commit_time_sync")
      .defaultValue(false)
      .withDocumentation("Whether skip last commit time syncing");

  public static final ConfigProperty<Boolean> ADB_SYNC_DROP_TABLE_BEFORE_CREATION = ConfigProperty
      .key("hoodie.datasource.adb.sync.drop_table_before_creation")
      .defaultValue(false)
      .withDocumentation("Whether drop table before creation");

  public AdbSyncConfig() {
    this(new TypedProperties());
  }

  public AdbSyncConfig(TypedProperties props) {
    super(props);

    adbUser = getString(ADB_SYNC_USER);
    adbPass = getString(ADB_SYNC_PASS);
    jdbcUrl = getString(ADB_SYNC_JDBC_URL);
    skipROSuffix = getBooleanOrDefault(ADB_SYNC_SKIP_RO_SUFFIX);
    skipRTSync = getBooleanOrDefault(ADB_SYNC_SKIP_RT_SYNC);
    useHiveStylePartitioning = getBooleanOrDefault(ADB_SYNC_USE_HIVE_STYLE_PARTITIONING);
    supportTimestamp = getBooleanOrDefault(ADB_SYNC_SUPPORT_TIMESTAMP);
    syncAsSparkDataSourceTable = getBooleanOrDefault(ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE);
    tableProperties = getString(ADB_SYNC_TABLE_PROPERTIES);
    serdeProperties = getString(ADB_SYNC_SERDE_PROPERTIES);
    sparkSchemaLengthThreshold = getIntOrDefault(ADB_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD);
    dbLocation = getString(ADB_SYNC_DB_LOCATION);
    autoCreateDatabase = getBooleanOrDefault(ADB_SYNC_AUTO_CREATE_DATABASE);
    skipLastCommitTimeSync = getBooleanOrDefault(ADB_SYNC_SKIP_LAST_COMMIT_TIME_SYNC);
    dropTableBeforeCreation = getBooleanOrDefault(ADB_SYNC_DROP_TABLE_BEFORE_CREATION);
  }

  public static TypedProperties toProps(AdbSyncConfig cfg) {
    TypedProperties properties = new TypedProperties();
    properties.put(META_SYNC_DATABASE_NAME.key(), cfg.databaseName);
    properties.put(META_SYNC_TABLE_NAME.key(), cfg.tableName);
    properties.put(ADB_SYNC_USER.key(), cfg.adbUser);
    properties.put(ADB_SYNC_PASS.key(), cfg.adbPass);
    properties.put(ADB_SYNC_JDBC_URL.key(), cfg.jdbcUrl);
    properties.put(META_SYNC_BASE_PATH.key(), cfg.basePath);
    properties.put(META_SYNC_PARTITION_FIELDS.key(), String.join(",", cfg.partitionFields));
    properties.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), cfg.partitionValueExtractorClass);
    properties.put(META_SYNC_ASSUME_DATE_PARTITION.key(), String.valueOf(cfg.assumeDatePartitioning));
    properties.put(ADB_SYNC_SKIP_RO_SUFFIX.key(), String.valueOf(cfg.skipROSuffix));
    properties.put(ADB_SYNC_SKIP_RT_SYNC.key(), String.valueOf(cfg.skipRTSync));
    properties.put(ADB_SYNC_USE_HIVE_STYLE_PARTITIONING.key(), String.valueOf(cfg.useHiveStylePartitioning));
    properties.put(META_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), String.valueOf(cfg.useFileListingFromMetadata));
    properties.put(ADB_SYNC_SUPPORT_TIMESTAMP.key(), String.valueOf(cfg.supportTimestamp));
    properties.put(ADB_SYNC_TABLE_PROPERTIES.key(), cfg.tableProperties);
    properties.put(ADB_SYNC_SERDE_PROPERTIES.key(), cfg.serdeProperties);
    properties.put(ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE.key(), String.valueOf(cfg.syncAsSparkDataSourceTable));
    properties.put(ADB_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.key(), String.valueOf(cfg.sparkSchemaLengthThreshold));
    properties.put(META_SYNC_SPARK_VERSION.key(), cfg.sparkVersion);
    properties.put(ADB_SYNC_DB_LOCATION.key(), cfg.dbLocation);
    properties.put(ADB_SYNC_AUTO_CREATE_DATABASE.key(), String.valueOf(cfg.autoCreateDatabase));
    properties.put(ADB_SYNC_SKIP_LAST_COMMIT_TIME_SYNC.key(), String.valueOf(cfg.skipLastCommitTimeSync));
    properties.put(ADB_SYNC_DROP_TABLE_BEFORE_CREATION.key(), String.valueOf(cfg.dropTableBeforeCreation));

    return properties;
  }

  @Override
  public String toString() {
    return "AdbSyncConfig{"
        + "adbUser='" + adbUser + '\''
        + ", adbPass='" + adbPass + '\''
        + ", jdbcUrl='" + jdbcUrl + '\''
        + ", skipROSuffix=" + skipROSuffix
        + ", skipRTSync=" + skipRTSync
        + ", useHiveStylePartitioning=" + useHiveStylePartitioning
        + ", supportTimestamp=" + supportTimestamp
        + ", syncAsSparkDataSourceTable=" + syncAsSparkDataSourceTable
        + ", tableProperties='" + tableProperties + '\''
        + ", serdeProperties='" + serdeProperties + '\''
        + ", sparkSchemaLengthThreshold=" + sparkSchemaLengthThreshold
        + ", dbLocation='" + dbLocation + '\''
        + ", autoCreateDatabase=" + autoCreateDatabase
        + ", skipLastCommitTimeSync=" + skipLastCommitTimeSync
        + ", dropTableBeforeCreation=" + dropTableBeforeCreation
        + ", help=" + help
        + ", databaseName='" + databaseName + '\''
        + ", tableName='" + tableName + '\''
        + ", basePath='" + basePath + '\''
        + ", baseFileFormat='" + baseFileFormat + '\''
        + ", partitionFields=" + partitionFields
        + ", partitionValueExtractorClass='" + partitionValueExtractorClass + '\''
        + ", assumeDatePartitioning=" + assumeDatePartitioning
        + ", decodePartition=" + decodePartition
        + ", useFileListingFromMetadata=" + useFileListingFromMetadata
        + ", isConditionalSync=" + isConditionalSync
        + ", sparkVersion='" + sparkVersion + '\''
        + '}';
  }
}
