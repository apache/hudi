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

import com.beust.jcommander.ParametersDelegate;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.beust.jcommander.Parameter;

import java.io.Serializable;

/**
 * Configs needed to sync data into Alibaba Cloud AnalyticDB(ADB).
 */
public class AdbSyncConfig extends HoodieSyncConfig {

  public final AdbSyncConfigParams adbSyncConfigParams = new AdbSyncConfigParams();

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

    adbSyncConfigParams.hiveSyncConfigParams.hiveUser = getString(ADB_SYNC_USER);
    adbSyncConfigParams.hiveSyncConfigParams.hivePass = getString(ADB_SYNC_PASS);
    adbSyncConfigParams.hiveSyncConfigParams.jdbcUrl = getString(ADB_SYNC_JDBC_URL);
    adbSyncConfigParams.hiveSyncConfigParams.skipROSuffix = getBooleanOrDefault(ADB_SYNC_SKIP_RO_SUFFIX);
    adbSyncConfigParams.skipRTSync = getBooleanOrDefault(ADB_SYNC_SKIP_RT_SYNC);
    adbSyncConfigParams.useHiveStylePartitioning = getBooleanOrDefault(ADB_SYNC_USE_HIVE_STYLE_PARTITIONING);
    adbSyncConfigParams.supportTimestamp = getBooleanOrDefault(ADB_SYNC_SUPPORT_TIMESTAMP);
    adbSyncConfigParams.syncAsSparkDataSourceTable = getBooleanOrDefault(ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE);
    adbSyncConfigParams.tableProperties = getString(ADB_SYNC_TABLE_PROPERTIES);
    adbSyncConfigParams.serdeProperties = getString(ADB_SYNC_SERDE_PROPERTIES);
    adbSyncConfigParams.sparkSchemaLengthThreshold = getIntOrDefault(ADB_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD);
    adbSyncConfigParams.dbLocation = getString(ADB_SYNC_DB_LOCATION);
    adbSyncConfigParams.autoCreateDatabase = getBooleanOrDefault(ADB_SYNC_AUTO_CREATE_DATABASE);
    adbSyncConfigParams.skipLastCommitTimeSync = getBooleanOrDefault(ADB_SYNC_SKIP_LAST_COMMIT_TIME_SYNC);
    adbSyncConfigParams.dropTableBeforeCreation = getBooleanOrDefault(ADB_SYNC_DROP_TABLE_BEFORE_CREATION);
  }

  public static TypedProperties toProps(AdbSyncConfig cfg) {
    TypedProperties properties = new TypedProperties();
    properties.put(META_SYNC_DATABASE_NAME.key(), cfg.hoodieSyncConfigParams.databaseName);
    properties.put(META_SYNC_TABLE_NAME.key(), cfg.hoodieSyncConfigParams.tableName);
    properties.put(ADB_SYNC_USER.key(), cfg.adbSyncConfigParams.hiveSyncConfigParams.hiveUser);
    properties.put(ADB_SYNC_PASS.key(), cfg.adbSyncConfigParams.hiveSyncConfigParams.hivePass);
    properties.put(ADB_SYNC_JDBC_URL.key(), cfg.adbSyncConfigParams.hiveSyncConfigParams.jdbcUrl);
    properties.put(META_SYNC_BASE_PATH.key(), cfg.hoodieSyncConfigParams.basePath);
    properties.put(META_SYNC_PARTITION_FIELDS.key(), String.join(",", cfg.hoodieSyncConfigParams.partitionFields));
    properties.put(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), cfg.hoodieSyncConfigParams.partitionValueExtractorClass);
    properties.put(META_SYNC_ASSUME_DATE_PARTITION.key(), String.valueOf(cfg.hoodieSyncConfigParams.assumeDatePartitioning));
    properties.put(ADB_SYNC_SKIP_RO_SUFFIX.key(), String.valueOf(cfg.adbSyncConfigParams.hiveSyncConfigParams.skipROSuffix));
    properties.put(ADB_SYNC_SKIP_RT_SYNC.key(), String.valueOf(cfg.adbSyncConfigParams.skipRTSync));
    properties.put(ADB_SYNC_USE_HIVE_STYLE_PARTITIONING.key(), String.valueOf(cfg.adbSyncConfigParams.useHiveStylePartitioning));
    properties.put(META_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), String.valueOf(cfg.hoodieSyncConfigParams.useFileListingFromMetadata));
    properties.put(ADB_SYNC_SUPPORT_TIMESTAMP.key(), String.valueOf(cfg.adbSyncConfigParams.supportTimestamp));
    properties.put(ADB_SYNC_TABLE_PROPERTIES.key(), cfg.adbSyncConfigParams.tableProperties);
    properties.put(ADB_SYNC_SERDE_PROPERTIES.key(), cfg.adbSyncConfigParams.serdeProperties);
    properties.put(ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE.key(), String.valueOf(cfg.adbSyncConfigParams.syncAsSparkDataSourceTable));
    properties.put(ADB_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.key(), String.valueOf(cfg.adbSyncConfigParams.sparkSchemaLengthThreshold));
    properties.put(META_SYNC_SPARK_VERSION.key(), cfg.hoodieSyncConfigParams.sparkVersion);
    properties.put(ADB_SYNC_DB_LOCATION.key(), cfg.adbSyncConfigParams.dbLocation);
    properties.put(ADB_SYNC_AUTO_CREATE_DATABASE.key(), String.valueOf(cfg.adbSyncConfigParams.autoCreateDatabase));
    properties.put(ADB_SYNC_SKIP_LAST_COMMIT_TIME_SYNC.key(), String.valueOf(cfg.adbSyncConfigParams.skipLastCommitTimeSync));
    properties.put(ADB_SYNC_DROP_TABLE_BEFORE_CREATION.key(), String.valueOf(cfg.adbSyncConfigParams.dropTableBeforeCreation));

    return properties;
  }

  @Override
  public String toString() {
    return "AdbSyncConfig{"
        + "adbUser='" + adbSyncConfigParams.hiveSyncConfigParams.hiveUser + '\''
        + ", adbPass='" + adbSyncConfigParams.hiveSyncConfigParams.hivePass + '\''
        + ", jdbcUrl='" + adbSyncConfigParams.hiveSyncConfigParams.jdbcUrl + '\''
        + ", skipROSuffix=" + adbSyncConfigParams.hiveSyncConfigParams.skipROSuffix
        + ", skipRTSync=" + adbSyncConfigParams.skipRTSync
        + ", useHiveStylePartitioning=" + adbSyncConfigParams.useHiveStylePartitioning
        + ", supportTimestamp=" + adbSyncConfigParams.supportTimestamp
        + ", syncAsSparkDataSourceTable=" + adbSyncConfigParams.syncAsSparkDataSourceTable
        + ", tableProperties='" + adbSyncConfigParams.tableProperties + '\''
        + ", serdeProperties='" + adbSyncConfigParams.serdeProperties + '\''
        + ", sparkSchemaLengthThreshold=" + adbSyncConfigParams.sparkSchemaLengthThreshold
        + ", dbLocation='" + adbSyncConfigParams.dbLocation + '\''
        + ", autoCreateDatabase=" + adbSyncConfigParams.autoCreateDatabase
        + ", skipLastCommitTimeSync=" + adbSyncConfigParams.skipLastCommitTimeSync
        + ", dropTableBeforeCreation=" + adbSyncConfigParams.dropTableBeforeCreation
        + ", help=" + adbSyncConfigParams.help
        + ", databaseName='" + hoodieSyncConfigParams.databaseName + '\''
        + ", tableName='" + hoodieSyncConfigParams.tableName + '\''
        + ", basePath='" + hoodieSyncConfigParams.basePath + '\''
        + ", baseFileFormat='" + hoodieSyncConfigParams.baseFileFormat + '\''
        + ", partitionFields=" + hoodieSyncConfigParams.partitionFields
        + ", partitionValueExtractorClass='" + hoodieSyncConfigParams.partitionValueExtractorClass + '\''
        + ", assumeDatePartitioning=" + hoodieSyncConfigParams.assumeDatePartitioning
        + ", decodePartition=" + hoodieSyncConfigParams.decodePartition
        + ", useFileListingFromMetadata=" + hoodieSyncConfigParams.useFileListingFromMetadata
        + ", isConditionalSync=" + hoodieSyncConfigParams.isConditionalSync
        + ", sparkVersion='" + hoodieSyncConfigParams.sparkVersion + '\''
        + '}';
  }

  public static class AdbSyncConfigParams implements Serializable {
    @ParametersDelegate()
    public HiveSyncConfig.HiveSyncConfigParams hiveSyncConfigParams = new HiveSyncConfig.HiveSyncConfigParams();

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
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
    @Parameter(names = {"--hive-style-partitioning"}, description = "Whether use hive style partitioning, true if like the following style: field1=value1/field2=value2")
    public Boolean useHiveStylePartitioning;
    @Parameter(names = {"--skip-rt-sync"}, description = "Whether skip the rt table when syncing")
    public Boolean skipRTSync;
    @Parameter(names = {"--db-location"}, description = "Database location")
    public String dbLocation;
    @Parameter(names = {"--auto-create-database"}, description = "Whether auto create adb database")
    public Boolean autoCreateDatabase = true;
    @Parameter(names = {"--skip-last-commit-time-sync"}, description = "Whether skip last commit time syncing")
    public Boolean skipLastCommitTimeSync = false;
    @Parameter(names = {"--drop-table-before-creation"}, description = "Whether drop table before creation")
    public Boolean dropTableBeforeCreation = false;

    public AdbSyncConfigParams() {
    }
  }
}
