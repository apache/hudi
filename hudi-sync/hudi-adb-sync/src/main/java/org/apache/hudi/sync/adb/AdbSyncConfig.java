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
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.hive.HiveSyncConfig;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * Configs needed to sync data into Alibaba Cloud AnalyticDB(ADB).
 */
public class AdbSyncConfig extends HiveSyncConfig {

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

  public AdbSyncConfig(Properties props) {
    super(props);
  }

  @Override
  public String getAbsoluteBasePath() {
    return generateAbsolutePathStr(new Path(getString(META_SYNC_BASE_PATH)));
  }

  public String getDatabasePath() {
    Path basePath = new Path(getString(META_SYNC_BASE_PATH));
    Path dbLocationPath;
    String dbLocation = getString(ADB_SYNC_DB_LOCATION);
    if (StringUtils.isNullOrEmpty(dbLocation)) {
      if (basePath.isRoot()) {
        dbLocationPath = basePath;
      } else {
        dbLocationPath = basePath.getParent();
      }
    } else {
      dbLocationPath = new Path(dbLocation);
    }
    return generateAbsolutePathStr(dbLocationPath);
  }

  public String generateAbsolutePathStr(Path path) {
    String absolutePathStr = path.toString();
    if (path.toUri().getScheme() == null) {
      absolutePathStr = getDefaultFs() + absolutePathStr;
    }
    return absolutePathStr.endsWith("/") ? absolutePathStr : absolutePathStr + "/";
  }

  public String getDefaultFs() {
    return getHadoopConf().get("fs.defaultFS");
  }

  public static class AdbSyncConfigParams {

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

    public boolean isHelp() {
      return hiveSyncConfigParams.isHelp();
    }

    public TypedProperties toProps() {
      final TypedProperties props = hiveSyncConfigParams.toProps();
      props.setPropertyIfNonNull(META_SYNC_DATABASE_NAME.key(), hiveSyncConfigParams.hoodieSyncConfigParams.databaseName);
      props.setPropertyIfNonNull(META_SYNC_TABLE_NAME.key(), hiveSyncConfigParams.hoodieSyncConfigParams.tableName);
      props.setPropertyIfNonNull(ADB_SYNC_USER.key(), hiveSyncConfigParams.hiveUser);
      props.setPropertyIfNonNull(ADB_SYNC_PASS.key(), hiveSyncConfigParams.hivePass);
      props.setPropertyIfNonNull(ADB_SYNC_JDBC_URL.key(), hiveSyncConfigParams.jdbcUrl);
      props.setPropertyIfNonNull(META_SYNC_BASE_PATH.key(), hiveSyncConfigParams.hoodieSyncConfigParams.basePath);
      props.setPropertyIfNonNull(META_SYNC_PARTITION_FIELDS.key(), String.join(",", hiveSyncConfigParams.hoodieSyncConfigParams.partitionFields));
      props.setPropertyIfNonNull(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), hiveSyncConfigParams.hoodieSyncConfigParams.partitionValueExtractorClass);
      props.setPropertyIfNonNull(META_SYNC_ASSUME_DATE_PARTITION.key(), String.valueOf(hiveSyncConfigParams.hoodieSyncConfigParams.assumeDatePartitioning));
      props.setPropertyIfNonNull(ADB_SYNC_SKIP_RO_SUFFIX.key(), String.valueOf(hiveSyncConfigParams.skipROSuffix));
      props.setPropertyIfNonNull(ADB_SYNC_SKIP_RT_SYNC.key(), String.valueOf(skipRTSync));
      props.setPropertyIfNonNull(ADB_SYNC_USE_HIVE_STYLE_PARTITIONING.key(), String.valueOf(useHiveStylePartitioning));
      props.setPropertyIfNonNull(META_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), String.valueOf(hiveSyncConfigParams.hoodieSyncConfigParams.useFileListingFromMetadata));
      props.setPropertyIfNonNull(ADB_SYNC_SUPPORT_TIMESTAMP.key(), String.valueOf(supportTimestamp));
      props.setPropertyIfNonNull(ADB_SYNC_TABLE_PROPERTIES.key(), tableProperties);
      props.setPropertyIfNonNull(ADB_SYNC_SERDE_PROPERTIES.key(), serdeProperties);
      props.setPropertyIfNonNull(ADB_SYNC_SYNC_AS_SPARK_DATA_SOURCE_TABLE.key(), String.valueOf(syncAsSparkDataSourceTable));
      props.setPropertyIfNonNull(ADB_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.key(), String.valueOf(sparkSchemaLengthThreshold));
      props.setPropertyIfNonNull(META_SYNC_SPARK_VERSION.key(), hiveSyncConfigParams.hoodieSyncConfigParams.sparkVersion);
      props.setPropertyIfNonNull(ADB_SYNC_DB_LOCATION.key(), dbLocation);
      props.setPropertyIfNonNull(ADB_SYNC_AUTO_CREATE_DATABASE.key(), String.valueOf(autoCreateDatabase));
      props.setPropertyIfNonNull(ADB_SYNC_SKIP_LAST_COMMIT_TIME_SYNC.key(), String.valueOf(skipLastCommitTimeSync));
      props.setPropertyIfNonNull(ADB_SYNC_DROP_TABLE_BEFORE_CREATION.key(), String.valueOf(dropTableBeforeCreation));
      return props;
    }
  }
}
