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

package org.apache.hudi.hive;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Properties;

/**
 * Configs needed to sync data into the Hive Metastore.
 */
public class HiveSyncConfig extends HoodieSyncConfig {

  /*
   * Config constants below are retained here for BWC purpose.
   */
  public static final ConfigProperty<String> HIVE_SYNC_ENABLED = HiveSyncConfigHolder.HIVE_SYNC_ENABLED;
  public static final ConfigProperty<String> HIVE_USER = HiveSyncConfigHolder.HIVE_USER;
  public static final ConfigProperty<String> HIVE_PASS = HiveSyncConfigHolder.HIVE_PASS;
  public static final ConfigProperty<String> HIVE_URL = HiveSyncConfigHolder.HIVE_URL;
  public static final ConfigProperty<String> HIVE_USE_PRE_APACHE_INPUT_FORMAT = HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
  /**
   * @deprecated Use {@link #HIVE_SYNC_MODE} instead of this config from 0.9.0
   */
  @Deprecated
  public static final ConfigProperty<String> HIVE_USE_JDBC = HiveSyncConfigHolder.HIVE_USE_JDBC;
  public static final ConfigProperty<String> METASTORE_URIS = HiveSyncConfigHolder.METASTORE_URIS;
  public static final ConfigProperty<String> HIVE_AUTO_CREATE_DATABASE = HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE;
  public static final ConfigProperty<String> HIVE_IGNORE_EXCEPTIONS = HiveSyncConfigHolder.HIVE_IGNORE_EXCEPTIONS;
  public static final ConfigProperty<String> HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE = HiveSyncConfigHolder.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE;
  public static final ConfigProperty<String> HIVE_SUPPORT_TIMESTAMP_TYPE = HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
  public static final ConfigProperty<String> HIVE_TABLE_PROPERTIES = HiveSyncConfigHolder.HIVE_TABLE_PROPERTIES;
  public static final ConfigProperty<String> HIVE_TABLE_SERDE_PROPERTIES = HiveSyncConfigHolder.HIVE_TABLE_SERDE_PROPERTIES;
  public static final ConfigProperty<String> HIVE_SYNC_AS_DATA_SOURCE_TABLE = HiveSyncConfigHolder.HIVE_SYNC_AS_DATA_SOURCE_TABLE;
  public static final ConfigProperty<Integer> HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD = HiveSyncConfigHolder.HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD;
  public static final ConfigProperty<Boolean> HIVE_CREATE_MANAGED_TABLE = HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE;
  public static final ConfigProperty<Integer> HIVE_BATCH_SYNC_PARTITION_NUM = HiveSyncConfigHolder.HIVE_BATCH_SYNC_PARTITION_NUM;
  public static final ConfigProperty<String> HIVE_SYNC_MODE = HiveSyncConfigHolder.HIVE_SYNC_MODE;
  public static final ConfigProperty<Boolean> HIVE_SYNC_BUCKET_SYNC = HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC;
  public static final ConfigProperty<String> HIVE_SYNC_BUCKET_SYNC_SPEC = HiveSyncConfigHolder.HIVE_SYNC_BUCKET_SYNC_SPEC;
  public static final ConfigProperty<String> HIVE_SYNC_COMMENT = HiveSyncConfigHolder.HIVE_SYNC_COMMENT;

  public static String getBucketSpec(String bucketCols, int bucketNum) {
    return "CLUSTERED BY (" + bucketCols + " INTO " + bucketNum + " BUCKETS";
  }

  public HiveSyncConfig(Properties props) {
    super(props);
    validateParameters();
  }

  public HiveSyncConfig(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    HiveConf hiveConf = hadoopConf instanceof HiveConf
        ? (HiveConf) hadoopConf : new HiveConf(hadoopConf, HiveConf.class);
    // HiveConf needs to load fs conf to allow instantiation via AWSGlueClientFactory
    hiveConf.addResource(getHadoopFileSystem().getConf());
    setHadoopConf(hiveConf);
    validateParameters();
  }

  public HiveConf getHiveConf() {
    return (HiveConf) getHadoopConf();
  }

  public boolean useBucketSync() {
    return getBooleanOrDefault(HIVE_SYNC_BUCKET_SYNC);
  }

  public static class HiveSyncConfigParams {

    @ParametersDelegate()
    public final HoodieSyncConfigParams hoodieSyncConfigParams = new HoodieSyncConfigParams();

    @Parameter(names = {"--user"}, description = "Hive username")
    public String hiveUser;
    @Parameter(names = {"--pass"}, description = "Hive password")
    public String hivePass;
    @Parameter(names = {"--jdbc-url"}, description = "Hive jdbc connect url")
    public String jdbcUrl;
    @Parameter(names = {"--use-pre-apache-input-format"},
        description = "Use InputFormat under com.uber.hoodie package "
            + "instead of org.apache.hudi package. Use this when you are in the process of migrating from "
            + "com.uber.hoodie to org.apache.hudi. Stop using this after you migrated the table definition to "
            + "org.apache.hudi input format.")
    public Boolean usePreApacheInputFormat;
    @Deprecated
    @Parameter(names = {"--use-jdbc"}, description = "Hive jdbc connect url")
    public Boolean useJdbc;
    @Parameter(names = {"--metastore-uris"}, description = "Hive metastore uris")
    public String metastoreUris;
    @Parameter(names = {"--sync-mode"}, description = "Mode to choose for Hive ops. Valid values are hms,glue,jdbc and hiveql")
    public String syncMode;
    @Parameter(names = {"--auto-create-database"}, description = "Auto create hive database")
    public Boolean autoCreateDatabase;
    @Parameter(names = {"--ignore-exceptions"}, description = "Ignore hive exceptions")
    public Boolean ignoreExceptions;
    @Parameter(names = {"--skip-ro-suffix"}, description = "Skip the `_ro` suffix for Read optimized table, when registering")
    public Boolean skipROSuffix;
    @Parameter(names = {"--table-properties"}, description = "Table properties to hive table")
    public String tableProperties;
    @Parameter(names = {"--serde-properties"}, description = "Serde properties to hive table")
    public String serdeProperties;
    @Parameter(names = {"--support-timestamp"}, description = "'INT64' with original type TIMESTAMP_MICROS is converted to hive 'timestamp' type."
        + "Disabled by default for backward compatibility.")
    public Boolean supportTimestamp;
    @Parameter(names = {"--managed-table"}, description = "Create a managed table")
    public Boolean createManagedTable;
    @Parameter(names = {"--batch-sync-num"}, description = "The number of partitions one batch when synchronous partitions to hive")
    public Integer batchSyncNum;
    @Parameter(names = {"--spark-datasource"}, description = "Whether sync this table as spark data source table.")
    public Boolean syncAsSparkDataSourceTable;
    @Parameter(names = {"--spark-schema-length-threshold"}, description = "The maximum length allowed in a single cell when storing additional schema information in Hive's metastore.")
    public Integer sparkSchemaLengthThreshold;
    @Parameter(names = {"--bucket-sync"}, description = "use bucket sync")
    public Boolean bucketSync;
    @Parameter(names = {"--bucket-spec"}, description = "bucket spec stored in metastore")
    public String bucketSpec;
    @Parameter(names = {"--sync-comment"}, description = "synchronize table comments to hive")
    public Boolean syncComment;
    @Parameter(names = {"--with-operation-field"}, description = "Whether to include the '_hoodie_operation' field in the metadata fields")
    public Boolean withOperationField; // TODO remove this as it's not used

    public boolean isHelp() {
      return hoodieSyncConfigParams.isHelp();
    }

    public TypedProperties toProps() {
      final TypedProperties props = hoodieSyncConfigParams.toProps();
      props.setPropertyIfNonNull(HIVE_USER.key(), hiveUser);
      props.setPropertyIfNonNull(HIVE_PASS.key(), hivePass);
      props.setPropertyIfNonNull(HIVE_URL.key(), jdbcUrl);
      props.setPropertyIfNonNull(HIVE_USE_PRE_APACHE_INPUT_FORMAT.key(), usePreApacheInputFormat);
      props.setPropertyIfNonNull(HIVE_USE_JDBC.key(), useJdbc);
      props.setPropertyIfNonNull(HIVE_SYNC_MODE.key(), syncMode);
      props.setPropertyIfNonNull(METASTORE_URIS.key(), metastoreUris);
      props.setPropertyIfNonNull(HIVE_AUTO_CREATE_DATABASE.key(), autoCreateDatabase);
      props.setPropertyIfNonNull(HIVE_IGNORE_EXCEPTIONS.key(), ignoreExceptions);
      props.setPropertyIfNonNull(HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.key(), skipROSuffix);
      props.setPropertyIfNonNull(HIVE_SUPPORT_TIMESTAMP_TYPE.key(), supportTimestamp);
      props.setPropertyIfNonNull(HIVE_TABLE_PROPERTIES.key(), tableProperties);
      props.setPropertyIfNonNull(HIVE_TABLE_SERDE_PROPERTIES.key(), serdeProperties);
      props.setPropertyIfNonNull(HIVE_SYNC_AS_DATA_SOURCE_TABLE.key(), syncAsSparkDataSourceTable);
      props.setPropertyIfNonNull(HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD.key(), sparkSchemaLengthThreshold);
      props.setPropertyIfNonNull(HIVE_CREATE_MANAGED_TABLE.key(), createManagedTable);
      props.setPropertyIfNonNull(HIVE_BATCH_SYNC_PARTITION_NUM.key(), batchSyncNum);
      props.setPropertyIfNonNull(HIVE_SYNC_BUCKET_SYNC.key(), bucketSync);
      props.setPropertyIfNonNull(HIVE_SYNC_BUCKET_SYNC_SPEC.key(), bucketSpec);
      props.setPropertyIfNonNull(HIVE_SYNC_COMMENT.key(), syncComment);
      return props;
    }
  }

  public void validateParameters() {
    ValidationUtils.checkArgument(getIntOrDefault(HIVE_BATCH_SYNC_PARTITION_NUM) > 0, "batch-sync-num for sync hive table must be greater than 0, pls check your parameter");
  }
}
