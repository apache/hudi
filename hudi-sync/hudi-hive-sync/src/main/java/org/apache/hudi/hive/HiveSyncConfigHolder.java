/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hive;

import org.apache.hudi.common.config.ConfigProperty;

public class HiveSyncConfigHolder {
  /*
   * NOTE: below are HIVE SYNC SPECIFIC CONFIGS which should be under HiveSyncConfig.java
   * But since DataSourceOptions.scala references constants to some of these, and HiveSyncConfig.java imports HiveConf,
   * it causes HiveConf ClassNotFound issue for loading DataSourceOptions.
   *
   * NOTE: DO NOT USE uppercase for the keys as they are internally lower-cased. Using upper-cases causes
   * unexpected issues with config getting reset
   */
  public static final ConfigProperty<String> HIVE_SYNC_ENABLED = ConfigProperty
      .key("hoodie.datasource.hive_sync.enable")
      .defaultValue("false")
      .withDocumentation("When set to true, register/sync the table to Apache Hive metastore.");
  public static final ConfigProperty<String> HIVE_USER = ConfigProperty
      .key("hoodie.datasource.hive_sync.username")
      .defaultValue("hive")
      .withDocumentation("hive user name to use");
  public static final ConfigProperty<String> HIVE_PASS = ConfigProperty
      .key("hoodie.datasource.hive_sync.password")
      .defaultValue("hive")
      .withDocumentation("hive password to use");
  public static final ConfigProperty<String> HIVE_URL = ConfigProperty
      .key("hoodie.datasource.hive_sync.jdbcurl")
      .defaultValue("jdbc:hive2://localhost:10000")
      .withDocumentation("Hive metastore url");
  public static final ConfigProperty<String> HIVE_USE_PRE_APACHE_INPUT_FORMAT = ConfigProperty
      .key("hoodie.datasource.hive_sync.use_pre_apache_input_format")
      .defaultValue("false")
      .withDocumentation("Flag to choose InputFormat under com.uber.hoodie package instead of org.apache.hudi package. "
          + "Use this when you are in the process of migrating from "
          + "com.uber.hoodie to org.apache.hudi. Stop using this after you migrated the table definition to org.apache.hudi input format");
  /**
   * @deprecated Use {@link #HIVE_SYNC_MODE} instead of this config from 0.9.0
   */
  @Deprecated
  public static final ConfigProperty<String> HIVE_USE_JDBC = ConfigProperty
      .key("hoodie.datasource.hive_sync.use_jdbc")
      .defaultValue("true")
      .deprecatedAfter("0.9.0")
      .withDocumentation("Use JDBC when hive synchronization is enabled");
  public static final ConfigProperty<String> METASTORE_URIS = ConfigProperty
      .key("hoodie.datasource.hive_sync.metastore.uris")
      .defaultValue("thrift://localhost:9083")
      .withDocumentation("Hive metastore url");
  public static final ConfigProperty<String> HIVE_AUTO_CREATE_DATABASE = ConfigProperty
      .key("hoodie.datasource.hive_sync.auto_create_database")
      .defaultValue("true")
      .withDocumentation("Auto create hive database if does not exists");
  public static final ConfigProperty<String> HIVE_IGNORE_EXCEPTIONS = ConfigProperty
      .key("hoodie.datasource.hive_sync.ignore_exceptions")
      .defaultValue("false")
      .withDocumentation("Ignore exceptions when syncing with Hive.");
  public static final ConfigProperty<String> HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE = ConfigProperty
      .key("hoodie.datasource.hive_sync.skip_ro_suffix")
      .defaultValue("false")
      .withDocumentation("Skip the _ro suffix for Read optimized table, when registering");
  public static final ConfigProperty<String> HIVE_SUPPORT_TIMESTAMP_TYPE = ConfigProperty
      .key("hoodie.datasource.hive_sync.support_timestamp")
      .defaultValue("false")
      .withDocumentation("‘INT64’ with original type TIMESTAMP_MICROS is converted to hive ‘timestamp’ type. "
          + "Disabled by default for backward compatibility.");
  public static final ConfigProperty<String> HIVE_TABLE_PROPERTIES = ConfigProperty
      .key("hoodie.datasource.hive_sync.table_properties")
      .noDefaultValue()
      .withDocumentation("Additional properties to store with table.");
  public static final ConfigProperty<String> HIVE_TABLE_SERDE_PROPERTIES = ConfigProperty
      .key("hoodie.datasource.hive_sync.serde_properties")
      .noDefaultValue()
      .withDocumentation("Serde properties to hive table.");
  public static final ConfigProperty<String> HIVE_SYNC_AS_DATA_SOURCE_TABLE = ConfigProperty
      .key("hoodie.datasource.hive_sync.sync_as_datasource")
      .defaultValue("true")
      .withDocumentation("");
  public static final ConfigProperty<Integer> HIVE_SYNC_SCHEMA_STRING_LENGTH_THRESHOLD = ConfigProperty
      .key("hoodie.datasource.hive_sync.schema_string_length_thresh")
      .defaultValue(4000)
      .withDocumentation("");
  // Create table as managed table
  public static final ConfigProperty<Boolean> HIVE_CREATE_MANAGED_TABLE = ConfigProperty
      .key("hoodie.datasource.hive_sync.create_managed_table")
      .defaultValue(false)
      .withDocumentation("Whether to sync the table as managed table.");
  public static final ConfigProperty<Integer> HIVE_BATCH_SYNC_PARTITION_NUM = ConfigProperty
      .key("hoodie.datasource.hive_sync.batch_num")
      .defaultValue(1000)
      .withDocumentation("The number of partitions one batch when synchronous partitions to hive.");
  public static final ConfigProperty<String> HIVE_SYNC_MODE = ConfigProperty
      .key("hoodie.datasource.hive_sync.mode")
      .noDefaultValue()
      .withDocumentation("Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.");
  public static final ConfigProperty<Boolean> HIVE_SYNC_BUCKET_SYNC = ConfigProperty
      .key("hoodie.datasource.hive_sync.bucket_sync")
      .defaultValue(false)
      .withDocumentation("Whether sync hive metastore bucket specification when using bucket index."
          + "The specification is 'CLUSTERED BY (trace_id) SORTED BY (trace_id ASC) INTO 65536 BUCKETS'");
  public static final ConfigProperty<String> HIVE_SYNC_BUCKET_SYNC_SPEC = ConfigProperty
      .key("hoodie.datasource.hive_sync.bucket_sync_spec")
      .defaultValue("")
      .withDocumentation("The hive metastore bucket specification when using bucket index."
          + "The specification is 'CLUSTERED BY (trace_id) SORTED BY (trace_id ASC) INTO 65536 BUCKETS'");
  public static final ConfigProperty<String> HIVE_SYNC_COMMENT = ConfigProperty
      .key("hoodie.datasource.hive_sync.sync_comment")
      .defaultValue("false")
      .withDocumentation("Whether to sync the table column comments while syncing the table.");
}
