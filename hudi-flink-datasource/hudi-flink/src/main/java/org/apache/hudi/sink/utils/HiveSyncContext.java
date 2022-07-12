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

package org.apache.hudi.sink.utils;

import org.apache.hudi.aws.sync.AWSGlueCatalogSyncTool;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.table.format.FilePathUtils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Properties;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_AUTO_CREATE_DATABASE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_IGNORE_EXCEPTIONS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_MODE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_TABLE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_TABLE_SERDE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
import static org.apache.hudi.hive.HiveSyncConfigHolder.METASTORE_URIS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_ASSUME_DATE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DECODE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_USE_FILE_LISTING_FROM_METADATA;

/**
 * Hive synchronization context.
 *
 * <p>Use this context to create the {@link HiveSyncTool} for synchronization.
 */
public class HiveSyncContext {

  private final Properties props;
  private final HiveConf hiveConf;
  private final String catalogSyncToolClassName;

  private HiveSyncContext(Properties props, HiveConf hiveConf, String catalogSyncToolClassName) {
    this.props = props;
    this.hiveConf = hiveConf;
    this.catalogSyncToolClassName = catalogSyncToolClassName;
  }

  public HiveSyncTool hiveSyncTool() {
    if (catalogSyncToolClassName.equalsIgnoreCase(AWSGlueCatalogSyncTool.class.getSimpleName())) {
      return new AWSGlueCatalogSyncTool(props, this.hiveConf);
    }
    return new HiveSyncTool(props, hiveConf);
  }

  public static HiveSyncContext create(Configuration conf, SerializableConfiguration serConf) {
    Properties props = buildSyncConfig(conf);
    String catalogSyncToolClassName = conf.getString(FlinkOptions.CATALOG_SYNC_TOOL_CLASS_NAME);
    org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(serConf.get());
    hiveConf.addResource(hadoopConf);
    return new HiveSyncContext(props, hiveConf, catalogSyncToolClassName);
  }

  @VisibleForTesting
  public static Properties buildSyncConfig(Configuration conf) {
    TypedProperties props = new TypedProperties();
    props.setPropertyIfNonNull(META_SYNC_BASE_PATH.key(), conf.getString(FlinkOptions.PATH));
    props.setPropertyIfNonNull(META_SYNC_BASE_FILE_FORMAT.key(), conf.getString(FlinkOptions.HIVE_SYNC_FILE_FORMAT));
    props.setPropertyIfNonNull(HIVE_USE_PRE_APACHE_INPUT_FORMAT.key(), "false");
    props.setPropertyIfNonNull(META_SYNC_DATABASE_NAME.key(), conf.getString(FlinkOptions.HIVE_SYNC_DB));
    props.setPropertyIfNonNull(META_SYNC_TABLE_NAME.key(), conf.getString(FlinkOptions.HIVE_SYNC_TABLE));
    props.setPropertyIfNonNull(HIVE_SYNC_MODE.key(), conf.getString(FlinkOptions.HIVE_SYNC_MODE));
    props.setPropertyIfNonNull(HIVE_USER.key(), conf.getString(FlinkOptions.HIVE_SYNC_USERNAME));
    props.setPropertyIfNonNull(HIVE_PASS.key(), conf.getString(FlinkOptions.HIVE_SYNC_PASSWORD));
    props.setPropertyIfNonNull(HIVE_URL.key(), conf.getString(FlinkOptions.HIVE_SYNC_JDBC_URL));
    props.setPropertyIfNonNull(METASTORE_URIS.key(), conf.getString(FlinkOptions.HIVE_SYNC_METASTORE_URIS));
    props.setPropertyIfNonNull(HIVE_TABLE_PROPERTIES.key(), conf.getString(FlinkOptions.HIVE_SYNC_TABLE_PROPERTIES));
    props.setPropertyIfNonNull(HIVE_TABLE_SERDE_PROPERTIES.key(), conf.getString(FlinkOptions.HIVE_SYNC_TABLE_SERDE_PROPERTIES));
    props.setPropertyIfNonNull(META_SYNC_PARTITION_FIELDS.key(), String.join(",", FilePathUtils.extractHivePartitionFields(conf)));
    props.setPropertyIfNonNull(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), conf.getString(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME));
    props.setPropertyIfNonNull(META_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), String.valueOf(conf.getBoolean(FlinkOptions.METADATA_ENABLED)));
    props.setPropertyIfNonNull(HIVE_IGNORE_EXCEPTIONS.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_IGNORE_EXCEPTIONS)));
    props.setPropertyIfNonNull(HIVE_SUPPORT_TIMESTAMP_TYPE.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_SUPPORT_TIMESTAMP)));
    props.setPropertyIfNonNull(HIVE_AUTO_CREATE_DATABASE.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_AUTO_CREATE_DB)));
    props.setPropertyIfNonNull(META_SYNC_DECODE_PARTITION.key(), String.valueOf(conf.getBoolean(FlinkOptions.URL_ENCODE_PARTITIONING)));
    props.setPropertyIfNonNull(HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_SKIP_RO_SUFFIX)));
    props.setPropertyIfNonNull(META_SYNC_ASSUME_DATE_PARTITION.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_ASSUME_DATE_PARTITION)));
    return props;
  }
}
