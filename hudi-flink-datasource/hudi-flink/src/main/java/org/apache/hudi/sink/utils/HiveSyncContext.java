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
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.table.format.FilePathUtils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Properties;

import static org.apache.hudi.hive.HiveSyncConfig.HIVE_AUTO_CREATE_DATABASE;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_IGNORE_EXCEPTIONS;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_MODE;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_TABLE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_TABLE_SERDE_PROPERTIES;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_USER;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_USE_JDBC;
import static org.apache.hudi.hive.HiveSyncConfig.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
import static org.apache.hudi.hive.HiveSyncConfig.METASTORE_URIS;
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

  private HiveSyncContext(Properties props, HiveConf hiveConf) {
    this.props = props;
    this.hiveConf = hiveConf;
  }

  public HiveSyncTool hiveSyncTool() {
    HiveSyncMode syncMode = HiveSyncMode.of(props.getProperty(HIVE_SYNC_MODE.key()));
    if (syncMode == HiveSyncMode.GLUE) {
      return new AWSGlueCatalogSyncTool(props, hiveConf);
    }
    return new HiveSyncTool(props, hiveConf);
  }

  public static HiveSyncContext create(Configuration conf, SerializableConfiguration serConf) {
    Properties props = buildSyncConfig(conf);
    org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(serConf.get());
    hiveConf.addResource(hadoopConf);
    return new HiveSyncContext(props, hiveConf);
  }

  @VisibleForTesting
  public static Properties buildSyncConfig(Configuration conf) {
    Properties props = new Properties();
    props.setProperty(META_SYNC_BASE_PATH.key(), conf.getString(FlinkOptions.PATH, ""));
    props.setProperty(META_SYNC_BASE_FILE_FORMAT.key(), conf.getString(FlinkOptions.HIVE_SYNC_FILE_FORMAT));
    props.setProperty(HIVE_USE_PRE_APACHE_INPUT_FORMAT.key(), "false");
    props.setProperty(META_SYNC_DATABASE_NAME.key(), conf.getString(FlinkOptions.HIVE_SYNC_DB));
    props.setProperty(META_SYNC_TABLE_NAME.key(), conf.getString(FlinkOptions.HIVE_SYNC_TABLE));
    props.setProperty(HIVE_SYNC_MODE.key(), conf.getString(FlinkOptions.HIVE_SYNC_MODE));
    props.setProperty(HIVE_USER.key(), conf.getString(FlinkOptions.HIVE_SYNC_USERNAME));
    props.setProperty(HIVE_PASS.key(), conf.getString(FlinkOptions.HIVE_SYNC_PASSWORD));
    props.setProperty(HIVE_URL.key(), conf.getString(FlinkOptions.HIVE_SYNC_JDBC_URL));
    props.setProperty(METASTORE_URIS.key(), conf.getString(FlinkOptions.HIVE_SYNC_METASTORE_URIS));
    props.setProperty(HIVE_TABLE_PROPERTIES.key(), conf.getString(FlinkOptions.HIVE_SYNC_TABLE_PROPERTIES, ""));
    props.setProperty(HIVE_TABLE_SERDE_PROPERTIES.key(), conf.getString(FlinkOptions.HIVE_SYNC_TABLE_SERDE_PROPERTIES, ""));
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), String.join(",", FilePathUtils.extractHivePartitionFields(conf)));
    props.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), conf.getString(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME));
    props.setProperty(HIVE_USE_JDBC.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_USE_JDBC)));
    props.setProperty(META_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), String.valueOf(conf.getBoolean(FlinkOptions.METADATA_ENABLED)));
    props.setProperty(HIVE_IGNORE_EXCEPTIONS.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_IGNORE_EXCEPTIONS)));
    props.setProperty(HIVE_SUPPORT_TIMESTAMP_TYPE.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_SUPPORT_TIMESTAMP)));
    props.setProperty(HIVE_AUTO_CREATE_DATABASE.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_AUTO_CREATE_DB)));
    props.setProperty(META_SYNC_DECODE_PARTITION.key(), String.valueOf(conf.getBoolean(FlinkOptions.URL_ENCODE_PARTITIONING)));
    props.setProperty(HIVE_SKIP_RO_SUFFIX_FOR_READ_OPTIMIZED_TABLE.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_SKIP_RO_SUFFIX)));
    props.setProperty(META_SYNC_ASSUME_DATE_PARTITION.key(), String.valueOf(conf.getBoolean(FlinkOptions.HIVE_SYNC_ASSUME_DATE_PARTITION)));
    return props;
  }
}
