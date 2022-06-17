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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.table.format.FilePathUtils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Arrays;

import static org.apache.hudi.hive.HiveSyncConfig.HIVE_SYNC_MODE;

/**
 * Hive synchronization context.
 *
 * <p>Use this context to create the {@link HiveSyncTool} for synchronization.
 */
public class HiveSyncContext {
  private final HiveSyncConfig syncConfig;

  private HiveSyncContext(HiveSyncConfig syncConfig) {
    this.syncConfig = syncConfig;
  }

  public HiveSyncTool hiveSyncTool() {
    HiveSyncMode syncMode = HiveSyncMode.of(syncConfig.getString(HIVE_SYNC_MODE));
    if (syncMode == HiveSyncMode.GLUE) {
      return new AWSGlueCatalogSyncTool(this.syncConfig);
    }
    return new HiveSyncTool(this.syncConfig);
  }

  public static HiveSyncContext create(Configuration conf, SerializableConfiguration serConf) {
    HiveSyncConfig syncConfig = buildSyncConfig(conf);
    org.apache.hadoop.conf.Configuration hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    String path = conf.getString(FlinkOptions.PATH);
    FileSystem fs = FSUtils.getFs(path, hadoopConf);
    HiveConf hiveConf = new HiveConf();
    if (!FlinkOptions.isDefaultValueDefined(conf, FlinkOptions.HIVE_SYNC_METASTORE_URIS)) {
      hadoopConf.set(HiveConf.ConfVars.METASTOREURIS.varname, conf.getString(FlinkOptions.HIVE_SYNC_METASTORE_URIS));
    }
    hiveConf.addResource(serConf.get());
    hiveConf.addResource(hadoopConf);
    syncConfig.setHadoopConf(hiveConf);
    return new HiveSyncContext(syncConfig);
  }

  @VisibleForTesting
  public static HiveSyncConfig buildSyncConfig(Configuration conf) {
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.hoodieSyncConfigParams.basePath = conf.getString(FlinkOptions.PATH);
    hiveSyncConfig.hoodieSyncConfigParams.baseFileFormat = conf.getString(FlinkOptions.HIVE_SYNC_FILE_FORMAT);
    hiveSyncConfig.hiveSyncConfigParams.usePreApacheInputFormat = false;
    hiveSyncConfig.hoodieSyncConfigParams.databaseName = conf.getString(FlinkOptions.HIVE_SYNC_DB);
    hiveSyncConfig.hoodieSyncConfigParams.tableName = conf.getString(FlinkOptions.HIVE_SYNC_TABLE);
    hiveSyncConfig.hiveSyncConfigParams.syncMode = conf.getString(FlinkOptions.HIVE_SYNC_MODE);
    hiveSyncConfig.hiveSyncConfigParams.hiveUser = conf.getString(FlinkOptions.HIVE_SYNC_USERNAME);
    hiveSyncConfig.hiveSyncConfigParams.hivePass = conf.getString(FlinkOptions.HIVE_SYNC_PASSWORD);
    hiveSyncConfig.hiveSyncConfigParams.tableProperties = conf.getString(FlinkOptions.HIVE_SYNC_TABLE_PROPERTIES);
    hiveSyncConfig.hiveSyncConfigParams.serdeProperties = conf.getString(FlinkOptions.HIVE_SYNC_TABLE_SERDE_PROPERTIES);
    hiveSyncConfig.hiveSyncConfigParams.jdbcUrl = conf.getString(FlinkOptions.HIVE_SYNC_JDBC_URL);
    hiveSyncConfig.hoodieSyncConfigParams.partitionFields = Arrays.asList(FilePathUtils.extractHivePartitionFields(conf));
    hiveSyncConfig.hoodieSyncConfigParams.partitionValueExtractorClass = conf.getString(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS_NAME);
    hiveSyncConfig.hiveSyncConfigParams.useJdbc = conf.getBoolean(FlinkOptions.HIVE_SYNC_USE_JDBC);
    hiveSyncConfig.hoodieSyncConfigParams.useFileListingFromMetadata = conf.getBoolean(FlinkOptions.METADATA_ENABLED);
    hiveSyncConfig.hiveSyncConfigParams.ignoreExceptions = conf.getBoolean(FlinkOptions.HIVE_SYNC_IGNORE_EXCEPTIONS);
    hiveSyncConfig.hiveSyncConfigParams.supportTimestamp = conf.getBoolean(FlinkOptions.HIVE_SYNC_SUPPORT_TIMESTAMP);
    hiveSyncConfig.hiveSyncConfigParams.autoCreateDatabase = conf.getBoolean(FlinkOptions.HIVE_SYNC_AUTO_CREATE_DB);
    hiveSyncConfig.hoodieSyncConfigParams.decodePartition = conf.getBoolean(FlinkOptions.URL_ENCODE_PARTITIONING);
    hiveSyncConfig.hiveSyncConfigParams.skipROSuffix = conf.getBoolean(FlinkOptions.HIVE_SYNC_SKIP_RO_SUFFIX);
    hiveSyncConfig.hoodieSyncConfigParams.assumeDatePartitioning = conf.getBoolean(FlinkOptions.HIVE_SYNC_ASSUME_DATE_PARTITION);
    hiveSyncConfig.hiveSyncConfigParams.withOperationField = conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED);
    return hiveSyncConfig;
  }
}
