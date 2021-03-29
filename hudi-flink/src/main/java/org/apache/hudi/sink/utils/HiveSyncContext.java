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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Hive synchronization context.
 *
 * <p>Use this context to create the {@link HiveSyncTool} for synchronization.
 */
public class HiveSyncContext {
  private final HiveSyncConfig syncConfig;
  private final HiveConf hiveConf;
  private final FileSystem fs;

  private HiveSyncContext(HiveSyncConfig syncConfig, HiveConf hiveConf, FileSystem fs) {
    this.syncConfig = syncConfig;
    this.hiveConf = hiveConf;
    this.fs = fs;
  }

  public HiveSyncTool hiveSyncTool() {
    return new HiveSyncTool(this.syncConfig, this.hiveConf, this.fs);
  }

  public static HiveSyncContext create(Configuration conf) {
    HiveSyncConfig syncConfig = buildSyncConfig(conf);
    org.apache.hadoop.conf.Configuration hadoopConf = StreamerUtil.getHadoopConf();
    String path = conf.getString(FlinkOptions.PATH);
    FileSystem fs = FSUtils.getFs(path, hadoopConf);
    HiveConf hiveConf = new HiveConf();
    hiveConf.addResource(fs.getConf());
    return new HiveSyncContext(syncConfig, hiveConf, fs);
  }

  private static HiveSyncConfig buildSyncConfig(Configuration conf) {
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.basePath = conf.getString(FlinkOptions.PATH);
    hiveSyncConfig.baseFileFormat = conf.getString(FlinkOptions.HIVE_SYNC_FILE_FORMAT);
    hiveSyncConfig.usePreApacheInputFormat = false;
    hiveSyncConfig.databaseName = conf.getString(FlinkOptions.HIVE_SYNC_DB);
    hiveSyncConfig.tableName = conf.getString(FlinkOptions.HIVE_SYNC_TABLE);
    hiveSyncConfig.hiveUser = conf.getString(FlinkOptions.HIVE_SYNC_USERNAME);
    hiveSyncConfig.hivePass = conf.getString(FlinkOptions.HIVE_SYNC_PASSWORD);
    hiveSyncConfig.jdbcUrl = conf.getString(FlinkOptions.HIVE_SYNC_JDBC_URL);
    hiveSyncConfig.partitionFields = Arrays.stream(conf.getString(FlinkOptions.HIVE_SYNC_PARTITION_FIELDS)
        .split(",")).map(String::trim).collect(Collectors.toList());
    hiveSyncConfig.partitionValueExtractorClass = conf.getString(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS);
    hiveSyncConfig.useJdbc = conf.getBoolean(FlinkOptions.HIVE_SYNC_USE_JDBC);
    // needs to support metadata table for flink
    hiveSyncConfig.useFileListingFromMetadata = false;
    hiveSyncConfig.verifyMetadataFileListing = false;
    hiveSyncConfig.ignoreExceptions = conf.getBoolean(FlinkOptions.HIVE_SYNC_IGNORE_EXCEPTIONS);
    hiveSyncConfig.supportTimestamp = conf.getBoolean(FlinkOptions.HIVE_SYNC_SUPPORT_TIMESTAMP);
    hiveSyncConfig.autoCreateDatabase = conf.getBoolean(FlinkOptions.HIVE_SYNC_AUTO_CREATE_DB);
    hiveSyncConfig.decodePartition = conf.getBoolean(FlinkOptions.PARTITION_PATH_URL_ENCODE);
    hiveSyncConfig.skipROSuffix = conf.getBoolean(FlinkOptions.HIVE_SYNC_SKIP_RO_SUFFIX);
    hiveSyncConfig.assumeDatePartitioning = conf.getBoolean(FlinkOptions.HIVE_SYNC_ASSUME_DATE_PARTITION);
    return hiveSyncConfig;
  }
}
