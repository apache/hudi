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

package org.apache.hudi.snowflake.sync;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.common.util.ManifestFileWriter;

import com.beust.jcommander.JCommander;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_STORAGE_INTEGRATION;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_SYNC_BASE_PATH;
import static org.apache.hudi.snowflake.sync.SnowflakeSyncConfig.SNOWFLAKE_SYNC_TABLE_NAME;

/**
 * Tool to sync a hoodie table with a snowflake table. Either use it as an api
 * SnowflakeSyncTool.syncHoodieTable(SnowflakeSyncConfig) or as a command line java -cp hoodie-hive.jar SnowflakeSyncTool [args]
 * <p>
 * This utility will get the schema from the latest commit and will sync snowflake table schema.
 * <p>
 * Example:
 * snoflake_profile.properties file:
 * URL = https://el48293.us-central1.gcp.snowflakecomputing.com:443
 * USER = hudidemo
 * PRIVATE_KEY_FILE = /Users/username/.ssh/rsa_key.p8
 * ROLE = ACCOUNTADMIN
 * WAREHOUSE = COMPUTE_WH
 * DB = hudi
 * SCHEMA = dwh:113
 *
 * command:
 * java -cp hudi-spark-bundle_2.12-0.12.0-SNAPSHOT.jar:hudi-snowflake-bundle-0.12.0-SNAPSHOT-jar-with-dependencies.jar:gcs-connector-hadoop2-latest.jar
 *   org.apache.hudi.snowflake.sync.SnowflakeSyncTool
 *   --properties-file snowflake_profile.properties
 *   --base-path gs://hudi-demo/stock_ticks_cow
 *   --table-name stock_ticks_cow
 *   --storage-integration hudi_demo_int
 *   --partitioned-by "date"
 *   --partition-extract-expr "\"date\" date as to_date(substr(metadata\$filename, 22, 10), 'YYYY-MM-DD')
 * <p>
 * Use these command line options, to enable along with delta streamer execution:
 *   --enable-sync
 *   --sync-tool-classes org.apache.hudi.snowflake.sync.SnowflakeSyncTool
 *
 * @Experimental
 */
public class SnowflakeSyncTool extends HoodieSyncTool {

  private static final Logger LOG = LogManager.getLogger(SnowflakeSyncTool.class);
  public final SnowflakeSyncConfig config;
  public final String tableName;
  public final String stageName;
  public final String manifestTableName;
  public final String versionsTableName;
  public final String snapshotViewName;

  public SnowflakeSyncTool(Properties props) {
    super(props);
    this.config = new SnowflakeSyncConfig(props);
    this.tableName = config.getString(SNOWFLAKE_SYNC_TABLE_NAME);
    stageName = tableName + "_stage";
    manifestTableName = tableName + "_manifest";
    versionsTableName = tableName + "_versions";
    snapshotViewName = tableName;
  }

  public static void main(String[] args) {
    final SnowflakeSyncConfig.SnowflakeSyncConfigParams params = new SnowflakeSyncConfig.SnowflakeSyncConfigParams();
    JCommander cmd = JCommander.newBuilder().addObject(params).build();
    cmd.parse(args);
    if (params.isHelp()) {
      cmd.usage();
      System.exit(0);
    }
    new SnowflakeSyncTool(params.toProps()).syncHoodieTable();
  }

  @Override
  public void syncHoodieTable() {
    try (HoodieSnowflakeSyncClient snowSyncClient = new HoodieSnowflakeSyncClient(config)) {
      switch (snowSyncClient.getTableType()) {
        case COPY_ON_WRITE:
          syncCoWTable(snowSyncClient);
          break;
        case MERGE_ON_READ:
        default:
          throw new UnsupportedOperationException(snowSyncClient.getTableType() + " table type is not supported yet.");
      }
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("Got runtime exception when snowflake syncing " + tableName, e);
    }
  }

  private void syncCoWTable(HoodieSnowflakeSyncClient snowSyncClient) {
    ValidationUtils.checkState(snowSyncClient.getTableType() == HoodieTableType.COPY_ON_WRITE);
    LOG.info("Sync hoodie table " + snapshotViewName + " at base path " + snowSyncClient.getBasePath());

    ManifestFileWriter manifestFileWriter = ManifestFileWriter.builder()
        .setConf(config.getHadoopConf())
        .setBasePath(config.getString(SNOWFLAKE_SYNC_SYNC_BASE_PATH))
        .setUseFileListingFromMetadata(false)
        .setAssumeDatePartitioning(false)
        .build();
    manifestFileWriter.writeManifestFile();

    snowSyncClient.createStage(stageName,
        config.getString(SNOWFLAKE_SYNC_SYNC_BASE_PATH),
        config.getString(SNOWFLAKE_SYNC_STORAGE_INTEGRATION));
    LOG.info("External temporary stage creation complete for " + stageName);
    snowSyncClient.createManifestTable(stageName, manifestTableName);
    LOG.info("Manifest table creation complete for " + manifestTableName);
    snowSyncClient.createVersionsTable(stageName, versionsTableName,
        config.getString(SNOWFLAKE_SYNC_PARTITION_FIELDS),
        config.getString(SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION));
    LOG.info("Versions table creation complete for " + versionsTableName);
    snowSyncClient.createSnapshotView(snapshotViewName, versionsTableName, manifestTableName);
    LOG.info("Snapshot view creation complete for " + snapshotViewName);
    LOG.info("Snowflake sync complete for " + snapshotViewName);
  }
}
