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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.sync.common.AbstractSyncTool;
import org.apache.hudi.sync.common.util.ManifestFileWriter;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Tool to sync a hoodie table with a snowflake table. Either use it as an api
 * SnowflakeSyncTool.syncHoodieTable(SnowflakeSyncConfig) or as a command line java -cp hoodie-hive.jar SnowflakeSyncTool [args]
 * <p>
 * This utility will get the schema from the latest commit and will sync snowflake table schema.
 *
 * @Experimental
 */
public class SnowflakeSyncTool extends AbstractSyncTool {

  private static final Logger LOG = LogManager.getLogger(SnowflakeSyncTool.class);

  public final SnowflakeSyncConfig cfg;
  public final String stageName;
  public final String manifestTableName;
  public final String versionsTableName;
  public final String snapshotViewName;

  public SnowflakeSyncTool(TypedProperties properties, Configuration conf, FileSystem fs) {
    super(properties, conf, fs);
    cfg = SnowflakeSyncConfig.fromProps(properties);
    stageName = cfg.tableName + "_stage";
    manifestTableName = cfg.tableName + "_manifest";
    versionsTableName = cfg.tableName + "_versions";
    snapshotViewName = cfg.tableName;
  }

  @Override
  public void syncHoodieTable() {
    try (HoodieSnowflakeSyncClient snowSyncClient = new HoodieSnowflakeSyncClient(SnowflakeSyncConfig.fromProps(props), fs)) {
      switch (snowSyncClient.getTableType()) {
        case COPY_ON_WRITE:
          syncCoWTable(snowSyncClient);
          break;
        case MERGE_ON_READ:
        default:
          throw new UnsupportedOperationException(snowSyncClient.getTableType() + " table type is not supported yet.");
      }
    } catch (Exception e) {
      throw new HoodieSnowflakeSyncException("Got runtime exception when snowflake syncing " + cfg.tableName, e);
    }
  }

  private void syncCoWTable(HoodieSnowflakeSyncClient snowSyncClient) {
    ValidationUtils.checkState(snowSyncClient.getTableType() == HoodieTableType.COPY_ON_WRITE);
    LOG.info("Sync hoodie table " + snapshotViewName + " at base path " + snowSyncClient.getBasePath());

    ManifestFileWriter manifestFileWriter = ManifestFileWriter.builder()
        .setConf(conf)
        .setBasePath(cfg.basePath)
        .setUseFileListingFromMetadata(cfg.useFileListingFromMetadata)
        .setAssumeDatePartitioning(cfg.assumeDatePartitioning)
        .build();
    manifestFileWriter.writeManifestFile();

    snowSyncClient.createStage(stageName, cfg.basePath, cfg.storageIntegration);
    LOG.info("External temporary stage creation complete for " + stageName);
    snowSyncClient.createManifestTable(stageName, manifestTableName);
    LOG.info("Manifest table creation complete for " + manifestTableName);
    snowSyncClient.createVersionsTable(stageName, versionsTableName, cfg.partitionFields, cfg.partitionExtractExpr);
    LOG.info("Versions table creation complete for " + versionsTableName);
    snowSyncClient.createSnapshotView(snapshotViewName, versionsTableName, manifestTableName);
    LOG.info("Snapshot view creation complete for " + snapshotViewName);
    LOG.info("Snowflake sync complete for " + snapshotViewName);
  }

  public static void main(String[] args) {
    SnowflakeSyncConfig cfg = new SnowflakeSyncConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    FileSystem fs = FSUtils.getFs(cfg.basePath, new Configuration());
    new SnowflakeSyncTool(cfg.toProps(), fs.getConf(), fs).syncHoodieTable();
  }
}
