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

package org.apache.hudi.bigquery;

import org.apache.hudi.bigquery.util.Utils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.InvalidTableException;
import org.apache.hudi.sync.common.AbstractSyncTool;

import com.beust.jcommander.JCommander;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;

/**
 * Tool to sync a hoodie table with a big query table. Either use it as an api
 * BigQuerySyncTool.syncHoodieTable(BigQuerySyncConfig) or as a command line java -cp hoodie-hive.jar BigQuerySyncTool [args]
 * <p>
 * This utility will get the schema from the latest commit and will sync big query table schema
 */
public class BigQuerySyncTool extends AbstractSyncTool {
  private static final Logger LOG = LogManager.getLogger(BigQuerySyncTool.class);
  public final BigQuerySyncConfig cfg;
  public final HoodieBigQueryClient hoodieBigQueryClient;
  public String projectId;
  public String datasetName;
  public String manifestTableName;
  public String versionsTableName;
  public String snapshotViewName;
  public String sourceUri;
  public String sourceUriPrefix;
  public List<String> partitionFields;

  private BigQuerySyncTool(Properties properties, Configuration conf, FileSystem fs) {
    super(new TypedProperties(properties), conf, fs);
    hoodieBigQueryClient = new HoodieBigQueryClient(Utils.propertiesToConfig(properties), fs);
    cfg = Utils.propertiesToConfig(properties);
    switch (hoodieBigQueryClient.getTableType()) {
      case COPY_ON_WRITE:
        projectId = cfg.projectId;
        datasetName = cfg.datasetName;
        manifestTableName = cfg.tableName + "_manifest";
        versionsTableName = cfg.tableName + "_versions";
        snapshotViewName = cfg.tableName;
        sourceUri = cfg.sourceUri;
        sourceUriPrefix = cfg.sourceUriPrefix;
        partitionFields = cfg.partitionFields;
        break;
      case MERGE_ON_READ:
        LOG.error("Not supported table type " + hoodieBigQueryClient.getTableType());
        throw new InvalidTableException(hoodieBigQueryClient.getBasePath());
      default:
        LOG.error("Unknown table type " + hoodieBigQueryClient.getTableType());
        throw new InvalidTableException(hoodieBigQueryClient.getBasePath());
    }
  }

  public static void main(String[] args) {
    // parse the params
    BigQuerySyncConfig cfg = new BigQuerySyncConfig();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    FileSystem fs = FSUtils.getFs(cfg.basePath, new Configuration());
    new BigQuerySyncTool(Utils.configToProperties(cfg), new Configuration(), fs).syncHoodieTable();
  }

  @Override
  public void syncHoodieTable() {
    try {
      switch (hoodieBigQueryClient.getTableType()) {
        case COPY_ON_WRITE:
          syncCoWTable();
          break;
        case MERGE_ON_READ:
          LOG.error("Not supported table type " + hoodieBigQueryClient.getTableType());
          throw new InvalidTableException(hoodieBigQueryClient.getBasePath());
        default:
          LOG.error("Unknown table type " + hoodieBigQueryClient.getTableType());
          throw new InvalidTableException(hoodieBigQueryClient.getBasePath());
      }
    } catch (RuntimeException re) {
      throw new HoodieException("Got runtime exception when big query syncing " + cfg.tableName, re);
    } finally {
      hoodieBigQueryClient.close();
    }
  }

  private void syncCoWTable() {
    LOG.info("Trying to sync hoodie table " + snapshotViewName + " with base path " + hoodieBigQueryClient.getBasePath()
        + " of type " + hoodieBigQueryClient.getTableType());

    // TODO: Invoke generate manifest routine to refresh the manifest files.
    if (!hoodieBigQueryClient.doesTableExist(projectId, datasetName, manifestTableName)) {
      hoodieBigQueryClient.createManifestTable(projectId, datasetName, manifestTableName, sourceUri);
      LOG.info("Manifest table creation complete for " + manifestTableName);
    }
    if (!hoodieBigQueryClient.doesTableExist(projectId, datasetName, versionsTableName)) {
      hoodieBigQueryClient.createVersionsTable(projectId, datasetName, versionsTableName, sourceUri, sourceUriPrefix, partitionFields);
      LOG.info("Versions table creation complete for " + versionsTableName);
    }
    if (!hoodieBigQueryClient.doesViewExist(projectId, datasetName, snapshotViewName)) {
      hoodieBigQueryClient.createSnapshotView(projectId, datasetName, snapshotViewName, manifestTableName, versionsTableName);
      LOG.info("Snapshot view creation complete for " + snapshotViewName);
    }

    // TODO: Implement automatic schema evolution when you add a new column.
    LOG.info("Sync table complete for " + snapshotViewName);
  }
}
