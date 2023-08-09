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

package org.apache.hudi.gcp.bigquery;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.common.util.ManifestFileWriter;

import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI_PREFIX;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SYNC_BASE_PATH;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_TABLE_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA;

/**
 * Tool to sync a hoodie table with a big query table. Either use it as an api
 * BigQuerySyncTool.syncHoodieTable(BigQuerySyncConfig) or as a command line java -cp hoodie-hive.jar BigQuerySyncTool [args]
 * <p>
 * This utility will get the schema from the latest commit and will sync big query table schema.
 *
 * @Experimental
 */
public class BigQuerySyncTool extends HoodieSyncTool {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySyncTool.class);

  public final BigQuerySyncConfig config;
  public final String tableName;
  public final String manifestTableName;
  public final String versionsTableName;
  public final String snapshotViewName;

  public BigQuerySyncTool(Properties props) {
    super(props);
    this.config = new BigQuerySyncConfig(props);
    this.tableName = config.getString(BIGQUERY_SYNC_TABLE_NAME);
    this.manifestTableName = tableName + "_manifest";
    this.versionsTableName = tableName + "_versions";
    this.snapshotViewName = tableName;
  }

  @Override
  public void syncHoodieTable() {
    try (HoodieBigQuerySyncClient bqSyncClient = new HoodieBigQuerySyncClient(config)) {
      switch (bqSyncClient.getTableType()) {
        case COPY_ON_WRITE:
          syncCoWTable(bqSyncClient);
          break;
        case MERGE_ON_READ:
        default:
          throw new UnsupportedOperationException(bqSyncClient.getTableType() + " table type is not supported yet.");
      }
    } catch (Exception e) {
      throw new HoodieBigQuerySyncException("Failed to sync BigQuery for table:" + tableName, e);
    }
  }

  private boolean tableExists(HoodieBigQuerySyncClient bqSyncClient, String tableName) {
    if (bqSyncClient.tableExists(tableName)) {
      LOG.info(tableName + " already exists");
      return true;
    }
    return false;
  }

  private void syncCoWTable(HoodieBigQuerySyncClient bqSyncClient) {
    ValidationUtils.checkState(bqSyncClient.getTableType() == HoodieTableType.COPY_ON_WRITE);
    LOG.info("Sync hoodie table " + snapshotViewName + " at base path " + bqSyncClient.getBasePath());

    if (!bqSyncClient.datasetExists()) {
      throw new HoodieBigQuerySyncException("Dataset not found: " + config.getString(BIGQUERY_SYNC_DATASET_NAME));
    }

    ManifestFileWriter manifestFileWriter = ManifestFileWriter.builder()
        .setConf(config.getHadoopConf())
        .setBasePath(config.getString(BIGQUERY_SYNC_SYNC_BASE_PATH))
        .setUseFileListingFromMetadata(config.getBoolean(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA))
        .setAssumeDatePartitioning(config.getBoolean(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING))
        .build();

    if (config.getBoolean(BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE)) {
      manifestFileWriter.writeManifestFile(true);

      if (!tableExists(bqSyncClient, tableName)) {
        bqSyncClient.createTableUsingBqManifestFile(
            tableName,
            manifestFileWriter.getManifestSourceUri(true),
            config.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX));
        LOG.info("Completed table " + tableName + " creation using the manifest file");
      }

      LOG.info("Sync table complete for " + tableName);
      return;
    }

    manifestFileWriter.writeManifestFile(false);

    if (!tableExists(bqSyncClient, manifestTableName)) {
      bqSyncClient.createManifestTable(manifestTableName, manifestFileWriter.getManifestSourceUri(false));
      LOG.info("Manifest table creation complete for " + manifestTableName);
    }

    if (!tableExists(bqSyncClient, versionsTableName)) {
      bqSyncClient.createVersionsTable(
          versionsTableName,
          config.getString(BIGQUERY_SYNC_SOURCE_URI),
          config.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX),
          config.getSplitStrings(BIGQUERY_SYNC_PARTITION_FIELDS));
      LOG.info("Versions table creation complete for " + versionsTableName);
    }

    if (!tableExists(bqSyncClient, snapshotViewName)) {
      bqSyncClient.createSnapshotView(snapshotViewName, versionsTableName, manifestTableName);
      LOG.info("Snapshot view creation complete for " + snapshotViewName);
    }

    // TODO: Implement automatic schema evolution when you add a new column.
    LOG.info("Sync table complete for " + snapshotViewName);
  }

  public static void main(String[] args) {
    final BigQuerySyncConfig.BigQuerySyncConfigParams params = new BigQuerySyncConfig.BigQuerySyncConfigParams();
    JCommander cmd = JCommander.newBuilder().addObject(params).build();
    cmd.parse(args);
    if (params.isHelp()) {
      cmd.usage();
      System.exit(0);
    }
    new BigQuerySyncTool(params.toProps()).syncHoodieTable();
  }
}
