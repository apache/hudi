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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Properties;

import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PARTITION_FIELDS;
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

  private static final Logger LOG = LogManager.getLogger(BigQuerySyncTool.class);

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
      throw new HoodieBigQuerySyncException("Got runtime exception when big query syncing " + tableName, e);
    }
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
    manifestFileWriter.writeManifestFile();

    if (!bqSyncClient.tableExists(manifestTableName)) {
      bqSyncClient.createManifestTable(manifestTableName, manifestFileWriter.getManifestSourceUri());
      LOG.info("Manifest table creation complete for " + manifestTableName);
    }
    if (!bqSyncClient.tableExists(versionsTableName)) {
      bqSyncClient.createVersionsTable(
          versionsTableName,
          config.getString(BIGQUERY_SYNC_SOURCE_URI),
          config.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX),
          config.getSplitStrings(BIGQUERY_SYNC_PARTITION_FIELDS));
      LOG.info("Versions table creation complete for " + versionsTableName);
    }
    if (!bqSyncClient.tableExists(snapshotViewName)) {
      bqSyncClient.createSnapshotView(snapshotViewName, versionsTableName, manifestTableName);
      LOG.info("Snapshot view creation complete for " + snapshotViewName);
    }

    // TODO: Implement automatic schema evolution when you add a new column.
    LOG.info("Sync table complete for " + snapshotViewName);
  }

  @Override
  public void close() {
    // no op
  }

  public static void main(String[] args) {
    final BigQuerySyncConfig.BigQuerySyncConfigParams params = new BigQuerySyncConfig.BigQuerySyncConfigParams();
    JCommander cmd = JCommander.newBuilder().addObject(params).build();
    cmd.parse(args);
    if (params.help) {
      cmd.usage();
      System.exit(0);
    }
    new BigQuerySyncTool(params.toProps()).syncHoodieTable();
  }
}
