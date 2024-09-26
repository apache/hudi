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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HadoopConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.sync.common.HoodieSyncTool;
import org.apache.hudi.sync.common.util.ManifestFileWriter;

import com.beust.jcommander.JCommander;
import com.google.cloud.bigquery.Schema;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_SOURCE_URI_PREFIX;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_TABLE_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE;
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
  private static final String SUFFIX_MANIFEST = "_manifest";
  private static final String SUFFIX_VERSIONS = "_versions";

  private final BigQuerySyncConfig config;
  private final String tableName;
  private final String manifestTableName;
  private final String versionsTableName;
  private final String snapshotViewName;
  private final ManifestFileWriter manifestFileWriter;
  private final HoodieBigQuerySyncClient bqSyncClient;
  private final HoodieTableMetaClient metaClient;
  private final BigQuerySchemaResolver bqSchemaResolver;

  public BigQuerySyncTool(Properties props) {
    this(props, HadoopConfigUtils.createHadoopConf(props), Option.empty());
  }

  public BigQuerySyncTool(Properties props, Configuration configuration, Option<HoodieTableMetaClient> metaClientOption) {
    // will build file writer, client, etc. from configs
    super(props, configuration);
    this.config = new BigQuerySyncConfig(props);
    this.tableName = config.getString(BIGQUERY_SYNC_TABLE_NAME);
    this.manifestTableName = tableName + SUFFIX_MANIFEST;
    this.versionsTableName = tableName + SUFFIX_VERSIONS;
    this.snapshotViewName = tableName;
    this.bqSyncClient = new HoodieBigQuerySyncClient(config, metaClientOption.orElseGet(() -> buildMetaClient(config)));
    // reuse existing meta client if not provided (only test cases will provide their own meta client)
    this.metaClient = bqSyncClient.getMetaClient();
    this.manifestFileWriter = buildManifestFileWriterFromConfig(metaClient, config);
    this.bqSchemaResolver = BigQuerySchemaResolver.getInstance();
  }

  @VisibleForTesting // allows us to pass in mocks for the writer and client
  BigQuerySyncTool(Properties properties, ManifestFileWriter manifestFileWriter, HoodieBigQuerySyncClient bigQuerySyncClient, HoodieTableMetaClient metaClient,
                   BigQuerySchemaResolver bigQuerySchemaResolver) {
    super(properties);
    this.config = new BigQuerySyncConfig(props);
    this.tableName = config.getString(BIGQUERY_SYNC_TABLE_NAME);
    this.manifestTableName = tableName + SUFFIX_MANIFEST;
    this.versionsTableName = tableName + SUFFIX_VERSIONS;
    this.snapshotViewName = tableName;
    this.bqSyncClient = bigQuerySyncClient;
    this.metaClient = metaClient;
    this.manifestFileWriter = manifestFileWriter;
    this.bqSchemaResolver = bigQuerySchemaResolver;
  }

  private static ManifestFileWriter buildManifestFileWriterFromConfig(HoodieTableMetaClient metaClient, BigQuerySyncConfig config) {
    return ManifestFileWriter.builder()
        .setMetaClient(metaClient)
        .setUseFileListingFromMetadata(config.getBoolean(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA))
        .build();
  }

  @Override
  public void syncHoodieTable() {
    switch (bqSyncClient.getTableType()) {
      case COPY_ON_WRITE:
      case MERGE_ON_READ:
        syncTable(bqSyncClient);
        break;
      default:
        throw new UnsupportedOperationException(bqSyncClient.getTableType() + " table type is not supported yet.");
    }
  }

  private boolean tableExists(HoodieBigQuerySyncClient bqSyncClient, String tableName) {
    if (bqSyncClient.tableExists(tableName)) {
      LOG.info("{} already exists. Skip table creation.", tableName);
      return true;
    }
    return false;
  }

  private void syncTable(HoodieBigQuerySyncClient bqSyncClient) {
    LOG.info("Sync hoodie table {} at base path {}", snapshotViewName, bqSyncClient.getBasePath());

    if (!bqSyncClient.datasetExists()) {
      throw new HoodieBigQuerySyncException("Dataset not found: " + config.getString(BIGQUERY_SYNC_DATASET_NAME));
    }

    List<String> partitionFields = !StringUtils.isNullOrEmpty(config.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX)) ? config.getSplitStrings(BIGQUERY_SYNC_PARTITION_FIELDS) : Collections.emptyList();
    Schema latestSchema = bqSchemaResolver.getTableSchema(metaClient, partitionFields);
    if (config.getBoolean(BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE)) {
      manifestFileWriter.writeManifestFile(true);
      // if table does not exist, create it using the manifest file
      // if table exists but is not yet using manifest file or needs to be recreated with the big-lake connection ID, update it to use manifest file
      if (bqSyncClient.tableNotExistsOrDoesNotMatchSpecification(tableName)) {
        bqSyncClient.createOrUpdateTableUsingBqManifestFile(
            tableName,
            manifestFileWriter.getManifestSourceUri(true),
            config.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX),
            latestSchema);
        LOG.info("Completed table {} creation using the manifest file", tableName);
      } else {
        bqSyncClient.updateTableSchema(tableName, latestSchema, partitionFields);
        LOG.info("Synced schema for {}", tableName);
      }

      LOG.info("Sync table complete for {}", tableName);
      return;
    }

    manifestFileWriter.writeManifestFile(false);

    if (!tableExists(bqSyncClient, manifestTableName)) {
      bqSyncClient.createManifestTable(manifestTableName, manifestFileWriter.getManifestSourceUri(false));
      LOG.info("Manifest table creation complete for {}", manifestTableName);
    }

    if (!tableExists(bqSyncClient, versionsTableName)) {
      bqSyncClient.createVersionsTable(
          versionsTableName,
          config.getString(BIGQUERY_SYNC_SOURCE_URI),
          config.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX),
          config.getSplitStrings(BIGQUERY_SYNC_PARTITION_FIELDS));
      LOG.info("Versions table creation complete for {}", versionsTableName);
    }

    if (!tableExists(bqSyncClient, snapshotViewName)) {
      bqSyncClient.createSnapshotView(snapshotViewName, versionsTableName, manifestTableName);
      LOG.info("Snapshot view creation complete for {}", snapshotViewName);
    }

    LOG.info("Sync table complete for {}", snapshotViewName);
  }

  @Override
  public void close() throws Exception {
    super.close();
    bqSyncClient.close();
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
