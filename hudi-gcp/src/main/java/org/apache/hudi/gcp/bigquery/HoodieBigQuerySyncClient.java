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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.HoodieSyncClient;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.HivePartitioningOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HoodieBigQuerySyncClient extends HoodieSyncClient {
  private static final Logger LOG = LogManager.getLogger(HoodieBigQuerySyncClient.class);

  private final BigQuerySyncConfig syncConfig;
  private transient BigQuery bigquery;

  public HoodieBigQuerySyncClient(final BigQuerySyncConfig syncConfig, final FileSystem fs) {
    super(syncConfig.basePath, syncConfig.assumeDatePartitioning, syncConfig.useFileListingFromMetadata,
        false, fs);
    this.syncConfig = syncConfig;
    this.createBigQueryConnection();
  }

  private void createBigQueryConnection() {
    if (bigquery == null) {
      try {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        bigquery = BigQueryOptions.newBuilder().setLocation(syncConfig.datasetLocation).build().getService();
        LOG.info("Successfully established BigQuery connection.");
      } catch (BigQueryException e) {
        throw new HoodieBigQuerySyncException("Cannot create bigQuery connection ", e);
      }
    }
  }

  @Override
  public void createTable(final String tableName, final MessageType storageSchema, final String inputFormatClass,
                          final String outputFormatClass, final String serdeClass,
                          final Map<String, String> serdeProperties, final Map<String, String> tableProperties) {
    // bigQuery create table arguments are different, so do nothing.
  }

  public void createManifestTable(String tableName, String sourceUri) {
    try {
      TableId tableId = TableId.of(syncConfig.projectId, syncConfig.datasetName, tableName);
      CsvOptions csvOptions = CsvOptions.newBuilder()
          .setFieldDelimiter(",")
          .setAllowJaggedRows(false)
          .setAllowQuotedNewLines(false)
          .setSkipLeadingRows(0)
          .build();
      Schema schema = Schema.of(
          Field.of("filename", StandardSQLTypeName.STRING));

      ExternalTableDefinition customTable =
          ExternalTableDefinition.newBuilder(sourceUri, schema, csvOptions)
              .setAutodetect(false)
              .setIgnoreUnknownValues(false)
              .setMaxBadRecords(0)
              .build();
      bigquery.create(TableInfo.of(tableId, customTable));
      LOG.info("Manifest External table created.");
    } catch (BigQueryException e) {
      throw new HoodieBigQuerySyncException("Manifest External table was not created ", e);
    }
  }

  public void createVersionsTable(String tableName, String sourceUri, String sourceUriPrefix, List<String> partitionFields) {
    try {
      ExternalTableDefinition customTable;
      TableId tableId = TableId.of(syncConfig.projectId, syncConfig.datasetName, tableName);

      if (partitionFields.isEmpty()) {
        customTable =
            ExternalTableDefinition.newBuilder(sourceUri, FormatOptions.parquet())
                .setAutodetect(true)
                .setIgnoreUnknownValues(true)
                .setMaxBadRecords(0)
                .build();
      } else {
        // Configuring partitioning options for partitioned table.
        HivePartitioningOptions hivePartitioningOptions =
            HivePartitioningOptions.newBuilder()
                .setMode("AUTO")
                .setRequirePartitionFilter(false)
                .setSourceUriPrefix(sourceUriPrefix)
                .build();
        customTable =
            ExternalTableDefinition.newBuilder(sourceUri, FormatOptions.parquet())
                .setAutodetect(true)
                .setHivePartitioningOptions(hivePartitioningOptions)
                .setIgnoreUnknownValues(true)
                .setMaxBadRecords(0)
                .build();
      }

      bigquery.create(TableInfo.of(tableId, customTable));
      LOG.info("External table created using hivepartitioningoptions");
    } catch (BigQueryException e) {
      throw new HoodieBigQuerySyncException("External table was not created ", e);
    }
  }

  public void createSnapshotView(String viewName, String versionsTableName, String manifestTableName) {
    try {
      TableId tableId = TableId.of(syncConfig.projectId, syncConfig.datasetName, viewName);
      String query =
          String.format(
              "SELECT * FROM `%s.%s.%s` WHERE _hoodie_file_name IN "
                  + "(SELECT filename FROM `%s.%s.%s`)",
              syncConfig.projectId,
              syncConfig.datasetName,
              versionsTableName,
              syncConfig.projectId,
              syncConfig.datasetName,
              manifestTableName);

      ViewDefinition viewDefinition =
          ViewDefinition.newBuilder(query).setUseLegacySql(false).build();

      bigquery.create(TableInfo.of(tableId, viewDefinition));
      LOG.info("View created successfully");
    } catch (BigQueryException e) {
      throw new HoodieBigQuerySyncException("View was not created ", e);
    }
  }

  @Override
  public Map<String, String> getTableSchema(String tableName) {
    // TODO: Implement automatic schema evolution when you add a new column.
    return Collections.emptyMap();
  }

  @Override
  public void addPartitionsToTable(final String tableName, final List<String> partitionsToAdd) {
    // bigQuery discovers the new partitions automatically, so do nothing.
    throw new UnsupportedOperationException("No support for addPartitionsToTable yet.");
  }

  public boolean datasetExists() {
    Dataset dataset = bigquery.getDataset(DatasetId.of(syncConfig.projectId, syncConfig.datasetName));
    return dataset != null;
  }

  @Override
  public boolean doesTableExist(final String tableName) {
    return tableExists(tableName);
  }

  @Override
  public boolean tableExists(String tableName) {
    TableId tableId = TableId.of(syncConfig.projectId, syncConfig.datasetName, tableName);
    Table table = bigquery.getTable(tableId, BigQuery.TableOption.fields());
    return table != null && table.exists();
  }

  @Override
  public Option<String> getLastCommitTimeSynced(final String tableName) {
    // bigQuery doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("Not support getLastCommitTimeSynced yet.");
  }

  @Override
  public void updateLastCommitTimeSynced(final String tableName) {
    // bigQuery doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("No support for updateLastCommitTimeSynced yet.");
  }

  @Override
  public Option<String> getLastReplicatedTime(String tableName) {
    // bigQuery doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("Not support getLastReplicatedTime yet.");
  }

  @Override
  public void updateLastReplicatedTimeStamp(String tableName, String timeStamp) {
    // bigQuery doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("No support for updateLastReplicatedTimeStamp yet.");
  }

  @Override
  public void deleteLastReplicatedTimeStamp(String tableName) {
    // bigQuery doesn't support tblproperties, so do nothing.
    throw new UnsupportedOperationException("No support for deleteLastReplicatedTimeStamp yet.");
  }

  @Override
  public void updatePartitionsToTable(final String tableName, final List<String> changedPartitions) {
    // bigQuery updates the partitions automatically, so do nothing.
    throw new UnsupportedOperationException("No support for updatePartitionsToTable yet.");
  }

  @Override
  public void dropPartitions(String tableName, List<String> partitionsToDrop) {
    // bigQuery discovers the new partitions automatically, so do nothing.
    throw new UnsupportedOperationException("No support for dropPartitions yet.");
  }

  @Override
  public void close() {
    // bigQuery has no connection close method, so do nothing.
  }
}
