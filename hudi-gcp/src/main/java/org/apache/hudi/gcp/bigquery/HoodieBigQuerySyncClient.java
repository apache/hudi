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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.hudi.sync.common.util.ManifestFileWriter;

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
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_BIG_LAKE_CONNECTION_ID;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_LOCATION;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_DATASET_NAME;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_PROJECT_ID;
import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_BILLING_PROJECT_ID;

import static org.apache.hudi.gcp.bigquery.BigQuerySyncConfig.BIGQUERY_SYNC_REQUIRE_PARTITION_FILTER;

public class HoodieBigQuerySyncClient extends HoodieSyncClient {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBigQuerySyncClient.class);

  protected final BigQuerySyncConfig config;
  private final String projectId;
  private final String billingProjectId;
  private final String bigLakeConnectionId;
  private final String datasetName;
  private final boolean requirePartitionFilter;
  private transient BigQuery bigquery;

  public HoodieBigQuerySyncClient(final BigQuerySyncConfig config, HoodieTableMetaClient metaClient) {
    this(config, createBigQueryConnection(config), metaClient);
  }

  @VisibleForTesting
  HoodieBigQuerySyncClient(final BigQuerySyncConfig config, final BigQuery bigquery, HoodieTableMetaClient metaClient) {
    super(config, metaClient);
    this.config = config;
    this.projectId = config.getString(BIGQUERY_SYNC_PROJECT_ID);
    this.billingProjectId = config.getStringOrDefault(BIGQUERY_SYNC_BILLING_PROJECT_ID, this.projectId);
    this.datasetName = config.getString(BIGQUERY_SYNC_DATASET_NAME);
    this.requirePartitionFilter = config.getBoolean(BIGQUERY_SYNC_REQUIRE_PARTITION_FILTER);
    this.bigquery = bigquery;
    this.bigLakeConnectionId = config.getString(BIGQUERY_SYNC_BIG_LAKE_CONNECTION_ID);
  }

  private static BigQuery createBigQueryConnection(BigQuerySyncConfig config) {
    try {
      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      return BigQueryOptions.newBuilder().setLocation(config.getString(BIGQUERY_SYNC_DATASET_LOCATION)).build().getService();
    } catch (BigQueryException e) {
      throw new HoodieBigQuerySyncException("Cannot create bigQuery connection ", e);
    }
  }

  public void createOrUpdateTableUsingBqManifestFile(String tableName, String bqManifestFileUri, String sourceUriPrefix, Schema schema) {
    try {
      String withClauses = String.format("( %s )", BigQuerySchemaResolver.schemaToSqlString(schema));
      String extraOptions = "enable_list_inference=true,";
      if (!StringUtils.isNullOrEmpty(sourceUriPrefix)) {
        withClauses += " WITH PARTITION COLUMNS";
        extraOptions += String.format(" hive_partition_uri_prefix=\"%s\", require_hive_partition_filter=%s,", sourceUriPrefix, requirePartitionFilter);
      }
      if (!StringUtils.isNullOrEmpty(bigLakeConnectionId)) {
        withClauses += String.format(" WITH CONNECTION `%s`", bigLakeConnectionId);
      }

      String query =
          String.format(
              "CREATE OR REPLACE EXTERNAL TABLE `%s.%s.%s` %s OPTIONS (%s "
                  + "uris=[\"%s\"], format=\"PARQUET\", file_set_spec_type=\"NEW_LINE_DELIMITED_MANIFEST\")",
              projectId,
              datasetName,
              tableName,
              withClauses,
              extraOptions,
              bqManifestFileUri);

      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
          .setUseLegacySql(false)
          .build();
      JobId jobId = JobId.newBuilder().setProject(billingProjectId).setRandomJob().build();
      Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

      queryJob = queryJob.waitFor();

      if (queryJob == null) {
        LOG.error("Job for table creation no longer exists");
      } else if (queryJob.getStatus().getError() != null) {
        LOG.error("Job for table creation failed: {}", queryJob.getStatus().getError().toString());
      } else {
        LOG.info("External table created using manifest file.");
      }
    } catch (InterruptedException | BigQueryException e) {
      throw new HoodieBigQuerySyncException("Failed to create external table using manifest file. ", e);
    }
  }

  public void createManifestTable(String tableName, String sourceUri) {
    try {
      TableId tableId = TableId.of(projectId, datasetName, tableName);
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

  /**
   * Updates the schema for the given table if the schema has changed. The schema passed in will not have the partition columns defined,
   * so we add them back to the schema with the values read from the existing BigQuery table. This allows us to keep the partition
   * field type in sync with how it is registered in BigQuery.
   * @param tableName name of the table in BigQuery
   * @param schema latest schema for the table
   */
  public void updateTableSchema(String tableName, Schema schema, List<String> partitionFields) {
    Table existingTable = bigquery.getTable(TableId.of(projectId, datasetName, tableName));
    ExternalTableDefinition definition = existingTable.getDefinition();
    Schema remoteTableSchema = definition.getSchema();
    List<Field> finalTableFields = new ArrayList<>(schema.getFields());
    // Add the partition fields into the schema to avoid conflicts while updating. And ensure the partition fields are at the end to
    // avoid unnecessary updates.
    List<Field> bqPartitionFields = remoteTableSchema.getFields().stream()
        .filter(field -> partitionFields.contains(field.getName()))
        .collect(Collectors.toList());
    finalTableFields.addAll(bqPartitionFields);
    Schema finalSchema = Schema.of(finalTableFields);
    boolean sameSchema = definition.getSchema() != null && definition.getSchema().equals(finalSchema);
    boolean samePartitionFilter = partitionFields.isEmpty()
        || (requirePartitionFilter == (definition.getHivePartitioningOptions().getRequirePartitionFilter() != null && definition.getHivePartitioningOptions().getRequirePartitionFilter()));
    if (sameSchema && samePartitionFilter) {
      LOG.info("No table update is needed.");
      return; // No need to update schema.
    }
    if (!StringUtils.isNullOrEmpty(bigLakeConnectionId)) {
      Table updatedTable =
              existingTable.toBuilder().setDefinition(StandardTableDefinition.of(finalSchema)).build();
      updatedTable.update();
    } else {
      ExternalTableDefinition.Builder builder = definition.toBuilder();
      builder.setSchema(finalSchema);
      builder.setAutodetect(false);
      if (definition.getHivePartitioningOptions() != null) {
        builder.setHivePartitioningOptions(definition.getHivePartitioningOptions().toBuilder().setRequirePartitionFilter(requirePartitionFilter).build());
      }
      Table updatedTable = existingTable.toBuilder()
              .setDefinition(builder.build())
              .build();
      bigquery.update(updatedTable);
    }
  }

  public void createVersionsTable(String tableName, String sourceUri, String sourceUriPrefix, List<String> partitionFields) {
    try {
      ExternalTableDefinition customTable;
      TableId tableId = TableId.of(projectId, datasetName, tableName);

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
      TableId tableId = TableId.of(projectId, datasetName, viewName);
      String query =
          String.format(
              "SELECT * FROM `%s.%s.%s` WHERE _hoodie_file_name IN "
                  + "(SELECT filename FROM `%s.%s.%s`)",
              projectId,
              datasetName,
              versionsTableName,
              projectId,
              datasetName,
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
  public Map<String, String> getMetastoreSchema(String tableName) {
    // TODO: Implement automatic schema evolution when you add a new column.
    return Collections.emptyMap();
  }

  public boolean datasetExists() {
    Dataset dataset = bigquery.getDataset(DatasetId.of(projectId, datasetName));
    return dataset != null;
  }

  @Override
  public boolean tableExists(String tableName) {
    TableId tableId = TableId.of(projectId, datasetName, tableName);
    Table table = bigquery.getTable(tableId, BigQuery.TableOption.fields());
    return table != null && table.exists();
  }

  /**
   * Checks for the existence of a table that uses the manifest file approach and matches other requirements.
   *
   * @param tableName name of the table
   * @return Returns true if the table does not exist or if the table does exist but does not use the manifest file or table base path is outdated. False otherwise.
   */
  public boolean tableNotExistsOrDoesNotMatchSpecification(String tableName) {
    TableId tableId = TableId.of(projectId, datasetName, tableName);
    Table table = bigquery.getTable(tableId);
    if (table == null || !table.exists()) {
      return true;
    }
    ExternalTableDefinition externalTableDefinition = table.getDefinition();
    boolean manifestDoesNotExist =
        externalTableDefinition.getSourceUris() == null
            || externalTableDefinition.getSourceUris().stream().noneMatch(uri -> uri.contains(ManifestFileWriter.ABSOLUTE_PATH_MANIFEST_FOLDER_NAME));
    if (isBasePathUpdated(externalTableDefinition)) {
      // if table base path is outdated, we need to replace the table.
      return true;
    }
    if (!StringUtils.isNullOrEmpty(config.getString(BIGQUERY_SYNC_BIG_LAKE_CONNECTION_ID))) {
      // If bigLakeConnectionId is present and connectionId is not present in table definition, we need to replace the table.
      return manifestDoesNotExist || externalTableDefinition.getConnectionId() == null;
    }
    return manifestDoesNotExist;
  }

  private boolean isBasePathUpdated(ExternalTableDefinition externalTableDefinition) {
    String basePath = StringUtils.stripEnd(getBasePath(), "/");
    if (externalTableDefinition.getHivePartitioningOptions() == null) {
      List<String> sourceUris = Option.ofNullable(externalTableDefinition.getSourceUris()).orElse(Collections.emptyList());
      // compare source uris with trailing slash to make sure the unwanted prefix matches are avoided
      String basePathWithTrailingSlash = String.format("%s/", basePath);
      boolean isTableBasePathUpdated = sourceUris.stream()
          .noneMatch(sourceUri -> sourceUri.startsWith(basePathWithTrailingSlash));
      if (isTableBasePathUpdated) {
        LOG.warn("Table base path from source uri updated from: {}, to new base path: {}", sourceUris, basePathWithTrailingSlash);
      }
      return isTableBasePathUpdated;
    }
    String basePathInTableDefinition = externalTableDefinition.getHivePartitioningOptions().getSourceUriPrefix();
    basePathInTableDefinition = StringUtils.stripEnd(basePathInTableDefinition, "/");
    boolean isTableBasePathUpdated = !basePathInTableDefinition.equals(basePath);
    if (isTableBasePathUpdated) {
      LOG.warn("Table base path from table definition updated from: {}, to new base path: {}", basePathInTableDefinition, basePath);
    }
    return isTableBasePathUpdated;
  }

  @Override
  public void close() {
    bigquery = null;
  }
}
