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

package org.apache.hudi.aws.sync;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.AbstractHiveSyncHoodieClient;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.util.HiveSchemaUtil;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient;
import org.apache.hudi.sync.common.PartitionValueExtractor;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.BatchUpdatePartitionRequest;
import com.amazonaws.services.glue.model.BatchUpdatePartitionRequestEntry;
import com.amazonaws.services.glue.model.BatchUpdatePartitionResult;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateDatabaseResult;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.esotericsoftware.minlog.Log;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class implements all the AWS APIs to enable syncing of a Hudi Table with the
 * AWS Glue Data Catalog (https://docs.aws.amazon.com/glue/latest/dg/populate-data-catalog.html).
 */
public class AWSGlueCatalogClient extends AbstractHiveSyncHoodieClient {

  private static final Logger LOG = LogManager.getLogger(AWSGlueCatalogClient.class);
  private final HoodieTimeline activeTimeline;
  private final AWSGlue awsGlueClient;
  private final HiveSyncConfig syncConfig;
  private final PartitionValueExtractor partitionValueExtractor;

  public AWSGlueCatalogClient(HiveSyncConfig cfg, FileSystem fs) {
    super(cfg.basePath, cfg.assumeDatePartitioning, cfg.useFileListingFromMetadata, cfg.withOperationField, fs);
    this.awsGlueClient = getGlueClient();
    this.syncConfig = cfg;
    activeTimeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(syncConfig.partitionValueExtractorClass).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + syncConfig.partitionValueExtractorClass, e);
    }
  }

  @Override
  public boolean databaseExists() {
    GetDatabaseRequest request = new GetDatabaseRequest();
    request.setName(syncConfig.databaseName);
    try {
      return (awsGlueClient.getDatabase(request).getDatabase() != null);
    } catch (EntityNotFoundException exception) {
      LOG.info("Database " + syncConfig.databaseName, exception);
    } catch (Exception exception) {
      LOG.error("Failed to check if database exists " + syncConfig.databaseName, exception);
      throw new HoodieHiveSyncException("Failed to check if database exists " + syncConfig.databaseName + " in region ", exception);
    }
    return false;
  }

  @Override
  public void createDatabase() {
    if (!databaseExists()) {
      CreateDatabaseRequest request = new CreateDatabaseRequest();
      request.setDatabaseInput(new DatabaseInput().withName(syncConfig.databaseName).withDescription("automatically created by hudi").withParameters(null).withLocationUri(null));
      try {
        CreateDatabaseResult result = awsGlueClient.createDatabase(request);
        LOG.info("Successfully created database in Glue: " + result.toString());
      } catch (AlreadyExistsException exception) {
        LOG.warn("Database " + syncConfig.databaseName + " already exists", exception);
      } catch (Exception exception) {
        LOG.error("Failed to create database " + syncConfig.databaseName, exception);
        throw new HoodieHiveSyncException("Failed to create database " + syncConfig.databaseName, exception);
      }
    }
  }

  @Override
  public boolean tableExists(String tableName) {
    GetTableRequest request = new GetTableRequest()
        .withDatabaseName(syncConfig.databaseName)
        .withName(tableName);
    try {
      awsGlueClient.getTable(request);
      return true;
    } catch (EntityNotFoundException exception) {
      LOG.info("Accessing non-existent Glue Table " + tableName + " in database " + syncConfig.databaseName, exception);
    } catch (Exception exception) {
      String errorMsg = "Fatal error while fetching Glue Table " + tableName + " in database " + syncConfig.databaseName;
      LOG.error(errorMsg, exception);
      throw new HoodieHiveSyncException(errorMsg, exception);
    }
    return false;
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema,
                          String inputFormatClass, String outputFormatClass,
                          String serdeClass, Map<String, String> serdeProperties,
                          Map<String, String> tableProperties) {
    if (!tableExists(tableName)) {
      CreateTableRequest request = new CreateTableRequest();
      Map<String, String> params = new HashMap<>();
      if (!syncConfig.createManagedTable) {
        params.put("EXTERNAL", "TRUE");
      }
      for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
        params.put(entry.getKey(), entry.getValue());
      }

      try {
        Map<String, String> mapSchema = HiveSchemaUtil.parquetSchemaToMapSchema(storageSchema, syncConfig.supportTimestamp, false);

        List<Column> schemaPartitionKeys = new ArrayList<>();
        List<Column> schemaWithoutPartitionKeys = new ArrayList<>();
        for (String key : mapSchema.keySet()) {
          String keyType = HiveSchemaUtil.getPartitionKeyType(mapSchema, key);
          Column column = new Column().withName(key).withType(keyType.toLowerCase()).withComment("");
          // In Glue, the full schema should exclude the partition keys
          if (syncConfig.partitionFields.contains(key)) {
            schemaPartitionKeys.add(column);
          } else {
            schemaWithoutPartitionKeys.add(column);
          }
        }

        StorageDescriptor storageDescriptor = new StorageDescriptor();
        serdeProperties.put("serialization.format", "1");
        storageDescriptor
            .withSerdeInfo(new SerDeInfo().withSerializationLibrary(serdeClass)
                .withParameters(serdeProperties))
            .withLocation(syncConfig.basePath.replaceFirst("s3a", "s3"))
            .withInputFormat(inputFormatClass)
            .withOutputFormat(outputFormatClass)
            .withColumns(schemaWithoutPartitionKeys);

        TableInput tableInput = new TableInput();
        tableInput.withName(tableName)
            .withTableType(TableType.EXTERNAL_TABLE.toString())
            .withParameters(params)
            .withPartitionKeys(schemaPartitionKeys)
            .withStorageDescriptor(storageDescriptor)
            .withLastAccessTime(new Date(System.currentTimeMillis()))
            .withLastAnalyzedTime(new Date(System.currentTimeMillis()));
        request.withDatabaseName(syncConfig.databaseName)
            .withTableInput(tableInput);

        CreateTableResult result = awsGlueClient.createTable(request);
        LOG.info("Successfully created table in Glue: " + result.toString());
      } catch (AlreadyExistsException exception) {
        LOG.warn("Table " + tableName + " already exists in database " + syncConfig.databaseName, exception);
      } catch (Exception exception) {
        LOG.error("Failed to create table " + tableName + " in database " + syncConfig.databaseName, exception);
        throw new HoodieHiveSyncException("Failed to create table " + tableName + " in database " + syncConfig.databaseName, exception);
      }
    }
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    try {
      Table table = getTable(syncConfig.databaseName, tableName);
      return Option.of(table.getParameters().getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last commit time synced from the database", e);
    }
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    // Set the last commit time from the Active Time line
    String lastCommitSynced = activeTimeline.lastInstant().get().getTimestamp();
    try {
      updateTableParameters(syncConfig.databaseName, tableName, Collections.singletonMap(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced), false);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get update last commit time synced to " + lastCommitSynced, e);
    }
  }

  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + tableName);
      return;
    }
    LOG.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableName);
    try {
      StorageDescriptor sd = getSd(syncConfig.databaseName, tableName);
      List<PartitionInput> partitionInputs = partitionsToAdd.stream().map(partition -> {
        StorageDescriptor partitionSd = sd.clone();
        String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        partitionSd.setLocation(fullPartitionPath);
        return new PartitionInput().withValues(partitionValues).withStorageDescriptor(partitionSd);
      }).collect(Collectors.toList());

      BatchCreatePartitionRequest request = new BatchCreatePartitionRequest();
      request.withDatabaseName(syncConfig.databaseName).withTableName(tableName).withPartitionInputList(partitionInputs);

      BatchCreatePartitionResult result = awsGlueClient.batchCreatePartition(request);
      if (result.getErrors() != null && !result.getErrors().isEmpty()) {
        throw new HoodieHiveSyncException("Fatal error for Add Partitions Failed " + tableName + " with errors: " + result.getErrors().toString());
      }
    } catch (Exception e) {
      LOG.error(syncConfig.databaseName + "." + tableName + " add partition failed", e);
      throw new HoodieHiveSyncException("Fatal error for Add Partitions Failed " + tableName + " in database " + syncConfig.databaseName, e);
    }
  }

  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + tableName);
      return;
    }
    LOG.info("Changing partitions " + changedPartitions.size() + " on " + tableName);
    try {
      StorageDescriptor sd = getSd(syncConfig.databaseName, tableName);
      List<BatchUpdatePartitionRequestEntry> updatePartitionEntries = changedPartitions.stream().map(partition -> {
        StorageDescriptor partitionSd = sd.clone();
        String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        sd.setLocation(fullPartitionPath);
        PartitionInput partitionInput = new PartitionInput().withValues(partitionValues).withStorageDescriptor(partitionSd);
        return new BatchUpdatePartitionRequestEntry().withPartitionInput(partitionInput).withPartitionValueList(partitionValues);
      }).collect(Collectors.toList());

      BatchUpdatePartitionRequest request = new BatchUpdatePartitionRequest();
      request.withDatabaseName(syncConfig.databaseName).withTableName(tableName).withEntries(updatePartitionEntries);

      BatchUpdatePartitionResult result = awsGlueClient.batchUpdatePartition(request);
      if (result.getErrors() != null && !result.getErrors().isEmpty()) {
        LOG.error("Fatal error for Update Partitions Failed " + tableName + " in database " + syncConfig.databaseName + " with errors: " + result.getErrors().toString());
        throw new HoodieHiveSyncException("Fatal error for Update Partitions Failed " + tableName + " with errors: " + result.getErrors().toString());
      }
    } catch (Exception e) {
      LOG.error("Fatal error for Update Partitions Failed " + tableName + " in database " + syncConfig.databaseName, e);
      throw new HoodieHiveSyncException("Fatal error for Update Partitions Failed " + tableName + " in database " + syncConfig.databaseName, e);
    }
  }

  @Override
  public Map<String, String> getTableSchema(String tableName) {
    try {
      // GlueMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      final long start = System.currentTimeMillis();
      Table table = getTable(syncConfig.databaseName, tableName);
      Map<String, String> partitionKeysMap =
          table.getPartitionKeys().stream().collect(Collectors.toMap(Column::getName, f -> f.getType().toUpperCase()));

      Map<String, String> columnsMap =
          table.getStorageDescriptor().getColumns().stream().collect(Collectors.toMap(Column::getName, f -> f.getType().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      final long end = System.currentTimeMillis();
      LOG.info(String.format("Time taken to getTableSchema: %s ms", (end - start)));
      return schema;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get Glue table schema for : " + tableName + " in database " + syncConfig.databaseName, e);
    }
  }

  @Override
  public void updateSchema(String tableName, MessageType newSchema) {
    // ToDo Cascade is set in Hive meta sync, but need to investigate how to configure it for Glue meta
    boolean cascade = syncConfig.partitionFields.size() > 0;
    try {
      Table table = getTable(syncConfig.databaseName, tableName);
      StorageDescriptor sd = getSd(syncConfig.databaseName, tableName);
      Map<String, String> mapNewSchema = HiveSchemaUtil.parquetSchemaToMapSchema(newSchema, syncConfig.supportTimestamp, false);

      List<Column> newColumns = mapNewSchema.keySet().stream().map(key -> {
        String keyType = HiveSchemaUtil.getPartitionKeyType(mapNewSchema, key);
        return new Column().withName(key).withType(keyType.toLowerCase()).withComment("");
      }).collect(Collectors.toList());
      sd.setColumns(newColumns);

      TableInput updatedTableInput = new TableInput();
      updatedTableInput.withName(tableName)
          .withTableType(table.getTableType())
          .withParameters(table.getParameters())
          .withPartitionKeys(table.getPartitionKeys())
          .withStorageDescriptor(sd)
          .withLastAccessTime(new Date(System.currentTimeMillis()))
          .withLastAnalyzedTime(new Date(System.currentTimeMillis()));

      UpdateTableRequest request = new UpdateTableRequest();
      request.withDatabaseName(syncConfig.databaseName)
          .withTableInput(updatedTableInput);
      awsGlueClient.updateTable(request);
    } catch (Exception e) {
      String errorMsg = "Fatal error for Update Schema for table " + tableName + " in database " + syncConfig.databaseName;
      LOG.error(errorMsg, e);
      throw new HoodieHiveSyncException(errorMsg, e);
    }
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  @Override
  public List<AbstractSyncHoodieClient.PartitionEvent> getPartitionEvents(String tableName, List<String> partitionStoragePartitions) {
    List<Partition> tablePartitions = scanTablePartitions(syncConfig.databaseName, tableName);

    Map<String, String> paths = new HashMap<>();
    for (Partition tablePartition : tablePartitions) {
      List<String> hivePartitionValues = tablePartition.getValues();
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getStorageDescriptor().getLocation())).toUri().getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }

    List<PartitionEvent> events = new ArrayList<>();
    for (String storagePartition : partitionStoragePartitions) {
      Path storagePartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, storagePartition);
      String fullStoragePartitionPath = Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      // Check if the partition values or if hdfs path is the same
      List<String> storagePartitionValues = partitionValueExtractor.extractPartitionValuesInPath(storagePartition);
      if (!storagePartitionValues.isEmpty()) {
        String storageValue = String.join(", ", storagePartitionValues);
        if (!paths.containsKey(storageValue)) {
          events.add(PartitionEvent.newPartitionAddEvent(storagePartition));
        } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
          events.add(PartitionEvent.newPartitionUpdateEvent(storagePartition));
        }
      }
    }
    return events;
  }

  /**
   * Update the table properties to the table.
   */
  @Override
  public void updateTableProperties(String tableName, Map<String, String> tableProperties) {
    if (tableProperties == null || tableProperties.isEmpty()) {
      return;
    }
    try {
      updateTableParameters(syncConfig.databaseName, tableName, tableProperties, true);
    } catch (Exception e) {
      String errorMsg = "Failed to update table properties for Glue table: " + tableName + " in database " + syncConfig.databaseName;
      Log.error(errorMsg, e);
      throw new HoodieHiveSyncException(errorMsg, e);
    }
  }

  @Override
  public void close() {
    awsGlueClient.shutdown();
  }

  private enum TableType {
    MANAGED_TABLE,
    EXTERNAL_TABLE,
    VIRTUAL_VIEW,
    INDEX_TABLE,
    MATERIALIZED_VIEW;

    private TableType() {
    }
  }

  private Table getTable(String databaseName, String tableName) throws HoodieHiveSyncException {
    GetTableRequest request = new GetTableRequest()
        .withDatabaseName(databaseName)
        .withName(tableName);
    try {
      return awsGlueClient.getTable(request).getTable();
    } catch (EntityNotFoundException exception) {
      String errorMsg = "Accessing non-existent Glue Table " + tableName + " in database " + databaseName;
      LOG.error(errorMsg, exception);
      throw new HoodieHiveSyncException(errorMsg, exception);
    } catch (Exception exception) {
      String errorMsg = "Fatal error while fetching Glue Table " + tableName + " in database " + databaseName;
      LOG.error(errorMsg, exception);
      throw new HoodieHiveSyncException(errorMsg, exception);
    }
  }

  private List<Partition> scanTablePartitions(String databaseName, String tableName) {
    try {
      GetPartitionsRequest request = new GetPartitionsRequest();
      request.withDatabaseName(databaseName).withTableName(tableName);
      GetPartitionsResult result = awsGlueClient.getPartitions(request);
      return result.getPartitions();
    } catch (Exception exception) {
      throw new HoodieHiveSyncException("Fatal error while scanning all table partitions for table "
          + tableName + " in database " + databaseName, exception);
    }
  }

  private StorageDescriptor getSd(String databaseName, String tableName) {
    return getTable(databaseName, tableName).getStorageDescriptor();
  }

  private void updateTableParameters(String databaseName, String tableName, Map<String, String> updatedParams, boolean shouldReplace) {
    try {
      Table table = getTable(databaseName, tableName);
      Map<String, String> finalParams = new HashMap<>();
      if (!shouldReplace) {
        finalParams.putAll(table.getParameters());
      }
      finalParams.putAll(updatedParams);

      TableInput updatedTableInput = new TableInput();
      updatedTableInput.withName(tableName)
          .withTableType(table.getTableType())
          .withParameters(finalParams)
          .withPartitionKeys(table.getPartitionKeys())
          .withStorageDescriptor(table.getStorageDescriptor())
          .withLastAccessTime(new Date(System.currentTimeMillis()))
          .withLastAnalyzedTime(new Date(System.currentTimeMillis()));

      UpdateTableRequest request = new UpdateTableRequest();
      request.withDatabaseName(syncConfig.databaseName)
          .withTableInput(updatedTableInput);
      awsGlueClient.updateTable(request);
    } catch (Exception e) {
      String errorMsg = "Failed to update the params: " + updatedParams + " for table " + tableName + " in database " + databaseName;
      Log.error(errorMsg, e);
      throw new HoodieHiveSyncException(errorMsg, e);
    }
  }

  private static AWSGlue getGlueClient() {
    return AWSGlueClientBuilder.standard().build();
  }
}