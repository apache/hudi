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
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.hudi.sync.common.model.Partition;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequestEntry;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hudi.aws.utils.S3Utils.s3aToS3;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_CREATE_MANAGED_TABLE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.apache.hudi.common.util.MapUtils.isNullOrEmpty;
import static org.apache.hudi.hive.util.HiveSchemaUtil.getPartitionKeyType;
import static org.apache.hudi.hive.util.HiveSchemaUtil.parquetSchemaToMapSchema;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.util.TableUtils.tableId;

/**
 * This class implements all the AWS APIs to enable syncing of a Hudi Table with the
 * AWS Glue Data Catalog (https://docs.aws.amazon.com/glue/latest/dg/populate-data-catalog.html).
 *
 * @Experimental
 */
public class AWSGlueCatalogSyncClient extends HoodieSyncClient {

  private static final Logger LOG = LogManager.getLogger(AWSGlueCatalogSyncClient.class);
  private static final int MAX_PARTITIONS_PER_REQUEST = 100;
  private static final long BATCH_REQUEST_SLEEP_MILLIS = 1000L;
  private final GlueClient awsGlue;
  private final String databaseName;

  public AWSGlueCatalogSyncClient(HiveSyncConfig config) {
    super(config);
    this.awsGlue = GlueClient.builder().build();
    this.databaseName = config.getStringOrDefault(META_SYNC_DATABASE_NAME);
  }

  @Override
  public List<Partition> getAllPartitions(String tableName) {
    try {
      List<Partition> partitions = new ArrayList<>();
      String nextToken = null;
      do {
        GetPartitionsResponse result = awsGlue.getPartitions(GetPartitionsRequest.builder()
            .databaseName(databaseName)
            .tableName(tableName)
            .nextToken(nextToken)
            .build());
        partitions.addAll(result.partitions().stream()
            .map(p -> new Partition(p.values(), p.storageDescriptor().location()))
            .collect(Collectors.toList()));
        nextToken = result.nextToken();
      } while (nextToken != null);
      return partitions;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Failed to get all partitions for table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + tableId(databaseName, tableName));
      return;
    }
    LOG.info("Adding " + partitionsToAdd.size() + " partition(s) in table " + tableId(databaseName, tableName));
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      StorageDescriptor sd = table.storageDescriptor();
      List<PartitionInput> partitionInputs = partitionsToAdd.stream().map(partition -> {
        String fullPartitionPath = FSUtils.getPartitionPath(getBasePath(), partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        StorageDescriptor partitionSD = sd.copy(copySd -> copySd.location(fullPartitionPath));
        return PartitionInput.builder().values(partitionValues).storageDescriptor(partitionSD).build();
      }).collect(Collectors.toList());

      for (List<PartitionInput> batch : CollectionUtils.batches(partitionInputs, MAX_PARTITIONS_PER_REQUEST)) {
        BatchCreatePartitionRequest request = BatchCreatePartitionRequest.builder()
                .databaseName(databaseName).tableName(tableName).partitionInputList(batch).build();

        BatchCreatePartitionResponse response = awsGlue.batchCreatePartition(request);
        if (CollectionUtils.nonEmpty(response.errors())) {
          if (response.errors().stream()
              .allMatch(
                  (error) -> "AlreadyExistsException".equals(error.errorDetail().errorCode()))) {
            LOG.warn("Partitions already exist in glue: " + response.errors());
          } else {
            throw new HoodieGlueSyncException("Fail to add partitions to " + tableId(databaseName, tableName)
              + " with error(s): " + response.errors());
          }
        }
        Thread.sleep(BATCH_REQUEST_SLEEP_MILLIS);
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to add partitions to " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + tableName);
      return;
    }
    LOG.info("Updating " + changedPartitions.size() + "partition(s) in table " + tableId(databaseName, tableName));
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      StorageDescriptor sd = table.storageDescriptor();
      List<BatchUpdatePartitionRequestEntry> updatePartitionEntries = changedPartitions.stream().map(partition -> {
        String fullPartitionPath = FSUtils.getPartitionPath(getBasePath(), partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        StorageDescriptor partitionSD = sd.copy(copySd -> copySd.location(fullPartitionPath));
        PartitionInput partitionInput = PartitionInput.builder().values(partitionValues).storageDescriptor(partitionSD).build();
        return BatchUpdatePartitionRequestEntry.builder().partitionInput(partitionInput).partitionValueList(partitionValues).build();
      }).collect(Collectors.toList());

      for (List<BatchUpdatePartitionRequestEntry> batch : CollectionUtils.batches(updatePartitionEntries, MAX_PARTITIONS_PER_REQUEST)) {
        BatchUpdatePartitionRequest request = BatchUpdatePartitionRequest.builder()
                .databaseName(databaseName).tableName(tableName).entries(batch).build();

        BatchUpdatePartitionResponse response = awsGlue.batchUpdatePartition(request);
        if (CollectionUtils.nonEmpty(response.errors())) {
          throw new HoodieGlueSyncException("Fail to update partitions to " + tableId(databaseName, tableName)
              + " with error(s): " + response.errors());
        }
        Thread.sleep(BATCH_REQUEST_SLEEP_MILLIS);
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update partitions to " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public void dropPartitions(String tableName, List<String> partitionsToDrop) {
    if (CollectionUtils.isNullOrEmpty(partitionsToDrop)) {
      LOG.info("No partitions to drop for " + tableName);
      return;
    }
    LOG.info("Drop " + partitionsToDrop.size() + "partition(s) in table " + tableId(databaseName, tableName));
    try {
      for (List<String> batch : CollectionUtils.batches(partitionsToDrop, MAX_PARTITIONS_PER_REQUEST)) {

        List<PartitionValueList> partitionValueLists = batch.stream().map(partition -> {
          PartitionValueList partitionValueList = PartitionValueList.builder()
                  .values(partitionValueExtractor.extractPartitionValuesInPath(partition))
                  .build();
          return partitionValueList;
        }).collect(Collectors.toList());

        BatchDeletePartitionRequest batchDeletePartitionRequest = BatchDeletePartitionRequest.builder()
            .databaseName(databaseName)
            .tableName(tableName)
            .partitionsToDelete(partitionValueLists)
            .build();

        BatchDeletePartitionResponse response = awsGlue.batchDeletePartition(batchDeletePartitionRequest);
        if (CollectionUtils.nonEmpty(response.errors())) {
          throw new HoodieGlueSyncException("Fail to drop partitions to " + tableId(databaseName, tableName)
              + " with error(s): " + response.errors());
        }
        Thread.sleep(BATCH_REQUEST_SLEEP_MILLIS);
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to drop partitions to " + tableId(databaseName, tableName), e);
    }
  }

  /**
   * Update the table properties to the table.
   */
  @Override
  public void updateTableProperties(String tableName, Map<String, String> tableProperties) {
    if (isNullOrEmpty(tableProperties)) {
      return;
    }
    try {
      updateTableParameters(awsGlue, databaseName, tableName, tableProperties, false);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update properties for table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public void updateTableSchema(String tableName, MessageType newSchema) {
    // ToDo Cascade is set in Hive meta sync, but need to investigate how to configure it for Glue meta
    boolean cascade = config.getSplitStrings(META_SYNC_PARTITION_FIELDS).size() > 0;
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      Map<String, String> newSchemaMap = parquetSchemaToMapSchema(newSchema, config.getBoolean(HIVE_SUPPORT_TIMESTAMP_TYPE), false);
      List<Column> newColumns = getColumnsFromSchema(newSchemaMap);
      StorageDescriptor sd = table.storageDescriptor();
      StorageDescriptor partitionSD = sd.copy(copySd -> copySd.columns(newColumns));
      final Instant now = Instant.now();
      TableInput updatedTableInput = TableInput.builder()
          .tableType(table.tableType())
          .parameters(table.parameters())
          .partitionKeys(table.partitionKeys())
          .storageDescriptor(partitionSD)
          .lastAccessTime(now)
          .lastAnalyzedTime(now)
          .build();

      UpdateTableRequest request = UpdateTableRequest.builder()
          .databaseName(databaseName)
          .tableInput(updatedTableInput)
          .build();

      awsGlue.updateTable(request);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update definition for table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public void createTable(String tableName,
      MessageType storageSchema,
      String inputFormatClass,
      String outputFormatClass,
      String serdeClass,
      Map<String, String> serdeProperties,
      Map<String, String> tableProperties) {
    if (tableExists(tableName)) {
      return;
    }
    Map<String, String> params = new HashMap<>();
    if (!config.getBoolean(HIVE_CREATE_MANAGED_TABLE)) {
      params.put("EXTERNAL", "TRUE");
    }
    params.putAll(tableProperties);

    try {
      Map<String, String> mapSchema = parquetSchemaToMapSchema(storageSchema, config.getBoolean(HIVE_SUPPORT_TIMESTAMP_TYPE), false);

      List<Column> schemaWithoutPartitionKeys = getColumnsFromSchema(mapSchema);

      // now create the schema partition
      List<Column> schemaPartitionKeys = config.getSplitStrings(META_SYNC_PARTITION_FIELDS).stream().map(partitionKey -> {
        String keyType = getPartitionKeyType(mapSchema, partitionKey);
        return Column.builder().name(partitionKey).type(keyType.toLowerCase()).comment("").build();
      }).collect(Collectors.toList());

      serdeProperties.put("serialization.format", "1");
      StorageDescriptor storageDescriptor = StorageDescriptor.builder()
          .serdeInfo(SerDeInfo.builder().serializationLibrary(serdeClass).parameters(serdeProperties).build())
          .location(s3aToS3(getBasePath()))
          .inputFormat(inputFormatClass)
          .outputFormat(outputFormatClass)
          .columns(schemaWithoutPartitionKeys)
          .build();

      final Instant now = Instant.now();
      TableInput tableInput = TableInput.builder()
          .name(tableName)
          .tableType(TableType.EXTERNAL_TABLE.toString())
          .parameters(params)
          .partitionKeys(schemaPartitionKeys)
          .storageDescriptor(storageDescriptor)
          .lastAccessTime(now)
          .lastAnalyzedTime(now)
          .build();

      CreateTableRequest request = CreateTableRequest.builder()
              .databaseName(databaseName)
              .tableInput(tableInput)
              .build();

      CreateTableResponse response = awsGlue.createTable(request);
      LOG.info("Created table " + tableId(databaseName, tableName) + " : " + response);
    } catch (AlreadyExistsException e) {
      LOG.warn("Table " + tableId(databaseName, tableName) + " already exists.", e);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to create " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public Map<String, String> getMetastoreSchema(String tableName) {
    try {
      // GlueMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      Table table = getTable(awsGlue, databaseName, tableName);
      Map<String, String> partitionKeysMap =
          table.partitionKeys().stream().collect(Collectors.toMap(Column::name, f -> f.type().toUpperCase()));

      Map<String, String> columnsMap =
          table.storageDescriptor().columns().stream().collect(Collectors.toMap(Column::name, f -> f.type().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      return schema;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get schema for table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public boolean tableExists(String tableName) {
    GetTableRequest request = GetTableRequest.builder()
        .databaseName(databaseName)
        .name(tableName)
        .build();
    try {
      return Objects.nonNull(awsGlue.getTable(request).table());
    } catch (EntityNotFoundException e) {
      LOG.info("Table not found: " + tableId(databaseName, tableName), e);
      return false;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get table: " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public boolean databaseExists(String databaseName) {
    GetDatabaseRequest request = GetDatabaseRequest.builder().name(databaseName).build();
    try {
      return Objects.nonNull(awsGlue.getDatabase(request).database());
    } catch (EntityNotFoundException e) {
      LOG.info("Database not found: " + databaseName, e);
      return false;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to check if database exists " + databaseName, e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    if (databaseExists(databaseName)) {
      return;
    }
    CreateDatabaseRequest request = CreateDatabaseRequest.builder()
            .databaseInput(DatabaseInput.builder()
            .name(databaseName)
            .description("Automatically created by " + this.getClass().getName())
            .parameters(null)
            .locationUri(null)
            .build()
    ).build();
    try {
      CreateDatabaseResponse result = awsGlue.createDatabase(request);
      LOG.info("Successfully created database in AWS Glue: " + result.toString());
    } catch (AlreadyExistsException e) {
      LOG.warn("AWS Glue Database " + databaseName + " already exists", e);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to create database " + databaseName, e);
    }
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      return Option.ofNullable(table.parameters().get(HOODIE_LAST_COMMIT_TIME_SYNC));
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get last sync commit time for " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public void close() {
    awsGlue.close();
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    if (!getActiveTimeline().lastInstant().isPresent()) {
      LOG.warn("No commit in active timeline.");
      return;
    }
    final String lastCommitTimestamp = getActiveTimeline().lastInstant().get().getTimestamp();
    try {
      updateTableParameters(awsGlue, databaseName, tableName, Collections.singletonMap(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitTimestamp), false);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update last sync commit time for " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public Option<String> getLastReplicatedTime(String tableName) {
    throw new UnsupportedOperationException("Not supported: `getLastReplicatedTime`");
  }

  @Override
  public void updateLastReplicatedTimeStamp(String tableName, String timeStamp) {
    throw new UnsupportedOperationException("Not supported: `updateLastReplicatedTimeStamp`");
  }

  @Override
  public void deleteLastReplicatedTimeStamp(String tableName) {
    throw new UnsupportedOperationException("Not supported: `deleteLastReplicatedTimeStamp`");
  }

  private List<Column> getColumnsFromSchema(Map<String, String> mapSchema) {
    List<Column> cols = new ArrayList<>();
    for (String key : mapSchema.keySet()) {
      // In Glue, the full schema should exclude the partition keys
      if (!config.getSplitStrings(META_SYNC_PARTITION_FIELDS).contains(key)) {
        String keyType = getPartitionKeyType(mapSchema, key);
        Column column = Column.builder().name(key).type(keyType.toLowerCase()).comment("").build();
        cols.add(column);
      }
    }
    return cols;
  }

  private enum TableType {
    MANAGED_TABLE,
    EXTERNAL_TABLE,
    VIRTUAL_VIEW,
    INDEX_TABLE,
    MATERIALIZED_VIEW
  }

  private static Table getTable(GlueClient awsGlue, String databaseName, String tableName) throws HoodieGlueSyncException {
    GetTableRequest request = GetTableRequest.builder()
        .databaseName(databaseName)
        .name(tableName)
        .build();
    try {
      return awsGlue.getTable(request).table();
    } catch (EntityNotFoundException e) {
      throw new HoodieGlueSyncException("Table not found: " + tableId(databaseName, tableName), e);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get table " + tableId(databaseName, tableName), e);
    }
  }

  private static void updateTableParameters(GlueClient awsGlue, String databaseName, String tableName, Map<String, String> updatingParams, boolean shouldReplace) {
    final Map<String, String> newParams = new HashMap<>();
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      if (!shouldReplace) {
        newParams.putAll(table.parameters());
      }
      newParams.putAll(updatingParams);

      final Instant now = Instant.now();
      TableInput updatedTableInput = TableInput.builder()
          .name(tableName)
          .tableType(table.tableType())
          .parameters(newParams)
          .partitionKeys(table.partitionKeys())
          .storageDescriptor(table.storageDescriptor())
          .lastAccessTime(now)
          .lastAnalyzedTime(now)
          .build();

      UpdateTableRequest request =  UpdateTableRequest.builder().databaseName(databaseName)
          .tableInput(updatedTableInput)
          .build();
      awsGlue.updateTable(request);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update params for table " + tableId(databaseName, tableName) + ": " + newParams, e);
    }
  }
}
