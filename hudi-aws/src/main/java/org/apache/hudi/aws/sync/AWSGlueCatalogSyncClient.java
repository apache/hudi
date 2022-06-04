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
import org.apache.hudi.hive.AbstractHiveSyncHoodieClient;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.sync.common.model.Partition;

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
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.hudi.aws.utils.S3Utils.s3aToS3;
import static org.apache.hudi.common.util.MapUtils.nonEmpty;
import static org.apache.hudi.hive.util.HiveSchemaUtil.getPartitionKeyType;
import static org.apache.hudi.hive.util.HiveSchemaUtil.parquetSchemaToMapSchema;
import static org.apache.hudi.sync.common.util.TableUtils.tableId;

/**
 * This class implements all the AWS APIs to enable syncing of a Hudi Table with the
 * AWS Glue Data Catalog (https://docs.aws.amazon.com/glue/latest/dg/populate-data-catalog.html).
 */
public class AWSGlueCatalogSyncClient extends AbstractHiveSyncHoodieClient {

  private static final Logger LOG = LogManager.getLogger(AWSGlueCatalogSyncClient.class);
  private static final int MAX_PARTITIONS_PER_REQUEST = 100;
  private static final long BATCH_REQUEST_SLEEP_MILLIS = 1000L;
  private final AWSGlue awsGlue;
  private final String databaseName;

  public AWSGlueCatalogSyncClient(HiveSyncConfig syncConfig, Configuration hadoopConf, FileSystem fs) {
    super(syncConfig, hadoopConf, fs);
    this.awsGlue = AWSGlueClientBuilder.standard().build();
    this.databaseName = syncConfig.databaseName;
  }

  @Override
  public List<Partition> getAllPartitions(String tableName) {
    try {
      List<Partition> partitions = new ArrayList<>();
      String nextToken = null;
      do {
        GetPartitionsResult result = awsGlue.getPartitions(new GetPartitionsRequest()
            .withDatabaseName(databaseName)
            .withTableName(tableName)
            .withNextToken(nextToken));
        partitions.addAll(result.getPartitions().stream()
            .map(p -> new Partition(p.getValues(), p.getStorageDescriptor().getLocation()))
            .collect(Collectors.toList()));
        nextToken = result.getNextToken();
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
      StorageDescriptor sd = table.getStorageDescriptor();
      List<PartitionInput> partitionInputs = partitionsToAdd.stream().map(partition -> {
        StorageDescriptor partitionSd = sd.clone();
        String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        partitionSd.setLocation(fullPartitionPath);
        return new PartitionInput().withValues(partitionValues).withStorageDescriptor(partitionSd);
      }).collect(Collectors.toList());

      for (List<PartitionInput> batch : CollectionUtils.batches(partitionInputs, MAX_PARTITIONS_PER_REQUEST)) {
        BatchCreatePartitionRequest request = new BatchCreatePartitionRequest();
        request.withDatabaseName(databaseName).withTableName(tableName).withPartitionInputList(batch);

        BatchCreatePartitionResult result = awsGlue.batchCreatePartition(request);
        if (CollectionUtils.nonEmpty(result.getErrors())) {
          throw new HoodieGlueSyncException("Fail to add partitions to " + tableId(databaseName, tableName)
              + " with error(s): " + result.getErrors());
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
      StorageDescriptor sd = table.getStorageDescriptor();
      List<BatchUpdatePartitionRequestEntry> updatePartitionEntries = changedPartitions.stream().map(partition -> {
        StorageDescriptor partitionSd = sd.clone();
        String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        sd.setLocation(fullPartitionPath);
        PartitionInput partitionInput = new PartitionInput().withValues(partitionValues).withStorageDescriptor(partitionSd);
        return new BatchUpdatePartitionRequestEntry().withPartitionInput(partitionInput).withPartitionValueList(partitionValues);
      }).collect(Collectors.toList());

      for (List<BatchUpdatePartitionRequestEntry> batch : CollectionUtils.batches(updatePartitionEntries, MAX_PARTITIONS_PER_REQUEST)) {
        BatchUpdatePartitionRequest request = new BatchUpdatePartitionRequest();
        request.withDatabaseName(databaseName).withTableName(tableName).withEntries(batch);

        BatchUpdatePartitionResult result = awsGlue.batchUpdatePartition(request);
        if (CollectionUtils.nonEmpty(result.getErrors())) {
          throw new HoodieGlueSyncException("Fail to update partitions to " + tableId(databaseName, tableName)
              + " with error(s): " + result.getErrors());
        }
        Thread.sleep(BATCH_REQUEST_SLEEP_MILLIS);
      }
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update partitions to " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public void dropPartitions(String tableName, List<String> partitionsToDrop) {
    throw new UnsupportedOperationException("Not support dropPartitionsToTable yet.");
  }

  /**
   * Update the table properties to the table.
   */
  @Override
  public void updateTableProperties(String tableName, Map<String, String> tableProperties) {
    if (nonEmpty(tableProperties)) {
      return;
    }
    try {
      updateTableParameters(awsGlue, databaseName, tableName, tableProperties, true);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update properties for table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public void updateTableDefinition(String tableName, MessageType newSchema) {
    // ToDo Cascade is set in Hive meta sync, but need to investigate how to configure it for Glue meta
    boolean cascade = syncConfig.partitionFields.size() > 0;
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      Map<String, String> newSchemaMap = parquetSchemaToMapSchema(newSchema, syncConfig.supportTimestamp, false);
      List<Column> newColumns = newSchemaMap.keySet().stream().map(key -> {
        String keyType = getPartitionKeyType(newSchemaMap, key);
        return new Column().withName(key).withType(keyType.toLowerCase()).withComment("");
      }).collect(Collectors.toList());
      StorageDescriptor sd = table.getStorageDescriptor();
      sd.setColumns(newColumns);

      final Date now = new Date();
      TableInput updatedTableInput = new TableInput()
          .withName(tableName)
          .withTableType(table.getTableType())
          .withParameters(table.getParameters())
          .withPartitionKeys(table.getPartitionKeys())
          .withStorageDescriptor(sd)
          .withLastAccessTime(now)
          .withLastAnalyzedTime(now);

      UpdateTableRequest request = new UpdateTableRequest()
          .withDatabaseName(databaseName)
          .withTableInput(updatedTableInput);

      awsGlue.updateTable(request);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update definition for table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public List<FieldSchema> getTableCommentUsingMetastoreClient(String tableName) {
    throw new UnsupportedOperationException("Not supported: `getTableCommentUsingMetastoreClient`");
  }

  @Override
  public void updateTableComments(String tableName, List<FieldSchema> oldSchema, List<Schema.Field> newSchema) {
    throw new UnsupportedOperationException("Not supported: `updateTableComments`");
  }

  @Override
  public void updateTableComments(String tableName, List<FieldSchema> oldSchema, Map<String, String> newComments) {
    throw new UnsupportedOperationException("Not supported: `updateTableComments`");
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
    CreateTableRequest request = new CreateTableRequest();
    Map<String, String> params = new HashMap<>();
    if (!syncConfig.createManagedTable) {
      params.put("EXTERNAL", "TRUE");
    }
    params.putAll(tableProperties);

    try {
      Map<String, String> mapSchema = parquetSchemaToMapSchema(storageSchema, syncConfig.supportTimestamp, false);

      List<Column> schemaWithoutPartitionKeys = new ArrayList<>();
      for (String key : mapSchema.keySet()) {
        String keyType = getPartitionKeyType(mapSchema, key);
        Column column = new Column().withName(key).withType(keyType.toLowerCase()).withComment("");
        // In Glue, the full schema should exclude the partition keys
        if (!syncConfig.partitionFields.contains(key)) {
          schemaWithoutPartitionKeys.add(column);
        }
      }

      // now create the schema partition
      List<Column> schemaPartitionKeys = syncConfig.partitionFields.stream().map(partitionKey -> {
        String keyType = getPartitionKeyType(mapSchema, partitionKey);
        return new Column().withName(partitionKey).withType(keyType.toLowerCase()).withComment("");
      }).collect(Collectors.toList());

      StorageDescriptor storageDescriptor = new StorageDescriptor();
      serdeProperties.put("serialization.format", "1");
      storageDescriptor
          .withSerdeInfo(new SerDeInfo().withSerializationLibrary(serdeClass).withParameters(serdeProperties))
          .withLocation(s3aToS3(syncConfig.basePath))
          .withInputFormat(inputFormatClass)
          .withOutputFormat(outputFormatClass)
          .withColumns(schemaWithoutPartitionKeys);

      final Date now = new Date();
      TableInput tableInput = new TableInput()
          .withName(tableName)
          .withTableType(TableType.EXTERNAL_TABLE.toString())
          .withParameters(params)
          .withPartitionKeys(schemaPartitionKeys)
          .withStorageDescriptor(storageDescriptor)
          .withLastAccessTime(now)
          .withLastAnalyzedTime(now);
      request.withDatabaseName(databaseName)
          .withTableInput(tableInput);

      CreateTableResult result = awsGlue.createTable(request);
      LOG.info("Created table " + tableId(databaseName, tableName) + " : " + result);
    } catch (AlreadyExistsException e) {
      LOG.warn("Table " + tableId(databaseName, tableName) + " already exists.", e);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to create " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public Map<String, String> getTableSchema(String tableName) {
    try {
      // GlueMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      Table table = getTable(awsGlue, databaseName, tableName);
      Map<String, String> partitionKeysMap =
          table.getPartitionKeys().stream().collect(Collectors.toMap(Column::getName, f -> f.getType().toUpperCase()));

      Map<String, String> columnsMap =
          table.getStorageDescriptor().getColumns().stream().collect(Collectors.toMap(Column::getName, f -> f.getType().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      return schema;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get schema for table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public boolean doesTableExist(String tableName) {
    return tableExists(tableName);
  }

  @Override
  public boolean tableExists(String tableName) {
    GetTableRequest request = new GetTableRequest()
        .withDatabaseName(databaseName)
        .withName(tableName);
    try {
      return Objects.nonNull(awsGlue.getTable(request).getTable());
    } catch (EntityNotFoundException e) {
      LOG.info("Table not found: " + tableId(databaseName, tableName), e);
      return false;
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get table: " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public boolean databaseExists(String databaseName) {
    GetDatabaseRequest request = new GetDatabaseRequest();
    request.setName(databaseName);
    try {
      return Objects.nonNull(awsGlue.getDatabase(request).getDatabase());
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
    CreateDatabaseRequest request = new CreateDatabaseRequest();
    request.setDatabaseInput(new DatabaseInput()
        .withName(databaseName)
        .withDescription("Automatically created by " + this.getClass().getName())
        .withParameters(null)
        .withLocationUri(null));
    try {
      CreateDatabaseResult result = awsGlue.createDatabase(request);
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
      return Option.ofNullable(table.getParameters().get(HOODIE_LAST_COMMIT_TIME_SYNC));
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get last sync commit time for " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public void close() {
    awsGlue.shutdown();
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    if (!activeTimeline.lastInstant().isPresent()) {
      LOG.warn("No commit in active timeline.");
      return;
    }
    final String lastCommitTimestamp = activeTimeline.lastInstant().get().getTimestamp();
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

  private enum TableType {
    MANAGED_TABLE,
    EXTERNAL_TABLE,
    VIRTUAL_VIEW,
    INDEX_TABLE,
    MATERIALIZED_VIEW
  }

  private static Table getTable(AWSGlue awsGlue, String databaseName, String tableName) throws HoodieGlueSyncException {
    GetTableRequest request = new GetTableRequest()
        .withDatabaseName(databaseName)
        .withName(tableName);
    try {
      return awsGlue.getTable(request).getTable();
    } catch (EntityNotFoundException e) {
      throw new HoodieGlueSyncException("Table not found: " + tableId(databaseName, tableName), e);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to get table " + tableId(databaseName, tableName), e);
    }
  }

  private static void updateTableParameters(AWSGlue awsGlue, String databaseName, String tableName, Map<String, String> updatingParams, boolean shouldReplace) {
    final Map<String, String> newParams = new HashMap<>();
    try {
      Table table = getTable(awsGlue, databaseName, tableName);
      if (!shouldReplace) {
        newParams.putAll(table.getParameters());
      }
      newParams.putAll(updatingParams);

      final Date now = new Date();
      TableInput updatedTableInput = new TableInput()
          .withName(tableName)
          .withTableType(table.getTableType())
          .withParameters(newParams)
          .withPartitionKeys(table.getPartitionKeys())
          .withStorageDescriptor(table.getStorageDescriptor())
          .withLastAccessTime(now)
          .withLastAnalyzedTime(now);

      UpdateTableRequest request = new UpdateTableRequest();
      request.withDatabaseName(databaseName)
          .withTableInput(updatedTableInput);
      awsGlue.updateTable(request);
    } catch (Exception e) {
      throw new HoodieGlueSyncException("Fail to update params for table " + tableId(databaseName, tableName) + ": " + newParams, e);
    }
  }
}
