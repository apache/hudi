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

package org.apache.hudi.hive;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.MapUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hive.ddl.DDLExecutor;
import org.apache.hudi.hive.ddl.HMSDDLExecutor;
import org.apache.hudi.hive.ddl.HiveQueryDDLExecutor;
import org.apache.hudi.hive.ddl.HiveSyncMode;
import org.apache.hudi.hive.ddl.JDBCExecutor;
import org.apache.hudi.hive.util.IMetaStoreClientUtil;
import org.apache.hudi.hive.util.PartitionFilterGenerator;
import org.apache.hudi.sync.common.HoodieSyncClient;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.hadoop.utils.HoodieHiveUtils.GLOBALLY_CONSISTENT_READ_TIMESTAMP;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getInputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getOutputFormatClassName;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getSerDeClassName;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_MODE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USE_JDBC;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USE_PRE_APACHE_INPUT_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_FILE_FORMAT;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.util.TableUtils.tableId;

/**
 * This class implements logic to sync a Hudi table with either the Hive server or the Hive Metastore.
 */
public class HoodieHiveSyncClient extends HoodieSyncClient {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieHiveSyncClient.class);
  protected final HiveSyncConfig config;
  private final String databaseName;
  private final Map<String, Table> initialTableByName = new HashMap<>();
  DDLExecutor ddlExecutor;
  private IMetaStoreClient client;

  public HoodieHiveSyncClient(HiveSyncConfig config, HoodieTableMetaClient metaClient) {
    super(config, metaClient);
    this.config = config;
    this.databaseName = config.getStringOrDefault(META_SYNC_DATABASE_NAME);

    // Support JDBC, HiveQL and metastore based implementations for backwards compatibility. Future users should
    // disable jdbc and depend on metastore client for all hive registrations
    try {
      this.client = IMetaStoreClientUtil.getMSC(config.getHiveConf());
      if (!StringUtils.isNullOrEmpty(config.getString(HIVE_SYNC_MODE))) {
        HiveSyncMode syncMode = HiveSyncMode.of(config.getString(HIVE_SYNC_MODE));
        switch (syncMode) {
          case HMS:
            ddlExecutor = new HMSDDLExecutor(config, this.client);
            break;
          case HIVEQL:
            ddlExecutor = new HiveQueryDDLExecutor(config, this.client);
            break;
          case JDBC:
            ddlExecutor = new JDBCExecutor(config);
            break;
          default:
            throw new HoodieHiveSyncException("Invalid sync mode given " + config.getString(HIVE_SYNC_MODE));
        }
      } else {
        ddlExecutor = config.getBoolean(HIVE_USE_JDBC) ? new JDBCExecutor(config) : new HiveQueryDDLExecutor(config, this.client);
      }
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to create HiveMetaStoreClient", e);
    }
  }

  private Table getInitialTable(String table) {
    return initialTableByName.computeIfAbsent(table, t -> {
      try {
        return client.getTable(databaseName, t);
      } catch (Exception ex) {
        throw new HoodieHiveSyncException("Failed to get table " + tableId(databaseName, table), ex);
      }
    });
  }

  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    ddlExecutor.addPartitionsToTable(tableName, partitionsToAdd);
  }

  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    ddlExecutor.updatePartitionsToTable(tableName, changedPartitions);
  }

  @Override
  public void dropPartitions(String tableName, List<String> partitionsToDrop) {
    ddlExecutor.dropPartitionsToTable(tableName, partitionsToDrop);
  }

  @Override
  public boolean updateTableProperties(String tableName, Map<String, String> tableProperties) {
    if (MapUtils.isNullOrEmpty(tableProperties)) {
      return false;
    }

    try {
      Table table = client.getTable(databaseName, tableName);
      Map<String, String> remoteTableProperties = table.getParameters();
      if (MapUtils.containsAll(remoteTableProperties, tableProperties)) {
        return false;
      }

      for (Map.Entry<String, String> entry : tableProperties.entrySet()) {
        table.putToParameters(entry.getKey(), entry.getValue());
      }
      client.alter_table(databaseName, tableName, table);
      return true;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to update table properties for table: "
          + tableName, e);
    }
  }

  @Override
  public boolean updateSerdeProperties(String tableName, Map<String, String> serdeProperties, boolean useRealtimeFormat) {
    if (MapUtils.isNullOrEmpty(serdeProperties)) {
      return false;
    }
    try {
      serdeProperties.putIfAbsent("serialization.format", "1");
      Table table = client.getTable(databaseName, tableName);
      final StorageDescriptor storageDescriptor = table.getSd();

      // check any change to serde properties
      final SerDeInfo remoteSerdeInfo = storageDescriptor.getSerdeInfo();
      boolean shouldUpdate;
      String serdeInfoName;
      if (remoteSerdeInfo == null) {
        serdeInfoName = null;
        shouldUpdate = true;
      } else {
        serdeInfoName = remoteSerdeInfo.getName();
        Map<String, String> remoteSerdeProperties = remoteSerdeInfo.getParameters();
        shouldUpdate = !MapUtils.containsAll(remoteSerdeProperties, serdeProperties);
      }

      // check if any change to input/output format
      HoodieFileFormat baseFileFormat = HoodieFileFormat.valueOf(config.getStringOrDefault(META_SYNC_BASE_FILE_FORMAT).toUpperCase());
      String inputFormatClassName = getInputFormatClassName(baseFileFormat, useRealtimeFormat, config.getBooleanOrDefault(HIVE_USE_PRE_APACHE_INPUT_FORMAT));
      if (!inputFormatClassName.equals(storageDescriptor.getInputFormat())) {
        shouldUpdate = true;
      }
      String outputFormatClassName = getOutputFormatClassName(baseFileFormat);
      if (!outputFormatClassName.equals(storageDescriptor.getOutputFormat())) {
        shouldUpdate = true;
      }

      if (!shouldUpdate) {
        LOG.debug("Table {} serdeProperties and formatClass already up to date, skip update.", tableName);
        return false;
      }

      storageDescriptor.setInputFormat(inputFormatClassName);
      storageDescriptor.setOutputFormat(outputFormatClassName);
      storageDescriptor.setSerdeInfo(new SerDeInfo(serdeInfoName, getSerDeClassName(baseFileFormat), serdeProperties));
      client.alter_table(databaseName, tableName, table);
      return true;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to update table serde info for table: " + tableName, e);
    }
  }

  @Override
  public void updateTableSchema(String tableName, MessageType newSchema, SchemaDifference schemaDiff) {
    ddlExecutor.updateTableDefinition(tableName, newSchema);
  }

  @Override
  public List<Partition> getAllPartitions(String tableName) {
    try {
      return client.listPartitions(databaseName, tableName, (short) -1)
          .stream()
          .map(p -> new Partition(p.getValues(), p.getSd().getLocation()))
          .collect(Collectors.toList());
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to get all partitions for table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public List<Partition> getPartitionsFromList(String tableName, List<String> partitions) {
    String filter = null;
    try {
      List<String> partitionKeys = config.getSplitStrings(META_SYNC_PARTITION_FIELDS).stream()
          .map(String::toLowerCase)
          .collect(Collectors.toList());

      List<FieldSchema> partitionFields = this.getMetastoreFieldSchemas(tableName)
          .stream()
          .filter(f -> partitionKeys.contains(f.getName()))
          .collect(Collectors.toList());
      filter = this.generatePushDownFilter(partitions, partitionFields);

      return client.listPartitionsByFilter(databaseName, tableName, filter, (short)-1)
          .stream()
          .map(p -> new Partition(p.getValues(), p.getSd().getLocation()))
          .collect(Collectors.toList());
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to get partitions for table "
          + tableId(databaseName, tableName) + " with filter " + filter, e);
    }
  }

  @Override
  public String generatePushDownFilter(List<String> writtenPartitions, List<FieldSchema> partitionFields) {
    return new PartitionFilterGenerator().generatePushDownFilter(writtenPartitions, partitionFields, config);
  }

  @Override
  public void createOrReplaceTable(String tableName,
                                   MessageType storageSchema,
                                   String inputFormatClass,
                                   String outputFormatClass,
                                   String serdeClass,
                                   Map<String, String> serdeProperties,
                                   Map<String, String> tableProperties) {

    if (!tableExists(tableName)) {
      createTable(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
      return;
    }
    try {
      // create temp table
      String tempTableName = generateTempTableName(tableName);
      createTable(tempTableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);

      // if create table is successful, drop the actual table
      // and rename temp table to actual table
      dropTable(tableName);

      Table table = client.getTable(databaseName, tempTableName);
      table.setTableName(tableName);
      client.alter_table(databaseName, tempTableName, table);
    } catch (Exception ex) {
      throw new HoodieHiveSyncException("failed to create table " + tableId(databaseName, tableName), ex);
    }
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass,
                          String outputFormatClass, String serdeClass,
                          Map<String, String> serdeProperties, Map<String, String> tableProperties) {
    ddlExecutor.createTable(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
  }

  @Override
  public Map<String, String> getMetastoreSchema(String tableName) {
    if (!tableExists(tableName)) {
      throw new IllegalArgumentException(
          "Failed to get schema for table " + tableName + " does not exist");
    }
    return ddlExecutor.getTableSchema(tableName);
  }

  @Override
  public boolean tableExists(String tableName) {
    try {
      return client.tableExists(databaseName, tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if table exists " + tableName, e);
    }
  }

  @Override
  public boolean databaseExists(String databaseName) {
    try {
      client.getDatabase(databaseName);
      return true;
    } catch (NoSuchObjectException noSuchObjectException) {
      // NoSuchObjectException is thrown when there is no existing database of the name.
      return false;
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if database exists " + databaseName, e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    ddlExecutor.createDatabase(databaseName);
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    // Get the last commit time from the TBLproperties
    try {
      return Option.ofNullable(getInitialTable(tableName).getParameters().getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last commit time synced from the table " + tableName, e);
    }
  }

  @Override
  public Option<String> getLastCommitCompletionTimeSynced(String tableName) {
    // Get the last commit completion time from the TBLproperties
    try {
      return Option.ofNullable(getInitialTable(tableName).getParameters().getOrDefault(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last commit completion time synced from the table " + tableName, e);
    }
  }

  public Option<String> getLastReplicatedTime(String tableName) {
    // Get the last replicated time from the TBLproperties
    try {
      Table table = client.getTable(databaseName, tableName);
      return Option.ofNullable(table.getParameters().getOrDefault(GLOBALLY_CONSISTENT_READ_TIMESTAMP, null));
    } catch (NoSuchObjectException e) {
      LOG.error("database.table [{}.{}] not found in hms", databaseName, tableName);
      return Option.empty();
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last replicated time from the table " + tableName, e);
    }
  }

  public void updateLastReplicatedTimeStamp(String tableName, String timeStamp) {
    if (getActiveTimeline().getInstantsAsStream().noneMatch(i -> i.requestedTime().equals(timeStamp))) {
      throw new HoodieHiveSyncException(
          "Not a valid completed timestamp " + timeStamp + " for table " + tableName);
    }
    try {
      Table table = client.getTable(databaseName, tableName);
      table.putToParameters(GLOBALLY_CONSISTENT_READ_TIMESTAMP, timeStamp);
      client.alter_table(databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to update last replicated time to " + timeStamp + " for " + tableName, e);
    }
  }

  public void deleteLastReplicatedTimeStamp(String tableName) {
    try {
      Table table = client.getTable(databaseName, tableName);
      String timestamp = table.getParameters().remove(GLOBALLY_CONSISTENT_READ_TIMESTAMP);
      client.alter_table(databaseName, tableName, table);
      if (timestamp != null) {
        LOG.info("deleted last replicated timestamp " + timestamp + " for table " + tableName);
      }
    } catch (NoSuchObjectException e) {
      // this is ok the table doesn't even exist.
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to delete last replicated timestamp for " + tableName, e);
    }
  }

  @Override
  public void close() {
    try {
      ddlExecutor.close();
      if (client != null) {
        Hive.closeCurrent();
        client = null;
      }
    } catch (Exception e) {
      LOG.error("Could not close connection ", e);
    }
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    // Set the last commit time and commit completion from the TBLproperties
    HoodieTimeline activeTimeline = getActiveTimeline();
    Option<String> lastCommitSynced = activeTimeline.lastInstant().map(HoodieInstant::requestedTime);
    Option<String> lastCommitCompletionSynced = activeTimeline.getLatestCompletionTime();
    if (lastCommitSynced.isPresent()) {
      try {
        Table table = client.getTable(databaseName, tableName);
        String basePath = config.getString(META_SYNC_BASE_PATH);
        StorageDescriptor sd = table.getSd();
        sd.setLocation(basePath);
        SerDeInfo serdeInfo = sd.getSerdeInfo();
        serdeInfo.putToParameters(ConfigUtils.TABLE_SERDE_PATH, basePath);
        table.putToParameters(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced.get());
        if (lastCommitCompletionSynced.isPresent()) {
          table.putToParameters(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC, lastCommitCompletionSynced.get());
        }
        client.alter_table(databaseName, tableName, table);
      } catch (Exception e) {
        throw new HoodieHiveSyncException("Failed to get update last commit time synced to " + lastCommitSynced, e);
      }
    }
  }

  @Override
  public List<FieldSchema> getMetastoreFieldSchemas(String tableName) {
    try {
      return client.getSchema(databaseName, tableName)
          .stream()
          .map(f -> new FieldSchema(f.getName(), f.getType(), f.getComment()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get field schemas from metastore for table : " + tableName, e);
    }
  }

  @Override
  public List<FieldSchema> getStorageFieldSchemas() {
    try {
      return tableSchemaResolver.getTableAvroSchema(false)
          .getFields()
          .stream()
          .map(f -> new FieldSchema(f.name(), f.schema().getType().getName(), f.doc()))
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get field schemas from storage : ", e);
    }
  }

  @Override
  public boolean updateTableComments(String tableName, List<FieldSchema> fromMetastore, List<FieldSchema> fromStorage) {
    Map<String, FieldSchema> metastoreMap = fromMetastore.stream().collect(Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), f -> f));
    Map<String, FieldSchema> storageMap = fromStorage.stream().collect(Collectors.toMap(f -> f.getName().toLowerCase(Locale.ROOT), f -> f));
    Map<String, Pair<String, String>> alterComments = new HashMap<>();
    metastoreMap.forEach((name, metastoreFieldSchema) -> {
      if (storageMap.containsKey(name)) {
        boolean updated = metastoreFieldSchema.updateComment(storageMap.get(name));
        if (updated) {
          alterComments.put(name, Pair.of(metastoreFieldSchema.getType(), metastoreFieldSchema.getCommentOrEmpty()));
        }
      }
    });
    if (alterComments.isEmpty()) {
      LOG.info(String.format("No comment difference of %s ", tableName));
      return false;
    } else {
      ddlExecutor.updateTableComments(tableName, alterComments);
      return true;
    }
  }

  @VisibleForTesting
  StorageDescriptor getMetastoreStorageDescriptor(String tableName) throws TException {
    return client.getTable(databaseName, tableName).getSd();
  }

  @Override
  public void dropTable(String tableName) {
    try {
      client.dropTable(databaseName, tableName);
      LOG.info("Successfully deleted table in Hive: {}.{}", databaseName, tableName);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to delete the table " + tableId(databaseName, tableName), e);
    }
  }

  @Override
  public String getTableLocation(String tableName) {
    try {
      return getInitialTable(tableName).getSd().getLocation();
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the basepath of the table " + tableId(databaseName, tableName), e);
    }
  }
}
