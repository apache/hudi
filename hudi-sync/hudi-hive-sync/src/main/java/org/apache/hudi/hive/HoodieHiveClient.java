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

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Database;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.StorageSchemes;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hudi.sync.common.AbstractSyncHoodieClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.schema.MessageType;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieHiveClient extends AbstractSyncHoodieClient {

  private static final String HOODIE_LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";

  private static final Logger LOG = LogManager.getLogger(HoodieHiveClient.class);
  private final PartitionValueExtractor partitionValueExtractor;
  private IMetaStoreClient client;
  private HiveSyncConfig syncConfig;
  private FileSystem fs;
  private HoodieTimeline activeTimeline;

  public HoodieHiveClient(HiveSyncConfig cfg, HiveConf configuration, FileSystem fs) {
    super(cfg.basePath, cfg.assumeDatePartitioning, cfg.useFileListingFromMetadata, cfg.verifyMetadataFileListing, fs);
    this.syncConfig = cfg;
    this.fs = fs;

    try {
      this.client = Hive.get(configuration).getMSC();
    } catch (MetaException | HiveException e) {
      throw new HoodieHiveSyncException("Failed to create HiveMetaStoreClient", e);
    }

    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(cfg.partitionValueExtractorClass).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + cfg.partitionValueExtractorClass, e);
    }

    activeTimeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
  }

  public HoodieTimeline getActiveTimeline() {
    return activeTimeline;
  }

  /**
   * Add the (NEW) partitions to the table.
   */
  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      LOG.info("No partitions to add for " + tableName);
      return;
    }
    LOG.info("Adding partitions " + partitionsToAdd.size() + " to table " + tableName);
    try {
      StorageDescriptor sd = client.getTable(syncConfig.databaseName, tableName).getSd();
      List<Partition> partitionList = partitionsToAdd.stream().map(partition -> {
        String fullPartitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition).toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        sd.setLocation(fullPartitionPath);
        return new Partition(partitionValues, syncConfig.databaseName, tableName, 0, 0, sd, null);
      }).collect(Collectors.toList());
      client.add_partitions(partitionList,true,false);
    } catch (TException e) {
      throw new HoodieHiveSyncException(syncConfig.databaseName + "." + tableName + " add partition failed", e);
    }
  }

  /**
   * Partition path has changed - update the path for te following partitions.
   */
  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      LOG.info("No partitions to change for " + tableName);
      return;
    }
    LOG.info("Changing partitions " + changedPartitions.size() + " on " + tableName);
    try {
      StorageDescriptor sd = client.getTable(syncConfig.databaseName, tableName).getSd();
      List<Partition> partitionList = changedPartitions.stream().map(partition -> {
        Path partitionPath = FSUtils.getPartitionPath(syncConfig.basePath, partition);
        String partitionScheme = partitionPath.toUri().getScheme();
        String fullPartitionPath = StorageSchemes.HDFS.getScheme().equals(partitionScheme)
                ? FSUtils.getDFSFullPartitionPath(fs, partitionPath) : partitionPath.toString();
        List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
        sd.setLocation(fullPartitionPath);
        return new Partition(partitionValues, syncConfig.databaseName, tableName, 0, 0, sd, null);
      }).collect(Collectors.toList());
      client.alter_partitions(syncConfig.databaseName, tableName, partitionList, null);
    } catch (TException e) {
      throw new HoodieHiveSyncException(syncConfig.databaseName + "." + tableName + " update partition failed", e);
    }
  }

  public void updatePartitionToTable(String tableName, Partition newPart, String partitionLocation) throws TException {
    StorageDescriptor sd = client.getTable(syncConfig.databaseName, tableName).getSd();
    sd.setLocation(partitionLocation);
    newPart.setSd(sd);
    client.alter_partition(syncConfig.databaseName, tableName, newPart, null);
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be added or updated.
   * Generate a list of PartitionEvent based on the changes required.
   */
  List<PartitionEvent> getPartitionEvents(List<Partition> tablePartitions, List<String> partitionStoragePartitions) {
    Map<String, String> paths = new HashMap<>();
    for (Partition tablePartition : tablePartitions) {
      List<String> hivePartitionValues = tablePartition.getValues();
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getSd().getLocation())).toUri().getPath();
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
   * Scan table partitions.
   */
  public List<Partition> scanTablePartitions(String tableName) throws TException {
    return client.listPartitions(syncConfig.databaseName, tableName, (short) -1);
  }

  void updateTableDefinition(String tableName, MessageType newSchema) {
    try {
      boolean cascade = syncConfig.partitionFields.size() > 0;
      List<FieldSchema> fieldSchema = HiveSchemaUtil.convertParquetSchemaToHiveFieldSchema(newSchema, syncConfig);
      Table table = client.getTable(syncConfig.databaseName, tableName);
      StorageDescriptor sd = table.getSd();
      sd.setCols(fieldSchema);
      table.setSd(sd);
      EnvironmentContext environmentContext = new EnvironmentContext();
      if (cascade) {
        LOG.info("partition table,need cascade");
        environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
      }
      client.alter_table_with_environmentContext(syncConfig.databaseName, tableName, table, environmentContext);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to update table for " + tableName, e);
    }
  }

  @Override
  public void createTable(String tableName, MessageType storageSchema, String inputFormatClass, String outputFormatClass, String serdeClass) {
    try {
      List<FieldSchema> fieldSchema = HiveSchemaUtil.convertParquetSchemaToHiveFieldSchema(storageSchema, syncConfig);
      Map<String, String> hiveSchema = HiveSchemaUtil.convertParquetSchemaToHiveSchema(storageSchema, syncConfig.supportTimestamp);
      List<FieldSchema> partitionSchema = syncConfig.partitionFields.stream().map(partitionKey -> {
        String partitionKeyType = HiveSchemaUtil.getPartitionKeyType(hiveSchema, partitionKey);
        return new FieldSchema(partitionKey, partitionKeyType.toLowerCase(), "");
      }).collect(Collectors.toList());
      Table newTb = new Table();
      newTb.setDbName(syncConfig.databaseName);
      newTb.setTableName(tableName);
      newTb.setCreateTime((int) System.currentTimeMillis());
      StorageDescriptor storageDescriptor = new StorageDescriptor();
      storageDescriptor.setCols(fieldSchema);
      storageDescriptor.setInputFormat(inputFormatClass);
      storageDescriptor.setOutputFormat(outputFormatClass);
      storageDescriptor.setLocation(syncConfig.basePath);
      Map<String, String> map = new HashMap();
      map.put("serialization.format", "1");
      storageDescriptor.setSerdeInfo(new SerDeInfo(null, serdeClass, map));
      newTb.setSd(storageDescriptor);
      newTb.setPartitionKeys(partitionSchema);
      newTb.putToParameters("EXTERNAL", "TRUE");
      newTb.setTableType(TableType.EXTERNAL_TABLE.toString());
      client.createTable(newTb);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("failed to create table " + tableName, e);
    }
  }

  /**
   * Get the table schema.
   */
  @Override
  public Map<String, String> getTableSchema(String tableName) {
    return getTableSchemaUsingMetastoreClient(tableName);
  }

  public Map<String, String> getTableSchemaUsingMetastoreClient(String tableName) {
    try {
      // HiveMetastoreClient returns partition keys separate from Columns, hence get both and merge to
      // get the Schema of the table.
      final long start = System.currentTimeMillis();
      Table table = this.client.getTable(syncConfig.databaseName, tableName);
      Map<String, String> partitionKeysMap =
          table.getPartitionKeys().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> columnsMap =
          table.getSd().getCols().stream().collect(Collectors.toMap(FieldSchema::getName, f -> f.getType().toUpperCase()));

      Map<String, String> schema = new HashMap<>();
      schema.putAll(columnsMap);
      schema.putAll(partitionKeysMap);
      final long end = System.currentTimeMillis();
      LOG.info(String.format("Time taken to getTableSchema: %s ms", (end - start)));
      return schema;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table schema for : " + tableName, e);
    }
  }

  /**
   * @return true if the configured table exists
   */
  @Override
  public boolean doesTableExist(String tableName) {
    try {
      return client.tableExists(syncConfig.databaseName, tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if table exists " + tableName, e);
    }
  }

  /**
   * @param databaseName
   * @return  true if the configured database exists
   */
  public boolean doesDataBaseExist(String databaseName) {
    try {
      Database database = client.getDatabase(databaseName);
      if (database != null && databaseName.equals(database.getName())) {
        return true;
      }
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to check if database exists " + databaseName, e);
    }
    return false;
  }

  public void createDataBase(String databaseName, String location, String description) {
    if (doesDataBaseExist(databaseName)) {
      LOG.info("hive database " + databaseName + "already exists");
      return;
    }
    try {
      Database database = new Database(databaseName, description, location, null);
      client.createDatabase(database);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to create database " + databaseName, e);
    }
  }

  public void dropDataBase(String databaseName) {
    if (doesDataBaseExist(databaseName)) {
      try {
        client.dropDatabase(databaseName);
      } catch (TException e) {
        throw new HoodieHiveSyncException("Failed to drop database " + databaseName, e);
      }
    }
  }

  public void dropTable(String fullTableName) {
    String dataBaseName = fullTableName.split("\\.")[0];
    String tableName = fullTableName.split("\\.")[1];
    try {
      client.tableExists(dataBaseName, tableName);
    } catch (TException e) {
      throw new HoodieHiveSyncException("Failed to drop table " + fullTableName, e);
    }
  }

  @Override
  public Option<String> getLastCommitTimeSynced(String tableName) {
    // Get the last commit time from the TBLproperties
    try {
      Table database = client.getTable(syncConfig.databaseName, tableName);
      return Option.ofNullable(database.getParameters().getOrDefault(HOODIE_LAST_COMMIT_TIME_SYNC, null));
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get the last commit time synced from the database", e);
    }
  }

  List<String> getAllTables(String db) throws Exception {
    return client.getAllTables(db);
  }

  @Override
  public void updateLastCommitTimeSynced(String tableName) {
    // Set the last commit time from the TBLproperties
    String lastCommitSynced = activeTimeline.lastInstant().get().getTimestamp();
    try {
      Table table = client.getTable(syncConfig.databaseName, tableName);
      table.putToParameters(HOODIE_LAST_COMMIT_TIME_SYNC, lastCommitSynced);
      client.alter_table(syncConfig.databaseName, tableName, table);
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get update last commit time synced to " + lastCommitSynced, e);
    }
  }

  public void close() {
    if (client != null) {
      Hive.closeCurrent();
      client = null;
    }
  }
}